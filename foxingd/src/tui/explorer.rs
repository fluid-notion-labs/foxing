use std::path::PathBuf;
use std::collections::HashSet;
use std::fs;
use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::Span,
    widgets::{Block, Borders, List, ListItem, ListState},
    Frame,
};
use crossterm::event::{KeyCode, KeyEvent};
use crate::config::{SourceConfig, TargetConfig, TargetProfile};
use std::sync::{Arc, atomic::AtomicBool};
#[derive(PartialEq, Clone, Copy)]
pub enum FocusPane {
    Source,
    Target,
}
pub struct FileNavigator {
    pub current_path: PathBuf,
    pub items: Vec<PathBuf>,
    pub state: ListState,
}
impl FileNavigator {
    fn new() -> Self {
        let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("/"));
        let mut nav = Self {
            current_path: cwd,
            items: Vec::new(),
            state: ListState::default(),
        };
        nav.refresh();
        nav
    }
    fn refresh(&mut self) {
        self.items.clear();
        if self.current_path.parent().is_some() {
            self.items.push(self.current_path.join(".."));
        }
        if let Ok(entries) = fs::read_dir(&self.current_path) {
            let mut dirs = Vec::new();
            let mut files = Vec::new();
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    dirs.push(path);
                } else {
                    files.push(path);
                }
            }
            dirs.sort();
            files.sort();
            self.items.append(&mut dirs);
            self.items.append(&mut files);
        }
        if self.items.is_empty() {
            self.state.select(None);
        } else {
            self.state.select(Some(0));
        }
    }
    fn navigate_up(&mut self) {
        if let Some(selected) = self.state.selected() {
            if selected > 0 {
                self.state.select(Some(selected - 1));
            }
        }
    }
    fn navigate_down(&mut self) {
        if let Some(selected) = self.state.selected() {
            if selected < self.items.len().saturating_sub(1) {
                self.state.select(Some(selected + 1));
            }
        }
    }
    fn enter_directory(&mut self) {
        if let Some(selected) = self.state.selected() {
            if let Some(path) = self.items.get(selected) {
                if path.ends_with("..") {
                    self.go_up();
                } else if path.is_dir() {
                    self.current_path = path.clone();
                    self.refresh();
                }
            }
        }
    }
    fn go_up(&mut self) {
        if let Some(parent) = self.current_path.parent() {
            self.current_path = parent.to_path_buf();
            self.refresh();
        }
    }
}
pub struct ExplorerState {
    pub left_nav: FileNavigator,
    pub right_nav: FileNavigator,
    pub focus: FocusPane,
    pub source_selection: Option<PathBuf>,
    pub target_selection: Option<PathBuf>,
    pub exclusions: HashSet<PathBuf>,
}
impl ExplorerState {
    pub fn new() -> Self {
        Self {
            left_nav: FileNavigator::new(),
            right_nav: FileNavigator::new(),
            focus: FocusPane::Source,
            source_selection: None,
            target_selection: None,
            exclusions: HashSet::new(),
        }
    }
    pub fn handle_input(&mut self, key: KeyEvent) -> Option<SourceConfig> {
        let active_nav = match self.focus {
            FocusPane::Source => &mut self.left_nav,
            FocusPane::Target => &mut self.right_nav,
        };
        match key.code {
            KeyCode::Tab => {
                self.focus = match self.focus {
                    FocusPane::Source => FocusPane::Target,
                    FocusPane::Target => FocusPane::Source,
                };
            },
            KeyCode::Up => active_nav.navigate_up(),
            KeyCode::Down => active_nav.navigate_down(),
            KeyCode::Enter => active_nav.enter_directory(),
            KeyCode::Esc => active_nav.go_up(),
            KeyCode::Char(' ') => {
                if let Some(idx) = active_nav.state.selected() {
                    if let Some(path) = active_nav.items.get(idx) {
                        if !path.ends_with("..") {
                            match self.focus {
                                FocusPane::Source => self.source_selection = Some(active_nav.current_path.clone()),
                                FocusPane::Target => self.target_selection = Some(active_nav.current_path.clone()),
                            }
                        }
                    }
                }
            },
            KeyCode::Char('x') => {
                 if let Some(idx) = active_nav.state.selected() {
                    if let Some(path) = active_nav.items.get(idx) {
                        if !path.ends_with("..") {
                            if self.exclusions.contains(path) {
                                self.exclusions.remove(path);
                            } else {
                                self.exclusions.insert(path.clone());
                            }
                        }
                    }
                 }
            },
            KeyCode::F(10) | KeyCode::Char('s') => {
                if self.source_selection.is_some() && self.target_selection.is_some() {
                    return Some(self.generate_config());
                }
            }
            _ => {}
        }
        None
    }
    pub fn generate_config(&self) -> SourceConfig {
        let src_path = self.source_selection.clone().unwrap_or_else(|| PathBuf::from("/"));
        let dst_path = self.target_selection.clone().unwrap_or_else(|| PathBuf::from("/"));
        let exclude_patterns: Vec<String> = self.exclusions.iter()
            .filter_map(|p| {
                if p.starts_with(&src_path) {
                    p.strip_prefix(&src_path).ok().map(|rel| rel.to_string_lossy().to_string())
                } else {
                    None
                }
            })
            .collect();
        let target_config = TargetConfig {
            path: dst_path,
            profile: TargetProfile::Auto,
            autotune_target_latency_ms: 50,
            target_bandwidth_mbps: None,
            target_iops: None,
            initial_sync: true,
            supports_reflink: Arc::new(AtomicBool::new(false)),
            vdo_optimization: false,
            vdo_stall_threshold: 1000,
            include: vec![],
            exclude: exclude_patterns,
            enable_versioning: false,
            max_versions: 0,
            max_versions_size_mb: 0,
            force_retention_files: vec![],
            force_retention_count: 0,
            worker_count: 4,
            batch_size: 64,
            worker_flush_interval_us: 100_000,
            io_buffer_size_mib: 4,
            ordering_scan_depth: 16,
            worker_hibernation_secs: 10,
            worker_retry_initial_ms: 10,
            worker_retry_max_ms: 1000,
            atomic_writes: false,
            source_uncached: false,
            target_uncached: false,
            segment_stall_timeout_override: None,
            segment_overall_timeout_override: None,
            postcopy_timeout_override: None,
            xattr_supported: Arc::new(AtomicBool::new(true)),
            direct_io_ok: Arc::new(AtomicBool::new(false)),
            rwf_uncached_ok: Arc::new(AtomicBool::new(false)),
            rwf_atomic_ok: Arc::new(AtomicBool::new(false)),
            include_regexes: vec![],
            exclude_regexes: vec![],
            force_retention_regexes: vec![],
            label: "".into(),
            paused: Arc::new(AtomicBool::new(false)),
            outage_journal: Arc::new(dashmap::DashSet::new()),
        };
        SourceConfig {
            path: src_path,
            targets: vec![target_config],
            rwf_uncached_ok: Arc::new(AtomicBool::new(false)),
            cross_subvolumes: false,
        }
    }
    pub fn render(&mut self, f: &mut Frame, area: Rect) {
        let chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(50), Constraint::Percentage(50)].as_ref())
            .split(area);
        self.render_pane(f, chunks[0], FocusPane::Source);
        self.render_pane(f, chunks[1], FocusPane::Target);
    }
    fn render_pane(&mut self, f: &mut Frame, area: Rect, pane_type: FocusPane) {
        let (nav, title, is_focused, selection) = match pane_type {
            FocusPane::Source => (&mut self.left_nav, " SOURCE ", self.focus == FocusPane::Source, &self.source_selection),
            FocusPane::Target => (&mut self.right_nav, " TARGET ", self.focus == FocusPane::Target, &self.target_selection),
        };
        let border_style = if is_focused {
            Style::default().fg(Color::Green)
        } else {
            Style::default().fg(Color::Gray)
        };
        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(border_style)
            .title(Span::styled(
                format!("{} {}", title, nav.current_path.display()),
                Style::default().add_modifier(Modifier::BOLD)
            ));
        let items: Vec<ListItem> = nav.items.iter().map(|path| {
            let name = if path.ends_with("..") {
                "..".to_string()
            } else {
                path.file_name()
                    .map(|n| n.to_string_lossy().into_owned())
                    .unwrap_or_else(|| "???".to_string())
            };
            let is_excluded = self.exclusions.contains(path);
            let is_dir = path.is_dir() || name == "..";
            let is_selected_root = Some(path) == selection.as_ref();
            let style = if is_excluded {
                Style::default().fg(Color::Red).add_modifier(Modifier::CROSSED_OUT)
            } else if is_selected_root {
                Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)
            } else if is_dir {
                Style::default().fg(Color::Blue)
            } else {
                Style::default().fg(Color::White)
            };
            let marker = if is_selected_root { " [ROOT] " } else { "" };
            let display_name = if is_dir && name != ".." { format!("{}/{}", name, marker) } else { format!("{}{}", name, marker) };
            ListItem::new(display_name).style(style)
        }).collect();
        let list = List::new(items)
            .block(block)
            .highlight_style(
                Style::default()
                    .bg(if is_focused { Color::Cyan } else { Color::DarkGray })
                    .fg(Color::Black)
                    .add_modifier(Modifier::BOLD)
            );
        f.render_stateful_widget(list, area, &mut nav.state);
    }
}
