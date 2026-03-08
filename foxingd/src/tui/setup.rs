// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2025 Joel Wirāmu Pauling <aenertia@aenertia.net>
//
// foxingd/src/tui/setup.rs — TUI setup wizard for initial configuration

//! Guided setup wizard for creating foxingd configuration via TUI.

use std::path::{PathBuf};
use std::fs;
use std::io;
use std::collections::HashSet;
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Span},
    widgets::{Block, Borders, List, ListItem, ListState, Paragraph},
    Frame, Terminal,
};
use crossterm::{
    event::{self, Event, KeyCode, KeyEvent},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use crate::config::{Config, SourceConfig, TargetConfig, TargetProfile};
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
pub struct SetupState {
    pub left_nav: FileNavigator,
    pub right_nav: FileNavigator,
    pub focus: FocusPane,
    pub source_selection: Option<PathBuf>,
    pub target_selection: Option<PathBuf>,
    pub includes: HashSet<PathBuf>,
    pub excludes: HashSet<PathBuf>,
}
impl SetupState {
    pub fn new() -> Self {
        Self {
            left_nav: FileNavigator::new(),
            right_nav: FileNavigator::new(),
            focus: FocusPane::Source,
            source_selection: None,
            target_selection: None,
            includes: HashSet::new(),
            excludes: HashSet::new(),
        }
    }
    pub fn handle_input(&mut self, key: KeyEvent) -> Option<Config> {
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
                match self.focus {
                    FocusPane::Source => self.source_selection = Some(active_nav.current_path.clone()),
                    FocusPane::Target => self.target_selection = Some(active_nav.current_path.clone()),
                }
            },
            KeyCode::Char('i') => {
                if let Some(idx) = active_nav.state.selected() {
                    if let Some(path) = active_nav.items.get(idx) {
                        if !path.ends_with("..") {
                            if self.includes.contains(path) {
                                self.includes.remove(path);
                            } else {
                                self.includes.insert(path.clone());
                                self.excludes.remove(path);
                            }
                        }
                    }
                }
            },
            KeyCode::Char('x') => {
                if let Some(idx) = active_nav.state.selected() {
                    if let Some(path) = active_nav.items.get(idx) {
                        if !path.ends_with("..") {
                            if self.excludes.contains(path) {
                                self.excludes.remove(path);
                            } else {
                                self.excludes.insert(path.clone());
                                self.includes.remove(path);
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
    pub fn generate_config(&self) -> Config {
        let src_path = self.source_selection.clone().unwrap_or_else(|| PathBuf::from("/"));
        let dst_path = self.target_selection.clone().unwrap_or_else(|| PathBuf::from("/"));
        let include_patterns: Vec<String> = self.includes.iter()
            .filter_map(|p| {
                if p.starts_with(&src_path) {
                    p.strip_prefix(&src_path).ok().map(|rel| rel.to_string_lossy().to_string())
                } else {
                    None
                }
            })
            .collect();
        let exclude_patterns: Vec<String> = self.excludes.iter()
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
            include: include_patterns,
            exclude: exclude_patterns,
            enable_versioning: true,
            max_versions: 24,
            max_versions_size_mb: 1024,
            force_retention_files: vec![],
            force_retention_count: 0,
            worker_count: 4,
            batch_size: 64,
            worker_flush_interval_us: 100_000,
            io_buffer_size_mib: 4,
            ordering_scan_depth: 16,
            worker_hibernation_secs: 600,
            worker_retry_initial_ms: 5,
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
            tombstone_journal: None,
        };
        let source_config = SourceConfig {
            path: src_path,
            targets: vec![target_config],
            rwf_uncached_ok: Arc::new(AtomicBool::new(false)),
            cross_subvolumes: false,
        };
        let mut config = Config::default();
        config.sources = vec![source_config];
        config
    }
    pub fn render(&mut self, f: &mut Frame, area: Rect) {
        let chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(50), Constraint::Percentage(50)].as_ref())
            .split(area);
        self.render_pane(f, chunks[0], FocusPane::Source);
        self.render_pane(f, chunks[1], FocusPane::Target);
        let footer_rect = Rect::new(area.x, area.height - 2, area.width, 2);
        let status_msg = format!(
            " Src: {:?} | Dst: {:?} | [Spc] Set Root | [i] Include | [x] Exclude | [S/F10] Save ",
            self.source_selection.as_ref().map(|p| p.file_name().unwrap_or_default().to_string_lossy()).unwrap_or("None".into()),
            self.target_selection.as_ref().map(|p| p.file_name().unwrap_or_default().to_string_lossy()).unwrap_or("None".into())
        );
        let p = Paragraph::new(status_msg)
            .style(Style::default().fg(Color::White).bg(Color::Blue));
        f.render_widget(p, footer_rect);
    }
    fn render_pane(&mut self, f: &mut Frame, area: Rect, pane_type: FocusPane) {
        let (nav, title, is_focused, selection) = match pane_type {
            FocusPane::Source => (&mut self.left_nav, " SOURCE SELECTION ", self.focus == FocusPane::Source, &self.source_selection),
            FocusPane::Target => (&mut self.right_nav, " TARGET SELECTION ", self.focus == FocusPane::Target, &self.target_selection),
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
            let is_dir = path.is_dir() || name == "..";
            let is_selected_root = Some(path) == selection.as_ref();
            let is_included = self.includes.contains(path);
            let is_excluded = self.excludes.contains(path);
            let mut style = if is_selected_root {
                Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD | Modifier::UNDERLINED)
            } else if is_dir {
                Style::default().fg(Color::Blue)
            } else {
                Style::default().fg(Color::White)
            };
            let mut marker = if is_selected_root { " [ROOT] " } else { "" }.to_string();
            if is_included {
                style = style.fg(Color::Green).add_modifier(Modifier::BOLD);
                marker.push_str("[+] ");
            } else if is_excluded {
                style = style.fg(Color::Red).add_modifier(Modifier::CROSSED_OUT);
                marker.push_str("[-] ");
            }
            let display_name = if is_dir && name != ".." {
                format!("{}/{}{}", name, if is_selected_root { "" } else { "/" }, marker)
            } else {
                format!("{}{}", name, marker)
            };
            ListItem::new(display_name).style(style)
        }).collect();
        let list = List::new(items)
            .block(block)
            .highlight_style(
                Style::default()
                    .bg(if is_focused { Color::DarkGray } else { Color::Black })
                    .add_modifier(Modifier::BOLD)
            );
        f.render_stateful_widget(list, area, &mut nav.state);
    }
}
pub fn run_interactive_setup() -> io::Result<Option<Config>> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    let mut state = SetupState::new();
    let mut result = None;
    loop {
        terminal.draw(|f| state.render(f, f.size()))?;
        if event::poll(std::time::Duration::from_millis(100))? {
            if let Event::Key(key) = event::read()? {
                if key.code == KeyCode::Char('q') {
                    break;
                }
                if let Some(config) = state.handle_input(key) {
                    result = Some(config);
                    break;
                }
            }
        }
    }
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    Ok(result)
}
