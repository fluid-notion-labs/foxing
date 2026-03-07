// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2025 Joel Wirāmu Pauling <aenertia@aenertia.net>
//
// foxingd/src/tui.rs — Terminal UI module — ratatui dashboard

//! Real-time terminal dashboard using ratatui for monitoring foxingd state.

use std::io;
use std::time::Duration;
use std::path::PathBuf;
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect, Alignment},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Paragraph, Row, Table, Tabs, Chart, Dataset, Axis, Sparkline, Wrap},
    symbols,
    Frame, Terminal,
};
use crossterm::{
    event::{DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyModifiers, KeyEvent},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use crate::api::SystemStatus;
use crate::tuner::TunerState;
use tokio::sync::mpsc;
use futures::{StreamExt, FutureExt};
use crossterm::event::EventStream;
use std::collections::{HashMap, VecDeque};
use fxcp_core::versioning::{FileVersion, VersionIndex};
use crate::config::SourceConfig;
use chrono::{DateTime, Utc};
use std::os::unix::fs::MetadataExt;
pub mod explorer;
use explorer::ExplorerState;
pub mod setup;

pub enum DataMode {
    Local,
    Remote(String),
}

#[derive(Debug, Clone)]
pub enum DaemonCommand {
    ForceFlush(PathBuf),
    PauseTarget(PathBuf),
    ResumeTarget(PathBuf),
    CreateSnapshot(PathBuf),
    RevertFile(PathBuf, u64),
    StartOneShot(SourceConfig),
}

#[derive(Clone, PartialEq, Eq)]
pub enum ActivePage {
    Dashboard,
    Targets,
    Debug,
    Logs,
    Help,
    TargetDetail(String),
    Versions(PathBuf),
    Setup,
}

impl ActivePage {
    fn next(&self) -> Self {
        match self {
            ActivePage::Dashboard => ActivePage::Targets,
            ActivePage::Targets | ActivePage::TargetDetail(_) | ActivePage::Versions(_) => ActivePage::Debug,
            ActivePage::Debug => ActivePage::Logs,
            ActivePage::Logs => ActivePage::Help,
            ActivePage::Help => ActivePage::Dashboard,
            ActivePage::Setup => ActivePage::Setup,
        }
    }

    fn title(&self) -> &str {
        match self {
            ActivePage::Dashboard => "Dashboard",
            ActivePage::Targets => "Targets",
            ActivePage::TargetDetail(_) => "Target Detail",
            ActivePage::Versions(_) => "Version Explorer",
            ActivePage::Debug => "Debug",
            ActivePage::Logs => "Logs",
            ActivePage::Help => "Help",
            ActivePage::Setup => "Setup",
        }
    }
}

#[derive(Debug)]
pub enum UiEvent {
    Tick,
    Input(KeyEvent),
    SystemUpdate(Box<SystemStatus>),
    VersionsLoaded(Vec<FileVersion>),
    Log(String),
}

struct MetricHistory {
    bandwidth: VecDeque<(f64, f64)>,
    latency: VecDeque<(f64, f64)>,
    buffer_util: VecDeque<u64>,
}

impl Default for MetricHistory {
    fn default() -> Self {
        Self {
            bandwidth: VecDeque::with_capacity(60),
            latency: VecDeque::with_capacity(60),
            buffer_util: VecDeque::with_capacity(100),
        }
    }
}

pub struct AppModel {
    pub mode: DataMode,
    pub active_page: ActivePage,
    pub state: SystemStatus,
    pub spinner_idx: usize,
    pub table_state: ratatui::widgets::TableState,
    history: HashMap<String, MetricHistory>,
    tick_count: f64,
    pub last_action_msg: String,
    pub versions_cache: Vec<FileVersion>,
    pub versions_table_state: ratatui::widgets::TableState,
    pub show_inspect_modal: bool,
    pub explorer: ExplorerState,
    pub logs: VecDeque<String>,
    pub help_text: String,
    pub log_scroll: u16,
}

impl AppModel {
    pub fn new(mode: DataMode, help_text: String) -> Self {
        Self {
            mode,
            active_page: ActivePage::Dashboard,
            state: SystemStatus::default(),
            spinner_idx: 0,
            table_state: ratatui::widgets::TableState::default(),
            history: HashMap::new(),
            tick_count: 0.0,
            last_action_msg: String::from("Ready."),
            versions_cache: Vec::new(),
            versions_table_state: ratatui::widgets::TableState::default(),
            show_inspect_modal: false,
            explorer: ExplorerState::new(),
            logs: VecDeque::with_capacity(1000),
            help_text,
            log_scroll: 0,
        }
    }

    pub fn on_tick(&mut self) {
        self.spinner_idx = (self.spinner_idx + 1) % 10;
        self.tick_count += 1.0;
    }

    pub fn on_log(&mut self, line: String) {
        if self.logs.len() >= 1000 {
            self.logs.pop_front();
        }
        self.logs.push_back(line);
    }

    pub fn on_system_update(&mut self, status: Box<SystemStatus>) {
        for (path, tgt) in &status.targets {
            let entry = self.history.entry(path.clone()).or_default();
            if let Some((bw, lat)) = tgt.history.first() {
                entry.bandwidth.push_back((self.tick_count, *bw));
                entry.latency.push_back((self.tick_count, *lat));
            }
            let util_val = (tgt.buffer_utilization * 100.0) as u64;
            entry.buffer_util.push_back(util_val);
            
            if entry.bandwidth.len() > 60 { entry.bandwidth.pop_front(); }
            if entry.latency.len() > 60 { entry.latency.pop_front(); }
            if entry.buffer_util.len() > 100 { entry.buffer_util.pop_front(); }
        }
        self.state = *status;
    }

    pub fn on_up(&mut self) {
        match self.active_page {
            ActivePage::Targets => {
                let i = match self.table_state.selected() {
                    Some(i) => if i == 0 { self.state.targets.len().saturating_sub(1) } else { i - 1 },
                    None => 0,
                };
                self.table_state.select(Some(i));
            },
            ActivePage::Versions(_) => {
                let i = match self.versions_table_state.selected() {
                    Some(i) => if i == 0 { self.versions_cache.len().saturating_sub(1) } else { i - 1 },
                    None => 0,
                };
                self.versions_table_state.select(Some(i));
            },
            ActivePage::Logs => {
                self.log_scroll = self.log_scroll.saturating_sub(1);
            },
            _ => {}
        }
    }

    pub fn on_down(&mut self) {
        match self.active_page {
            ActivePage::Targets => {
                let i = match self.table_state.selected() {
                    Some(i) => if i >= self.state.targets.len().saturating_sub(1) { 0 } else { i + 1 },
                    None => 0,
                };
                self.table_state.select(Some(i));
            },
            ActivePage::Versions(_) => {
                let i = match self.versions_table_state.selected() {
                    Some(i) => if i >= self.versions_cache.len().saturating_sub(1) { 0 } else { i + 1 },
                    None => 0,
                };
                self.versions_table_state.select(Some(i));
            },
            ActivePage::Logs => {
                self.log_scroll = self.log_scroll.saturating_add(1);
            },
            _ => {}
        }
    }

    fn get_sorted_targets(&self) -> Vec<String> {
        let mut keys: Vec<_> = self.state.targets.keys().cloned().collect();
        keys.sort();
        keys
    }

    pub fn get_selected_target(&self) -> Option<String> {
        if let Some(idx) = self.table_state.selected() {
            let keys = self.get_sorted_targets();
            keys.get(idx).cloned()
        } else {
            None
        }
    }

    pub fn get_selected_version(&self) -> Option<&FileVersion> {
        if let Some(idx) = self.versions_table_state.selected() {
            self.versions_cache.get(idx)
        } else {
            None
        }
    }

    pub fn on_enter(&mut self) {
        if self.active_page == ActivePage::Targets {
            if let Some(key) = self.get_selected_target() {
                self.active_page = ActivePage::TargetDetail(key);
            }
        }
    }

    pub fn on_escape(&mut self) {
        match self.active_page {
            ActivePage::TargetDetail(_) => {
                self.active_page = ActivePage::Targets;
            },
            ActivePage::Versions(_) => {
                if self.show_inspect_modal {
                    self.show_inspect_modal = false;
                } else {
                    self.active_page = ActivePage::Targets;
                    self.versions_cache.clear();
                }
            },
            _ => {}
        }
    }

    pub fn ui(&mut self, f: &mut Frame) {
        if self.active_page == ActivePage::Setup {
            self.explorer.render(f, f.size());
            let area = f.size();
            let help_rect = Rect::new(0, area.height - 1, area.width, 1);
            let help = Paragraph::new(" EXPLORER: <Tab> Switch Pane | <Enter> Enter Dir | <Space> Toggle Exclude | <R> RUN ")
                .style(Style::default().fg(Color::Black).bg(Color::Cyan));
            f.render_widget(help, help_rect);
            return;
        }

        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .margin(1)
            .constraints([
                Constraint::Length(3),
                Constraint::Length(3),
                Constraint::Min(0),
                Constraint::Length(1),
                Constraint::Length(1),
            ].as_ref())
            .split(f.size());

        let title = match &self.mode {
            DataMode::Local => "FOXING: ONE-SHOT REPLICATION".to_string(),
            DataMode::Remote(url) => format!("FOXING: DAEMON MONITOR ({})", url),
        };

        let spinner_frames = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];
        let spinner = spinner_frames[self.spinner_idx];
        let header_text = format!("{}  {}", spinner, title);
        let header = Paragraph::new(header_text)
            .style(Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD))
            .alignment(Alignment::Center)
            .block(Block::default().borders(Borders::BOTTOM));
        f.render_widget(header, chunks[0]);

        let titles: Vec<Line> = ["Dashboard", "Targets", "Versions", "Debug", "Logs", "Help"]
            .iter()
            .map(|t| {
                let style = if *t == self.active_page.title() || (*t == "Targets" && matches!(self.active_page, ActivePage::TargetDetail(_))) {
                    Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)
                } else {
                    Style::default().fg(Color::Green)
                };
                Line::from(Span::styled(*t, style))
            })
            .collect();

        let tabs = Tabs::new(titles)
            .block(Block::default().borders(Borders::BOTTOM))
            .select(match self.active_page {
                ActivePage::Dashboard => 0,
                ActivePage::Targets | ActivePage::TargetDetail(_) => 1,
                ActivePage::Versions(_) => 2,
                ActivePage::Debug => 3,
                ActivePage::Logs => 4,
                ActivePage::Help => 5,
                ActivePage::Setup => 0,
            });
        f.render_widget(tabs, chunks[1]);

        match self.active_page.clone() {
            ActivePage::Dashboard => self.render_dashboard(f, chunks[2]),
            ActivePage::Targets => self.render_targets(f, chunks[2]),
            ActivePage::TargetDetail(path) => self.render_target_detail(f, chunks[2], &path),
            ActivePage::Versions(path) => self.render_versions(f, chunks[2], &path),
            ActivePage::Debug => self.render_debug(f, chunks[2]),
            ActivePage::Logs => self.render_logs(f, chunks[2]),
            ActivePage::Help => self.render_help(f, chunks[2]),
            _ => {}
        }

        let cmd_bar = Paragraph::new(format!("> {}", self.last_action_msg))
            .style(Style::default().fg(Color::White).bg(Color::Black).add_modifier(Modifier::BOLD));
        f.render_widget(cmd_bar, chunks[3]);

        let hint = match self.active_page {
            ActivePage::Targets => "Cmds: (f)lush, (p)ause, (s)napshot, (v)ersions | Enter: details",
            ActivePage::Versions(_) => "Cmds: (i)nspect, (r)evert | Esc: back",
            ActivePage::TargetDetail(_) => "Esc: back, q: quit",
            ActivePage::Logs => "Up/Down: scroll logs",
            _ => "Tab: switch view, q: quit",
        };
        let footer = Paragraph::new(hint)
            .style(Style::default().fg(Color::Gray));
        f.render_widget(footer, chunks[4]);

        if self.show_inspect_modal && matches!(self.active_page, ActivePage::Versions(_)) {
            self.render_inspect_modal(f);
        }
    }

    fn render_help(&self, f: &mut Frame, area: Rect) {
        let p = Paragraph::new(self.help_text.as_str())
            .block(Block::default().title("Configuration & Usage Guide").borders(Borders::ALL))
            .wrap(Wrap { trim: false });
        f.render_widget(p, area);
    }

    fn render_logs(&self, f: &mut Frame, area: Rect) {
        let log_content: String = self.logs.iter().cloned().collect::<Vec<String>>().join("");
        
        let p = Paragraph::new(log_content)
            .block(Block::default().title("Daemon Logs (Live)").borders(Borders::ALL))
            .wrap(Wrap { trim: false })
            .scroll((self.log_scroll, 0));
        f.render_widget(p, area);
    }

    fn render_versions(&mut self, f: &mut Frame, area: Rect, root: &PathBuf) {
        let block = Block::default()
            .borders(Borders::ALL)
            .title(format!("Version Explorer: {:?}", root));

        if self.versions_cache.is_empty() {
            let p = Paragraph::new("No versions found or scanning...").block(block);
            f.render_widget(p, area);
            return;
        }

        let header_cells = ["Epoch", "Timestamp", "Size (MB)", "Path"]
            .iter().map(|h| Cell::from(*h).style(Style::default().fg(Color::Yellow)));
        let header = Row::new(header_cells).height(1).bottom_margin(1);
        let rows = self.versions_cache.iter().map(|v| {
            let dt = DateTime::<Utc>::from_timestamp(v.timestamp, 0)
                .map(|t| t.format("%Y-%m-%d %H:%M:%S").to_string())
                .unwrap_or_else(|| "Unknown".to_string());
            let size_mb = v.size as f64 / 1024.0 / 1024.0;
            let rel_path = v.path.file_name().map(|n| n.to_string_lossy()).unwrap_or_default();
            Row::new(vec![
                Cell::from(v.epoch_seq.to_string()),
                Cell::from(dt),
                Cell::from(format!("{:.2}", size_mb)),
                Cell::from(rel_path),
            ])
        });
        let t = Table::new(
            rows,
            [
                Constraint::Length(10),
                Constraint::Length(25),
                Constraint::Length(15),
                Constraint::Min(20),
            ]
        )
        .header(header)
        .block(block)
        .highlight_style(Style::default().add_modifier(Modifier::REVERSED));
        f.render_stateful_widget(t, area, &mut self.versions_table_state);
    }

    fn render_inspect_modal(&self, f: &mut Frame) {
        if let Some(v) = self.get_selected_version() {
            let area = centered_rect(60, 40, f.size());
            let block = Block::default().title("Inspect Version").borders(Borders::ALL).style(Style::default().bg(Color::DarkGray));
            let dt = DateTime::<Utc>::from_timestamp(v.timestamp, 0)
                .map(|t| t.to_string()).unwrap_or_default();
            let text = format!(
                "Epoch: {}\nTimestamp: {}\nInode: {}\nSize: {} bytes\n\nFull Path:\n{:?}\n\nContent Hash:\n{:?}",
                v.epoch_seq, dt, v.inode, v.size, v.path, v.content_hash
            );
            let p = Paragraph::new(text)
                .block(block)
                .wrap(Wrap { trim: true });
            f.render_widget(p, area);
        }
    }

    fn render_dashboard(&self, f: &mut Frame, area: Rect) {
        let chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(50), Constraint::Percentage(50)].as_ref())
            .split(area);

        let load_color = if self.state.load_avg_1m > 4.0 || self.state.governor_stressed { Color::Red } else { Color::Green };
        let health_text = format!(
            "Load Avg (1m): {:.2}\nGovernor Stress: {}\nEvents Dropped: {}\nLive Additions: {}",
            self.state.load_avg_1m,
            self.state.governor_stressed,
            self.state.global_events_dropped,
            self.state.live_additions
        );
        let health_block = Paragraph::new(health_text)
            .block(Block::default().title("System Health").borders(Borders::ALL).style(Style::default().fg(load_color)));
        f.render_widget(health_block, chunks[0]);

        let right_chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Percentage(50), Constraint::Percentage(50)].as_ref())
            .split(chunks[1]);

        let mut total_pending = 0;
        let mut total_batch_capacity = 0;
        for t in self.state.targets.values() {
            total_pending += t.pending_events;
            total_batch_capacity += t.batch_size;
        }
        let agg_text = format!(
            "Active Targets: {}\nTotal Pending Events: {}\nAgg. Batch Capacity: {}\n",
            self.state.targets.len(),
            total_pending,
            total_batch_capacity
        );
        f.render_widget(Paragraph::new(agg_text).block(Block::default().title("Performance Summary").borders(Borders::ALL)), right_chunks[0]);

        let mut buffer_data: Vec<u64> = vec![];
        if let Some((_, hist)) = self.history.iter().next() {
            buffer_data = hist.buffer_util.iter().cloned().collect();
        }
        let sparkline = Sparkline::default()
            .block(Block::default().title("Buffer Utilization (Sample)").borders(Borders::ALL))
            .data(&buffer_data)
            .style(Style::default().fg(Color::Magenta));
        f.render_widget(sparkline, right_chunks[1]);
    }

    fn render_targets(&mut self, f: &mut Frame, area: Rect) {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Percentage(60), Constraint::Percentage(40)].as_ref())
            .split(area);

        let header_cells = ["Path", "State", "Lag (ms)", "Pending", "Batch", "Coalesce (KB)", "Buf %"]
            .iter().map(|h| Cell::from(*h).style(Style::default().fg(Color::Yellow)));
        let header = Row::new(header_cells).height(1).bottom_margin(1);
        let keys = self.get_sorted_targets();
        let rows = keys.iter().map(|path| {
            let t = &self.state.targets[path];
            let state_style = match t.tuner_state {
                TunerState::Steady => Style::default().fg(Color::Green),
                TunerState::Muted | TunerState::Conservative => Style::default().fg(Color::Red),
                TunerState::Startup | TunerState::Drain | TunerState::ProbeBW => Style::default().fg(Color::Blue),
            };
            Row::new(vec![
                Cell::from(path.as_str()),
                Cell::from(format!("{:?}", t.tuner_state)).style(state_style),
                Cell::from(format!("{:.2}", t.latency_ms)),
                Cell::from(t.pending_events.to_string()),
                Cell::from(t.batch_size.to_string()),
                Cell::from(t.coalesce_window_kb.to_string()),
                Cell::from(format!("{:.1}%", t.buffer_utilization * 100.0)),
            ])
        });
        let t = Table::new(
            rows,
            [
                Constraint::Percentage(30),
                Constraint::Percentage(10),
                Constraint::Percentage(10),
                Constraint::Percentage(10),
                Constraint::Percentage(10),
                Constraint::Percentage(15),
                Constraint::Percentage(15),
            ]
        )
        .header(header)
        .block(Block::default().borders(Borders::ALL).title("Target Status (BBR Tuner)"))
        .highlight_style(Style::default().add_modifier(Modifier::REVERSED));
        f.render_stateful_widget(t, chunks[0], &mut self.table_state);

        if let Some(idx) = self.table_state.selected() {
            if let Some(key) = keys.get(idx) {
                if let Some(history) = self.history.get(key) {
                    let bw_data: Vec<(f64, f64)> = history.bandwidth.iter().cloned().collect();
                    let lat_data: Vec<(f64, f64)> = history.latency.iter().cloned().collect();
                    let min_x = bw_data.first().map(|v| v.0).unwrap_or(0.0);
                    let max_x = bw_data.last().map(|v| v.0).unwrap_or(100.0);

                    let datasets = vec![
                        Dataset::default()
                            .name("Bandwidth (MB)")
                            .marker(symbols::Marker::Braille)
                            .graph_type(ratatui::widgets::GraphType::Line)
                            .style(Style::default().fg(Color::Yellow))
                            .data(&bw_data),
                        Dataset::default()
                            .name("Latency (ms)")
                            .marker(symbols::Marker::Braille)
                            .graph_type(ratatui::widgets::GraphType::Line)
                            .style(Style::default().fg(Color::Cyan))
                            .data(&lat_data),
                    ];
                    let chart = Chart::new(datasets)
                        .block(Block::default().title(format!("Telemetry: {}", key)).borders(Borders::ALL))
                        .x_axis(Axis::default()
                            .title("Tick")
                            .style(Style::default().fg(Color::Gray))
                            .bounds([min_x, max_x])
                            .labels(vec![
                                Span::styled(format!("{:.0}", min_x), Style::default().add_modifier(Modifier::BOLD)),
                                Span::styled(format!("{:.0}", max_x), Style::default().add_modifier(Modifier::BOLD)),
                            ]))
                        .y_axis(Axis::default()
                            .title("Value")
                            .style(Style::default().fg(Color::Gray))
                            .bounds([0.0, 100.0])
                            .labels(vec![
                                Span::styled("0", Style::default().add_modifier(Modifier::BOLD)),
                                Span::styled("50", Style::default().add_modifier(Modifier::BOLD)),
                                Span::styled("100", Style::default().add_modifier(Modifier::BOLD)),
                            ]));
                    f.render_widget(chart, chunks[1]);
                }
            }
        }
    }

    fn render_target_detail(&self, f: &mut Frame, area: Rect, path: &str) {
        if let Some(target) = self.state.targets.get(path) {
            let chunks = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Percentage(50), Constraint::Percentage(50)].as_ref())
                .split(area);

            let stats_text = format!(
                "Path: {}\n\nLatency: {:.2} ms\nPending Events: {}\nBuffer Utilization: {:.1}%\n\nTuner State: {:?}\nBatch Size: {}\nCoalesce Window: {} KB",
                path,
                target.latency_ms,
                target.pending_events,
                target.buffer_utilization * 100.0,
                target.tuner_state,
                target.batch_size,
                target.coalesce_window_kb
            );
            f.render_widget(
                Paragraph::new(stats_text).block(Block::default().title("Detailed Metrics").borders(Borders::ALL)),
                chunks[0]
            );

            let ops_text = format!(
                "Operations breakdown:\n\nReflink (CoW): {}\nOffload (NFS/Net): {}\nStandard Copy: {}",
                target.ops_reflink,
                target.ops_offload,
                target.ops_standard
            );
            f.render_widget(
                Paragraph::new(ops_text).block(Block::default().title("I/O Methods").borders(Borders::ALL)),
                chunks[1]
            );
        } else {
            f.render_widget(
                Paragraph::new("Target not found (might have been removed)").block(Block::default().borders(Borders::ALL)),
                area
            );
        }
    }

    fn render_debug(&self, f: &mut Frame, area: Rect) {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Percentage(30), Constraint::Percentage(70)].as_ref())
            .split(area);

        let text = format!(
            "BPF Malformed: {}\nBPF Unwatched: {}\nWorker Timeouts: {}\nSidecars Created: {}\nGen Mismatches: {}",
            self.state.debug.bpf_events_malformed,
            self.state.debug.bpf_events_unwatched,
            self.state.debug.worker_shutdown_timeouts,
            self.state.debug.sidecars_created,
            self.state.debug.generation_mismatches
        );
        f.render_widget(
            Paragraph::new(text).block(Block::default().title("General Diagnostics").borders(Borders::ALL)),
            chunks[0]
        );

        let header_cells = ["Device ID", "Sequence", "Events"].iter().map(|h| Cell::from(*h));
        let header = Row::new(header_cells).height(1).bottom_margin(1);
        let rows = self.state.debug.bpf_device_stats.iter().map(|(hex, stats)| {
            Row::new(vec![
                Cell::from(hex.clone()),
                Cell::from(stats.sequence.to_string()),
                Cell::from(stats.event_count.to_string()),
            ])
        });
        let t = Table::new(
            rows,
            [Constraint::Percentage(33), Constraint::Percentage(33), Constraint::Percentage(33)]
        )
        .header(header)
        .block(Block::default().borders(Borders::ALL).title("BPF Watcher Status"));
        f.render_widget(t, chunks[1]);
    }
}

pub struct TuiApp {
    model: AppModel,
    terminal: Terminal<CrosstermBackend<std::io::Stdout>>,
    command_tx: mpsc::Sender<DaemonCommand>,
    ui_tx: mpsc::Sender<UiEvent>,
}

impl TuiApp {
    pub fn new(mode: DataMode, command_tx: mpsc::Sender<DaemonCommand>, ui_tx: mpsc::Sender<UiEvent>, help_text: String) -> io::Result<Self> {
        let stdout = io::stdout();
        let backend = CrosstermBackend::new(stdout);
        let terminal = Terminal::new(backend)?;
        Ok(Self {
            model: AppModel::new(mode, help_text),
            terminal,
            command_tx,
            ui_tx,
        })
    }

    pub async fn run(&mut self, mut rx: mpsc::Receiver<UiEvent>) -> Result<(), io::Error> {
        enable_raw_mode()?;
        execute!(self.terminal.backend_mut(), EnterAlternateScreen, EnableMouseCapture)?;

        let mut reader = EventStream::new();
        let mut interval = tokio::time::interval(Duration::from_millis(500));

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    self.handle_event(UiEvent::Tick);
                }
                Some(event_result) = reader.next().fuse() => {
                    match event_result {
                        Ok(Event::Key(key)) => {
                            if self.handle_event(UiEvent::Input(key)) {
                                break;
                            }
                        }
                        Ok(_) => {},
                        Err(e) => return Err(e),
                    }
                }
                Some(event) = rx.recv() => {
                    self.handle_event(event);
                }
            }
            self.terminal.draw(|f| self.model.ui(f))?;
        }

        disable_raw_mode()?;
        execute!(self.terminal.backend_mut(), LeaveAlternateScreen, DisableMouseCapture)?;
        self.terminal.show_cursor()?;
        Ok(())
    }

    fn spawn_version_load(&self, target_path: PathBuf) {
        let tx = self.ui_tx.clone();
        tokio::spawn(async move {
            let index = VersionIndex::new(target_path.clone());
            index.index_directory();
            index.wait_for_scan();
            let mut versions = Vec::new();
            if let Ok(entries) = std::fs::read_dir(target_path.join(".mirror").join(".versions")) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if let Ok(meta) = entry.metadata() {
                        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                            let parts: Vec<&str> = name.split('_').collect();
                            if parts.len() >= 3 {
                                if let (Ok(epoch), Ok(ts)) = (parts[1].parse::<u64>(), parts[2].parse::<i64>()) {
                                    versions.push(FileVersion {
                                        inode: parts[0].parse().unwrap_or(0),
                                        epoch_seq: epoch,
                                        timestamp: ts,
                                        path: path.clone(),
                                        size: meta.len(),
                                        mtime: meta.mtime(),
                                        content_hash: None,
                                    });
                                }
                            }
                        }
                    }
                }
            }
            versions.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
            let _ = tx.send(UiEvent::VersionsLoaded(versions)).await;
        });
    }

    fn handle_event(&mut self, event: UiEvent) -> bool {
        match event {
            UiEvent::Tick => {
                self.model.on_tick();
            }
            UiEvent::SystemUpdate(status) => {
                self.model.on_system_update(status);
            }
            UiEvent::VersionsLoaded(versions) => {
                self.model.versions_cache = versions;
                self.model.last_action_msg = format!("Loaded {} versions.", self.model.versions_cache.len());
                if !self.model.versions_cache.is_empty() {
                    self.model.versions_table_state.select(Some(0));
                }
            }
            UiEvent::Log(line) => {
                self.model.on_log(line);
            }
            UiEvent::Input(key) => {
                if let KeyCode::Char('q') = key.code { return true; }
                if let KeyCode::Char('c') = key.code {
                    if key.modifiers.contains(KeyModifiers::CONTROL) { return true; }
                }

                if self.model.active_page == ActivePage::Setup {
                    if let Some(config) = self.model.explorer.handle_input(key) {
                        let _ = self.command_tx.try_send(DaemonCommand::StartOneShot(config));
                        self.model.last_action_msg = "Starting One-Shot Replication...".to_string();
                        self.model.active_page = ActivePage::Dashboard;
                    }
                    return false;
                }

                match key.code {
                    KeyCode::Tab => self.model.active_page = self.model.active_page.next(),
                    KeyCode::Up => self.model.on_up(),
                    KeyCode::Down => self.model.on_down(),
                    KeyCode::Enter => self.model.on_enter(),
                    KeyCode::Esc => self.model.on_escape(),
                    KeyCode::Char('f') if self.model.active_page == ActivePage::Targets => {
                        if let Some(target) = self.model.get_selected_target() {
                            let _ = self.command_tx.try_send(DaemonCommand::ForceFlush(PathBuf::from(&target)));
                            self.model.last_action_msg = format!("Sent ForceFlush to {}", target);
                        }
                    },
                    KeyCode::Char('p') if self.model.active_page == ActivePage::Targets => {
                        if let Some(target) = self.model.get_selected_target() {
                            let _ = self.command_tx.try_send(DaemonCommand::PauseTarget(PathBuf::from(&target)));
                            self.model.last_action_msg = format!("Sent Toggle Pause to {}", target);
                        }
                    },
                    KeyCode::Char('s') if self.model.active_page == ActivePage::Targets => {
                        if let Some(target) = self.model.get_selected_target() {
                            let _ = self.command_tx.try_send(DaemonCommand::CreateSnapshot(PathBuf::from(&target)));
                            self.model.last_action_msg = format!("Requested Snapshot for {}", target);
                        }
                    },
                    KeyCode::Char('v') if self.model.active_page == ActivePage::Targets => {
                        if let Some(target) = self.model.get_selected_target() {
                            let path = PathBuf::from(&target);
                            self.model.active_page = ActivePage::Versions(path.clone());
                            self.model.last_action_msg = format!("Scanning versions for {:?}...", path);
                            self.spawn_version_load(path);
                        }
                    },
                    KeyCode::Char('i') if matches!(self.model.active_page, ActivePage::Versions(_)) => {
                        self.model.show_inspect_modal = !self.model.show_inspect_modal;
                    },
                    KeyCode::Char('r') if matches!(self.model.active_page, ActivePage::Versions(_)) => {
                        if let Some(v) = self.model.get_selected_version() {
                            if let ActivePage::Versions(_) = self.model.active_page {
                                let _ = self.command_tx.try_send(DaemonCommand::RevertFile(v.path.clone(), v.epoch_seq));
                                self.model.last_action_msg = format!("Revert requested for Epoch {}", v.epoch_seq);
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
        false
    }
}

fn centered_rect(percent_x: u16, percent_y: u16, r: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ].as_ref())
        .split(r);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ].as_ref())
        .split(popup_layout[1])[1]
}
