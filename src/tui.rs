use std::io;
use std::time::{Duration, Instant};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect, Alignment},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Paragraph, Row, Table, Tabs},
    Frame, Terminal,
};
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyModifiers},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use crate::api::SystemStatus;
use crate::tuner::TunerState;
pub enum DataMode {
    Local,
    Remote(String),
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum ActivePage {
    Dashboard,
    Targets,
    Debug,
}
impl ActivePage {
    fn next(self) -> Self {
        match self {
            ActivePage::Dashboard => ActivePage::Targets,
            ActivePage::Targets => ActivePage::Debug,
            ActivePage::Debug => ActivePage::Dashboard,
        }
    }
}
pub struct TuiApp {
    mode: DataMode,
    active_page: ActivePage,
    state: SystemStatus,
    last_tick: Instant,
    spinner_idx: usize,
}
impl TuiApp {
    pub fn new(mode: DataMode) -> Self {
        Self {
            mode,
            active_page: ActivePage::Dashboard,
            state: SystemStatus::default(),
            last_tick: Instant::now(),
            spinner_idx: 0,
        }
    }
    pub fn run<F>(&mut self, mut fetcher: F) -> Result<(), io::Error>
    where F: FnMut() -> Option<SystemStatus>
    {
        enable_raw_mode()?;
        let mut stdout = io::stdout();
        execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
        let backend = CrosstermBackend::new(stdout);
        let mut terminal = Terminal::new(backend)?;
        let tick_rate = Duration::from_millis(250);
        let spinner_frames = vec!["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];
        loop {
            if let Some(new_state) = fetcher() {
                self.state = new_state;
            }
            self.spinner_idx = (self.spinner_idx + 1) % spinner_frames.len();
            terminal.draw(|f| self.ui(f))?;
            let timeout = tick_rate
                .checked_sub(self.last_tick.elapsed())
                .unwrap_or_else(|| Duration::from_secs(0));
            if crossterm::event::poll(timeout)? {
                if let Event::Key(key) = event::read()? {
                    match key.code {
                        KeyCode::Char('q') => break,
                        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => break,
                        KeyCode::Tab => self.active_page = self.active_page.next(),
                        _ => {}
                    }
                }
            }
            if self.last_tick.elapsed() >= tick_rate {
                self.last_tick = Instant::now();
            }
        }
        disable_raw_mode()?;
        execute!(terminal.backend_mut(), LeaveAlternateScreen, DisableMouseCapture)?;
        terminal.show_cursor()?;
        Ok(())
    }
    fn ui(&self, f: &mut Frame) {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .margin(1)
            .constraints([
                Constraint::Length(3),
                Constraint::Length(3),
                Constraint::Min(0),
                Constraint::Length(1),
            ].as_ref())
            .split(f.size());
        let title = match &self.mode {
            DataMode::Local => "FOXING: ONE-SHOT REPLICATION".to_string(),
            DataMode::Remote(url) => format!("FOXING: DAEMON MONITOR ({})", url),
        };
        let header = Paragraph::new(title)
            .style(Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD))
            .alignment(Alignment::Center)
            .block(Block::default().borders(Borders::BOTTOM));
        f.render_widget(header, chunks[0]);
        let titles: Vec<Line> = ["Dashboard", "Targets", "Debug"]
            .iter()
            .map(|t| Line::from(Span::styled(*t, Style::default().fg(Color::Green))))
            .collect();
        let tabs = Tabs::new(titles)
            .block(Block::default().borders(Borders::BOTTOM))
            .select(self.active_page as usize)
            .highlight_style(Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD));
        f.render_widget(tabs, chunks[1]);
        match self.active_page {
            ActivePage::Dashboard => self.render_dashboard(f, chunks[2]),
            ActivePage::Targets => self.render_targets(f, chunks[2]),
            ActivePage::Debug => self.render_debug(f, chunks[2]),
        }
        let footer = Paragraph::new("Press 'Tab' to switch views, 'q' to quit.")
            .style(Style::default().fg(Color::Gray));
        f.render_widget(footer, chunks[3]);
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
        f.render_widget(Paragraph::new(agg_text).block(Block::default().title("Performance Summary").borders(Borders::ALL)), chunks[1]);
    }
    fn render_targets(&self, f: &mut Frame, area: Rect) {
        let header_cells = ["Path", "State", "Lag (ms)", "Pending", "Batch", "Coalesce (KB)", "Buf %"]
            .iter().map(|h| Cell::from(*h).style(Style::default().fg(Color::Yellow)));
        let header = Row::new(header_cells).height(1).bottom_margin(1);
        let rows = self.state.targets.iter().map(|(path, t)| {
            let state_style = match t.tuner_state {
                TunerState::Steady => Style::default().fg(Color::Green),
                TunerState::HighLoad => Style::default().fg(Color::Yellow),
                TunerState::Muted | TunerState::CriticalDrain => Style::default().fg(Color::Red),
                _ => Style::default(),
            };
            Row::new(vec![
                Cell::from(path.clone()),
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
        .block(Block::default().borders(Borders::ALL).title("Target Status (BBR Tuner)"));
        f.render_widget(t, area);
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
