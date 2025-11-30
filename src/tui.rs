use std::io;
use std::time::{Duration};
use ratatui::{
    backend::CrosstermBackend,
    widgets::{Block, Borders, Paragraph, Table, Row, Cell, Tabs},
    layout::{Layout, Constraint, Direction, Alignment},
    style::{Style, Color, Modifier},
    text::{Span, Line},
    Terminal,
};
use crossterm::{
    event::{self, Event, KeyCode},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use crate::api::{SystemStatus};
use crate::worker::TunerState;

pub enum DataMode {
    Local,
    Remote(String), 
}

#[derive(PartialEq, Clone, Copy)]
pub enum ActivePage {
    Dashboard = 0,
    Targets = 1,
    Debug = 2,
}

impl ActivePage {
    fn next(&self) -> Self {
        match self {
            Self::Dashboard => Self::Targets,
            Self::Targets => Self::Debug,
            Self::Debug => Self::Dashboard,
        }
    }
}

pub struct TuiApp {
    mode: DataMode,
    state: SystemStatus,
    active_page: ActivePage,
    spinner_idx: usize,
}

impl TuiApp {
    pub fn new(mode: DataMode) -> Self {
        Self {
            mode,
            state: SystemStatus::default(),
            active_page: ActivePage::Dashboard,
            spinner_idx: 0,
        }
    }

    pub fn run<F>(&mut self, mut fetcher: F) -> Result<(), io::Error> 
    where F: FnMut() -> Option<SystemStatus> 
    {
        enable_raw_mode()?;
        let mut stdout = io::stdout();
        execute!(stdout, EnterAlternateScreen)?;
        let backend = CrosstermBackend::new(stdout);
        let mut terminal = Terminal::new(backend)?;

        let spinner_frames = ["|", "/", "-", "\\"];

        loop {
            if let Some(new_state) = fetcher() {
                self.state = new_state;
            }

            self.spinner_idx = (self.spinner_idx + 1) % spinner_frames.len();

            terminal.draw(|f| {
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

                // Use url here to silence unused field warning
                let title = match &self.mode {
                    DataMode::Local => "FOXING: ONE-SHOT REPLICATION".to_string(),
                    DataMode::Remote(url) => format!("FOXING: DAEMON MONITOR ({})", url),
                };
                let header = Paragraph::new(title)
                    .style(Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD))
                    .alignment(Alignment::Center)
                    .block(Block::default().borders(Borders::BOTTOM));
                f.render_widget(header, chunks[0]);

                let titles: Vec<Line> = ["1. Dashboard", "2. Targets", "3. Debug/Internals"]
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

                let footer = Paragraph::new("Tab: Switch View | 'q': Quit")
                    .style(Style::default().fg(Color::Gray));
                f.render_widget(footer, chunks[3]);
            })?;

            if event::poll(Duration::from_millis(100))? {
                if let Event::Key(key) = event::read()? {
                    match key.code {
                        KeyCode::Char('q') | KeyCode::Esc => break,
                        KeyCode::Char('c') if key.modifiers.contains(event::KeyModifiers::CONTROL) => break,
                        KeyCode::Tab => self.active_page = self.active_page.next(),
                        KeyCode::Char('1') => self.active_page = ActivePage::Dashboard,
                        KeyCode::Char('2') => self.active_page = ActivePage::Targets,
                        KeyCode::Char('3') => self.active_page = ActivePage::Debug,
                        _ => {}
                    }
                }
            }
        }

        disable_raw_mode()?;
        execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
        terminal.show_cursor()?;
        Ok(())
    }

    fn render_dashboard(&self, f: &mut Frame, area: ratatui::layout::Rect) {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(4), 
                Constraint::Length(8), 
                Constraint::Min(0),
            ].as_ref())
            .split(area);

        let load_color = if self.state.load_avg_1m > 4.0 { Color::Red } else { Color::Green };
        let gov_state = if self.state.governor_stressed { "STRESSED (Throttling)" } else { "Nominal" };
        let health_text = format!(
            " Load Avg (1m): {:.2}   Governor: {}\n Live Events:   {}     Dropped: {}",
            self.state.load_avg_1m, gov_state,
            self.state.live_additions, self.state.global_events_dropped
        );
        f.render_widget(Paragraph::new(health_text).block(Block::default().title("System Health").borders(Borders::ALL).style(Style::default().fg(load_color))), chunks[0]);

        let mut total_reflink = 0;
        let mut total_offload = 0;
        let mut total_std = 0;
        let mut total_mb = 0.0;
        
        for t in self.state.targets.values() {
            total_reflink += t.ops_reflink;
            total_offload += t.ops_offload;
            total_std += t.ops_standard;
            total_mb += t.throughput_mb;
        }
        let total_ops = total_reflink + total_offload + total_std;
        let fast_pct = if total_ops > 0 { ((total_reflink + total_offload) as f64 / total_ops as f64) * 100.0 } else { 0.0 };

        let agg_text = format!(
            " Total Throughput: {:.1} MB/s\n Acceleration:     {:.1}% FAST\n \n Ops Breakdown:\n   Reflink: {}\n   Offload: {}\n   Standard: {}",
            total_mb, fast_pct, total_reflink, total_offload, total_std
        );
        f.render_widget(Paragraph::new(agg_text).block(Block::default().title("Performance Summary").borders(Borders::ALL)), chunks[1]);
    }

    fn render_targets(&self, f: &mut Frame, area: ratatui::layout::Rect) {
        let header_cells = ["Target Path", "Tuner State", "Throughput", "Latency", "Buffer", "Pending"]
            .iter().map(|h| Cell::from(*h).style(Style::default().fg(Color::Yellow)));
        let header = Row::new(header_cells).height(1).bottom_margin(1);
        
        let rows = self.state.targets.iter().map(|(path, t)| {
            let color = match t.tuner_state {
                TunerState::Muted | TunerState::SpacePressure => Color::Red,
                TunerState::ProbeBW => Color::Green,
                _ => Color::Yellow,
            };
            Row::new(vec![
                Cell::from(path.as_str()),
                Cell::from(format!("{:?}", t.tuner_state)).style(Style::default().fg(color)),
                Cell::from(format!("{:.1} MB/s", t.throughput_mb)),
                Cell::from(format!("{:.1} ms", t.latency_ms)),
                Cell::from(format!("{:.1}%", t.buffer_utilization * 100.0)),
                Cell::from(t.pending_events.to_string()),
            ])
        });

        let t = Table::new(
            rows,
            [
                Constraint::Percentage(40),
                Constraint::Percentage(15),
                Constraint::Percentage(15),
                Constraint::Percentage(10),
                Constraint::Percentage(10),
                Constraint::Percentage(10),
            ]
        )
        .header(header)
        .block(Block::default().borders(Borders::ALL).title("Target Details"));
        f.render_widget(t, area);
    }

    fn render_debug(&self, f: &mut Frame, area: ratatui::layout::Rect) {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Percentage(40),
                Constraint::Percentage(60),
            ].as_ref())
            .split(area);

        let text = format!(
            "KERNEL / BPF DIAGNOSTICS\n\
             ------------------------\n\
             BPF Sequence Gaps:    {}\n\
             Malformed Events:     {}\n\
             Unwatched Device Evt: {}\n\n\
             \
             WORKER DIAGNOSTICS\n\
             ------------------\n\
             Shutdown Timeouts:    {}\n\
             Sidecars Created:     {}\n\
             Gen Mismatches:       {}\n",
             self.state.debug.bpf_sequence_gaps,
             self.state.debug.bpf_events_malformed,
             self.state.debug.bpf_events_unwatched,
             self.state.debug.worker_shutdown_timeouts,
             self.state.debug.sidecars_created,
             self.state.debug.generation_mismatches,
        );
        
        f.render_widget(
            Paragraph::new(text).block(Block::default().title("General Diagnostics").borders(Borders::ALL)), 
            chunks[0]
        );

        let header_cells = ["Device ID (Hex)", "Dev ID (Dec)", "Seq #", "Events Processed"]
            .iter().map(|h| Cell::from(*h).style(Style::default().fg(Color::Yellow)));
        let header = Row::new(header_cells).height(1).bottom_margin(1);

        let rows = self.state.debug.bpf_device_stats.iter().map(|(hex, stats)| {
            Row::new(vec![
                Cell::from(hex.as_str()),
                Cell::from(stats.dev_id_raw.to_string()),
                Cell::from(stats.last_sequence.to_string()),
                Cell::from(stats.total_events.to_string()),
            ])
        });

        let t = Table::new(
            rows,
            [
                Constraint::Percentage(25),
                Constraint::Percentage(25),
                Constraint::Percentage(25),
                Constraint::Percentage(25),
            ]
        )
        .header(header)
        .block(Block::default().borders(Borders::ALL).title("BPF Watcher Status"));
        
        f.render_widget(t, chunks[1]);
    }
}

type Frame<'a> = ratatui::Frame<'a>;
