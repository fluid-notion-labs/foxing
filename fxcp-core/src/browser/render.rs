// SPDX-License-Identifier: GPL-2.0-or-later
// fxcp-core/src/browser/render.rs — TUI rendering (dual-pane layout)

use ratatui::prelude::*;
use ratatui::widgets::*;
use super::model::{BrowserModel, Focus};
use super::navigator::FileNavigator;

pub fn draw(f: &mut Frame, model: &BrowserModel) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),  // Header
            Constraint::Min(10),   // Panes
            Constraint::Length(2), // Status
            Constraint::Length(1),  // Help
        ])
        .split(f.size());

    // Header
    let header = Paragraph::new(Line::from(vec![
        Span::styled(" FOXING FILE BROWSER ", Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)),
        Span::raw("  "),
        Span::styled(
            match model.mode {
                super::model::BrowserMode::FilesystemSnapshots => "Filesystem ↔ Snapshots",
                super::model::BrowserMode::ArchiveBrowse => "Archive ↔ Target",
            },
            Style::default().fg(Color::Yellow),
        ),
    ]));
    f.render_widget(header, chunks[0]);

    // Dual pane
    let panes = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(chunks[1]);

    draw_pane(f, &model.left, panes[0], model.focus == Focus::Left);
    draw_pane(f, &model.right, panes[1], model.focus == Focus::Right);

    // Status bar
    let status_text = if model.status_message.is_empty() {
        if let Some(entry) = match model.focus {
            Focus::Left => model.left.selected_entry(),
            Focus::Right => model.right.selected_entry(),
        } {
            format!(
                " {} │ {} │ {}",
                entry.name,
                if entry.is_dir { "DIR".to_string() } else { format_size(entry.size) },
                entry.modified
            )
        } else {
            " No selection".to_string()
        }
    } else {
        format!(" {}", model.status_message)
    };

    let status = Paragraph::new(status_text)
        .style(Style::default().bg(Color::DarkGray).fg(Color::White));
    f.render_widget(status, chunks[2]);

    // Help bar
    let help = Paragraph::new(Line::from(vec![
        Span::styled("Tab", Style::default().fg(Color::Cyan)),
        Span::raw(":Switch "),
        Span::styled("↑↓", Style::default().fg(Color::Cyan)),
        Span::raw(":Nav "),
        Span::styled("Enter", Style::default().fg(Color::Cyan)),
        Span::raw(":Open "),
        Span::styled("C", Style::default().fg(Color::Cyan)),
        Span::raw(":Copy "),
        Span::styled("R", Style::default().fg(Color::Cyan)),
        Span::raw(":Restore "),
        Span::styled("I", Style::default().fg(Color::Cyan)),
        Span::raw(":Info "),
        Span::styled("F5", Style::default().fg(Color::Cyan)),
        Span::raw(":Refresh "),
        Span::styled("Q", Style::default().fg(Color::Cyan)),
        Span::raw(":Quit"),
    ]))
    .style(Style::default().bg(Color::Black).fg(Color::Gray));
    f.render_widget(help, chunks[3]);
}

fn draw_pane(f: &mut Frame, nav: &FileNavigator, area: Rect, focused: bool) {
    let border_style = if focused {
        Style::default().fg(Color::Cyan)
    } else {
        Style::default().fg(Color::DarkGray)
    };

    let items: Vec<Row> = nav.entries.iter().enumerate().map(|(i, entry)| {
        let marker = if entry.is_dir { "▸ " } else { "  " };
        let size_str = if entry.is_dir { String::new() } else { format_size(entry.size) };

        let style = if i == nav.selected && focused {
            Style::default().bg(Color::Blue).fg(Color::White).add_modifier(Modifier::BOLD)
        } else if i == nav.selected {
            Style::default().bg(Color::DarkGray).fg(Color::White)
        } else if entry.is_dir {
            Style::default().fg(Color::Cyan)
        } else {
            Style::default()
        };

        Row::new(vec![
            Cell::from(format!("{}{}", marker, entry.name)),
            Cell::from(size_str).style(Style::default().fg(Color::Yellow)),
        ]).style(style)
    }).collect();

    let widths = [Constraint::Min(20), Constraint::Length(10)];
    let table = Table::new(items, widths)
        .block(Block::default()
            .borders(Borders::ALL)
            .border_style(border_style)
            .title(Span::styled(
                format!(" {} ", nav.title),
                Style::default().fg(Color::White).add_modifier(Modifier::BOLD),
            ))
        )
        .highlight_style(Style::default());

    f.render_widget(table, area);
}

fn format_size(bytes: u64) -> String {
    if bytes >= 1_073_741_824 { format!("{:.1}G", bytes as f64 / 1_073_741_824.0) }
    else if bytes >= 1_048_576 { format!("{:.1}M", bytes as f64 / 1_048_576.0) }
    else if bytes >= 1024 { format!("{:.0}K", bytes as f64 / 1024.0) }
    else { format!("{}B", bytes) }
}
