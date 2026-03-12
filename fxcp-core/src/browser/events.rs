// SPDX-License-Identifier: GPL-2.0-or-later
// fxcp-core/src/browser/events.rs — Key event handling

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use super::model::BrowserModel;

/// Handle a key event. Returns true if the browser should quit.
pub fn handle_key(model: &mut BrowserModel, key: KeyEvent) -> bool {
    match key.code {
        KeyCode::Char('q') | KeyCode::Char('Q') => {
            model.should_quit = true;
            return true;
        }
        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            model.should_quit = true;
            return true;
        }

        // Navigation
        KeyCode::Up | KeyCode::Char('k') => model.active_nav().move_up(),
        KeyCode::Down | KeyCode::Char('j') => model.active_nav().move_down(),
        KeyCode::Enter => { model.active_nav().enter(); }
        KeyCode::Backspace | KeyCode::Esc => model.active_nav().go_up(),
        KeyCode::Tab => model.toggle_focus(),

        // Page navigation
        KeyCode::PageUp => {
            for _ in 0..20 { model.active_nav().move_up(); }
        }
        KeyCode::PageDown => {
            for _ in 0..20 { model.active_nav().move_down(); }
        }
        KeyCode::Home => { model.active_nav().selected = 0; }
        KeyCode::End => {
            let len = model.active_nav().entries.len();
            model.active_nav().selected = len.saturating_sub(1);
        }

        // Operations
        KeyCode::Char('r') | KeyCode::Char('R') => {
            model.status_message = "Restore: not yet implemented in TUI".into();
        }
        KeyCode::Char('c') | KeyCode::Char('C') => {
            model.status_message = "Copy: not yet implemented in TUI".into();
        }
        KeyCode::Char('e') | KeyCode::Char('E') => {
            model.status_message = "Export: not yet implemented in TUI".into();
        }
        KeyCode::Char('i') | KeyCode::Char('I') => {
            if let Some(entry) = model.active_nav().selected_entry() {
                model.status_message = format!(
                    "{} | {} | {}",
                    entry.name,
                    if entry.is_dir { "DIR".to_string() } else { format_size(entry.size) },
                    entry.modified
                );
            }
        }

        // Refresh
        KeyCode::F(5) => {
            model.left.refresh();
            model.right.refresh();
            model.status_message = "Refreshed".into();
        }

        _ => {}
    }
    false
}

fn format_size(bytes: u64) -> String {
    if bytes >= 1_073_741_824 { format!("{:.1} GB", bytes as f64 / 1_073_741_824.0) }
    else if bytes >= 1_048_576 { format!("{:.1} MB", bytes as f64 / 1_048_576.0) }
    else if bytes >= 1024 { format!("{:.1} KB", bytes as f64 / 1024.0) }
    else { format!("{} B", bytes) }
}
