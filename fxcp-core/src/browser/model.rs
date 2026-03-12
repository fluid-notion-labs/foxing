// SPDX-License-Identifier: GPL-2.0-or-later
// fxcp-core/src/browser/model.rs — Browser state model

use std::path::PathBuf;
use super::navigator::FileNavigator;

/// Browser operating mode.
#[derive(Debug, Clone, PartialEq)]
pub enum BrowserMode {
    /// Left=filesystem, Right=snapshot list/tree
    FilesystemSnapshots,
    /// Left=archive contents, Right=target filesystem
    ArchiveBrowse,
}

/// Which pane has focus.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Focus {
    Left,
    Right,
}

/// Main browser state.
pub struct BrowserModel {
    pub left: FileNavigator,
    pub right: FileNavigator,
    pub focus: Focus,
    pub mode: BrowserMode,
    pub status_message: String,
    pub should_quit: bool,
}

impl BrowserModel {
    pub fn new(left_path: &std::path::Path, right_path: &std::path::Path, mode: BrowserMode) -> Self {
        Self {
            left: FileNavigator::new(left_path),
            right: FileNavigator::new(right_path),
            focus: Focus::Left,
            mode,
            status_message: String::new(),
            should_quit: false,
        }
    }

    pub fn active_nav(&mut self) -> &mut FileNavigator {
        match self.focus {
            Focus::Left => &mut self.left,
            Focus::Right => &mut self.right,
        }
    }

    pub fn toggle_focus(&mut self) {
        self.focus = match self.focus {
            Focus::Left => Focus::Right,
            Focus::Right => Focus::Left,
        };
    }
}

/// The main browser application.
pub struct BrowserApp {
    pub model: BrowserModel,
}

impl BrowserApp {
    pub fn new(path: &std::path::Path) -> Self {
        let versions_dir = path.join(".foxing_versions");
        let right_path = if versions_dir.exists() { versions_dir } else { path.to_path_buf() };

        Self {
            model: BrowserModel::new(path, &right_path, BrowserMode::FilesystemSnapshots),
        }
    }

    pub fn new_archive(archive_path: &std::path::Path, target_path: &std::path::Path) -> Self {
        // For archive browsing, extract to a temp dir or list contents
        Self {
            model: BrowserModel::new(archive_path.parent().unwrap_or(std::path::Path::new(".")),
                                     target_path,
                                     BrowserMode::ArchiveBrowse),
        }
    }

    /// Run the TUI event loop.
    pub fn run(&mut self) -> std::io::Result<()> {
        use crossterm::{
            terminal::{enable_raw_mode, disable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
            execute,
            event::{self, Event, KeyCode, KeyModifiers},
        };
        use ratatui::prelude::*;

        enable_raw_mode()?;
        let mut stdout = std::io::stdout();
        execute!(stdout, EnterAlternateScreen)?;
        let backend = CrosstermBackend::new(stdout);
        let mut terminal = Terminal::new(backend)?;

        loop {
            terminal.draw(|f| super::render::draw(f, &self.model))?;

            if event::poll(std::time::Duration::from_millis(100))? {
                if let Event::Key(key) = event::read()? {
                    if super::events::handle_key(&mut self.model, key) {
                        break;
                    }
                }
            }

            if self.model.should_quit { break; }
        }

        disable_raw_mode()?;
        execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
        Ok(())
    }
}
