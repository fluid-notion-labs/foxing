// SPDX-License-Identifier: GPL-2.0-or-later
// fxcp-core/src/browser/navigator.rs — File/snapshot navigation

use std::path::{Path, PathBuf};
use std::fs;

/// A single entry in a file listing (file or directory).
#[derive(Debug, Clone)]
pub struct Entry {
    pub name: String,
    pub path: PathBuf,
    pub is_dir: bool,
    pub size: u64,
    pub modified: String,
}

/// Navigates a filesystem directory tree.
pub struct FileNavigator {
    pub current_dir: PathBuf,
    pub entries: Vec<Entry>,
    pub selected: usize,
    pub title: String,
}

impl FileNavigator {
    pub fn new(root: &Path) -> Self {
        let mut nav = Self {
            current_dir: root.to_path_buf(),
            entries: Vec::new(),
            selected: 0,
            title: root.to_string_lossy().to_string(),
        };
        nav.refresh();
        nav
    }

    pub fn refresh(&mut self) {
        self.entries.clear();
        self.title = self.current_dir.to_string_lossy().to_string();

        // Parent directory entry
        if self.current_dir.parent().is_some() {
            self.entries.push(Entry {
                name: "..".into(),
                path: self.current_dir.join(".."),
                is_dir: true,
                size: 0,
                modified: String::new(),
            });
        }

        let mut dirs = Vec::new();
        let mut files = Vec::new();

        if let Ok(read_dir) = fs::read_dir(&self.current_dir) {
            for entry in read_dir.flatten() {
                let name = entry.file_name().to_string_lossy().to_string();
                // Skip hidden foxing metadata
                if name.starts_with(".foxing") && !name.contains("versions") { continue; }

                let meta = entry.metadata().ok();
                let is_dir = meta.as_ref().map(|m| m.is_dir()).unwrap_or(false);
                let size = meta.as_ref().map(|m| m.len()).unwrap_or(0);
                let modified = meta.as_ref().and_then(|m| m.modified().ok())
                    .map(|t| {
                        let d = t.duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
                        chrono::DateTime::from_timestamp(d.as_secs() as i64, 0)
                            .map(|dt| dt.format("%Y-%m-%d %H:%M").to_string())
                            .unwrap_or_default()
                    })
                    .unwrap_or_default();

                let e = Entry { name, path: entry.path(), is_dir, size, modified };
                if is_dir { dirs.push(e); } else { files.push(e); }
            }
        }

        dirs.sort_by(|a, b| a.name.cmp(&b.name));
        files.sort_by(|a, b| a.name.cmp(&b.name));
        self.entries.extend(dirs);
        self.entries.extend(files);

        if self.selected >= self.entries.len() {
            self.selected = self.entries.len().saturating_sub(1);
        }
    }

    pub fn move_up(&mut self) {
        if self.selected > 0 { self.selected -= 1; }
    }

    pub fn move_down(&mut self) {
        if self.selected + 1 < self.entries.len() { self.selected += 1; }
    }

    pub fn enter(&mut self) -> bool {
        if let Some(entry) = self.entries.get(self.selected) {
            if entry.is_dir {
                if entry.name == ".." {
                    if let Some(parent) = self.current_dir.parent() {
                        self.current_dir = parent.to_path_buf();
                    }
                } else {
                    self.current_dir = entry.path.clone();
                }
                self.selected = 0;
                self.refresh();
                return true;
            }
        }
        false
    }

    pub fn go_up(&mut self) {
        if let Some(parent) = self.current_dir.parent() {
            self.current_dir = parent.to_path_buf();
            self.selected = 0;
            self.refresh();
        }
    }

    pub fn selected_entry(&self) -> Option<&Entry> {
        self.entries.get(self.selected)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_navigator_opens_real_dir() {
        let nav = FileNavigator::new(std::path::Path::new("/tmp"));
        assert!(!nav.entries.is_empty());
        assert_eq!(nav.title, "/tmp");
    }

    #[test]
    fn test_navigator_move_up_down() {
        let mut nav = FileNavigator::new(std::path::Path::new("/tmp"));
        let initial = nav.selected;
        nav.move_down();
        if nav.entries.len() > 1 {
            assert_eq!(nav.selected, initial + 1);
        }
        nav.move_up();
        assert_eq!(nav.selected, initial);
    }

    #[test]
    fn test_navigator_move_up_at_zero() {
        let mut nav = FileNavigator::new(std::path::Path::new("/tmp"));
        nav.selected = 0;
        nav.move_up();
        assert_eq!(nav.selected, 0);
    }

    #[test]
    fn test_navigator_parent_entry() {
        let nav = FileNavigator::new(std::path::Path::new("/tmp"));
        assert!(nav.entries.iter().any(|e| e.name == ".."));
    }

    #[test]
    fn test_navigator_dirs_before_files() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("aaa_dir")).unwrap();
        std::fs::write(dir.path().join("bbb_file.txt"), "test").unwrap();
        let nav = FileNavigator::new(dir.path());
        // After "..", dirs come before files
        let non_parent: Vec<_> = nav.entries.iter().filter(|e| e.name != "..").collect();
        if non_parent.len() >= 2 {
            assert!(non_parent[0].is_dir);
            assert!(!non_parent[1].is_dir);
        }
    }
}
