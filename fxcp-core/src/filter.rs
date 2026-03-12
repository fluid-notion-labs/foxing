// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2025 Joel Wirāmu Pauling <aenertia@aenertia.net>
//
// fxcp-core/src/filter.rs — Path filtering with include/exclude patterns

use std::path::Path;

/// Compiled filter rules for include/exclude matching.
pub struct FilterRules {
    pub excludes: Vec<glob::Pattern>,
    pub includes: Vec<glob::Pattern>,
}

impl FilterRules {
    /// Build filter rules from string patterns.
    pub fn new(exclude: &[String], include: &[String]) -> Self {
        Self {
            excludes: exclude.iter()
                .filter_map(|p| glob::Pattern::new(p).ok())
                .collect(),
            includes: include.iter()
                .filter_map(|p| glob::Pattern::new(p).ok())
                .collect(),
        }
    }

    /// Returns true if the path should be skipped.
    /// A path is skipped if it matches any exclude pattern,
    /// UNLESS it also matches an include pattern (rsync semantics).
    pub fn should_skip(&self, rel: &Path) -> bool {
        let excluded = self.excludes.iter().any(|p| p.matches_path(rel));
        if !excluded { return false; }
        // Include overrides exclude
        !self.includes.iter().any(|p| p.matches_path(rel))
    }
}

/// Read patterns from a file, one per line.
/// Empty lines and lines starting with '#' are ignored.
pub fn read_patterns(path: &Path) -> anyhow::Result<Vec<String>> {
    let content = std::fs::read_to_string(path)?;
    Ok(content.lines()
        .map(|l| l.trim())
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .map(|l| l.to_string())
        .collect())
}

/// Split positional args into (sources, destination).
/// Last arg is always the destination. Requires at least 2 args.
pub fn split_paths(paths: Vec<std::path::PathBuf>) -> anyhow::Result<(Vec<std::path::PathBuf>, std::path::PathBuf)> {
    if paths.len() < 2 {
        anyhow::bail!("requires at least a source and destination");
    }
    let mut paths = paths;
    let destination = paths.pop().unwrap();
    Ok((paths, destination))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    // ---- FilterRules tests ----

    #[test]
    fn test_no_filters_skips_nothing() {
        let rules = FilterRules::new(&[], &[]);
        assert!(!rules.should_skip(Path::new("foo.rs")));
        assert!(!rules.should_skip(Path::new("dir/bar.tmp")));
    }

    #[test]
    fn test_exclude_matches() {
        let rules = FilterRules::new(&["*.tmp".into()], &[]);
        assert!(rules.should_skip(Path::new("foo.tmp")));
        assert!(!rules.should_skip(Path::new("foo.rs")));
    }

    #[test]
    fn test_exclude_glob_directory() {
        let rules = FilterRules::new(&[".git/**".into()], &[]);
        assert!(rules.should_skip(Path::new(".git/config")));
        assert!(rules.should_skip(Path::new(".git/refs/heads/main")));
        assert!(!rules.should_skip(Path::new("src/main.rs")));
    }

    #[test]
    fn test_include_overrides_exclude() {
        let rules = FilterRules::new(
            &["*.tmp".into()],
            &["important.tmp".into()],
        );
        assert!(rules.should_skip(Path::new("junk.tmp")));
        assert!(!rules.should_skip(Path::new("important.tmp")));
        assert!(!rules.should_skip(Path::new("foo.rs")));
    }

    #[test]
    fn test_exclude_all_include_specific() {
        let rules = FilterRules::new(
            &["*".into()],
            &["*.rs".into()],
        );
        assert!(!rules.should_skip(Path::new("main.rs")));
        assert!(rules.should_skip(Path::new("readme.md")));
        assert!(rules.should_skip(Path::new("data.bin")));
    }

    #[test]
    fn test_multiple_excludes() {
        let rules = FilterRules::new(
            &["*.tmp".into(), "*.bak".into(), "*.swp".into()],
            &[],
        );
        assert!(rules.should_skip(Path::new("foo.tmp")));
        assert!(rules.should_skip(Path::new("bar.bak")));
        assert!(rules.should_skip(Path::new("baz.swp")));
        assert!(!rules.should_skip(Path::new("good.rs")));
    }

    #[test]
    fn test_multiple_includes() {
        let rules = FilterRules::new(
            &["*".into()],
            &["*.rs".into(), "*.toml".into(), "Makefile".into()],
        );
        assert!(!rules.should_skip(Path::new("main.rs")));
        assert!(!rules.should_skip(Path::new("Cargo.toml")));
        assert!(!rules.should_skip(Path::new("Makefile")));
        assert!(rules.should_skip(Path::new("readme.md")));
    }

    #[test]
    fn test_nested_path_matching() {
        let rules = FilterRules::new(&["target/**".into()], &[]);
        assert!(rules.should_skip(Path::new("target/debug/fxcp")));
        assert!(rules.should_skip(Path::new("target/release/foxingd")));
        assert!(!rules.should_skip(Path::new("src/main.rs")));
    }

    #[test]
    fn test_invalid_pattern_ignored() {
        // Invalid glob pattern should be silently ignored
        let rules = FilterRules::new(&["[invalid".into(), "*.tmp".into()], &[]);
        assert!(rules.should_skip(Path::new("foo.tmp")));
        assert!(!rules.should_skip(Path::new("foo.rs")));
    }

    // ---- split_paths tests ----

    #[test]
    fn test_split_two_args() {
        let paths = vec![PathBuf::from("src"), PathBuf::from("dst")];
        let (sources, dest) = split_paths(paths).unwrap();
        assert_eq!(sources, vec![PathBuf::from("src")]);
        assert_eq!(dest, PathBuf::from("dst"));
    }

    #[test]
    fn test_split_three_args() {
        let paths = vec![
            PathBuf::from("file1"),
            PathBuf::from("file2"),
            PathBuf::from("dest/"),
        ];
        let (sources, dest) = split_paths(paths).unwrap();
        assert_eq!(sources, vec![PathBuf::from("file1"), PathBuf::from("file2")]);
        assert_eq!(dest, PathBuf::from("dest/"));
    }

    #[test]
    fn test_split_many_args() {
        let paths = vec![
            PathBuf::from("a"), PathBuf::from("b"),
            PathBuf::from("c"), PathBuf::from("d"),
            PathBuf::from("target/"),
        ];
        let (sources, dest) = split_paths(paths).unwrap();
        assert_eq!(sources.len(), 4);
        assert_eq!(dest, PathBuf::from("target/"));
    }

    #[test]
    fn test_split_one_arg_fails() {
        let paths = vec![PathBuf::from("only")];
        assert!(split_paths(paths).is_err());
    }

    #[test]
    fn test_split_empty_fails() {
        let paths: Vec<PathBuf> = vec![];
        assert!(split_paths(paths).is_err());
    }

    // ---- read_patterns tests ----

    #[test]
    fn test_read_patterns_basic() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("patterns.txt");
        std::fs::write(&file, "*.tmp\n*.bak\n*.swp\n").unwrap();
        let patterns = read_patterns(&file).unwrap();
        assert_eq!(patterns, vec!["*.tmp", "*.bak", "*.swp"]);
    }

    #[test]
    fn test_read_patterns_comments_and_blanks() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("patterns.txt");
        std::fs::write(&file, "# This is a comment\n*.tmp\n\n# Another comment\n*.bak\n  \n").unwrap();
        let patterns = read_patterns(&file).unwrap();
        assert_eq!(patterns, vec!["*.tmp", "*.bak"]);
    }

    #[test]
    fn test_read_patterns_empty_file() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("empty.txt");
        std::fs::write(&file, "").unwrap();
        let patterns = read_patterns(&file).unwrap();
        assert!(patterns.is_empty());
    }

    #[test]
    fn test_read_patterns_whitespace_trimmed() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("patterns.txt");
        std::fs::write(&file, "  *.tmp  \n  *.bak  \n").unwrap();
        let patterns = read_patterns(&file).unwrap();
        assert_eq!(patterns, vec!["*.tmp", "*.bak"]);
    }

    #[test]
    fn test_read_patterns_missing_file() {
        assert!(read_patterns(Path::new("/nonexistent/file")).is_err());
    }
}
