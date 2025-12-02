use std::path::PathBuf;
use std::time::Instant;
use serde::{Serialize, Deserialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExpectedState {
    None,
    WriteBulk,
    FsyncCommit,
}

impl ExpectedState {
    pub fn as_str(&self) -> &'static str {
        match self {
            ExpectedState::None => "None",
            ExpectedState::WriteBulk => "WriteBulk",
            ExpectedState::FsyncCommit => "FsyncCommit",
        }
    }
}

#[derive(Clone, Debug)]
pub struct DirtyEntry {
    pub first_dirty: Instant,
    pub path: PathBuf,
    pub seq: u64,
    pub projid: u32,
    pub expected_state: ExpectedState,
    pub persisted_state: ExpectedState,
}

impl Default for DirtyEntry {
    fn default() -> Self {
        Self {
            first_dirty: Instant::now(),
            path: PathBuf::new(),
            seq: 0,
            projid: 0,
            expected_state: ExpectedState::None,
            persisted_state: ExpectedState::None,
        }
    }
}
