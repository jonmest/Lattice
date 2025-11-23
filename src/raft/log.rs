use std::{collections::HashMap, string};

use crate::raft::raft_proto::LogEntry;

// #[derive(Clone)]
// pub struct LogEntry {
//     pub term: u64,
//     pub index: u64,
//     pub command: Vec<u8>,
// }

pub trait Log {
    fn append(&mut self, term: u64, command: Vec<u8>) -> u64;
    fn get(&self, index: u64) -> Option<LogEntry>;
    fn truncate(&mut self, from_index: u64);
    fn last_index(&self) -> u64;
    fn last_term(&self) -> u64;
    fn entries_from(&self, start: u64) -> &[LogEntry];
}

pub struct LatticeLog {
    entries: Vec<LogEntry>,
}

impl LatticeLog {
    pub fn new(path: &str) -> Self {
        Self {
            entries: Vec::new(),
        }
    }
}

impl Log for LatticeLog {
    fn append(&mut self, term: u64, command: Vec<u8>) -> u64 {
        let index = self.entries.len() as u64;
        self.entries.push(LogEntry {
            term,
            index,
            command,
        });
        index
    }

    fn get(&self, index: u64) -> Option<LogEntry> {
        self.entries.get(index as usize).cloned()
    }

    fn truncate(&mut self, from_index: u64) {
        self.entries.truncate(from_index as usize);
    }

    fn last_index(&self) -> u64 {
        self.entries.len() as u64
    }

    fn last_term(&self) -> u64 {
        self.entries.last().map(|e| e.term).unwrap_or(0)
    }

    fn entries_from(&self, start: u64) -> &[LogEntry] {
        &self.entries[start as usize..]
    }
}
