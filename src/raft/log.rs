use std::io;

use crate::raft::{
    binary_log::{BinaryLog, BinaryLogEntry},
    raft_proto::LogEntry,
};

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
    binary_log: BinaryLog,
}

impl LatticeLog {
    pub fn new(path: &str) -> io::Result<Self> {
        let binary_log = BinaryLog::open(path)?;
        let entries: Vec<LogEntry> = binary_log.read_all()?;

        Ok(Self {
            entries,
            binary_log,
        })
    }

    fn append_persisted_log(&mut self, entry: &LogEntry) -> io::Result<()> {
        self.binary_log
            .append_msgpack(&BinaryLogEntry::from_proto(entry))?;
        self.binary_log.sync()?;
        Ok(())
    }
}

impl Log for LatticeLog {
    fn append(&mut self, term: u64, command: Vec<u8>) -> u64 {
        let index = self.entries.len() as u64;
        let entry = LogEntry {
            term,
            index,
            command,
        };
        self.append_persisted_log(&entry);
        self.entries.push(entry);
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
