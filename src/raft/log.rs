use std::io;

use crate::raft::{
    binary_log::{BinaryLog, BinaryLogEntry},
    raft_proto::LogEntry,
};

pub trait Log {
    fn append(&mut self, term: u64, command: Vec<u8>) -> Result<u64, std::io::Error>;
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
    fn append(&mut self, term: u64, command: Vec<u8>) -> Result<u64, std::io::Error> {
        let index = self.entries.len() as u64;
        let entry = LogEntry {
            term,
            index,
            command,
        };
        self.append_persisted_log(&entry)?;
        self.entries.push(entry);
        Ok(index)
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn temp_path(name: &str) -> String {
        let mut p = std::env::temp_dir();
        p.push(format!("lattice_log_test_{}.bin", name));
        p.to_string_lossy().into_owned()
    }

    #[test]
    fn test_lattice_log_append_get_truncate() {
        let path = temp_path("basic");
        let _ = fs::remove_file(&path);

        // create an empty binary log first
        let _ = BinaryLog::open(&path).expect("open binlog");

        let mut l = LatticeLog::new(&path).expect("create lattice log");

        assert_eq!(l.last_index(), 0);
        assert_eq!(l.last_term(), 0);

        let idx = l.append(1, b"cmd1".to_vec()).expect("append");
        assert_eq!(idx, 0);
        assert_eq!(l.last_index(), 1);
        assert_eq!(l.last_term(), 1);

        let got = l.get(0).expect("get entry");
        assert_eq!(got.term, 1);
        assert_eq!(got.command, b"cmd1".to_vec());

        l.truncate(0);
        assert_eq!(l.last_index(), 0);

        let _ = fs::remove_file(&path);
    }
}
