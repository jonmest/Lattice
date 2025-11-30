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
        let index = self.entries.len() as u64 + 1;
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
        if index == 0 {
            return None;
        }
        self.entries.get((index - 1) as usize).cloned()
    }

    fn truncate(&mut self, from_index: u64) {
        let keep_count = if from_index == 0 {
            0
        } else {
            (from_index - 1) as usize
        };

        self.entries.truncate(keep_count);
        let _ = self.binary_log.truncate(keep_count);
    }

    fn last_index(&self) -> u64 {
        self.entries.len() as u64
    }

    fn last_term(&self) -> u64 {
        self.entries.last().map(|e| e.term).unwrap_or(0)
    }

    fn entries_from(&self, start: u64) -> &[LogEntry] {
        if start == 0 {
            &self.entries
        } else {
            &self.entries[(start - 1) as usize..]
        }
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

        let _ = BinaryLog::open(&path).expect("open binlog");

        let mut l = LatticeLog::new(&path).expect("create lattice log");

        assert_eq!(l.last_index(), 0);
        assert_eq!(l.last_term(), 0);

        let idx = l.append(1, b"cmd1".to_vec()).expect("append");
        assert_eq!(idx, 1);
        assert_eq!(l.last_index(), 1);
        assert_eq!(l.last_term(), 1);

        let got = l.get(1).expect("get entry");
        assert_eq!(got.term, 1);
        assert_eq!(got.command, b"cmd1".to_vec());

        l.truncate(0);
        assert_eq!(l.last_index(), 0);

        let _ = fs::remove_file(&path);
    }
}
