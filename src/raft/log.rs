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

    #[test]
    fn test_get_with_invalid_index() {
        let path = temp_path("invalid_index");
        let _ = fs::remove_file(&path);

        let _ = BinaryLog::open(&path).expect("open binlog");
        let mut l = LatticeLog::new(&path).expect("create lattice log");

        assert!(l.get(0).is_none());
        assert!(l.get(1).is_none());

        l.append(1, vec![1]).expect("append");
        assert!(l.get(0).is_none());
        assert!(l.get(1).is_some());
        assert!(l.get(2).is_none());

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn test_entries_from() {
        let path = temp_path("entries_from");
        let _ = fs::remove_file(&path);

        let _ = BinaryLog::open(&path).expect("open binlog");
        let mut l = LatticeLog::new(&path).expect("create lattice log");

        for i in 1..=5 {
            l.append(i, vec![i as u8]).expect("append");
        }

        let all = l.entries_from(0);
        assert_eq!(all.len(), 5);

        let from_one = l.entries_from(1);
        assert_eq!(from_one.len(), 5);
        assert_eq!(from_one[0].index, 1);

        let from_three = l.entries_from(3);
        assert_eq!(from_three.len(), 3);
        assert_eq!(from_three[0].index, 3);

        let from_six = l.entries_from(6);
        assert_eq!(from_six.len(), 0);

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn test_truncate_preserves_index() {
        let path = temp_path("truncate_index");
        let _ = fs::remove_file(&path);

        let _ = BinaryLog::open(&path).expect("open binlog");
        let mut l = LatticeLog::new(&path).expect("create lattice log");

        for i in 1..=5 {
            l.append(i, vec![i as u8]).expect("append");
        }

        l.truncate(3);
        assert_eq!(l.last_index(), 2);
        assert_eq!(l.last_term(), 2);

        let idx = l.append(10, vec![99]).expect("append after truncate");
        assert_eq!(idx, 3);
        assert_eq!(l.last_index(), 3);
        assert_eq!(l.last_term(), 10);

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn test_multiple_appends_with_different_terms() {
        let path = temp_path("multi_term");
        let _ = fs::remove_file(&path);

        let _ = BinaryLog::open(&path).expect("open binlog");
        let mut l = LatticeLog::new(&path).expect("create lattice log");

        l.append(1, vec![1]).expect("append term 1");
        l.append(1, vec![2]).expect("append term 1");
        l.append(2, vec![3]).expect("append term 2");
        l.append(2, vec![4]).expect("append term 2");
        l.append(3, vec![5]).expect("append term 3");

        assert_eq!(l.last_index(), 5);
        assert_eq!(l.last_term(), 3);

        assert_eq!(l.get(1).unwrap().term, 1);
        assert_eq!(l.get(2).unwrap().term, 1);
        assert_eq!(l.get(3).unwrap().term, 2);
        assert_eq!(l.get(4).unwrap().term, 2);
        assert_eq!(l.get(5).unwrap().term, 3);

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn test_truncate_persistence() {
        let path = temp_path("truncate_persist");
        let _ = fs::remove_file(&path);

        {
            let _ = BinaryLog::open(&path).expect("open binlog");
            let mut l = LatticeLog::new(&path).expect("create lattice log");

            for i in 1..=10 {
                l.append(1, vec![i as u8]).expect("append");
            }

            l.truncate(5);
            assert_eq!(l.last_index(), 4);
        }

        {
            let l = LatticeLog::new(&path).expect("reopen log");
            assert_eq!(l.last_index(), 4);
            assert!(l.get(5).is_none());
            assert!(l.get(4).is_some());
        }

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn test_empty_log_properties() {
        let path = temp_path("empty");
        let _ = fs::remove_file(&path);

        let _ = BinaryLog::open(&path).expect("open binlog");
        let l = LatticeLog::new(&path).expect("create lattice log");

        assert_eq!(l.last_index(), 0);
        assert_eq!(l.last_term(), 0);
        assert_eq!(l.entries_from(0).len(), 0);
        assert_eq!(l.entries_from(1).len(), 0);

        let _ = fs::remove_file(&path);
    }
}
