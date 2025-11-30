use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::mem;

use serde::{Deserialize, Serialize};

use crate::raft::raft_proto::LogEntry;
use std::io::{BufReader, Read};

use rmp_serde::from_slice;
use serde::de::DeserializeOwned;

fn read_all<T: DeserializeOwned>(path: &str) -> io::Result<Vec<T>> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);

    let mut out = Vec::new();
    loop {
        let mut len_bytes = [0u8; 4];
        match reader.read_exact(&mut len_bytes) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                break;
            }
            Err(e) => return Err(e),
        }

        let len = u32::from_le_bytes(len_bytes) as usize;
        let mut buf = vec![0u8; len];
        match reader.read_exact(&mut buf) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                break;
            }
            Err(e) => return Err(e),
        }

        let value: T = from_slice(&buf)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

        out.push(value);
    }

    Ok(out)
}

#[derive(Serialize, Deserialize)]
pub struct BinaryLogEntry {
    term: u64,
    index: u64,
    command: Vec<u8>,
}

impl BinaryLogEntry {
    pub fn from_proto(proto_entry: &LogEntry) -> Self {
        Self {
            term: proto_entry.term,
            index: proto_entry.index,
            command: proto_entry.command.clone(),
        }
    }

    pub fn to_proto(&self) -> LogEntry {
        LogEntry {
            term: self.term,
            index: self.index,
            command: self.command.clone(),
        }
    }
}

pub struct BinaryLog {
    path: String,
    writer: BufWriter<File>,
}

impl BinaryLog {
    pub fn open(path: &str) -> io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;

        Ok(Self {
            writer: BufWriter::new(file),
            path: path.to_string(),
        })
    }

    pub fn append_raw(&mut self, record: &[u8]) -> io::Result<()> {
        let len = record.len();
        assert!(len <= u32::MAX as usize);

        let len_bytes = (len as u32).to_le_bytes();

        self.writer.write_all(&len_bytes)?;
        self.writer.write_all(record)?;
        Ok(())
    }

    pub fn append_msgpack<T: Serialize>(&mut self, value: &T) -> io::Result<()> {
        let buf = rmp_serde::to_vec_named(value).map_err(|e| io::Error::other(e.to_string()))?;
        self.append_raw(&buf)
    }

    pub fn sync(&mut self) -> io::Result<()> {
        self.writer.flush()?;
        self.writer.get_ref().sync_data()
    }

    pub fn read_all(&self) -> io::Result<Vec<LogEntry>> {
        match read_all::<BinaryLogEntry>(&self.path) {
            Ok(val) => Ok(val
                .iter()
                .map(|item| item.to_proto())
                .collect::<Vec<LogEntry>>()),
            Err(e) => Err(e),
        }
    }

    pub fn truncate(&mut self, keep_count: usize) -> io::Result<()> {
        let entries = read_all::<BinaryLogEntry>(&self.path)?;
        let to_keep = &entries[..keep_count.min(entries.len())];

        self.writer.flush()?;

        let file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(&self.path)?;
        let new_writer = BufWriter::new(file);
        let _ = mem::replace(&mut self.writer, new_writer);

        for entry in to_keep {
            self.append_msgpack(entry)?;
        }
        self.sync()?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::raft_proto::LogEntry;

    fn temp_path(name: &str) -> String {
        let mut p = std::env::temp_dir();
        p.push(format!("lattice_test_{}.bin", name));
        p.to_string_lossy().into_owned()
    }

    #[test]
    fn test_append_and_read() {
        let path = temp_path("append_read");

        // ensure file doesn't exist from previous runs
        let _ = std::fs::remove_file(&path);

        let mut log = BinaryLog::open(&path).expect("open log");

        let entry = BinaryLogEntry {
            term: 7,
            index: 42,
            command: b"hello".to_vec(),
        };

        log.append_msgpack(&entry).expect("append");
        log.sync().expect("sync");

        let read = log.read_all().expect("read_all");
        assert_eq!(read.len(), 1);
        let proto: LogEntry = read[0].clone();
        assert_eq!(proto.term, 7);
        assert_eq!(proto.command, b"hello".to_vec());

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_multiple_appends() {
        let path = temp_path("multiple_appends");
        let _ = std::fs::remove_file(&path);

        let mut log = BinaryLog::open(&path).expect("open log");

        for i in 0..10 {
            let entry = BinaryLogEntry {
                term: i,
                index: i,
                command: format!("cmd{}", i).into_bytes(),
            };
            log.append_msgpack(&entry).expect("append");
        }
        log.sync().expect("sync");

        let entries = log.read_all().expect("read_all");
        assert_eq!(entries.len(), 10);
        for i in 0..10 {
            assert_eq!(entries[i as usize].term, i);
            assert_eq!(entries[i as usize].index, i);
            assert_eq!(entries[i as usize].command, format!("cmd{}", i).into_bytes());
        }

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_truncate_empty_log() {
        let path = temp_path("truncate_empty");
        let _ = std::fs::remove_file(&path);

        let mut log = BinaryLog::open(&path).expect("open log");
        log.truncate(0).expect("truncate");

        let entries = log.read_all().expect("read_all");
        assert_eq!(entries.len(), 0);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_truncate_partial() {
        let path = temp_path("truncate_partial");
        let _ = std::fs::remove_file(&path);

        let mut log = BinaryLog::open(&path).expect("open log");

        for i in 0..10 {
            let entry = BinaryLogEntry {
                term: i,
                index: i,
                command: format!("cmd{}", i).into_bytes(),
            };
            log.append_msgpack(&entry).expect("append");
        }
        log.sync().expect("sync");

        log.truncate(5).expect("truncate to 5");

        let entries = log.read_all().expect("read_all");
        assert_eq!(entries.len(), 5);
        for i in 0..5 {
            assert_eq!(entries[i as usize].term, i);
        }

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_truncate_then_append() {
        let path = temp_path("truncate_append");
        let _ = std::fs::remove_file(&path);

        let mut log = BinaryLog::open(&path).expect("open log");

        for i in 0..5 {
            let entry = BinaryLogEntry {
                term: i,
                index: i,
                command: vec![i as u8],
            };
            log.append_msgpack(&entry).expect("append");
        }
        log.sync().expect("sync");

        log.truncate(3).expect("truncate");

        let entry = BinaryLogEntry {
            term: 99,
            index: 3,
            command: vec![99],
        };
        log.append_msgpack(&entry).expect("append after truncate");
        log.sync().expect("sync");

        let entries = log.read_all().expect("read_all");
        assert_eq!(entries.len(), 4);
        assert_eq!(entries[3].term, 99);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_persistence_across_reopens() {
        let path = temp_path("persistence");
        let _ = std::fs::remove_file(&path);

        {
            let mut log = BinaryLog::open(&path).expect("open log");
            for i in 0..3 {
                let entry = BinaryLogEntry {
                    term: i,
                    index: i,
                    command: vec![i as u8],
                };
                log.append_msgpack(&entry).expect("append");
            }
            log.sync().expect("sync");
        }

        {
            let log = BinaryLog::open(&path).expect("reopen log");
            let entries = log.read_all().expect("read_all");
            assert_eq!(entries.len(), 3);
            for i in 0..3 {
                assert_eq!(entries[i as usize].term, i);
            }
        }

        let _ = std::fs::remove_file(&path);
    }
}
