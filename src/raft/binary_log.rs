use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};

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
        assert!(len <= u32::max as usize);

        let len_bytes = (len as u32).to_le_bytes();

        self.writer.write_all(&len_bytes)?;
        self.writer.write_all(record)?;
        Ok(())
    }

    pub fn append_msgpack<T: Serialize>(&mut self, value: &T) -> io::Result<()> {
        let buf = rmp_serde::to_vec_named(value)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
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
}
