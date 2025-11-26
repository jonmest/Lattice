use std::{
    collections::HashMap,
    io::{self, Error, ErrorKind},
};

use rmp_serde::from_slice;
use serde::{Deserialize, Serialize};

pub mod kv_proto {
    tonic::include_proto!("kv");
}
pub mod service;

#[derive(Serialize, Deserialize)]
pub enum KvCommand {
    Set { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
    Get { key: Vec<u8> },
}

pub enum ApplyResult<'a> {
    Set(Option<Vec<u8>>),
    Delete(Option<Vec<u8>>),
    Get(Option<&'a Vec<u8>>),
}

#[derive(Default)]
pub struct LatticeStore {
    map: HashMap<Vec<u8>, Vec<u8>>,
}

impl LatticeStore {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    pub fn apply_command(&mut self, command: &[u8]) -> io::Result<ApplyResult> {
        let command: KvCommand =
            from_slice(command).map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;

        Ok(self.apply(command))
    }

    pub fn apply(&mut self, command: KvCommand) -> ApplyResult {
        match command {
            KvCommand::Set { key, value } => ApplyResult::Set(self.set(key, value)),
            KvCommand::Delete { key } => ApplyResult::Delete(self.delete(&key)),
            KvCommand::Get { key } => ApplyResult::Get(self.get(&key)),
        }
    }

    pub fn get(&self, key: &Vec<u8>) -> Option<&Vec<u8>> {
        self.map.get(key)
    }

    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Option<Vec<u8>> {
        self.map.insert(key, value)
    }

    pub fn delete(&mut self, key: &Vec<u8>) -> Option<Vec<u8>> {
        self.map.remove(key)
    }
}
