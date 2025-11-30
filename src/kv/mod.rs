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

pub enum ApplyResult {
    Set(Option<Vec<u8>>),
    Delete(Option<Vec<u8>>),
    Get(Option<Vec<u8>>),
}

#[derive(Default, Serialize, Deserialize)]
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
            KvCommand::Get { key } => ApplyResult::Get(self.get(&key).cloned()),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_get_delete() {
        let mut store = LatticeStore::new();

        let key = b"k1".to_vec();
        let val = b"v1".to_vec();

        assert!(store.get(&key).is_none());

        let prev = store.set(key.clone(), val.clone());
        assert!(prev.is_none());
        assert_eq!(store.get(&key).unwrap(), &val);

        let removed = store.delete(&key);
        assert_eq!(removed.unwrap(), val);
        assert!(store.get(&key).is_none());
    }

    #[test]
    fn test_apply_command_serialization() {
        let mut store = LatticeStore::new();

        let cmd = KvCommand::Set {
            key: b"k2".to_vec(),
            value: b"v2".to_vec(),
        };

        let bytes = rmp_serde::to_vec_named(&cmd).expect("serialize cmd");

        match store.apply_command(&bytes).expect("apply cmd") {
            ApplyResult::Set(Some(_)) | ApplyResult::Set(None) => {}
            _ => panic!("unexpected apply result"),
        }
    }

    #[test]
    fn test_overwrite_key() {
        let mut store = LatticeStore::new();
        let key = b"key".to_vec();

        let prev1 = store.set(key.clone(), b"v1".to_vec());
        assert!(prev1.is_none());

        let prev2 = store.set(key.clone(), b"v2".to_vec());
        assert_eq!(prev2.unwrap(), b"v1".to_vec());

        assert_eq!(store.get(&key).unwrap(), &b"v2".to_vec());
    }

    #[test]
    fn test_delete_nonexistent() {
        let mut store = LatticeStore::new();
        let key = b"missing".to_vec();

        let result = store.delete(&key);
        assert!(result.is_none());
    }

    #[test]
    fn test_multiple_keys() {
        let mut store = LatticeStore::new();

        for i in 0..10 {
            let key = format!("k{}", i).into_bytes();
            let val = format!("v{}", i).into_bytes();
            store.set(key, val);
        }

        for i in 0..10 {
            let key = format!("k{}", i).into_bytes();
            let val = format!("v{}", i).into_bytes();
            assert_eq!(store.get(&key).unwrap(), &val);
        }
    }

    #[test]
    fn test_apply_set_command() {
        let mut store = LatticeStore::new();
        let cmd = KvCommand::Set {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        };

        match store.apply(cmd) {
            ApplyResult::Set(prev) => assert!(prev.is_none()),
            _ => panic!("unexpected result"),
        }

        assert_eq!(store.get(&b"k".to_vec()).unwrap(), b"v");
    }

    #[test]
    fn test_apply_delete_command() {
        let mut store = LatticeStore::new();
        store.set(b"k".to_vec(), b"v".to_vec());

        let cmd = KvCommand::Delete {
            key: b"k".to_vec(),
        };

        match store.apply(cmd) {
            ApplyResult::Delete(prev) => assert_eq!(prev.unwrap(), b"v".to_vec()),
            _ => panic!("unexpected result"),
        }

        assert!(store.get(&b"k".to_vec()).is_none());
    }

    #[test]
    fn test_apply_get_command() {
        let mut store = LatticeStore::new();
        store.set(b"k".to_vec(), b"v".to_vec());

        let cmd = KvCommand::Get {
            key: b"k".to_vec(),
        };

        match store.apply(cmd) {
            ApplyResult::Get(val) => assert_eq!(val.unwrap(), b"v".to_vec()),
            _ => panic!("unexpected result"),
        }
    }

    #[test]
    fn test_empty_store() {
        let store = LatticeStore::new();
        assert!(store.get(&b"any".to_vec()).is_none());
    }

    #[test]
    fn test_set_then_delete_then_set() {
        let mut store = LatticeStore::new();
        let key = b"k".to_vec();

        store.set(key.clone(), b"v1".to_vec());
        store.delete(&key);
        store.set(key.clone(), b"v2".to_vec());

        assert_eq!(store.get(&key).unwrap(), &b"v2".to_vec());
    }
}
