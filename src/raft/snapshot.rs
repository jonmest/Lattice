use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::kv::LatticeStore;

#[derive(Serialize, Deserialize, Clone)]
pub struct SnapshotMetadata {
    pub last_included_index: u64,
    pub last_included_term: u64,
}

#[derive(Serialize, Deserialize)]
pub struct Snapshot {
    pub metadata: SnapshotMetadata,
    pub data: Vec<u8>,
}

impl Snapshot {
    pub fn new(last_included_index: u64, last_included_term: u64, store: &LatticeStore) -> io::Result<Self> {
        let data = rmp_serde::to_vec_named(store)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

        Ok(Self {
            metadata: SnapshotMetadata {
                last_included_index,
                last_included_term,
            },
            data,
        })
    }

    pub fn restore_store(&self) -> io::Result<LatticeStore> {
        rmp_serde::from_slice(&self.data)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
    }

    pub fn save(&self, path: &str) -> io::Result<()> {
        let temp_path = format!("{}.tmp", path);

        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temp_path)?;

        let mut writer = BufWriter::new(file);

        let serialized = rmp_serde::to_vec_named(self)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

        let len = serialized.len() as u32;
        writer.write_all(&len.to_le_bytes())?;
        writer.write_all(&serialized)?;
        writer.flush()?;
        writer.get_ref().sync_all()?;

        std::fs::rename(&temp_path, path)?;

        Ok(())
    }

    pub fn load(path: &str) -> io::Result<Option<Self>> {
        if !Path::new(path).exists() {
            return Ok(None);
        }

        let file = File::open(path)?;
        let mut reader = BufReader::new(file);

        let mut len_bytes = [0u8; 4];
        match reader.read_exact(&mut len_bytes) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                return Ok(None);
            }
            Err(e) => return Err(e),
        }

        let len = u32::from_le_bytes(len_bytes) as usize;
        let mut buf = vec![0u8; len];
        reader.read_exact(&mut buf)?;

        let snapshot: Snapshot = rmp_serde::from_slice(&buf)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

        Ok(Some(snapshot))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv::LatticeStore;

    fn temp_path(name: &str) -> String {
        let mut p = std::env::temp_dir();
        p.push(format!("lattice_snapshot_test_{}.snap", name));
        p.to_string_lossy().into_owned()
    }

    #[test]
    fn test_snapshot_save_and_load() {
        let path = temp_path("save_load");
        let _ = std::fs::remove_file(&path);

        let mut store = LatticeStore::new();
        store.set(b"key1".to_vec(), b"value1".to_vec());
        store.set(b"key2".to_vec(), b"value2".to_vec());

        let snapshot = Snapshot::new(10, 5, &store).expect("create snapshot");
        snapshot.save(&path).expect("save snapshot");

        let loaded = Snapshot::load(&path).expect("load snapshot").expect("snapshot exists");

        assert_eq!(loaded.metadata.last_included_index, 10);
        assert_eq!(loaded.metadata.last_included_term, 5);

        let restored_store = loaded.restore_store().expect("restore store");
        assert_eq!(restored_store.get(&b"key1".to_vec()), Some(&b"value1".to_vec()));
        assert_eq!(restored_store.get(&b"key2".to_vec()), Some(&b"value2".to_vec()));

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_load_nonexistent_snapshot() {
        let path = temp_path("nonexistent");
        let _ = std::fs::remove_file(&path);

        let result = Snapshot::load(&path).expect("load should not error");
        assert!(result.is_none());
    }

    #[test]
    fn test_snapshot_atomic_write() {
        let path = temp_path("atomic");
        let _ = std::fs::remove_file(&path);

        let mut store = LatticeStore::new();
        store.set(b"test".to_vec(), b"data".to_vec());

        let snapshot1 = Snapshot::new(5, 2, &store).expect("create snapshot 1");
        snapshot1.save(&path).expect("save snapshot 1");

        store.set(b"test".to_vec(), b"updated".to_vec());
        let snapshot2 = Snapshot::new(10, 3, &store).expect("create snapshot 2");
        snapshot2.save(&path).expect("save snapshot 2");

        let loaded = Snapshot::load(&path).expect("load").expect("exists");
        assert_eq!(loaded.metadata.last_included_index, 10);
        assert_eq!(loaded.metadata.last_included_term, 3);

        std::fs::remove_file(&path).ok();
    }
}
