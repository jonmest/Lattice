use std::collections::HashMap;

pub mod kv {
    tonic::include_proto!("kv");
}

pub struct LatticeStore {
    map: HashMap<String, String>,
}

impl Default for LatticeStore {
    fn default() -> Self {
        Self {
            map: HashMap::new(),
        }
    }
}

impl LatticeStore {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }
    pub fn get(&self, key: &String) -> Option<&String> {
        self.map.get(key)
    }

    pub fn set(&mut self, key: String, value: String) {
        self.map.insert(key, value);
    }
}
