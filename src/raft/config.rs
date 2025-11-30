use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum ConfigChange {
    AddServer { id: SocketAddr },
    RemoveServer { id: SocketAddr },
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum RaftCommand {
    Kv(crate::kv::KvCommand),
    Config(ConfigChange),
}
