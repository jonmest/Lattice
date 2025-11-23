use std::{collections::HashMap, hash::Hash, net::SocketAddr, sync::Arc};

use tokio::sync::RwLock;

use crate::{
    kv::{LatticeStore, kv::key_value_store_server::KeyValueStore},
    raft::{LatticeRaftNode, log::LatticeLog, peer::Peer, raft::raft_node_server::RaftNodeServer},
};

mod kv;
mod raft;

#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = "./log.db";
    let log = Arc::new(RwLock::new(LatticeLog::new(path)));
    let store = Arc::new(RwLock::new(LatticeStore::new()));

    let id: SocketAddr = "127.0.0.1:50051".parse().unwrap();
    let peers: HashMap<SocketAddr, Peer> = HashMap::new();

    let raft_handle = tokio::spawn(async move {
        let address = "[::1]:50051".parse().unwrap();
        let raft_node = LatticeRaftNode::new(id, peers, store, log);

        println!("RaftNode server listening on {}", address);

        tonic::transport::Server::builder()
            .add_service(RaftNodeServer::new(raft_node))
            .serve(address)
            .await
            .unwrap();
    });

    tokio::try_join!(raft_handle)?;

    Ok(())
}
