use std::{collections::HashMap, sync::Arc};

use tokio::sync::RwLock;

use crate::{
    kv::{LatticeStore, kv::key_value_store_server::KeyValueStore},
    raft::{LatticeRaftNode, raft::raft_node_server::RaftNodeServer},
};

mod kv;
mod raft;

#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let map = LatticeStore::default();
    let kv_store = Arc::new(RwLock::new(map));
    let kv_store_clone = Arc::clone(&kv_store);

    let raft_handle = tokio::spawn(async move {
        let address = "[::1]:50051".parse().unwrap();
        let raft_node = LatticeRaftNode::new(kv_store_clone);

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
