mod kv;
mod raft;

use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use tokio::sync::RwLock;

use crate::{
    kv::{
        LatticeStore,
        kv_proto::key_value_store_server::{KeyValueStore, KeyValueStoreServer},
        service::KvStoreService,
    },
    raft::{
        LatticeNode, LatticeRaftGrpcService, log::LatticeLog, peer::Peer,
        raft_proto::raft_node_server::RaftNodeServer,
    },
};

#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = "./log.db";
    let id: SocketAddr = "127.0.0.1:50051".parse().unwrap();
    let peers: HashMap<SocketAddr, Peer> = HashMap::new();

    let log = Arc::new(RwLock::new(LatticeLog::new(path)?));
    let store = Arc::new(RwLock::new(LatticeStore::new()));

    let raft_node = Arc::new(LatticeNode::new(id, peers, store.clone(), log));
    let raft_node_clone = raft_node.clone();

    let raft_loop = tokio::spawn(async move {
        println!("Raft loop initiated.");
        raft_node_clone.clone().run().await.unwrap();
    });

    let raft_server = tokio::spawn(async move {
        let address = "[::1]:50051".parse().unwrap();
        let service = LatticeRaftGrpcService::new(raft_node.clone());

        println!("RaftNode server listening on {}", address);

        tonic::transport::Server::builder()
            .add_service(RaftNodeServer::new(service))
            .serve(address)
            .await
            .unwrap();
    });

    let kv_service = tokio::spawn(async move {
        let address = "[::1]:50052".parse().unwrap();
        let service = KvStoreService::new(store.clone());
        println!("Key Value Store Service listening on {}", address);
        tonic::transport::Server::builder()
            .add_service(KeyValueStoreServer::new(service))
            .serve(address)
            .await
            .unwrap();
    });

    tokio::try_join!(raft_loop, raft_server, kv_service)?;

    Ok(())
}
