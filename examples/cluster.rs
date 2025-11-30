use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};

use tokio::sync::RwLock;

mod kv {
    pub use lattice::kv::*;
}

mod raft {
    pub use lattice::raft::*;
}

use kv::{
    LatticeStore,
    kv_proto::{
        key_value_store_server::KeyValueStoreServer,
        key_value_store_client::KeyValueStoreClient,
        GetRequest, PutRequest, DeleteRequest,
    },
    service::KvStoreService,
};

use raft::{
    LatticeNode, LatticeRaftGrpcService,
    log::LatticeLog,
    peer::Peer,
    raft_proto::raft_node_server::RaftNodeServer,
};

async fn start_node(
    id: SocketAddr,
    raft_addr: SocketAddr,
    kv_addr: SocketAddr,
    peer_addrs: Vec<SocketAddr>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let log_path = format!("./data/node_{}.db", raft_addr.port());
    let snapshot_path = format!("./data/node_{}.snap", raft_addr.port());
    std::fs::create_dir_all("./data").ok();

    let mut peers = HashMap::new();
    for peer_addr in peer_addrs {
        if peer_addr != raft_addr {
            tokio::time::sleep(Duration::from_millis(100)).await;
            match Peer::new(peer_addr).await {
                Ok(peer) => {
                    peers.insert(peer_addr, peer);
                }
                Err(e) => {
                    eprintln!("Failed to connect to peer {}: {}", peer_addr, e);
                }
            }
        }
    }

    let log = Arc::new(RwLock::new(LatticeLog::new(&log_path)?));
    let store = Arc::new(RwLock::new(LatticeStore::new()));

    let raft_node = Arc::new(LatticeNode::new(
        id,
        peers,
        store.clone(),
        log,
        snapshot_path,
        Duration::from_millis(1500),
    ));

    // Restore from snapshot if one exists
    raft_node.restore_from_snapshot().await?;

    let raft_node_clone = raft_node.clone();
    let raft_node_server = raft_node.clone();
    let signal_node = raft_node.clone();

    let raft_loop = tokio::spawn(async move {
        println!("[Node {}] Starting Raft loop", id);
        if let Err(e) = raft_node_clone.run().await {
            eprintln!("[Node {}] Raft loop error: {}", id, e);
        }
    });

    let raft_server = tokio::spawn(async move {
        let service = LatticeRaftGrpcService::new(raft_node_server.clone());
        println!("[Node {}] Raft server listening on {}", id, raft_addr);

        tonic::transport::Server::builder()
            .add_service(RaftNodeServer::new(service))
            .serve(raft_addr)
            .await
            .unwrap();
    });

    let kv_service = tokio::spawn(async move {
        let service = KvStoreService::new(store.clone());
        println!("[Node {}] KV service listening on {}", id, kv_addr);

        tonic::transport::Server::builder()
            .add_service(KeyValueStoreServer::new(service))
            .serve(kv_addr)
            .await
            .unwrap();
    });

    // Handle shutdown signals
    let signal_handler = tokio::spawn(async move {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("Failed to install SIGTERM handler");
        let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
            .expect("Failed to install SIGINT handler");

        tokio::select! {
            _ = sigterm.recv() => println!("[Node {}] Received SIGTERM", id),
            _ = sigint.recv() => println!("[Node {}] Received SIGINT", id),
        }

        println!("[Node {}] Initiating graceful shutdown...", id);
        if let Err(e) = signal_node.graceful_shutdown().await {
            eprintln!("[Node {}] Error during graceful shutdown: {}", id, e);
        }
    });

    tokio::select! {
        result = tokio::try_join!(raft_loop, raft_server, kv_service) => result?,
        _ = signal_handler => {},
    }

    Ok(())
}

async fn run_client() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tokio::time::sleep(Duration::from_secs(2)).await;

    let kv_addrs = vec![
        "http://[::1]:50152",
        "http://[::1]:50252",
        "http://[::1]:50352",
    ];

    println!("\n=== Starting client operations ===\n");

    for addr in &kv_addrs {
        match KeyValueStoreClient::connect(addr.to_string()).await {
            Ok(mut client) => {
                println!("Connected to KV service at {}", addr);

                println!("  PUT key1 = value1");
                match client.put(PutRequest {
                    key: b"key1".to_vec(),
                    value: b"value1".to_vec(),
                }).await {
                    Ok(_) => println!("  ✓ PUT successful"),
                    Err(e) => println!("  ✗ PUT failed: {}", e),
                }

                tokio::time::sleep(Duration::from_millis(500)).await;

                println!("  GET key1");
                match client.get(GetRequest {
                    key: b"key1".to_vec(),
                }).await {
                    Ok(resp) => {
                        let inner = resp.into_inner();
                        let value = String::from_utf8_lossy(&inner.value);
                        println!("  ✓ GET successful: {}", value);
                    }
                    Err(e) => println!("  ✗ GET failed: {}", e),
                }

                println!("  DELETE key1");
                match client.delete(DeleteRequest {
                    key: b"key1".to_vec(),
                }).await {
                    Ok(_) => println!("  ✓ DELETE successful"),
                    Err(e) => println!("  ✗ DELETE failed: {}", e),
                }

                println!();
                break;
            }
            Err(e) => {
                eprintln!("Failed to connect to {}: {}", addr, e);
            }
        }
    }

    Ok(())
}

#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("=== Starting 3-Node Lattice Cluster ===\n");

    let node_configs = vec![
        (
            "127.0.0.1:50051".parse::<SocketAddr>().unwrap(),
            "[::1]:50051".parse().unwrap(),
            "[::1]:50152".parse().unwrap(),
        ),
        (
            "127.0.0.1:50151".parse::<SocketAddr>().unwrap(),
            "[::1]:50151".parse().unwrap(),
            "[::1]:50252".parse().unwrap(),
        ),
        (
            "127.0.0.1:50251".parse::<SocketAddr>().unwrap(),
            "[::1]:50251".parse().unwrap(),
            "[::1]:50352".parse().unwrap(),
        ),
    ];

    let raft_addrs: Vec<SocketAddr> = node_configs.iter().map(|(_, r, _)| *r).collect();

    let mut handles = vec![];

    for (id, raft_addr, kv_addr) in node_configs {
        let raft_addrs_clone = raft_addrs.clone();
        let handle = tokio::spawn(async move {
            start_node(id, raft_addr, kv_addr, raft_addrs_clone).await
        });
        handles.push(handle);
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    tokio::spawn(async {
        if let Err(e) = run_client().await {
            eprintln!("Client error: {}", e);
        }
    });

    for handle in handles {
        handle.await??;
    }

    Ok(())
}
