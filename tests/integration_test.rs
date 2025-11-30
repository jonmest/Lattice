use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};

use tokio::sync::RwLock;

use lattice::{
    kv::{
        LatticeStore,
        kv_proto::{
            key_value_store_server::KeyValueStoreServer,
            key_value_store_client::KeyValueStoreClient,
            PutRequest, GetRequest,
        },
        service::KvStoreService,
    },
    raft::{
        LatticeNode, LatticeRaftGrpcService,
        log::LatticeLog,
        peer::Peer,
        raft_proto::raft_node_server::RaftNodeServer,
    },
};

#[tokio::test]
async fn test_single_node_kv_operations() {
    let test_id = "single_node";
    let log_path = format!("./test_data/{}.db", test_id);
    let snapshot_path = format!("./test_data/{}.snap", test_id);
    std::fs::create_dir_all("./test_data").ok();
    let _ = std::fs::remove_file(&log_path);
    let _ = std::fs::remove_file(&snapshot_path);

    let id: SocketAddr = "127.0.0.1:60051".parse().unwrap();
    let raft_addr = "[::1]:60051".parse().unwrap();
    let kv_addr = "[::1]:60052".parse().unwrap();

    let peers = HashMap::new();
    let log = Arc::new(RwLock::new(LatticeLog::new(&log_path).unwrap()));
    let store = Arc::new(RwLock::new(LatticeStore::new()));

    let raft_node = Arc::new(LatticeNode::new(id, peers, store.clone(), log, snapshot_path.clone()));

    let raft_server_handle = {
        let raft_node = raft_node.clone();
        tokio::spawn(async move {
            let service = LatticeRaftGrpcService::new(raft_node);
            tonic::transport::Server::builder()
                .add_service(RaftNodeServer::new(service))
                .serve(raft_addr)
                .await
                .unwrap();
        })
    };

    let kv_server_handle = tokio::spawn(async move {
        let service = KvStoreService::new(store);
        tonic::transport::Server::builder()
            .add_service(KeyValueStoreServer::new(service))
            .serve(kv_addr)
            .await
            .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(500)).await;

    let mut client = KeyValueStoreClient::connect("http://[::1]:60052")
        .await
        .expect("connect to kv service");

    client
        .put(PutRequest {
            key: b"test_key".to_vec(),
            value: b"test_value".to_vec(),
        })
        .await
        .expect("put operation");

    let response = client
        .get(GetRequest {
            key: b"test_key".to_vec(),
        })
        .await
        .expect("get operation");

    assert_eq!(response.into_inner().value, b"test_value".to_vec());

    raft_server_handle.abort();
    kv_server_handle.abort();

    std::fs::remove_file(&log_path).ok();
    std::fs::remove_file(&snapshot_path).ok();
}

#[tokio::test]
async fn test_two_node_cluster() {
    let test_id = "two_node";
    std::fs::create_dir_all("./test_data").ok();

    let node1_log = format!("./test_data/{}_node1.db", test_id);
    let node2_log = format!("./test_data/{}_node2.db", test_id);
    let node1_snap = format!("./test_data/{}_node1.snap", test_id);
    let node2_snap = format!("./test_data/{}_node2.snap", test_id);
    let _ = std::fs::remove_file(&node1_log);
    let _ = std::fs::remove_file(&node2_log);
    let _ = std::fs::remove_file(&node1_snap);
    let _ = std::fs::remove_file(&node2_snap);

    let node1_id: SocketAddr = "127.0.0.1:61051".parse().unwrap();
    let node1_raft_addr = "[::1]:61051".parse().unwrap();
    let node1_kv_addr = "[::1]:61052".parse().unwrap();

    let node2_id: SocketAddr = "127.0.0.1:61151".parse().unwrap();
    let node2_raft_addr = "[::1]:61151".parse().unwrap();
    let node2_kv_addr = "[::1]:61152".parse().unwrap();

    let start_node = |id: SocketAddr, raft_addr: SocketAddr, kv_addr: SocketAddr,
                      log_path: String, snapshot_path: String, peer_addr: Option<SocketAddr>| async move {
        let mut peers = HashMap::new();
        if let Some(peer) = peer_addr {
            tokio::time::sleep(Duration::from_millis(200)).await;
            if let Ok(p) = Peer::new(peer).await {
                peers.insert(peer, p);
            }
        }

        let log = Arc::new(RwLock::new(LatticeLog::new(&log_path).unwrap()));
        let store = Arc::new(RwLock::new(LatticeStore::new()));
        let raft_node = Arc::new(LatticeNode::new(id, peers, store.clone(), log, snapshot_path));

        let raft_handle = {
            let raft_node = raft_node.clone();
            tokio::spawn(async move {
                let service = LatticeRaftGrpcService::new(raft_node);
                tonic::transport::Server::builder()
                    .add_service(RaftNodeServer::new(service))
                    .serve(raft_addr)
                    .await
                    .unwrap();
            })
        };

        let kv_handle = tokio::spawn(async move {
            let service = KvStoreService::new(store);
            tonic::transport::Server::builder()
                .add_service(KeyValueStoreServer::new(service))
                .serve(kv_addr)
                .await
                .unwrap();
        });

        (raft_handle, kv_handle)
    };

    let (node1_raft, node1_kv) = start_node(
        node1_id, node1_raft_addr, node1_kv_addr, node1_log.clone(), node1_snap.clone(), None
    ).await;

    tokio::time::sleep(Duration::from_millis(300)).await;

    let (node2_raft, node2_kv) = start_node(
        node2_id, node2_raft_addr, node2_kv_addr, node2_log.clone(), node2_snap.clone(), Some(node1_raft_addr)
    ).await;

    tokio::time::sleep(Duration::from_secs(1)).await;

    let mut client = KeyValueStoreClient::connect("http://[::1]:61052")
        .await
        .expect("connect to node1 kv service");

    client
        .put(PutRequest {
            key: b"cluster_key".to_vec(),
            value: b"cluster_value".to_vec(),
        })
        .await
        .expect("put operation on node1");

    tokio::time::sleep(Duration::from_millis(500)).await;

    let response = client
        .get(GetRequest {
            key: b"cluster_key".to_vec(),
        })
        .await
        .expect("get operation");

    assert_eq!(response.into_inner().value, b"cluster_value".to_vec());

    node1_raft.abort();
    node1_kv.abort();
    node2_raft.abort();
    node2_kv.abort();

    std::fs::remove_file(&node1_log).ok();
    std::fs::remove_file(&node2_log).ok();
    std::fs::remove_file(&node1_snap).ok();
    std::fs::remove_file(&node2_snap).ok();
}
