use std::sync::Arc;

use tokio::sync::RwLock;
use tonic::Request;

use crate::{
    kv::{
        ApplyResult, KvCommand, LatticeStore,
        kv_proto::{
            CompareAndSetRequest, CompareAndSetResponse, DeleteRequest, DeleteResponse, GetRequest,
            GetResponse, PutRequest, PutResponse, key_value_store_server::KeyValueStore,
        },
    },
    raft::LatticeNode,
};

pub struct KvStoreService {
    store: Arc<RwLock<LatticeStore>>,
    raft_node: Option<Arc<LatticeNode>>,
}

impl KvStoreService {
    pub fn new(store: Arc<RwLock<LatticeStore>>) -> Self {
        Self {
            store,
            raft_node: None,
        }
    }

    pub fn with_raft_node(store: Arc<RwLock<LatticeStore>>, raft_node: Arc<LatticeNode>) -> Self {
        Self {
            store,
            raft_node: Some(raft_node),
        }
    }
}

#[tonic::async_trait]
impl KeyValueStore for KvStoreService {
    async fn get(
        &self,
        request: Request<GetRequest>,
    ) -> Result<tonic::Response<GetResponse>, tonic::Status> {
        let key = request.into_inner().key;

        // If we have a raft node, try lease-based read
        if let Some(ref raft_node) = self.raft_node {
            let read_request = crate::raft::raft_proto::ReadQueryRequest {
                key: key.clone(),
            };

            match raft_node.handle_read_query(read_request).await {
                Ok(response) if response.success => {
                    return Ok(tonic::Response::new(GetResponse {
                        value: response.value,
                    }));
                }
                Ok(_) => {
                    // Lease not valid, fall back to direct read
                }
                Err(_) => {
                    // Error during lease read, fall back to direct read
                }
            }
        }

        // Fallback: direct read from store (when no raft node or lease invalid)
        match self.store.read().await.get(&key) {
            Some(val) => Ok(tonic::Response::new(GetResponse { value: val.clone() })),
            None => Err(tonic::Status::unknown("message")),
        }
    }

    async fn put(
        &self,
        request: Request<PutRequest>,
    ) -> Result<tonic::Response<PutResponse>, tonic::Status> {
        let body = request.into_inner();
        let command = KvCommand::Set {
            key: body.key,
            value: body.value,
        };

        match self.store.write().await.apply(command) {
            ApplyResult::Set(_) => Ok(tonic::Response::new(PutResponse { ok: true })),
            _ => Err(tonic::Status::unknown("message")),
        }
    }

    async fn compare_and_set(
        &self,
        request: Request<CompareAndSetRequest>,
    ) -> Result<tonic::Response<CompareAndSetResponse>, tonic::Status> {
        let req = request.into_inner();

        match self.store.read().await.get(&req.key) {
            Some(val) => {
                if val == &req.expected {
                    let command = KvCommand::Set {
                        key: req.key,
                        value: req.value,
                    };

                    match self.store.write().await.apply(command) {
                        ApplyResult::Set(_) => {
                            Ok(tonic::Response::new(CompareAndSetResponse { ok: true }))
                        }
                        _ => Err(tonic::Status::unknown("message")),
                    }
                } else {
                    Ok(tonic::Response::new(CompareAndSetResponse { ok: false }))
                }
            }
            None => Err(tonic::Status::unknown("message")),
        }
    }

    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<tonic::Response<DeleteResponse>, tonic::Status> {
        let body = request.into_inner();
        let command = KvCommand::Delete { key: body.key };

        match self.store.write().await.apply(command) {
            ApplyResult::Delete(_) => Ok(tonic::Response::new(DeleteResponse { ok: true })),
            _ => Err(tonic::Status::unknown("message")),
        }
    }
}
