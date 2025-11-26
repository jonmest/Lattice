use std::sync::Arc;

use tokio::sync::RwLock;
use tonic::Request;

use crate::kv::{
    ApplyResult, KvCommand, LatticeStore,
    kv_proto::{
        CompareAndSetRequest, CompareAndSetResponse, DeleteRequest, DeleteResponse, GetRequest,
        GetResponse, PutRequest, PutResponse, key_value_store_server::KeyValueStore,
    },
};

pub struct KvStoreService {
    store: Arc<RwLock<LatticeStore>>,
}

impl KvStoreService {
    pub fn new(store: Arc<RwLock<LatticeStore>>) -> Self {
        Self { store }
    }
}

#[tonic::async_trait]
impl KeyValueStore for KvStoreService {
    async fn get(
        &self,
        request: Request<GetRequest>,
    ) -> Result<tonic::Response<GetResponse>, tonic::Status> {
        match self.store.read().await.get(&request.into_inner().key) {
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
            ApplyResult::Set(_) => Ok(tonic::Response::new(DeleteResponse { ok: true })),
            _ => Err(tonic::Status::unknown("message")),
        }
    }
}
