pub mod raft_proto {
    tonic::include_proto!("raft");
}
mod binary_log;
pub mod config;
pub mod log;
pub mod node;
pub mod peer;
pub mod snapshot;
pub mod state;
use std::sync::Arc;

pub use node::LatticeNode;
use raft_proto::{
    AddServerRequest, AddServerResponse, AppendEntriesRequest, AppendEntriesResponse,
    InstallSnapshotChunk, InstallSnapshotResponse, ReadQueryRequest, ReadQueryResponse,
    RemoveServerRequest, RemoveServerResponse, TimeoutNowRequest, TimeoutNowResponse,
    TransferLeadershipRequest, TransferLeadershipResponse, VoteRequest, VoteResponse,
    raft_node_server::RaftNode,
};

pub struct LatticeRaftGrpcService {
    node: Arc<LatticeNode>,
}

impl LatticeRaftGrpcService {
    pub fn new(node: Arc<LatticeNode>) -> Self {
        Self { node }
    }
}

#[tonic::async_trait]
impl RaftNode for LatticeRaftGrpcService {
    async fn request_vote(
        &self,
        request: tonic::Request<VoteRequest>,
    ) -> Result<tonic::Response<VoteResponse>, tonic::Status> {
        match self.node.handle_request_vote(request.into_inner()).await {
            Ok(response) => Ok(tonic::Response::new(response)),
            Err(_) => Err(tonic::Status::internal("Something went wrong.")),
        }
    }

    async fn append_entries(
        &self,
        request: tonic::Request<AppendEntriesRequest>,
    ) -> Result<tonic::Response<AppendEntriesResponse>, tonic::Status> {
        let res = self
            .node
            .handle_append_entries_request(request.into_inner())
            .await;

        match res {
            Ok(data) => Ok(tonic::Response::new(data)),
            Err(e) => Err(tonic::Status::internal(e.to_string())),
        }
    }

    async fn install_snapshot(
        &self,
        request: tonic::Request<InstallSnapshotChunk>,
    ) -> Result<tonic::Response<InstallSnapshotResponse>, tonic::Status> {
        match self.node.handle_install_snapshot(request.into_inner()).await {
            Ok(response) => Ok(tonic::Response::new(response)),
            Err(e) => Err(tonic::Status::internal(e.to_string())),
        }
    }

    async fn add_server(
        &self,
        request: tonic::Request<AddServerRequest>,
    ) -> Result<tonic::Response<AddServerResponse>, tonic::Status> {
        match self.node.handle_add_server(request.into_inner()).await {
            Ok(response) => Ok(tonic::Response::new(response)),
            Err(e) => Err(tonic::Status::internal(e.to_string())),
        }
    }

    async fn remove_server(
        &self,
        request: tonic::Request<RemoveServerRequest>,
    ) -> Result<tonic::Response<RemoveServerResponse>, tonic::Status> {
        match self.node.handle_remove_server(request.into_inner()).await {
            Ok(response) => Ok(tonic::Response::new(response)),
            Err(e) => Err(tonic::Status::internal(e.to_string())),
        }
    }

    async fn read_query(
        &self,
        request: tonic::Request<ReadQueryRequest>,
    ) -> Result<tonic::Response<ReadQueryResponse>, tonic::Status> {
        match self.node.handle_read_query(request.into_inner()).await {
            Ok(response) => Ok(tonic::Response::new(response)),
            Err(e) => Err(tonic::Status::internal(e.to_string())),
        }
    }

    async fn transfer_leadership(
        &self,
        request: tonic::Request<TransferLeadershipRequest>,
    ) -> Result<tonic::Response<TransferLeadershipResponse>, tonic::Status> {
        match self.node.handle_transfer_leadership(request.into_inner()).await {
            Ok(response) => Ok(tonic::Response::new(response)),
            Err(e) => Err(tonic::Status::internal(e.to_string())),
        }
    }

    async fn timeout_now(
        &self,
        request: tonic::Request<TimeoutNowRequest>,
    ) -> Result<tonic::Response<TimeoutNowResponse>, tonic::Status> {
        match self.node.handle_timeout_now(request.into_inner()).await {
            Ok(response) => Ok(tonic::Response::new(response)),
            Err(e) => Err(tonic::Status::internal(e.to_string())),
        }
    }
}
