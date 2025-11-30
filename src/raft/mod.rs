pub mod raft_proto {
    tonic::include_proto!("raft");
}
mod binary_log;
pub mod log;
pub mod node;
pub mod peer;
pub mod snapshot;
pub mod state;
use std::sync::Arc;

pub use node::LatticeNode;
use raft_proto::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotChunk, InstallSnapshotResponse,
    VoteRequest, VoteResponse, raft_node_server::RaftNode,
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
        _request: tonic::Request<InstallSnapshotChunk>,
    ) -> Result<tonic::Response<InstallSnapshotResponse>, tonic::Status> {
        todo!()
    }
}
