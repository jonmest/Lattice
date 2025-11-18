pub mod raft {
    tonic::include_proto!("raft");
}

use raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotChunk, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
    raft_node_client::RaftNodeClient,
    raft_node_server::{RaftNode, RaftNodeServer},
};

#[derive(Default)]
pub struct LatticeRaftNode {}

#[tonic::async_trait]
impl RaftNode for LatticeRaftNode {
    async fn request_vote(
        &self,
        request: tonic::Request<VoteRequest>,
    ) -> Result<tonic::Response<VoteResponse>, tonic::Status> {
        let req = request.into_inner();

        println!("Got vote request from {}", req.candidate_id);

        let response = VoteResponse {
            term: req.term,
            granted: true,
        };

        Ok(tonic::Response::new(response))
    }

    async fn append_entries(
        &self,
        request: tonic::Request<AppendEntriesRequest>,
    ) -> Result<tonic::Response<AppendEntriesResponse>, tonic::Status> {
        todo!()
    }

    async fn install_snapshot(
        &self,
        request: tonic::Request<InstallSnapshotChunk>,
    ) -> Result<tonic::Response<InstallSnapshotResponse>, tonic::Status> {
        todo!()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;
    let raft_node = LatticeRaftNode::default();

    println!("RaftNode server listening on {}", addr);

    tonic::transport::Server::builder()
        .add_service(RaftNodeServer::new(raft_node))
        .serve(addr)
        .await?;

    Ok(())
}
