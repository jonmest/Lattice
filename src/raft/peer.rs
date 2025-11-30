use std::net::SocketAddr;

use crate::raft::raft_proto::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotChunk,
    InstallSnapshotResponse, VoteRequest, VoteResponse, raft_node_client::RaftNodeClient,
};

pub struct Peer {
    pub next_index: u64,
    pub match_index: u64,
    pub address: SocketAddr,
    pub connection: RaftNodeClient<tonic::transport::Channel>,
}

impl Peer {
    pub async fn new(address: SocketAddr) -> Result<Self, Box<dyn std::error::Error>> {
        let client = RaftNodeClient::connect(address.to_string()).await?;
        Ok(Self {
            address,
            match_index: 0,
            next_index: 0,
            connection: client,
        })
    }

    pub async fn send_vote_request(
        &mut self,
        req: VoteRequest,
    ) -> std::result::Result<VoteResponse, tonic::Status> {
        let response = self
            .connection
            .request_vote(tonic::Request::new(req))
            .await?;
        Ok(response.into_inner())
    }

    pub async fn send_append_entries(
        &mut self,
        req: AppendEntriesRequest,
    ) -> std::result::Result<AppendEntriesResponse, tonic::Status> {
        let response = self
            .connection
            .append_entries(tonic::Request::new(req))
            .await?;
        Ok(response.into_inner())
    }

    pub async fn send_install_snapshot(
        &mut self,
        req: InstallSnapshotChunk,
    ) -> std::result::Result<InstallSnapshotResponse, tonic::Status> {
        let response = self
            .connection
            .install_snapshot(tonic::Request::new(req))
            .await?;
        Ok(response.into_inner())
    }
}
