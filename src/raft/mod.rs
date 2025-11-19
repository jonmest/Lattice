pub mod raft {
    tonic::include_proto!("raft");
}
mod state;
use crate::{kv::LatticeStore, raft::state::RaftNodeRole};
use raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotChunk, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
    raft_node_client::RaftNodeClient,
    raft_node_server::{RaftNode, RaftNodeServer},
};
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct LatticeRaftNode {
    store: Arc<RwLock<LatticeStore>>,
    voted_for: RwLock<Option<String>>,
    current_term: RwLock<u64>,
    /// Todo: Add log entries
    // Volatile
    commit_index: RwLock<u64>,
    last_applied: RwLock<u64>,
    role: RwLock<RaftNodeRole>,
}

impl LatticeRaftNode {
    pub fn new(store: Arc<RwLock<LatticeStore>>) -> LatticeRaftNode {
        Self {
            store,
            current_term: RwLock::new(0),
            commit_index: RwLock::new(0),
            last_applied: RwLock::new(0),
            voted_for: RwLock::new(None),
            role: RwLock::new(RaftNodeRole::Follower),
        }
    }
}

#[tonic::async_trait]
impl RaftNode for LatticeRaftNode {
    async fn request_vote(
        &self,
        request: tonic::Request<VoteRequest>,
    ) -> Result<tonic::Response<VoteResponse>, tonic::Status> {
        let req = request.into_inner();
        println!("Got vote request from {}", req.candidate_id);

        let mut current_term = *self.current_term.read().await;
        let mut voted_for = self.voted_for.read().await.clone();

        if req.term > current_term {
            *self.current_term.write().await = req.term;
            *self.voted_for.write().await = None;
            current_term = req.term;
            voted_for = None;
        }

        let term_ok = req.term == current_term; // must be same term
        let vote_ok = voted_for.is_none() || voted_for.as_ref() == Some(&req.candidate_id);

        // TODO proper log comparison when added log entries
        let log_ok = true;

        let granted = term_ok && vote_ok && log_ok;

        if granted {
            *self.voted_for.write().await = Some(req.candidate_id);
        }

        let response = VoteResponse {
            term: current_term,
            granted,
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
