use crate::raft::{log::Log, raft_proto::AppendEntriesResponse};
use std::{cmp::min, collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    sync::RwLock,
    time::{Instant, sleep},
};

use crate::{
    kv::{KvCommand, LatticeStore},
    raft::{
        log::LatticeLog,
        peer::Peer,
        raft_proto::{AppendEntriesRequest, VoteRequest, VoteResponse},
        state::RaftNodeRole,
    },
};

pub struct LatticeNode {
    id: RwLock<SocketAddr>,
    timer: RwLock<Instant>,
    timeout: Duration,
    store: Arc<RwLock<LatticeStore>>,
    log: Arc<RwLock<LatticeLog>>,
    voted_for: RwLock<Option<String>>,
    current_term: RwLock<u64>,

    // Volatile
    commit_index: RwLock<u64>,
    last_applied: RwLock<u64>,
    role: RwLock<RaftNodeRole>,

    peers: RwLock<HashMap<SocketAddr, Peer>>,
    leader: RwLock<Option<SocketAddr>>,
}

impl LatticeNode {
    pub fn new(
        id: SocketAddr,
        peers: HashMap<SocketAddr, Peer>,
        store: Arc<RwLock<LatticeStore>>,
        log: Arc<RwLock<LatticeLog>>,
    ) -> LatticeNode {
        Self {
            id: RwLock::new(id),
            timer: RwLock::new(Instant::now()),
            timeout: Duration::from_secs(2),
            store,
            log,
            current_term: RwLock::new(0),
            commit_index: RwLock::new(0),
            last_applied: RwLock::new(0),
            voted_for: RwLock::new(None),
            role: RwLock::new(RaftNodeRole::Follower),
            peers: RwLock::new(peers),
            leader: RwLock::new(None),
        }
    }
}

impl LatticeNode {
    pub async fn apply_commited_entries(&self) -> Result<(), rmp_serde::decode::Error> {
        let commit_index = *self.commit_index.read().await;
        let mut last_applied_guard = self.last_applied.write().await;
        
        while *last_applied_guard < commit_index {
            *last_applied_guard += 1;
            
            let entry = self.log.read().await.get(*last_applied_guard);
            if let Some(entry) = entry {
                let command = rmp_serde::from_slice::<KvCommand>(&entry.command[..])?;

                &self.store.write().await.apply(command);
            }
        }
        Ok(())
    }

    async fn run_leader_tick(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut peers = self.peers.write().await;
        let log = self.log.read().await;
        let current_term = *self.current_term.read().await;

        for peer in peers.values_mut() {
            let to_send = {
                if peer.next_index > log.last_index() {
                    &[]
                } else {
                    log.entries_from(peer.next_index)
                }
            };
            let prev_log_item = &log.get(peer.next_index - 1);
            let prev_log_item = &prev_log_item.as_ref().unwrap();

            let req = AppendEntriesRequest {
                term: current_term,
                leader_id: self.id.read().await.to_string(),
                prev_log_index: prev_log_item.index,
                prev_log_term: prev_log_item.term,
                leader_commit: *self.commit_index.read().await,
                entries: to_send.to_vec(),
            };
            let response = peer.send_append_entries(req).await?;

            if response.term > current_term {
                *self.current_term.write().await = response.term;
                *self.voted_for.write().await = None;
                *self.role.write().await = RaftNodeRole::Follower;
                return Ok(());
            }

            if !response.success {
                self.peers
                    .write()
                    .await
                    .entry(peer.address)
                    .and_modify(|p| {
                        if p.next_index > 1 {
                            p.next_index -= 1;
                        }
                    });
            } else {
                self.peers
                    .write()
                    .await
                    .entry(peer.address)
                    .and_modify(|p| p.match_index = response.cursor);
            }
        }

        // Advance commit_index based on what's been replicated to a majority
        let current_commit = *self.commit_index.read().await;
        let last_log_index = log.last_index();
        let population = peers.len() + 1; // peers + self

        // Try to find the highest index N that has been replicated to a majority
        for n in (current_commit + 1)..=last_log_index {
            let mut replication_count = 1; // Count self

            // Count how many peers have replicated up to index n
            for peer in peers.values() {
                if peer.match_index >= n {
                    replication_count += 1;
                }
            }

            // If a majority has replicated index n, we can commit it
            if replication_count >= population.div_ceil(2) {
                // Only commit entries from current term (Raft safety requirement)
                if let Some(entry) = log.get(n) {
                    if entry.term == current_term {
                        *self.commit_index.write().await = n;
                    }
                }
            }
        }

        Ok(())
    }

    async fn run_follower_tick(&self) -> Result<(), Box<dyn std::error::Error>> {
        *self.timer.write().await = Instant::now();
        let duration = self.timer.read().await.elapsed();
        if duration > self.timeout {
            *self.role.write().await = RaftNodeRole::Candidate;

            let mut current_term = self.current_term.write().await;
            *current_term += 1;
            let new_term = *current_term;
            drop(current_term);

            *self.voted_for.write().await = Some(self.id.read().await.to_string());

            let population: u64 = self.peers.read().await.len().try_into().unwrap();
            let majority_threshold = population.div_ceil(2);
            let mut votes = 1;

            for p in self.peers.write().await.values_mut() {
                let req = VoteRequest {
                    term: new_term,
                    last_log_index: self.log.read().await.last_index(),
                    last_log_term: self.log.read().await.last_term(),
                    candidate_id: self.id.read().await.to_string(),
                };

                let vote_response = p.send_vote_request(req).await?;

                if vote_response.term > new_term {
                    *self.current_term.write().await = vote_response.term;
                    *self.voted_for.write().await = None;
                    *self.role.write().await = RaftNodeRole::Follower;
                    return Ok(());
                }

                if vote_response.granted {
                    votes += 1;
                }
            }
            if votes >= majority_threshold {
                *self.role.write().await = RaftNodeRole::Leader;

                let last_log_index = self.log.read().await.last_index();
                let mut peers = self.peers.write().await;
                for peer in peers.values_mut() {
                    peer.next_index = last_log_index + 1;
                    peer.match_index = 0;
                }
            } else {
                *self.role.write().await = RaftNodeRole::Follower;
            }
        }
        Ok(())
    }

    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        loop {
            match *self.role.read().await {
                RaftNodeRole::Leader => self.run_leader_tick().await?,
                _ => self.run_follower_tick().await?,
            };
            sleep(Duration::from_millis(200)).await;
        }
    }

    pub async fn handle_request_vote(
        &self,
        vote_request: VoteRequest,
    ) -> Result<VoteResponse, Box<dyn std::error::Error>> {
        let mut current_term = *self.current_term.read().await;
        let mut voted_for = self.voted_for.read().await.clone();
        let last_term = self.log.read().await.last_term();
        let last_log_index = self.log.read().await.last_index();

        if vote_request.term > current_term {
            *self.current_term.write().await = vote_request.term;
            *self.voted_for.write().await = None;
            *self.role.write().await = RaftNodeRole::Follower;
            current_term = vote_request.term;
            voted_for = None;
        }

        let term_ok = vote_request.term == current_term;
        let vote_ok = voted_for.is_none() || voted_for.as_ref() == Some(&vote_request.candidate_id);

        let log_ok = vote_request.last_log_term > last_term
            || (vote_request.last_log_term == last_term
                && vote_request.last_log_index >= last_log_index);

        let granted = term_ok && vote_ok && log_ok;

        if granted {
            *self.voted_for.write().await = Some(vote_request.candidate_id);
        }

        let response = VoteResponse {
            term: current_term,
            granted,
        };

        *self.timer.write().await = Instant::now();

        Ok(response)
    }

    pub async fn handle_append_entries_request(
        &self,
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, Box<dyn std::error::Error>> {
        let mut current_term = *self.current_term.read().await;
        // fail if remote term is lower than current term
        if request.term < current_term {
            return Ok(AppendEntriesResponse {
                term: current_term,
                success: false,

                cursor: *self.last_applied.read().await,
            });
        }

        if request.term > current_term {
            *self.current_term.write().await = request.term;
            *self.voted_for.write().await = None;
            *self.role.write().await = RaftNodeRole::Follower;
            current_term = request.term;
        }
        *self.leader.write().await = Some(
            request
                .leader_id
                .parse()
                .expect("Unable to parse peer address"),
        );

        *self.timer.write().await = Instant::now();

        if request.prev_log_index > 0 {
            match self.log.read().await.get(request.prev_log_index) {
                None => {
                    return Ok(AppendEntriesResponse {
                        term: current_term,
                        success: false,
                        cursor: *self.last_applied.read().await,
                    });
                }
                Some(entry) => {
                    if entry.term != request.prev_log_term {
                        return Ok(AppendEntriesResponse {
                            term: current_term,
                            success: false,
                            cursor: *self.last_applied.read().await,
                        });
                    }
                }
            }
        }

        let mut log = self.log.write().await;
        let mut next_index = request.prev_log_index + 1;
        for new_entry in request.entries {
            if let Some(existing_entry) = log.get(next_index) {
                if existing_entry.term != new_entry.term {
                    // delete old entry and everything that follows
                    log.truncate(next_index);
                    log.append(new_entry.term, new_entry.command)?;
                }
            } else {
                log.append(new_entry.term, new_entry.command)?;
            }
            next_index += 1;
        }

        if request.leader_commit > *self.commit_index.read().await {
            let last_new_entry_index = self.log.read().await.last_index();
            *self.commit_index.write().await = min(request.leader_commit, last_new_entry_index);
        }

        match self.apply_commited_entries().await {
            Ok(_) => Ok(AppendEntriesResponse {
                term: current_term,
                success: true,
                cursor: *self.last_applied.read().await,
            }),
            Err(e) => Err(e.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::raft_proto::{AppendEntriesRequest, LogEntry, VoteRequest};
    use std::collections::HashMap;
    use tokio::sync::RwLock as TokioRwLock;

    fn temp_path(name: &str) -> String {
        let mut p = std::env::temp_dir();
        p.push(format!("lattice_node_test_{}.bin", name));
        p.to_string_lossy().into_owned()
    }

    #[tokio::test]
    async fn test_handle_request_vote_grants() {
        let addr: SocketAddr = "127.0.0.1:9001".parse().unwrap();
        let peers: HashMap<SocketAddr, Peer> = HashMap::new();

        let store = Arc::new(TokioRwLock::new(crate::kv::LatticeStore::new()));

        let path = temp_path("vote");
        let _ = std::fs::remove_file(&path);
        let _ = crate::raft::binary_log::BinaryLog::open(&path).expect("open");
        let log_obj = crate::raft::log::LatticeLog::new(&path).expect("create lattice log");
        let log = Arc::new(TokioRwLock::new(log_obj));

        let node = LatticeNode::new(addr, peers, store.clone(), log.clone());

        let req = VoteRequest {
            term: 1,
            last_log_index: 0,
            last_log_term: 0,
            candidate_id: "candidate-1".to_string(),
        };

        let resp = node.handle_request_vote(req).await.expect("vote");
        assert_eq!(resp.granted, true);

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_handle_append_entries_appends() {
        let addr: SocketAddr = "127.0.0.1:9002".parse().unwrap();
        let peers: HashMap<SocketAddr, Peer> = HashMap::new();

        let store = Arc::new(TokioRwLock::new(crate::kv::LatticeStore::new()));

        let path = temp_path("append");
        let _ = std::fs::remove_file(&path);
        let _ = crate::raft::binary_log::BinaryLog::open(&path).expect("open");
        let log_inner = crate::raft::log::LatticeLog::new(&path).expect("create lattice log");
        let log = Arc::new(TokioRwLock::new(log_inner));

        let node = LatticeNode::new(addr, peers, store.clone(), log.clone());

        // make a kv set command
        let cmd = crate::kv::KvCommand::Set {
            key: b"kx".to_vec(),
            value: b"vx".to_vec(),
        };
        let cmd_bytes = rmp_serde::to_vec_named(&cmd).expect("serialize cmd");

        let entry = LogEntry {
            term: 1,
            index: 1,
            command: cmd_bytes.clone(),
        };

        let req = AppendEntriesRequest {
            term: 1,
            leader_id: addr.to_string(),
            prev_log_index: 0,
            prev_log_term: 0,
            leader_commit: 1,
            entries: vec![entry],
        };

        let resp = node
            .handle_append_entries_request(req)
            .await
            .expect("append_entries");
        assert!(resp.success);

        let read_log = log.read().await;
        let got = read_log.get(1).expect("get appended");
        assert_eq!(got.command, cmd_bytes);

        let _ = std::fs::remove_file(&path);
    }
}
