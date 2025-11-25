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
        peer::{Peer, connect_to_node},
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
    async fn apply_commited_entries(&self) -> Result<(), rmp_serde::decode::Error> {
        let mut last_applied = *self.last_applied.write().await;
        let commit_index = *self.commit_index.read().await;
        let log = self.log.read().await;
        let mut store = self.store.write().await;

        while last_applied < commit_index {
            last_applied += 1;

            if let Some(entry) = log.get(last_applied) {
                let command = rmp_serde::from_slice::<KvCommand>(&entry.command[..])?;
                store.apply(command);
            }
        }

        Ok(())
    }

    async fn run_leader_tick(&self) -> Result<(), Box<dyn std::error::Error>> {
        let peers = self.peers.read().await;
        let log = self.log.read().await;
        let current_term = *self.current_term.read().await;

        for peer in peers.values() {
            let to_send = {
                if peer.next_index > log.last_index() {
                    &[]
                } else {
                    log.entries_from(peer.next_index)
                }
            };
            let prev_log_item = &log.get(peer.next_index - 1);
            let prev_log_item = &prev_log_item.as_ref().unwrap();

            let req = tonic::Request::new(AppendEntriesRequest {
                term: current_term,
                leader_id: self.id.read().await.to_string(),
                prev_log_index: prev_log_item.index,
                prev_log_term: prev_log_item.term,
                leader_commit: *self.commit_index.read().await,
                entries: to_send.to_vec(),
            });
            let mut client = connect_to_node(&peer.addr).await?;
            let response = client.append_entries(req).await?.into_inner();
            if !response.success {
                self.peers
                    .write()
                    .await
                    .entry(peer.addr)
                    .and_modify(|p| p.next_index -= 1);
            }
        }

        Ok(())
    }

    async fn run_follower_tick(&self) -> Result<(), Box<dyn std::error::Error>> {
        *self.timer.write().await = Instant::now();
        let duration = self.timer.read().await.elapsed();
        if duration > self.timeout {
            *self.role.write().await = RaftNodeRole::Candidate;

            let population: u64 = self.peers.read().await.len().try_into().unwrap();
            let majority_threshold = population.div_ceil(2);
            let mut votes = 0;
            // request election
            for p in self.peers.read().await.values() {
                let req = tonic::Request::new(VoteRequest {
                    term: *self.current_term.read().await,
                    last_log_index: self.log.read().await.last_index(),
                    last_log_term: self.log.read().await.last_term(),
                    candidate_id: self.id.read().await.to_string(),
                });

                // todo: eagerly setup connection earlier
                let mut client = connect_to_node(&p.addr).await?;
                let response = client.request_vote(req).await?;
                let vote_response = response.into_inner();
                if vote_response.granted {
                    votes += 1;
                }
            }
            if votes >= majority_threshold {
                *self.role.write().await = RaftNodeRole::Leader;
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
            });
        }

        // if our term is outdated, update and turn into follower
        if request.term > current_term {
            current_term = request.term;
            *self.voted_for.write().await = None;
            *self.role.write().await = RaftNodeRole::Follower;
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
                    });
                }
                Some(entry) => {
                    if entry.term != request.prev_log_term {
                        return Ok(AppendEntriesResponse {
                            term: current_term,
                            success: false,
                        });
                    }
                }
            }
        }

        let mut next_index = request.prev_log_index + 1;
        for new_entry in request.entries {
            let mut log = self.log.write().await;
            if let Some(existing_entry) = self.log.read().await.get(next_index) {
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
            }),
            Err(e) => Err(e.into()),
        }
    }
}
