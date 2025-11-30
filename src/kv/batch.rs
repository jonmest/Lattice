use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{oneshot, RwLock, Mutex};
use tokio::time::sleep;

use crate::kv::{ApplyResult, KvCommand, LatticeStore};
use crate::raft::config::RaftCommand;
use crate::raft::log::{LatticeLog, Log};

type ResponseSender = oneshot::Sender<ApplyResult>;

pub struct BatchRequest {
    pub command: KvCommand,
    pub response_tx: ResponseSender,
}

pub struct BatchCollector {
    batch_size: usize,
    batch_timeout: Duration,
    pending: Arc<Mutex<Vec<BatchRequest>>>,
    log: Arc<RwLock<LatticeLog>>,
    store: Arc<RwLock<LatticeStore>>,
    current_term: Arc<RwLock<u64>>,
}

impl BatchCollector {
    pub fn new(
        log: Arc<RwLock<LatticeLog>>,
        store: Arc<RwLock<LatticeStore>>,
        current_term: Arc<RwLock<u64>>,
    ) -> Self {
        Self {
            batch_size: 100,
            batch_timeout: Duration::from_millis(10),
            pending: Arc::new(Mutex::new(Vec::new())),
            log,
            store,
            current_term,
        }
    }

    pub async fn submit(&self, command: KvCommand) -> Result<ApplyResult, Box<dyn std::error::Error + Send + Sync>> {
        let (tx, rx) = oneshot::channel();

        let mut pending = self.pending.lock().await;
        pending.push(BatchRequest {
            command,
            response_tx: tx,
        });

        let should_flush = pending.len() >= self.batch_size;
        drop(pending);

        if should_flush {
            self.flush().await?;
        }

        rx.await.map_err(|e| e.into())
    }

    pub async fn flush(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut pending = self.pending.lock().await;
        if pending.is_empty() {
            return Ok(());
        }

        let batch: Vec<BatchRequest> = pending.drain(..).collect();
        drop(pending);

        let commands: Vec<KvCommand> = batch.iter()
            .map(|req| req.command.clone())
            .collect();

        if commands.is_empty() {
            return Ok(());
        }

        // Create batch command
        let batch_cmd = if commands.len() == 1 {
            commands.into_iter().next().unwrap()
        } else {
            KvCommand::Batch { commands }
        };

        let raft_cmd = RaftCommand::Kv(batch_cmd);
        let cmd_bytes = rmp_serde::to_vec_named(&raft_cmd)?;

        // Append to log
        let term = *self.current_term.read().await;
        let mut log = self.log.write().await;
        log.append(term, cmd_bytes)?;
        drop(log);

        // Apply immediately (simplified - in real impl, wait for commit)
        let mut store = self.store.write().await;
        let result = store.apply(if batch.len() == 1 {
            batch[0].command.clone()
        } else {
            KvCommand::Batch { commands: batch.iter().map(|r| r.command.clone()).collect() }
        });

        // Distribute responses
        match result {
            ApplyResult::Batch(results) => {
                for (req, res) in batch.into_iter().zip(results) {
                    let _ = req.response_tx.send(res);
                }
            }
            single_result => {
                if let Some(req) = batch.into_iter().next() {
                    let _ = req.response_tx.send(single_result);
                }
            }
        }

        Ok(())
    }

    pub async fn start_background_flusher(self: Arc<Self>) {
        loop {
            sleep(self.batch_timeout).await;
            let _ = self.flush().await;
        }
    }
}
