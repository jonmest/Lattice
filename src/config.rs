use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatticeConfig {
    pub raft: RaftConfig,
    pub storage: StorageConfig,
    pub batching: BatchingConfig,
    pub observability: ObservabilityConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftConfig {
    /// Election timeout in milliseconds
    pub election_timeout_ms: u64,
    /// Heartbeat interval in milliseconds
    pub heartbeat_interval_ms: u64,
    /// Snapshot threshold (create snapshot after this many log entries)
    pub snapshot_threshold: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Path to the Raft log file
    pub log_path: String,
    /// Path to snapshot files
    pub snapshot_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchingConfig {
    /// Maximum number of operations in a batch
    pub max_batch_size: usize,
    /// Maximum time to wait before flushing a batch (milliseconds)
    pub batch_timeout_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObservabilityConfig {
    /// Enable OpenTelemetry tracing
    pub enable_tracing: bool,
    /// OTLP endpoint for traces (e.g., "http://localhost:4317")
    pub otlp_endpoint: Option<String>,
    /// Service name for tracing
    pub service_name: String,
}

impl Default for LatticeConfig {
    fn default() -> Self {
        Self {
            raft: RaftConfig::default(),
            storage: StorageConfig::default(),
            batching: BatchingConfig::default(),
            observability: ObservabilityConfig::default(),
        }
    }
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            election_timeout_ms: 2000,
            heartbeat_interval_ms: 200,
            snapshot_threshold: 1000,
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            log_path: "./log.db".to_string(),
            snapshot_path: "./snapshot.snap".to_string(),
        }
    }
}

impl Default for BatchingConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 100,
            batch_timeout_ms: 10,
        }
    }
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            enable_tracing: false,
            otlp_endpoint: None,
            service_name: "lattice".to_string(),
        }
    }
}

impl RaftConfig {
    pub fn election_timeout(&self) -> Duration {
        Duration::from_millis(self.election_timeout_ms)
    }

    pub fn heartbeat_interval(&self) -> Duration {
        Duration::from_millis(self.heartbeat_interval_ms)
    }
}

impl BatchingConfig {
    pub fn batch_timeout(&self) -> Duration {
        Duration::from_millis(self.batch_timeout_ms)
    }
}

impl LatticeConfig {
    pub fn from_file(path: &str) -> Result<Self, config::ConfigError> {
        let settings = config::Config::builder()
            .add_source(config::File::with_name(path))
            .add_source(config::Environment::with_prefix("LATTICE"))
            .build()?;

        settings.try_deserialize()
    }

    pub fn from_file_or_default(path: &str) -> Self {
        Self::from_file(path).unwrap_or_default()
    }
}
