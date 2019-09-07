use serde::de::Error;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct ClusterConfig {
    #[serde(default)]
    pub compression_strategy: CompressionStrategy,
    #[serde(default)]
    pub migration_config: MigrationConfig,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            compression_strategy: CompressionStrategy::default(),
            migration_config: MigrationConfig::default(),
        }
    }
}

impl ClusterConfig {
    pub fn set_field(&mut self, field: String, value: String) {
        let field = field.to_lowercase();
    }
}

pub struct AtomicClusterConfig {
    compression_strategy: AtomicCompressionStrategy,
    migration_config: AtomicMigrationConfig,
}

impl Default for AtomicClusterConfig {
    fn default() -> Self {
        Self {
            compression_strategy: AtomicCompressionStrategy::default(),
            migration_config: AtomicMigrationConfig::default(),
        }
    }
}

impl AtomicClusterConfig {
    pub fn from_cluster_config(cluster_config: ClusterConfig) -> Self {
        let ClusterConfig {
            compression_strategy,
            migration_config,
        } = cluster_config;
        Self {
            compression_strategy: AtomicCompressionStrategy::new(compression_strategy),
            migration_config: AtomicMigrationConfig::from_config(migration_config),
        }
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum CompressionStrategy {
    Disabled = 0,
    // Only allow SET, SETEX, PSETEX, SETNX, GET, GETSET commands for String data type
    // as once compression is enabled other commands will get the wrong result.
    SetGetOnly = 1,
    // Allow all the String commands. User need to use lua script to
    // bypass the compression.
    AllowAll = 2,
}

impl Default for CompressionStrategy {
    fn default() -> Self {
        CompressionStrategy::Disabled
    }
}

impl CompressionStrategy {
    pub fn from_str(s: &str) -> Option<Self> {
        let lowercase = s.to_lowercase();
        match lowercase.as_str() {
            "disabled" => Some(Self::Disabled),
            "set_get_only" => Some(Self::SetGetOnly),
            "allow_all" => Some(Self::AllowAll),
            _ => None,
        }
    }

    pub fn to_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::SetGetOnly => "set_get_only",
            Self::AllowAll => "allow_all",
        }
    }
}

impl Serialize for CompressionStrategy {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.clone().to_str())
    }
}

impl<'de> Deserialize<'de> for CompressionStrategy {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Self::from_str(&s)
            .ok_or_else(|| D::Error::custom(format!("invalid compression strategy {}", s)))
    }
}

#[derive(Debug)]
pub struct AtomicCompressionStrategy {
    inner: AtomicU8,
}

impl Default for AtomicCompressionStrategy {
    fn default() -> Self {
        Self::new(CompressionStrategy::Disabled)
    }
}

impl AtomicCompressionStrategy {
    pub fn new(strategy: CompressionStrategy) -> Self {
        Self {
            inner: AtomicU8::new(strategy as u8),
        }
    }

    pub fn set_strategy(&self, strategy: CompressionStrategy) {
        self.inner.store(strategy as u8, Ordering::SeqCst);
    }

    pub fn get_strategy(&self) -> CompressionStrategy {
        match self.inner.load(Ordering::SeqCst) {
            0 => CompressionStrategy::Disabled,
            1 => CompressionStrategy::SetGetOnly,
            _ => CompressionStrategy::AllowAll,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct MigrationConfig {
    offset_threshold: u64,
    max_migration_time: u64,
    max_blocking_time: u64,
    min_blocking_time: u64,
    max_redirection_time: u64,
    switch_retry_interval: u64,
}

impl Default for MigrationConfig {
    fn default() -> Self {
        Self {
            offset_threshold: 50000,
            max_migration_time: 10 * 60 * 1000, // 10 minutes, should leave some time for replication
            max_blocking_time: 10_000,          // 10 seconds waiting for commit
            min_blocking_time: 100,             // 100ms
            max_redirection_time: 5000,         // 5s, to wait for coordinator to update meta
            switch_retry_interval: 10,          // 10ms
        }
    }
}

pub struct AtomicMigrationConfig {
    offset_threshold: AtomicU64,
    max_migration_time: AtomicU64,
    max_blocking_time: AtomicU64,
    min_blocking_time: AtomicU64,
    max_redirection_time: AtomicU64,
    switch_retry_interval: AtomicU64,
}

impl Default for AtomicMigrationConfig {
    fn default() -> Self {
        Self::from_config(MigrationConfig::default())
    }
}

impl AtomicMigrationConfig {
    pub fn from_config(config: MigrationConfig) -> Self {
        Self {
            offset_threshold: AtomicU64::new(config.offset_threshold),
            max_migration_time: AtomicU64::new(config.max_migration_time),
            max_blocking_time: AtomicU64::new(config.max_blocking_time),
            min_blocking_time: AtomicU64::new(config.min_blocking_time),
            max_redirection_time: AtomicU64::new(config.max_redirection_time),
            switch_retry_interval: AtomicU64::new(config.switch_retry_interval),
        }
    }

    pub fn get_offset_threshold(&self) -> u64 {
        self.offset_threshold.load(Ordering::SeqCst)
    }
    pub fn get_max_migration_time(&self) -> u64 {
        self.max_migration_time.load(Ordering::SeqCst)
    }
    pub fn get_max_blocking_time(&self) -> u64 {
        self.max_blocking_time.load(Ordering::SeqCst)
    }
    pub fn get_min_blocking_time(&self) -> u64 {
        self.min_blocking_time.load(Ordering::SeqCst)
    }
    pub fn get_max_redirection_time(&self) -> u64 {
        self.max_redirection_time.load(Ordering::SeqCst)
    }
    pub fn get_switch_retry_interval(&self) -> u64 {
        self.switch_retry_interval.load(Ordering::SeqCst)
    }

    pub fn set_offset_threshold(&self, offset_threshold: u64) {
        self.offset_threshold
            .store(offset_threshold, Ordering::SeqCst)
    }
    pub fn set_max_migration_time(&self, max_migration_time: u64) {
        self.max_migration_time
            .store(max_migration_time, Ordering::SeqCst)
    }
    pub fn set_max_blocking_time(&self, max_blocking_time: u64) {
        self.max_blocking_time
            .store(max_blocking_time, Ordering::SeqCst)
    }
    pub fn set_min_blocking_time(&self, min_blocking_time: u64) {
        self.min_blocking_time
            .store(min_blocking_time, Ordering::SeqCst)
    }
    pub fn set_max_redirection_time(&self, max_redirection_time: u64) {
        self.max_redirection_time
            .store(max_redirection_time, Ordering::SeqCst)
    }
    pub fn set_switch_retry_interval(&self, switch_retry_interval: u64) {
        self.switch_retry_interval
            .store(switch_retry_interval, Ordering::SeqCst)
    }
}
