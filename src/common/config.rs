use serde::de::Error;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

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
    pub fn set_field(&mut self, field: &str, value: &str) -> Result<(), ConfigError> {
        let field = field.to_lowercase();
        match field.as_str() {
            "compression_strategy" => {
                let strategy = CompressionStrategy::from_str(&value)
                    .ok_or_else(|| ConfigError::InvalidValue)?;
                self.compression_strategy = strategy;
            }
            _ => {
                if field.starts_with("migration_") {
                    let f = field
                        .splitn(2, '_')
                        .nth(1)
                        .ok_or_else(|| ConfigError::FieldNotFound)?;
                    return self.migration_config.set_field(f, value);
                } else {
                    return Err(ConfigError::FieldNotFound);
                }
            }
        }
        Ok(())
    }

    pub fn to_str_map(&self) -> HashMap<String, String> {
        vec![
            (
                "compression_strategy",
                self.compression_strategy.to_str().to_string(),
            ),
            (
                "migration_offset_threshold",
                self.migration_config.offset_threshold.to_string(),
            ),
            (
                "migration_max_migration_time",
                self.migration_config.max_migration_time.to_string(),
            ),
            (
                "migration_max_blocking_time",
                self.migration_config.max_blocking_time.to_string(),
            ),
            (
                "migration_min_blocking_time",
                self.migration_config.min_blocking_time.to_string(),
            ),
            (
                "migration_max_redirection_time",
                self.migration_config.max_redirection_time.to_string(),
            ),
            (
                "migration_switch_retry_interval",
                self.migration_config.switch_retry_interval.to_string(),
            ),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect()
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

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct MigrationConfig {
    pub offset_threshold: u64,
    pub max_migration_time: u64,
    pub max_blocking_time: u64,
    pub min_blocking_time: u64,
    pub max_redirection_time: u64,
    pub switch_retry_interval: u64,
}

impl MigrationConfig {
    fn set_field(&mut self, field: &str, value: &str) -> Result<(), ConfigError> {
        let field = field.to_lowercase();
        match field.as_str() {
            "offset_threshold" => {
                let v = value
                    .parse::<u64>()
                    .map_err(|_| ConfigError::InvalidValue)?;
                self.offset_threshold = v;
            }
            "max_migration_time" => {
                let v = value
                    .parse::<u64>()
                    .map_err(|_| ConfigError::InvalidValue)?;
                self.max_migration_time = v;
            }
            "max_blocking_time" => {
                let v = value
                    .parse::<u64>()
                    .map_err(|_| ConfigError::InvalidValue)?;
                self.max_blocking_time = v;
            }
            "min_blocking_time" => {
                let v = value
                    .parse::<u64>()
                    .map_err(|_| ConfigError::InvalidValue)?;
                self.min_blocking_time = v;
            }
            "max_redirection_time" => {
                let v = value
                    .parse::<u64>()
                    .map_err(|_| ConfigError::InvalidValue)?;
                self.max_redirection_time = v;
            }
            "switch_retry_interval" => {
                let v = value
                    .parse::<u64>()
                    .map_err(|_| ConfigError::InvalidValue)?;
                self.switch_retry_interval = v;
            }
            _ => return Err(ConfigError::FieldNotFound),
        }
        Ok(())
    }
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

    #[allow(dead_code)]
    pub fn set_offset_threshold(&self, offset_threshold: u64) {
        self.offset_threshold
            .store(offset_threshold, Ordering::SeqCst)
    }
    #[allow(dead_code)]
    pub fn set_max_migration_time(&self, max_migration_time: u64) {
        self.max_migration_time
            .store(max_migration_time, Ordering::SeqCst)
    }
    #[allow(dead_code)]
    pub fn set_max_blocking_time(&self, max_blocking_time: u64) {
        self.max_blocking_time
            .store(max_blocking_time, Ordering::SeqCst)
    }
    #[allow(dead_code)]
    pub fn set_min_blocking_time(&self, min_blocking_time: u64) {
        self.min_blocking_time
            .store(min_blocking_time, Ordering::SeqCst)
    }
    #[allow(dead_code)]
    pub fn set_max_redirection_time(&self, max_redirection_time: u64) {
        self.max_redirection_time
            .store(max_redirection_time, Ordering::SeqCst)
    }
    #[allow(dead_code)]
    pub fn set_switch_retry_interval(&self, switch_retry_interval: u64) {
        self.switch_retry_interval
            .store(switch_retry_interval, Ordering::SeqCst)
    }
}

#[derive(Debug)]
pub enum ConfigError {
    ReadonlyField,
    FieldNotFound,
    InvalidValue,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_set_field() {
        let mut migration_config = MigrationConfig::default();
        migration_config
            .set_field("offset_threshold", "233")
            .expect("test_config_set_field");
        assert_eq!(migration_config.offset_threshold, 233);

        let mut cluster_config = ClusterConfig::default();
        cluster_config
            .set_field("migration_offset_threshold", "666")
            .expect("test_config_set_field");
        assert_eq!(cluster_config.migration_config.offset_threshold, 666);
    }
}
