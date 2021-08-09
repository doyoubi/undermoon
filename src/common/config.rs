use serde::de::Error;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
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
                let strategy =
                    CompressionStrategy::from_str(value).map_err(|_| ConfigError::InvalidValue)?;
                self.compression_strategy = strategy;
            }
            _ => {
                if field.starts_with("migration_") {
                    let f = field
                        .splitn(2, '_')
                        .nth(1)
                        .ok_or(ConfigError::FieldNotFound)?;
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
                "migration_max_migration_time",
                self.migration_config.max_migration_time.to_string(),
            ),
            (
                "migration_max_blocking_time",
                self.migration_config.max_blocking_time.to_string(),
            ),
            (
                "migration_scan_interval",
                self.migration_config.scan_interval.to_string(),
            ),
            (
                "migration_scan_count",
                self.migration_config.scan_count.to_string(),
            ),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionStrategy {
    Disabled = 0,
    // Only allow SET, SETEX, PSETEX, SETNX, GET, GETSET , MGET, MSET, MSETNX commands for String data type
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

pub struct InvalidCompressionStr;

impl FromStr for CompressionStrategy {
    type Err = InvalidCompressionStr;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let lowercase = s.to_lowercase();
        match lowercase.as_str() {
            "disabled" => Ok(Self::Disabled),
            "set_get_only" => Ok(Self::SetGetOnly),
            "allow_all" => Ok(Self::AllowAll),
            _ => Err(InvalidCompressionStr),
        }
    }
}

impl CompressionStrategy {
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
        serializer.serialize_str((*self).to_str())
    }
}

impl<'de> Deserialize<'de> for CompressionStrategy {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Self::from_str(&s)
            .map_err(|_| D::Error::custom(format!("invalid compression strategy {}", s)))
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct MigrationConfig {
    pub max_migration_time: u64,
    pub max_blocking_time: u64,
    pub scan_interval: u64,
    pub scan_count: u64,
}

impl MigrationConfig {
    fn set_field(&mut self, field: &str, value: &str) -> Result<(), ConfigError> {
        let field = field.to_lowercase();
        match field.as_str() {
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
            "scan_interval" => {
                let v = value
                    .parse::<u64>()
                    .map_err(|_| ConfigError::InvalidValue)?;
                self.scan_interval = v;
            }
            "scan_count" => {
                let v = value
                    .parse::<u64>()
                    .map_err(|_| ConfigError::InvalidValue)?;
                if v == 0 {
                    return Err(ConfigError::InvalidValue);
                }
                self.scan_count = v;
            }
            _ => return Err(ConfigError::FieldNotFound),
        }
        Ok(())
    }
}

impl Default for MigrationConfig {
    fn default() -> Self {
        Self {
            max_migration_time: 3 * 60 * 60, // 3 hours
            max_blocking_time: 10_000,       // 10 seconds waiting for switch
            scan_interval: 500,              // 500 microseconds
            scan_count: 16,
        }
    }
}

pub struct AtomicMigrationConfig {
    max_migration_time: AtomicU64,
    max_blocking_time: AtomicU64,
    scan_interval: AtomicU64,
    scan_count: AtomicU64,
}

impl Default for AtomicMigrationConfig {
    fn default() -> Self {
        Self::from_config(MigrationConfig::default())
    }
}

impl AtomicMigrationConfig {
    pub fn from_config(config: MigrationConfig) -> Self {
        Self {
            max_migration_time: AtomicU64::new(config.max_migration_time),
            max_blocking_time: AtomicU64::new(config.max_blocking_time),
            scan_interval: AtomicU64::new(config.scan_interval),
            scan_count: AtomicU64::new(config.scan_count),
        }
    }

    pub fn get_max_migration_time(&self) -> u64 {
        self.max_migration_time.load(Ordering::SeqCst)
    }

    pub fn get_max_blocking_time(&self) -> u64 {
        self.max_blocking_time.load(Ordering::SeqCst)
    }

    pub fn get_scan_interval(&self) -> u64 {
        self.scan_interval.load(Ordering::SeqCst)
    }

    pub fn get_scan_count(&self) -> u64 {
        self.scan_count.load(Ordering::SeqCst)
    }
}

#[derive(Debug)]
pub enum ConfigError {
    ReadonlyField,
    FieldNotFound,
    InvalidValue,
    Forbidden,
}

impl ToString for ConfigError {
    fn to_string(&self) -> String {
        match self {
            Self::ReadonlyField => "READONLY_FIELD".to_string(),
            Self::FieldNotFound => "FIELD_NOT_FOUND".to_string(),
            Self::InvalidValue => "INVALID_VALUE".to_string(),
            Self::Forbidden => "FORBIDDEN".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_set_field() {
        let mut migration_config = MigrationConfig::default();
        migration_config.set_field("scan_count", "233").unwrap();
        assert_eq!(migration_config.scan_count, 233);

        let mut cluster_config = ClusterConfig::default();
        cluster_config
            .set_field("migration_scan_count", "666")
            .unwrap();
        assert_eq!(cluster_config.migration_config.scan_count, 666);
    }
}
