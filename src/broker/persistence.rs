use super::store::MetaStore;
use chrono::Utc;
use futures::Future;
use std::error::Error;
use std::fmt;
use std::io;
use std::pin::Pin;
use std::str;
use std::sync::{Arc, RwLock};
use tokio::fs::{rename, File};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;

pub trait MetaStorage {
    fn store<'s>(
        &'s self,
        store: Arc<RwLock<MetaStore>>,
    ) -> Pin<Box<dyn Future<Output = Result<(), MetaSyncError>> + Send + 's>>;
    fn load<'s>(
        &'s self,
    ) -> Pin<Box<dyn Future<Output = Result<MetaStore, MetaSyncError>> + Send + 's>>;
}

pub struct JsonFileStorage {
    json_file: JsonFile,
    lock: Mutex<()>,
}

impl JsonFileStorage {
    pub fn new(filename: String) -> Self {
        Self {
            json_file: JsonFile::new(filename),
            lock: Mutex::new(()),
        }
    }

    async fn store_impl(&self, store: Arc<RwLock<MetaStore>>) -> Result<(), MetaSyncError> {
        let _guard = self.lock.lock().await;
        self.json_file.store(store).await
    }

    async fn load_impl(&self) -> Result<MetaStore, MetaSyncError> {
        let _guard = self.lock.lock().await;
        self.json_file.load().await
    }
}

impl MetaStorage for JsonFileStorage {
    fn store<'s>(
        &'s self,
        store: Arc<RwLock<MetaStore>>,
    ) -> Pin<Box<dyn Future<Output = Result<(), MetaSyncError>> + Send + 's>> {
        Box::pin(self.store_impl(store))
    }

    fn load<'s>(
        &'s self,
    ) -> Pin<Box<dyn Future<Output = Result<MetaStore, MetaSyncError>> + Send + 's>> {
        Box::pin(self.load_impl())
    }
}

struct JsonFile {
    filename: String,
}

impl JsonFile {
    fn new(filename: String) -> Self {
        Self { filename }
    }

    async fn store(&self, store: Arc<RwLock<MetaStore>>) -> Result<(), MetaSyncError> {
        let json_str = {
            let store = store.read().map_err(|_| MetaSyncError::Lock)?;

            serde_json::to_string(&(*store)).map_err(|err| {
                error!("failed to convert MetaStore to json {}", err);
                MetaSyncError::Json
            })?
        };

        let data = json_str.into_bytes();

        let now = Utc::now().timestamp_nanos();
        let tmp_filename = format!("{}-{}", self.filename, now);
        let mut tmp_file = File::create(tmp_filename.as_str())
            .await
            .map_err(MetaSyncError::Io)?;
        tmp_file
            .write_all(data.as_slice())
            .await
            .map_err(MetaSyncError::Io)?;

        rename(tmp_filename.as_str(), self.filename.as_str())
            .await
            .map_err(MetaSyncError::Io)?;
        Ok(())
    }

    async fn load(&self) -> Result<MetaStore, MetaSyncError> {
        let mut file = File::open(self.filename.as_str())
            .await
            .map_err(MetaSyncError::Io)?;
        let mut contents = vec![];
        file.read_to_end(&mut contents)
            .await
            .map_err(MetaSyncError::Io)?;

        let json_str = str::from_utf8(&contents).map_err(|err| {
            error!("invalid json utf8 data {}", err);
            MetaSyncError::Json
        })?;

        let store = serde_json::from_str(json_str).map_err(|err| {
            error!("invalid json data {}", err);
            MetaSyncError::Json
        })?;

        Ok(store)
    }
}

#[derive(Debug)]
pub enum MetaSyncError {
    Io(io::Error),
    Json,
    Lock,
}

impl MetaSyncError {
    pub fn to_code(&self) -> &str {
        match self {
            Self::Io(_) => "PERSISTENCE_IO_ERROR",
            Self::Json => "PERSISTENCE_JSON_ERROR",
            Self::Lock => "PERSISTENCE_LOCK_ERROR",
        }
    }
}

impl fmt::Display for MetaSyncError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.to_code())
    }
}

impl Error for MetaSyncError {
    fn cause(&self) -> Option<&dyn Error> {
        match self {
            Self::Io(io_error) => Some(io_error),
            _ => None,
        }
    }
}

impl PartialEq for MetaSyncError {
    fn eq(&self, other: &Self) -> bool {
        self.to_code() == other.to_code()
    }
}
