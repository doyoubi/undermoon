use super::store::MetaStore;
use chrono::Utc;
use futures::Future;
use std::error::Error;
use std::fmt;
use std::io;
use std::pin::Pin;
use std::str;
use tokio::fs::{rename, File};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;

pub trait MetaStorage {
    fn store<'a>(
        &'static self,
        store: &'a MetaStore,
    ) -> Pin<Box<dyn Future<Output = Result<(), PersistenceError>> + Send + 'a>>;
    fn load(
        &'static self,
    ) -> Pin<Box<dyn Future<Output = Result<MetaStore, PersistenceError>> + Send + 'static>>;
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

    async fn store_impl(&self, store: &MetaStore) -> Result<(), PersistenceError> {
        let _guard = self.lock.lock().await;
        self.json_file.store(store).await
    }

    async fn load_impl(&self) -> Result<MetaStore, PersistenceError> {
        let _guard = self.lock.lock().await;
        self.json_file.load().await
    }
}

impl MetaStorage for JsonFileStorage {
    fn store<'a>(
        &'static self,
        store: &'a MetaStore,
    ) -> Pin<Box<dyn Future<Output = Result<(), PersistenceError>> + Send + 'a>> {
        Box::pin(self.store_impl(store))
    }

    fn load(
        &'static self,
    ) -> Pin<Box<dyn Future<Output = Result<MetaStore, PersistenceError>> + Send + 'static>> {
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

    async fn store(&self, store: &MetaStore) -> Result<(), PersistenceError> {
        let json_str = serde_json::to_string(store).map_err(|err| {
            error!("failed to convert MetaStore to json {}", err);
            PersistenceError::Json
        })?;

        let data = json_str.into_bytes();

        let now = Utc::now().timestamp_nanos();
        let tmp_filename = format!("{}-{}", self.filename, now);
        let mut tmp_file = File::create(tmp_filename.as_str())
            .await
            .map_err(PersistenceError::Io)?;
        tmp_file
            .write_all(data.as_slice())
            .await
            .map_err(PersistenceError::Io)?;

        rename(tmp_filename.as_str(), self.filename.as_str())
            .await
            .map_err(PersistenceError::Io)?;
        Ok(())
    }

    async fn load(&self) -> Result<MetaStore, PersistenceError> {
        let mut file = File::open(self.filename.as_str())
            .await
            .map_err(PersistenceError::Io)?;
        let mut contents = vec![];
        file.read_to_end(&mut contents)
            .await
            .map_err(PersistenceError::Io)?;

        let json_str = str::from_utf8(&contents).map_err(|err| {
            error!("invalid json utf8 data {}", err);
            PersistenceError::Json
        })?;

        let store = serde_json::from_str(json_str).map_err(|err| {
            error!("invalid json data {}", err);
            PersistenceError::Json
        })?;

        Ok(store)
    }
}

#[derive(Debug)]
pub enum PersistenceError {
    Io(io::Error),
    Json,
}

impl PersistenceError {
    pub fn to_code(&self) -> &str {
        match self {
            Self::Io(_) => "PERSISTENCE_IO_ERROR",
            Self::Json => "PERSISTENCE_JSON_ERROR",
        }
    }
}

impl fmt::Display for PersistenceError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.to_code())
    }
}

impl Error for PersistenceError {
    fn cause(&self) -> Option<&dyn Error> {
        match self {
            Self::Io(io_error) => Some(io_error),
            _ => None,
        }
    }
}
