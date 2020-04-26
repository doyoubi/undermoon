use super::broker::{MetaDataBrokerError, MetaManipulationBrokerError};
use crate::common::cluster::{MigrationTaskMeta, Proxy};
use crate::protocol::RedisClientError;
use futures::{future, stream, Future, FutureExt, Stream, StreamExt, TryFutureExt};
use futures_batch::ChunksTimeoutStreamExt;
use mockall::automock;
use std::error::Error;
use std::fmt;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

pub trait ProxiesRetriever: Sync + Send + 'static {
    fn retrieve_proxies<'s>(
        &'s self,
    ) -> Pin<Box<dyn Stream<Item = Result<String, CoordinateError>> + Send + 's>>;
}

pub type ProxyFailure = String; // proxy address

pub trait FailureChecker: Sync + Send + 'static {
    fn check<'s>(
        &'s self,
        address: String,
    ) -> Pin<Box<dyn Future<Output = Result<Option<String>, CoordinateError>> + Send + 's>>;
}

pub trait FailureReporter: Sync + Send + 'static {
    fn report<'s>(
        &'s self,
        address: String,
    ) -> Pin<Box<dyn Future<Output = Result<(), CoordinateError>> + Send + 's>>;
}

pub trait FailureDetector {
    type Retriever: ProxiesRetriever;
    type Checker: FailureChecker;
    type Reporter: FailureReporter;

    fn new(retriever: Self::Retriever, checker: Self::Checker, reporter: Self::Reporter) -> Self;
    fn run<'s>(&'s self) -> Pin<Box<dyn Future<Output = Result<(), CoordinateError>> + Send + 's>>;
}

pub struct ParFailureDetector<
    Retriever: ProxiesRetriever,
    Checker: FailureChecker,
    Reporter: FailureReporter,
> {
    retriever: Retriever,
    checker: Arc<Checker>,
    reporter: Arc<Reporter>,
}

impl<T: ProxiesRetriever, C: FailureChecker, P: FailureReporter> ParFailureDetector<T, C, P> {
    async fn check_and_report(
        checker: &C,
        reporter: &P,
        address: String,
    ) -> Result<(), CoordinateError> {
        let address = match checker.check(address).await? {
            Some(addr) => addr,
            None => return Ok(()),
        };
        if let Err(err) = reporter.report(address).await {
            error!("failed to report failure: {:?}", err);
            return Err(err);
        }
        Ok(())
    }

    async fn run_impl(&self) -> Result<(), CoordinateError> {
        let checker = self.checker.clone();
        let reporter = self.reporter.clone();
        const BATCH_SIZE: usize = 30;
        const BATCH_TIME: Duration = Duration::from_millis(1);

        let mut res = Ok(());
        let mut s = self
            .retriever
            .retrieve_proxies()
            .chunks_timeout(BATCH_SIZE, BATCH_TIME);

        while let Some(results) = s.next().await {
            let mut proxies = vec![];
            for r in results {
                match r {
                    Ok(proxy) => proxies.push(proxy),
                    Err(err) => {
                        error!("failed to get proxy: {:?}", err);
                        res = Err(err);
                    }
                }
            }
            let futs: Vec<_> = proxies
                .into_iter()
                .map(|address| Self::check_and_report(&checker, &reporter, address))
                .collect();
            let results = future::join_all(futs).await;
            for r in results.into_iter() {
                if let Err(err) = r {
                    error!("faild to check and report error: {:?}", err);
                    res = Err(err);
                }
            }
        }
        res
    }
}

impl<T: ProxiesRetriever, C: FailureChecker, P: FailureReporter> FailureDetector
    for ParFailureDetector<T, C, P>
{
    type Retriever = T;
    type Checker = C;
    type Reporter = P;

    fn new(retriever: T, checker: C, reporter: P) -> Self {
        Self {
            retriever,
            checker: Arc::new(checker),
            reporter: Arc::new(reporter),
        }
    }

    fn run<'s>(&'s self) -> Pin<Box<dyn Future<Output = Result<(), CoordinateError>> + Send + 's>> {
        Box::pin(self.run_impl())
    }
}

pub trait ProxyFailureRetriever: Sync + Send + 'static {
    fn retrieve_proxy_failures<'s>(
        &'s self,
    ) -> Pin<Box<dyn Stream<Item = Result<String, CoordinateError>> + Send + 's>>;
}

pub trait ProxyFailureHandler: Sync + Send + 'static {
    fn handle_proxy_failure<'s>(
        &'s self,
        proxy_failure: ProxyFailure,
    ) -> Pin<Box<dyn Future<Output = Result<(), CoordinateError>> + Send + 's>>;
}

pub trait FailureHandler {
    type PFRetriever: ProxyFailureRetriever;
    type Handler: ProxyFailureHandler;

    fn new(proxy_failure_retriever: Self::PFRetriever, handler: Self::Handler) -> Self;
    fn run<'s>(&'s self) -> Pin<Box<dyn Stream<Item = Result<(), CoordinateError>> + Send + 's>>;
}

pub struct ParFailureHandler<PFRetriever: ProxyFailureRetriever, Handler: ProxyFailureHandler> {
    proxy_failure_retriever: PFRetriever,
    handler: Arc<Handler>,
}

impl<P: ProxyFailureRetriever, H: ProxyFailureHandler> ParFailureHandler<P, H> {
    async fn run_impl(&self) -> Result<(), CoordinateError> {
        let handler = self.handler.clone();
        const BATCH_SIZE: usize = 10;
        const BATCH_TIME: Duration = Duration::from_millis(1);

        let mut res = Ok(());

        let mut s = self
            .proxy_failure_retriever
            .retrieve_proxy_failures()
            .chunks_timeout(BATCH_SIZE, BATCH_TIME);
        while let Some(results) = s.next().await {
            let mut proxies = vec![];
            for r in results {
                match r {
                    Ok(proxy) => proxies.push(proxy),
                    Err(err) => {
                        error!("failed to get proxy: {:?}", err);
                        res = Err(err);
                    }
                }
            }
            let futs: Vec<_> = proxies
                .into_iter()
                .map(|proxy_address| {
                    handler
                        .handle_proxy_failure(proxy_address.clone())
                        .or_else(move |err| {
                            error!("Failed to handler proxy failre {} {:?}", proxy_address, err);
                            future::ok(())
                        })
                })
                .collect();
            let results = future::join_all(futs).await;
            for r in results.into_iter() {
                if let Err(err) = r {
                    error!("faild to check and report error: {:?}", err);
                    res = Err(err);
                }
            }
        }
        res
    }
}

impl<P: ProxyFailureRetriever, H: ProxyFailureHandler> FailureHandler for ParFailureHandler<P, H> {
    type PFRetriever = P;
    type Handler = H;

    fn new(proxy_failure_retriever: P, handler: H) -> Self {
        Self {
            proxy_failure_retriever,
            handler: Arc::new(handler),
        }
    }

    fn run<'s>(&'s self) -> Pin<Box<dyn Stream<Item = Result<(), CoordinateError>> + Send + 's>> {
        Box::pin(
            self.run_impl()
                .map(|res| stream::iter(vec![res]))
                .flatten_stream(),
        )
    }
}

// Clippy accidentally thinks [automock] is an index expression.
#[allow(clippy::indexing_slicing)]
mod trait_mod {
    use super::*;

    #[automock]
    pub trait ProxyMetaSender: Sync + Send + 'static {
        fn send_meta<'s>(
            &'s self,
            proxy: Proxy,
        ) -> Pin<Box<dyn Future<Output = Result<(), CoordinateError>> + Send + 's>>;
    }
}

pub use self::trait_mod::{MockProxyMetaSender, ProxyMetaSender};

pub trait ProxyMetaRetriever: Sync + Send + 'static {
    fn get_proxy_meta<'s>(
        &'s self,
        address: String,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Proxy>, CoordinateError>> + Send + 's>>;
}

pub trait ProxyMetaSynchronizer {
    type PRetriever: ProxiesRetriever;
    type MRetriever: ProxyMetaRetriever;
    type Sender: ProxyMetaSender;

    fn new(
        proxy_retriever: Self::PRetriever,
        meta_retriever: Self::MRetriever,
        sender: Self::Sender,
    ) -> Self;
    fn run<'s>(&'s self) -> Pin<Box<dyn Stream<Item = Result<(), CoordinateError>> + Send + 's>>;
}

pub struct ProxyMetaRespSynchronizer<
    PRetriever: ProxiesRetriever,
    MRetriever: ProxyMetaRetriever,
    Sender: ProxyMetaSender,
> {
    proxy_retriever: PRetriever,
    meta_retriever: Arc<MRetriever>,
    sender: Arc<Sender>,
}

impl<P: ProxiesRetriever, M: ProxyMetaRetriever, S: ProxyMetaSender>
    ProxyMetaRespSynchronizer<P, M, S>
{
    async fn retrieve_and_send_meta(
        meta_retriever: &M,
        sender: &S,
        address: String,
    ) -> Result<(), CoordinateError> {
        let proxy_opt = meta_retriever.get_proxy_meta(address.clone()).await?;
        let proxy = match proxy_opt {
            Some(proxy) => proxy,
            None => return Ok(()),
        };
        if let Err(err) = sender.send_meta(proxy).await {
            error!("failed to set meta: {} {:?}", address, err);
            return Err(err);
        }
        Ok(())
    }

    async fn run_impl(&self) -> Result<(), CoordinateError> {
        let meta_retriever = self.meta_retriever.clone();
        let sender = self.sender.clone();
        const BATCH_SIZE: usize = 10;
        const BATCH_TIME: Duration = Duration::from_millis(1);

        let mut res = Ok(());
        let mut s = self
            .proxy_retriever
            .retrieve_proxies()
            .chunks_timeout(BATCH_SIZE, BATCH_TIME);
        while let Some(results) = s.next().await {
            let mut proxies = vec![];
            for r in results {
                match r {
                    Ok(proxy) => proxies.push(proxy),
                    Err(err) => {
                        error!("failed to get proxy: {:?}", err);
                        res = Err(err);
                    }
                }
            }
            let futs: Vec<_> = proxies
                .into_iter()
                .map(|address| Self::retrieve_and_send_meta(&meta_retriever, &sender, address))
                .collect();
            let results = future::join_all(futs).await;
            for r in results.into_iter() {
                if let Err(err) = r {
                    error!("failed to retrieve and send meta, error: {:?}", err);
                    res = Err(err);
                }
            }
        }
        res
    }
}

impl<P: ProxiesRetriever, M: ProxyMetaRetriever, S: ProxyMetaSender> ProxyMetaSynchronizer
    for ProxyMetaRespSynchronizer<P, M, S>
{
    type PRetriever = P;
    type MRetriever = M;
    type Sender = S;

    fn new(
        proxy_retriever: Self::PRetriever,
        meta_retriever: Self::MRetriever,
        sender: Self::Sender,
    ) -> Self {
        Self {
            proxy_retriever,
            meta_retriever: Arc::new(meta_retriever),
            sender: Arc::new(sender),
        }
    }

    fn run<'s>(&'s self) -> Pin<Box<dyn Stream<Item = Result<(), CoordinateError>> + Send + 's>> {
        Box::pin(
            self.run_impl()
                .map(|res| stream::iter(vec![res]))
                .flatten_stream(),
        )
    }
}

pub trait MigrationStateChecker: Sync + Send + 'static {
    fn check<'s>(
        &'s self,
        address: String,
    ) -> Pin<Box<dyn Stream<Item = Result<MigrationTaskMeta, CoordinateError>> + Send + 's>>;
}

pub trait MigrationCommitter: Sync + Send + 'static {
    fn commit<'s>(
        &'s self,
        meta: MigrationTaskMeta,
    ) -> Pin<Box<dyn Future<Output = Result<(), CoordinateError>> + Send + 's>>;
}

pub trait MigrationStateSynchronizer: Sync + Send + 'static {
    type PRetriever: ProxiesRetriever;
    type Checker: MigrationStateChecker;
    type Committer: MigrationCommitter;
    type MRetriever: ProxyMetaRetriever;
    type Sender: ProxyMetaSender;

    fn new(
        proxy_retriever: Self::PRetriever,
        checker: Self::Checker,
        committer: Self::Committer,
        meta_retriever: Self::MRetriever,
        sender: Self::Sender,
    ) -> Self;
    fn run<'s>(&'s self) -> Pin<Box<dyn Stream<Item = Result<(), CoordinateError>> + Send + 's>>;
}

pub struct ParMigrationStateSynchronizer<
    PR: ProxiesRetriever,
    SC: MigrationStateChecker,
    MC: MigrationCommitter,
    MR: ProxyMetaRetriever,
    S: ProxyMetaSender,
> {
    proxy_retriever: PR,
    checker: Arc<SC>,
    committer: Arc<MC>,
    meta_retriever: Arc<MR>,
    sender: Arc<S>,
}

impl<
        PR: ProxiesRetriever,
        SC: MigrationStateChecker,
        MC: MigrationCommitter,
        MR: ProxyMetaRetriever,
        S: ProxyMetaSender,
    > ParMigrationStateSynchronizer<PR, SC, MC, MR, S>
{
    async fn set_cluster_meta(
        address: String,
        meta_retriever: &MR,
        sender: &S,
    ) -> Result<(), CoordinateError> {
        let proxy_opt = meta_retriever.get_proxy_meta(address.clone()).await?;
        let proxy = match proxy_opt {
            Some(proxy) => proxy,
            None => {
                error!(
                    "proxy can't be found after committing migration {}",
                    address
                );
                return Ok(());
            }
        };
        info!("sending meta after committing migration {}", address);
        sender.send_meta(proxy).await
    }

    async fn sync_migration_state(
        commiter: &MC,
        meta_retriever: &MR,
        sender: &S,
        meta: MigrationTaskMeta,
    ) -> Result<(), CoordinateError> {
        let (src_address, dst_address) = match meta.slot_range.tag.get_migration_meta() {
            Some(migration_meta) => (
                migration_meta.src_proxy_address.clone(),
                migration_meta.dst_proxy_address.clone(),
            ),
            None => {
                error!("invalid migration task meta {:?}, skip it.", meta);
                return Ok(());
            }
        };

        if let Err(err) = commiter.commit(meta).await {
            error!("failed to commit migration state: {:?}", err);
            return Err(err);
        }

        // Send to dst first to make sure the slots will always have owner.
        Self::set_cluster_meta(dst_address, meta_retriever, sender).await?;
        Self::set_cluster_meta(src_address, meta_retriever, sender).await?;

        Ok(())
    }

    async fn check_and_sync(
        checker: &SC,
        committer: &MC,
        meta_retriever: &MR,
        sender: &S,
        address: String,
    ) -> Result<(), CoordinateError> {
        let mut s = checker.check(address.clone());
        while let Some(res) = s.next().await {
            let task_meta = match res {
                Ok(meta) => meta,
                Err(err) => return Err(err),
            };
            Self::sync_migration_state(committer, meta_retriever, sender, task_meta).await?;
        }
        Ok(())
    }

    async fn run_impl(&self) -> Result<(), CoordinateError> {
        let checker = self.checker.clone();
        let committer = self.committer.clone();
        let meta_retriever = self.meta_retriever.clone();
        let sender = self.sender.clone();

        const CHUNK_SIZE: usize = 10;
        const BATCH_SIZE: Duration = Duration::from_millis(1);

        let mut res = Ok(());
        let mut s = self
            .proxy_retriever
            .retrieve_proxies()
            .chunks_timeout(CHUNK_SIZE, BATCH_SIZE);
        while let Some(results) = s.next().await {
            let mut proxies = vec![];
            for r in results {
                match r {
                    Ok(proxy) => proxies.push(proxy),
                    Err(err) => {
                        error!("failed to get proxy: {:?}", err);
                        res = Err(err);
                    }
                }
            }
            let futs: Vec<_> = proxies
                .into_iter()
                .map(|address| {
                    Self::check_and_sync(&checker, &committer, &meta_retriever, &sender, address)
                })
                .collect();
            let results = future::join_all(futs).await;
            for r in results.into_iter() {
                if let Err(err) = r {
                    error!("faild to sync migration state, error: {:?}", err);
                    res = Err(err);
                }
            }
        }
        res
    }
}

impl<
        PR: ProxiesRetriever,
        SC: MigrationStateChecker,
        MC: MigrationCommitter,
        MR: ProxyMetaRetriever,
        S: ProxyMetaSender,
    > MigrationStateSynchronizer for ParMigrationStateSynchronizer<PR, SC, MC, MR, S>
{
    type PRetriever = PR;
    type Checker = SC;
    type Committer = MC;
    type MRetriever = MR;
    type Sender = S;

    fn new(
        proxy_retriever: Self::PRetriever,
        checker: Self::Checker,
        committer: Self::Committer,
        meta_retriever: Self::MRetriever,
        sender: Self::Sender,
    ) -> Self {
        Self {
            proxy_retriever,
            checker: Arc::new(checker),
            committer: Arc::new(committer),
            meta_retriever: Arc::new(meta_retriever),
            sender: Arc::new(sender),
        }
    }

    fn run<'s>(&'s self) -> Pin<Box<dyn Stream<Item = Result<(), CoordinateError>> + Send + 's>> {
        Box::pin(
            self.run_impl()
                .map(|res| stream::iter(vec![res]))
                .flatten_stream(),
        )
    }
}

#[derive(Debug)]
pub enum CoordinateError {
    Io(io::Error),
    MetaMani(MetaManipulationBrokerError),
    MetaData(MetaDataBrokerError),
    Redis(RedisClientError),
    InvalidReply,
    InvalidAddress,
    InvalidConfig,
}

impl fmt::Display for CoordinateError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for CoordinateError {
    fn description(&self) -> &str {
        "coordinate error"
    }

    fn cause(&self) -> Option<&dyn Error> {
        match self {
            CoordinateError::Io(err) => Some(err),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio;

    struct DummyChecker {}

    impl FailureChecker for DummyChecker {
        fn check(
            &self,
            _address: String,
        ) -> Pin<Box<dyn Future<Output = Result<Option<String>, CoordinateError>> + Send>> {
            Box::pin(future::ok(None))
        }
    }

    async fn check<C: FailureChecker>(checker: C) {
        let res = checker.check("".to_string()).await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_reporter() {
        let checker = DummyChecker {};
        check(checker).await;
    }
}
