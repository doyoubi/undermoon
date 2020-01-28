use super::broker::{MetaDataBrokerError, MetaManipulationBrokerError};
use crate::common::cluster::{Host, MigrationTaskMeta};
use crate::protocol::RedisClientError;
use futures::{future, Future, TryFutureExt, Stream, StreamExt};
use std::error::Error;
use std::fmt;
use std::io;
use std::sync::Arc;
use std::pin::Pin;

pub trait ProxiesRetriever: Sync + Send + 'static {
    fn retrieve_proxies(&self) -> Pin<Box<dyn Stream<Item = Result<String, CoordinateError>> + Send>>;
}

pub type ProxyFailure = String; // proxy address

pub trait FailureChecker: Sync + Send + 'static {
    fn check(
        &self,
        address: String,
    ) -> Pin<Box<dyn Future<Output = Result<Option<String>, CoordinateError>> + Send>>;
}

pub trait FailureReporter: Sync + Send + 'static {
    fn report(&self, address: String)
        -> Pin<Box<dyn Future<Output = Result<(), CoordinateError>> + Send>>;
}

pub trait FailureDetector {
    type Retriever: ProxiesRetriever;
    type Checker: FailureChecker;
    type Reporter: FailureReporter;

    fn new(retriever: Self::Retriever, checker: Self::Checker, reporter: Self::Reporter) -> Self;
    fn run(&self) -> Pin<Box<dyn Future<Output = Result<(), CoordinateError>> + Send>>;
}

pub struct SeqFailureDetector<
    Retriever: ProxiesRetriever,
    Checker: FailureChecker,
    Reporter: FailureReporter,
> {
    retriever: Retriever,
    checker: Arc<Checker>,
    reporter: Arc<Reporter>,
}

impl<T: ProxiesRetriever, C: FailureChecker, P: FailureReporter> SeqFailureDetector<T, C, P>
{
    async fn check_and_report(checker: &C, reporter: &P, address: String) -> Result<(), CoordinateError> {
        let address = match checker.check(address).await? {
            Some(addr) => addr,
            None => return Ok(()),
        };
        if let Err(err) = reporter.report(address).await {
            error!("failed to report failure: {:?}", err);
            return Err(err)
        }
        Ok(())
    }

    async fn run_impl(&self) -> Result<(), CoordinateError> {
        let checker = self.checker.clone();
        let reporter = self.reporter.clone();
        const BATCH_SIZE: usize = 30;

        let mut res = Ok(());

        for results in self.retriever.retrieve_proxies().chunks(BATCH_SIZE).next().await {
            let mut proxies = vec![];
            for r in results {
                match r {
                    Ok(proxy) => proxies.push(proxy),
                    Err(err) => {
                        error!("failed to get proxy: {:?}", err);
                        res = Err(err);
                    },
                }
            }
            let futs: Vec<_> = proxies.into_iter().map(|address| Self::check_and_report(&checker, &reporter, address)).collect();
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
    for SeqFailureDetector<T, C, P>
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

    fn run(&self) -> Pin<Box<dyn Future<Output = Result<(), CoordinateError>> + Send>> {
        Box::pin(self.run_impl())
    }
}

pub trait ProxyFailureRetriever: Sync + Send + 'static {
    fn retrieve_proxy_failures(
        &self,
    ) -> Pin<Box<dyn Stream<Item = Result<String, CoordinateError>> + Send>>;
}

pub trait ProxyFailureHandler: Sync + Send + 'static {
    fn handle_proxy_failure(
        &self,
        proxy_failure: ProxyFailure,
    ) -> Pin<Box<dyn Future<Output = Result<(), CoordinateError>> + Send>>;
}

pub trait FailureHandler {
    type PFRetriever: ProxyFailureRetriever;
    type Handler: ProxyFailureHandler;

    fn new(proxy_failure_retriever: Self::PFRetriever, handler: Self::Handler) -> Self;
    fn run(&self) -> Pin<Box<dyn Stream<Item = Result<(), CoordinateError>> + Send>>;
}

pub struct SeqFailureHandler<PFRetriever: ProxyFailureRetriever, Handler: ProxyFailureHandler> {
    proxy_failure_retriever: PFRetriever,
    handler: Arc<Handler>,
}

impl<P: ProxyFailureRetriever, H: ProxyFailureHandler> FailureHandler for SeqFailureHandler<P, H> {
    type PFRetriever = P;
    type Handler = H;

    fn new(proxy_failure_retriever: P, handler: H) -> Self {
        Self {
            proxy_failure_retriever,
            handler: Arc::new(handler),
        }
    }

    fn run(&self) -> Pin<Box<dyn Stream<Item = Result<(), CoordinateError>> + Send>> {
        let handler = self.handler.clone();
        Box::new(
            self.proxy_failure_retriever
                .retrieve_proxy_failures()
                .and_then(move |proxy_address| {
                    let proxy_address_clone = proxy_address.clone();
                    handler
                        .handle_proxy_failure(proxy_address_clone)
                        .or_else(move |err| {
                            error!("Failed to handler proxy failre {} {:?}", proxy_address, err);
                            future::ok(())
                        })
                }),
        )
    }
}

pub trait HostMetaSender: Sync + Send + 'static {
    fn send_meta(&self, host: Host) -> Pin<Box<dyn Future<Output = Result<(), CoordinateError>> + Send>>;
}

pub trait HostMetaRetriever: Sync + Send + 'static {
    fn get_host_meta(
        &self,
        address: String,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Host>, CoordinateError>> + Send>>;
}

pub trait HostMetaSynchronizer {
    type PRetriever: ProxiesRetriever;
    type MRetriever: HostMetaRetriever;
    type Sender: HostMetaSender;

    fn new(
        proxy_retriever: Self::PRetriever,
        meta_retriever: Self::MRetriever,
        sender: Self::Sender,
    ) -> Self;
    fn run(&self) -> Pin<Box<dyn Stream<Item = Result<(), CoordinateError>> + Send>>;
}

pub struct HostMetaRespSynchronizer<
    PRetriever: ProxiesRetriever,
    MRetriever: HostMetaRetriever,
    Sender: HostMetaSender,
> {
    proxy_retriever: PRetriever,
    meta_retriever: Arc<MRetriever>,
    sender: Arc<Sender>,
}

impl<P: ProxiesRetriever, M: HostMetaRetriever, S: HostMetaSender> HostMetaSynchronizer
    for HostMetaRespSynchronizer<P, M, S>
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

    fn run(&self) -> Pin<Box<dyn Stream<Item = Result<(), CoordinateError>> + Send>> {
        let meta_retriever = self.meta_retriever.clone();
        let sender = self.sender.clone();
        Box::new(
            self.proxy_retriever
                .retrieve_proxies()
                .map(move |address| meta_retriever.get_host_meta(address))
                .buffer_unordered(10)
                .filter_map(|a| a)
                .and_then(move |host| {
                    sender.send_meta(host).then(|res| {
                        if let Err(e) = res {
                            error!("failed to set meta: {:?}", e);
                        }
                        future::ok(())
                    })
                }),
        )
    }
}

pub trait MigrationStateChecker: Sync + Send + 'static {
    fn check(
        &self,
        address: String,
    ) -> Pin<Box<dyn Stream<Item = Result<MigrationTaskMeta, CoordinateError>> + Send>>;
}

pub trait MigrationCommitter: Sync + Send + 'static {
    fn commit(
        &self,
        meta: MigrationTaskMeta,
    ) -> Pin<Box<dyn Future<Output = Result<(), CoordinateError>> + Send>>;
}

pub trait MigrationStateSynchronizer: Sync + Send + 'static {
    type PRetriever: ProxiesRetriever;
    type Checker: MigrationStateChecker;
    type Committer: MigrationCommitter;
    type MRetriever: HostMetaRetriever;
    type Sender: HostMetaSender;

    fn new(
        proxy_retriever: Self::PRetriever,
        checker: Self::Checker,
        committer: Self::Committer,
        meta_retriever: Self::MRetriever,
        sender: Self::Sender,
    ) -> Self;
    fn run(&self) -> Pin<Box<dyn Stream<Item = Result<(), CoordinateError>> + Send>>;
}

pub struct SeqMigrationStateSynchronizer<
    PR: ProxiesRetriever,
    SC: MigrationStateChecker,
    MC: MigrationCommitter,
    MR: HostMetaRetriever,
    S: HostMetaSender,
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
        MR: HostMetaRetriever,
        S: HostMetaSender,
    > SeqMigrationStateSynchronizer<PR, SC, MC, MR, S>
{
    async fn set_db_meta(
        address: String,
        meta_retriever: Arc<MR>,
        sender: Arc<S>,
    ) -> Result<(), CoordinateError> {
        let host_opt = meta_retriever.get_host_meta(address.clone()).await?;
        let host = match host_opt {
            Some(host) => host,
            None => {
                error!("host can't be found after committing migration {}", address);
                return Ok(())
            }
        };
        info!("sending meta after committing migration {}", address);
        sender.send_meta(host).await
    }
}

impl<
        PR: ProxiesRetriever,
        SC: MigrationStateChecker,
        MC: MigrationCommitter,
        MR: HostMetaRetriever,
        S: HostMetaSender,
    > MigrationStateSynchronizer for SeqMigrationStateSynchronizer<PR, SC, MC, MR, S>
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

    fn run(&self) -> Pin<Box<dyn Stream<Item = Result<(), CoordinateError>> + Send>> {
        let checker = self.checker.clone();
        let committer = self.committer.clone();
        let meta_retriever = self.meta_retriever.clone();
        let sender = self.sender.clone();
        Box::new(
            self.proxy_retriever
                .retrieve_proxies()
                .map(move |address| checker.check(address))
                .flatten()
                .and_then(
                    move |meta| -> Box<
                        dyn Future<Item = (), Error = CoordinateError> + Send + 'static,
                    > {
                        let (src_address, dst_address) =
                            match meta.slot_range.tag.get_migration_meta() {
                                Some(migration_meta) => (
                                    migration_meta.src_proxy_address.clone(),
                                    migration_meta.dst_proxy_address.clone(),
                                ),
                                None => {
                                    error!("invalid migration task meta {:?}, skip it.", meta);
                                    return Box::new(future::ok(()));
                                }
                            };

                        let meta_retriever_clone1 = meta_retriever.clone();
                        let sender_clone1 = sender.clone();
                        let meta_retriever_clone2 = meta_retriever.clone();
                        let sender_clone2 = sender.clone();
                        Box::new(
                            committer
                                .commit(meta)
                                .map_err(|e| {
                                    error!("failed to commit migration state: {:?}", e);
                                    e
                                })
                                .and_then(move |()| {
                                    // Send to dst first to make sure the slots will always have owner.
                                    Self::set_db_meta(
                                        dst_address,
                                        meta_retriever_clone1.clone(),
                                        sender_clone1.clone(),
                                    )
                                })
                                .and_then(move |()| {
                                    Self::set_db_meta(
                                        src_address,
                                        meta_retriever_clone2.clone(),
                                        sender_clone2.clone(),
                                    )
                                }),
                        )
                    },
                ),
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

    struct DummyChecker {}

    impl FailureChecker for DummyChecker {
        fn check(
            &self,
            _address: String,
        ) -> Pin<Box<dyn Future<Output = Result<Option<String>, CoordinateError>> + Send>> {
            Box::pin(future::ok(None))
        }
    }

    fn check<C: FailureChecker>(checker: C) {
        checker
            .check("".to_string())
            .wait()
            .expect("test_failure_checker");
    }

    #[test]
    fn test_reporter() {
        let checker = DummyChecker {};
        check(checker);
    }
}
