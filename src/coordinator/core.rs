use super::broker::{MetaDataBrokerError, MetaManipulationBrokerError};
use common::cluster::{Host, MigrationTaskMeta, Node};
use futures::{future, Future, Stream};
use protocol::RedisClientError;
use std::error::Error;
use std::fmt;
use std::io;
use std::sync::Arc;

pub trait ProxiesRetriever: Sync + Send + 'static {
    fn retrieve_proxies(&self) -> Box<dyn Stream<Item = String, Error = CoordinateError> + Send>;
}

pub type NodeFailure = Node;

pub trait FailureChecker: Sync + Send + 'static {
    fn check(
        &self,
        address: String,
    ) -> Box<dyn Future<Item = Option<String>, Error = CoordinateError> + Send>;
}

pub trait FailureReporter: Sync + Send + 'static {
    fn report(&self, address: String)
        -> Box<dyn Future<Item = (), Error = CoordinateError> + Send>;
}

pub trait FailureDetector {
    type Retriever: ProxiesRetriever;
    type Checker: FailureChecker;
    type Reporter: FailureReporter;

    fn new(retriever: Self::Retriever, checker: Self::Checker, reporter: Self::Reporter) -> Self;
    fn run(&self) -> Box<dyn Stream<Item = (), Error = CoordinateError> + Send>;
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

    fn run(&self) -> Box<dyn Stream<Item = (), Error = CoordinateError> + Send> {
        let checker = self.checker.clone();
        let reporter = self.reporter.clone();
        Box::new(
            self.retriever
                .retrieve_proxies()
                .map(move |address| checker.check(address))
                .buffer_unordered(10)
                .filter_map(|a| a)
                .and_then(move |address| {
                    reporter.report(address).then(|res| {
                        if let Err(e) = res {
                            error!("failed to report failure: {:?}", e);
                        }
                        future::ok(())
                    })
                }),
        )
    }
}

pub trait ProxyFailureRetriever: Sync + Send + 'static {
    fn retrieve_proxy_failures(
        &self,
    ) -> Box<dyn Stream<Item = String, Error = CoordinateError> + Send>;
}

pub trait NodeFailureRetriever: Sync + Send + 'static {
    fn retrieve_node_failures(
        &self,
        failed_proxy_address: String,
    ) -> Box<dyn Stream<Item = NodeFailure, Error = CoordinateError> + Send>;
}

pub trait NodeFailureHandler: Sync + Send + 'static {
    fn handle_node_failure(
        &self,
        failure_node: NodeFailure,
    ) -> Box<dyn Future<Item = (), Error = CoordinateError> + Send>;
}

pub trait FailureHandler {
    type PFRetriever: ProxyFailureRetriever;
    type NFRetriever: NodeFailureRetriever;
    type Handler: NodeFailureHandler;

    fn new(
        proxy_failure_retriever: Self::PFRetriever,
        node_failure_retriever: Self::NFRetriever,
        handler: Self::Handler,
    ) -> Self;
    fn run(&self) -> Box<dyn Stream<Item = (), Error = CoordinateError> + Send>;
}

pub struct SeqFailureHandler<
    PFRetriever: ProxyFailureRetriever,
    NFRetriever: NodeFailureRetriever,
    Handler: NodeFailureHandler,
> {
    proxy_failure_retriever: PFRetriever,
    node_failure_retriever: Arc<NFRetriever>,
    handler: Arc<Handler>,
}

impl<P: ProxyFailureRetriever, N: NodeFailureRetriever, H: NodeFailureHandler> FailureHandler
    for SeqFailureHandler<P, N, H>
{
    type PFRetriever = P;
    type NFRetriever = N;
    type Handler = H;

    fn new(proxy_failure_retriever: P, node_failure_retriever: N, handler: H) -> Self {
        Self {
            proxy_failure_retriever,
            node_failure_retriever: Arc::new(node_failure_retriever),
            handler: Arc::new(handler),
        }
    }

    fn run(&self) -> Box<dyn Stream<Item = (), Error = CoordinateError> + Send> {
        let node_failure_retriever = self.node_failure_retriever.clone();
        let handler = self.handler.clone();
        Box::new(
            self.proxy_failure_retriever
                .retrieve_proxy_failures()
                .and_then(move |proxy_address| {
                    let cloned_handler = handler.clone();
                    node_failure_retriever
                        .retrieve_node_failures(proxy_address)
                        .for_each(move |node_failure| {
                            cloned_handler.handle_node_failure(node_failure)
                        })
                }),
        )
    }
}

#[derive(Clone, Debug)]
pub struct HostMeta {
    pub local: Host,
    pub peer: Host,
}

pub trait HostMetaSender: Sync + Send + 'static {
    fn send_meta(
        &self,
        host: HostMeta,
    ) -> Box<dyn Future<Item = (), Error = CoordinateError> + Send>;
}

pub trait HostMetaRetriever: Sync + Send + 'static {
    fn get_host_meta(
        &self,
        address: String,
    ) -> Box<dyn Future<Item = Option<HostMeta>, Error = CoordinateError> + Send>;
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
    fn run(&self) -> Box<dyn Stream<Item = (), Error = CoordinateError> + Send>;
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

    fn run(&self) -> Box<dyn Stream<Item = (), Error = CoordinateError> + Send> {
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
    ) -> Box<dyn Stream<Item = MigrationTaskMeta, Error = CoordinateError> + Send>;
}

pub trait MigrationCommitter: Sync + Send + 'static {
    fn commit(
        &self,
        meta: MigrationTaskMeta,
    ) -> Box<dyn Future<Item = (), Error = CoordinateError> + Send>;
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
    fn run(&self) -> Box<dyn Stream<Item = (), Error = CoordinateError> + Send>;
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
    fn set_db_meta(
        address: String,
        meta_retriever: Arc<MR>,
        sender: Arc<S>,
    ) -> impl Future<Item = (), Error = CoordinateError> + Send + 'static {
        meta_retriever.get_host_meta(address.clone()).and_then(move |host_opt| -> Box<dyn Future<Item=(), Error=CoordinateError> + Send + 'static> {
            let host = match host_opt {
                Some(host) => host,
                None => {
                    error!("host can't be found after committing migration {}", address);
                    return Box::new(future::ok(()))
                }
            };
            info!("sending meta after committing migration {}", address);
            sender.send_meta(host)
        })
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

    fn run(&self) -> Box<dyn Stream<Item = (), Error = CoordinateError> + Send> {
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

    fn cause(&self) -> Option<&Error> {
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
        ) -> Box<dyn Future<Item = Option<String>, Error = CoordinateError> + Send> {
            Box::new(future::ok(None))
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
