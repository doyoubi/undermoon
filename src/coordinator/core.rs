use std::io;
use std::fmt;
use std::error::Error;
use std::sync::Arc;
use futures::{future, Future, Stream};
use ::common::cluster::{Host, Node};
use super::cluster::FullMetaData;
use super::broker::{ElectionBrokerError, MetaDataBrokerError};

pub trait ProxiesRetriever: Sync + Send + 'static {
    fn retrieve_proxies(&self) -> Box<dyn Stream<Item = String, Error = CoordinateError> + Send>;
}

pub struct ProxyFailure {
    proxy_address: String,
    report_id: String,
}

type NodeFailure = Node;

pub trait FailureChecker: Sync + Send + 'static {
    fn check(&self, address: String) -> Box<dyn Future<Item = Option<String>, Error = CoordinateError> + Send>;
}

pub trait FailureReporter: Sync + Send + 'static {
    fn report(&self, address: String) -> Box<dyn Future<Item = (), Error = CoordinateError> + Send>;
}

pub trait FailureDetector {
    type Retriever: ProxiesRetriever;
    type Checker: FailureChecker;
    type Reporter: FailureReporter;

    fn new(retriever: Self::Retriever, checker: Self::Checker, reporter: Self::Reporter) -> Self;
    fn run(&self) -> Box<dyn Stream<Item = (), Error = CoordinateError> + Send>;
}

pub struct SeqFailureDetector<Retriever: ProxiesRetriever, Checker: FailureChecker, Reporter: FailureReporter> {
    retriever: Retriever,
    checker: Arc<Checker>,
    reporter: Arc<Reporter>,
}

impl<T: ProxiesRetriever, C: FailureChecker, P: FailureReporter> FailureDetector
    for SeqFailureDetector<T, C, P> {

    type Retriever = T;
    type Checker = C;
    type Reporter = P;

    fn new(retriever: T, checker: C, reporter: P) -> Self {
        Self{
            retriever,
            checker: Arc::new(checker),
            reporter: Arc::new(reporter),
        }
    }

    fn run(&self) -> Box<dyn Stream<Item = (), Error = CoordinateError> + Send> {
        let checker = self.checker.clone();
        let reporter = self.reporter.clone();
        Box::new(
            self.retriever.retrieve_proxies()
                .map(move |address| checker.check(address))
                .buffer_unordered(10)
                .skip_while(|address| future::ok(address.is_none())).map(Option::unwrap)
                .and_then(move |address| reporter.report(address))
        )
    }
}

pub trait ProxyFailureRetriever: Sync + Send + 'static {
    fn retrieve_proxy_failures(&self) -> Box<dyn Stream<Item = ProxyFailure, Error = CoordinateError> + Send>;
}

pub trait NodeFailureRetriever: Sync + Send + 'static {
    fn retrieve_node_failures(&self, proxy_failure: ProxyFailure) -> Box<dyn Stream<Item = NodeFailure, Error = CoordinateError> + Send>;
}

pub trait NodeFailureHandler: Sync + Send + 'static {
    fn handle_node_failure(&self, failure_node: NodeFailure) -> Box<dyn Future<Item = (), Error = CoordinateError> + Send>;
}

pub trait FailureHandler {
    type PFRetriever: ProxyFailureRetriever;
    type NFRetriever: NodeFailureRetriever;
    type Handler: NodeFailureHandler;

    fn new(proxy_failure_retriever: Self::PFRetriever, node_failure_retriever: Self::NFRetriever, handler: Self::Handler) -> Self;
    fn run(&self) -> Box<dyn Stream<Item = (), Error = CoordinateError> + Send>;
}

pub struct SeqFailureHandler<PFRetriever: ProxyFailureRetriever, NFRetriever: NodeFailureRetriever, Handler: NodeFailureHandler> {
    proxy_failure_retriever: PFRetriever,
    node_failure_retriever: Arc<NFRetriever>,
    handler: Arc<Handler>,
}

impl<P: ProxyFailureRetriever, N: NodeFailureRetriever, H: NodeFailureHandler> FailureHandler
for SeqFailureHandler<P, N, H> {

    type PFRetriever = P;
    type NFRetriever = N;
    type Handler = H;

    fn new(proxy_failure_retriever: P, node_failure_retriever: N, handler: H) -> Self {
        Self{
            proxy_failure_retriever,
            node_failure_retriever: Arc::new(node_failure_retriever),
            handler: Arc::new(handler),
        }
    }

    fn run(&self) -> Box<dyn Stream<Item = (), Error = CoordinateError> + Send> {
        let node_failure_retriever = self.node_failure_retriever.clone();
        let handler = self.handler.clone();
        Box::new(
            self.proxy_failure_retriever.retrieve_proxy_failures()
                .and_then(move |proxy_failure| {
                    let cloned_handler = handler.clone();
                    node_failure_retriever.retrieve_node_failures(proxy_failure)
                        .for_each(move |node_failure| {
                            cloned_handler.handle_node_failure(node_failure)
                        })
                })
        )
    }
}

pub trait HostMetaSender: Sync + Send + 'static {
    fn handle_node_failure(&self, host: Host) -> Box<dyn Future<Item = (), Error = CoordinateError> + Send>;
}

pub trait HostMetaSynchronizer {
    type Retriever: ProxiesRetriever;
    type Sender: HostMetaSender;

    fn new(retriever: Self::Retriever, sender: Self::Sender) -> Self;
    fn run(&self) -> Box<dyn Stream<Item = (), Error = CoordinateError> + Send>;
}

#[derive(Debug)]
pub enum CoordinateError {
    Io(io::Error),
    Election(ElectionBrokerError),
    MetaData(MetaDataBrokerError),
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
        fn check(&self, address: String) -> Box<dyn Future<Item = Option<String>, Error = CoordinateError> + Send> {
            Box::new(future::ok(None))
        }
    }

    fn check<C: FailureChecker>(checker: C) {
        checker.check("".to_string()).wait();
    }

    #[test]
    fn test_reporter() {
        let checker = DummyChecker{};
        check(checker);
    }
}

