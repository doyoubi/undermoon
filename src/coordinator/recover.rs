use super::broker::{MetaDataBroker, MetaManipulationBroker};
use super::core::{
    CoordinateError, NodeFailure, NodeFailureHandler, NodeFailureRetriever, ProxyFailureRetriever,
};
use futures::{future, stream, Future, Stream};

pub struct BrokerProxyFailureRetriever<B: MetaDataBroker> {
    broker: B,
}

impl<B: MetaDataBroker> BrokerProxyFailureRetriever<B> {
    pub fn new(broker: B) -> Self {
        Self { broker }
    }
}

impl<B: MetaDataBroker> ProxyFailureRetriever for BrokerProxyFailureRetriever<B> {
    fn retrieve_proxy_failures(
        &self,
    ) -> Box<dyn Stream<Item = String, Error = CoordinateError> + Send> {
        Box::new(
            self.broker
                .get_failures()
                .map_err(CoordinateError::MetaData),
        )
    }
}

pub struct BrokerNodeFailureRetriever<B: MetaDataBroker> {
    broker: B,
}

impl<B: MetaDataBroker> BrokerNodeFailureRetriever<B> {
    pub fn new(broker: B) -> Self {
        Self { broker }
    }
}

impl<B: MetaDataBroker> NodeFailureRetriever for BrokerNodeFailureRetriever<B> {
    fn retrieve_node_failures(
        &self,
        failed_proxy_address: String,
    ) -> Box<dyn Stream<Item = NodeFailure, Error = CoordinateError> + Send> {
        let fut = self
            .broker
            .get_host(failed_proxy_address.clone())
            .map_err(CoordinateError::MetaData)
            .map(move |host| {
                let nodes = match host {
                    None => {
                        warn!("can't find failed proxy {}", failed_proxy_address);
                        vec![]
                    }
                    Some(h) => h.into_nodes(),
                };
                stream::iter_ok(nodes)
            })
            .flatten_stream();
        Box::new(fut)
    }
}

pub struct ReplaceNodeHandler<DB: MetaDataBroker, MB: MetaManipulationBroker + Clone> {
    data_broker: DB,
    mani_broker: MB,
}

impl<DB: MetaDataBroker, MB: MetaManipulationBroker + Clone> ReplaceNodeHandler<DB, MB> {
    pub fn new(data_broker: DB, mani_broker: MB) -> Self {
        Self {
            data_broker,
            mani_broker,
        }
    }
}

impl<DB: MetaDataBroker, MB: MetaManipulationBroker + Clone> NodeFailureHandler
    for ReplaceNodeHandler<DB, MB>
{
    fn handle_node_failure(
        &self,
        failure_node: NodeFailure,
    ) -> Box<dyn Future<Item = (), Error = CoordinateError> + Send> {
        let address1 = failure_node.get_address().clone();
        let address2 = address1.clone();
        let cluster_name1 = failure_node.get_cluster_name().clone();
        let cluster_name2 = cluster_name1.clone();
        let mani_broker = self.mani_broker.clone();

        let fut = self
            .data_broker
            .get_cluster(failure_node.get_cluster_name().clone())
            .map_err(move |e| {
                error!(
                    "failed to get corresponding cluster {} {} {:?}",
                    cluster_name1, address1, e
                );
                CoordinateError::MetaData(e)
            })
            .and_then(
                move |cluster| -> Box<dyn Future<Item = (), Error = CoordinateError> + Send> {
                    let c = match cluster {
                        None => {
                            warn!("cluster {} not found for node", cluster_name2);
                            return Box::new(future::ok(()));
                        }
                        Some(c) => c,
                    };
                    if c.get_nodes()
                        .iter()
                        .find(|n| n.get_address().eq(&address2))
                        .is_none()
                    {
                        return Box::new(future::ok(()));
                    }
                    Box::new(
                        mani_broker
                            .replace_node(c.get_epoch(), failure_node)
                            .map_err(|e| {
                                error!("failed to replace node {:?}", e);
                                CoordinateError::MetaMani(e)
                            })
                            .map(|new_node| {
                                info!("successfully replace it with new node {:?}", new_node);
                            }),
                    )
                },
            );
        Box::new(fut)
    }
}
