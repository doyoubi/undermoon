use super::broker::MetaDataBroker;
use super::core::{CoordinateError, FailureChecker, FailureReporter, ProxiesRetriever};
use crate::common::cluster::Cluster;
use crate::protocol::{RedisClient, RedisClientFactory};
use futures::{future, stream, Future, FutureExt, Stream, StreamExt, TryFutureExt, TryStreamExt};
use futures_batch::ChunksTimeoutStreamExt;
use std::cmp;
use std::collections::HashSet;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

pub struct BrokerProxiesRetriever<B: MetaDataBroker> {
    meta_data_broker: Arc<B>,
}

impl<B: MetaDataBroker> BrokerProxiesRetriever<B> {
    pub fn new(meta_data_broker: Arc<B>) -> Self {
        Self { meta_data_broker }
    }
}

impl<B: MetaDataBroker> ProxiesRetriever for BrokerProxiesRetriever<B> {
    fn retrieve_proxies<'s>(
        &'s self,
    ) -> Pin<Box<dyn Stream<Item = Result<String, CoordinateError>> + Send + 's>> {
        Box::pin(
            self.meta_data_broker
                .get_host_addresses()
                .map_err(CoordinateError::MetaData),
        )
    }
}

// Sometimes we can't just call MetaDataBroker::get_host_addresses() to
// get the addresses. There's a corner case we need to solve.
// When the cluster is added some new proxies, the client may be redirected
// to the new nodes especially when the migration starts.
// But this time the metadata might have not synchronized to the new nodes
// yet. This will cause a `db not found` error since the new nodes is still
// uninitialized. Thus we should always synchronize the metadata to the
// new nodes first.
//
// NOTICE: The clients should not directly use the address list from
// the MetaDataBroker::get_host_addresses(). They should check whether
// the server proxies are ready. Only after all the proxies get synchronized,
// can the clients use the addresses from MetaDataBroker::get_host_addresses().
pub struct BrokerOrderedProxiesRetriever<B: MetaDataBroker> {
    meta_data_broker: Arc<B>,
}

impl<B: MetaDataBroker> BrokerOrderedProxiesRetriever<B> {
    pub fn new(meta_data_broker: Arc<B>) -> Self {
        Self { meta_data_broker }
    }

    async fn get_ordered_proxies(&self) -> Vec<Result<String, CoordinateError>> {
        let (mut results, added_tag) = self.get_all_cluster_ordered_proxies().await;
        let mut free_results = self.get_free_proxies(added_tag).await;
        results.append(&mut free_results);
        results
    }

    async fn get_free_proxies(
        &self,
        added_tag: HashSet<String>,
    ) -> Vec<Result<String, CoordinateError>> {
        self.meta_data_broker
            .get_host_addresses()
            .filter(move |res| {
                let filter = match res {
                    Ok(addr) => !added_tag.contains(addr),
                    _ => true,
                };
                future::ready(filter)
            })
            .map_err(CoordinateError::MetaData)
            .collect::<Vec<_>>()
            .await
    }

    async fn get_all_cluster_ordered_proxies(
        &self,
    ) -> (Vec<Result<String, CoordinateError>>, HashSet<String>) {
        let mut res = vec![];
        let mut added_tag = HashSet::new();

        const CHUNK_SIZE: usize = 10;
        const BATCH_SIZE: Duration = Duration::from_millis(1);

        let mut cluster_names = self
            .meta_data_broker
            .get_cluster_names()
            .chunks_timeout(CHUNK_SIZE, BATCH_SIZE);
        let meta_data_broker = self.meta_data_broker.clone();

        while let Some(results) = cluster_names.next().await {
            let mut cluster_names = vec![];
            for result in results.into_iter() {
                match result {
                    Ok(cluster_name) => cluster_names.push(cluster_name),
                    Err(err) => {
                        res.push(Err(CoordinateError::MetaData(err)));
                        continue;
                    }
                };
            }

            if cluster_names.is_empty() {
                continue;
            }

            let futs: Vec<_> = cluster_names
                .into_iter()
                .map(|cluster_name| {
                    meta_data_broker
                        .get_cluster(cluster_name)
                        .map_err(CoordinateError::MetaData)
                })
                .collect();
            let results = future::join_all(futs).await;

            for r in results.into_iter() {
                let cluster: Cluster = match r {
                    Ok(Some(cluster)) => cluster,
                    Ok(None) => continue,
                    Err(err) => {
                        res.push(Err(err));
                        continue;
                    }
                };

                let mut nodes = cluster.get_nodes().to_vec();
                // The order is:
                // nodes with slots including migrating slots < nodes without slots < nodes with importing slots
                nodes.sort_unstable_by(|a, b| {
                    for slot_range in a.get_slots() {
                        if slot_range.tag.is_importing() {
                            return cmp::Ordering::Less;
                        }
                    }
                    for slot_range in b.get_slots() {
                        if slot_range.tag.is_importing() {
                            return cmp::Ordering::Greater;
                        }
                    }
                    if a.get_slots().is_empty() {
                        return cmp::Ordering::Less;
                    }
                    if b.get_slots().is_empty() {
                        return cmp::Ordering::Greater;
                    }
                    cmp::Ordering::Equal
                });

                for node in &nodes {
                    let proxy_address = node.get_proxy_address().to_string();
                    if added_tag.contains(&proxy_address) {
                        continue;
                    }
                    added_tag.insert(proxy_address.clone());
                    res.push(Ok(proxy_address));
                }
            }
        }
        (res, added_tag)
    }
}

impl<B: MetaDataBroker> ProxiesRetriever for BrokerOrderedProxiesRetriever<B> {
    fn retrieve_proxies<'s>(
        &'s self,
    ) -> Pin<Box<dyn Stream<Item = Result<String, CoordinateError>> + Send + 's>> {
        Box::pin(
            self.get_ordered_proxies()
                .map(stream::iter)
                .flatten_stream(),
        )
    }
}

pub struct PingFailureDetector<F: RedisClientFactory> {
    client_factory: Arc<F>,
}

impl<F: RedisClientFactory> PingFailureDetector<F> {
    pub fn new(client_factory: Arc<F>) -> Self {
        Self { client_factory }
    }

    async fn ping(&self, address: String) -> Result<Option<String>, CoordinateError> {
        let mut client = match self.client_factory.create_client(address.clone()).await {
            Ok(client) => client,
            Err(err) => {
                error!("PingFailureDetector::check failed to connect: {:?}", err);
                return Ok(Some(address));
            }
        };

        // The connection pool might get a stale connection.
        // Return err instead for retry.
        let ping_command = vec!["PING".to_string().into_bytes()];
        match client.execute_single(ping_command).await {
            Ok(_) => Ok(None),
            Err(err) => {
                error!("PingFailureDetector::check failed to send PING: {:?}", err);
                Err(CoordinateError::Redis(err))
            }
        }
    }

    async fn check_impl(&self, address: String) -> Result<Option<String>, CoordinateError> {
        const RETRY: usize = 3;
        for i in 1..=RETRY {
            match self.ping(address.clone()).await {
                Ok(None) => return Ok(None),
                _ if i == RETRY => return Ok(Some(address)),
                _ => continue,
            }
        }
        Ok(Some(address))
    }
}

impl<F: RedisClientFactory> FailureChecker for PingFailureDetector<F> {
    fn check<'s>(
        &'s self,
        address: String,
    ) -> Pin<Box<dyn Future<Output = Result<Option<String>, CoordinateError>> + Send + 's>> {
        Box::pin(self.check_impl(address))
    }
}

pub struct BrokerFailureReporter<B: MetaDataBroker> {
    reporter_id: String,
    meta_data_broker: Arc<B>,
}

impl<B: MetaDataBroker> BrokerFailureReporter<B> {
    pub fn new(reporter_id: String, meta_data_broker: Arc<B>) -> Self {
        Self {
            reporter_id,
            meta_data_broker,
        }
    }
}

impl<B: MetaDataBroker> FailureReporter for BrokerFailureReporter<B> {
    fn report<'s>(
        &'s self,
        address: String,
    ) -> Pin<Box<dyn Future<Output = Result<(), CoordinateError>> + Send + 's>> {
        Box::pin(
            self.meta_data_broker
                .add_failure(address, self.reporter_id.clone())
                .map_err(CoordinateError::MetaData),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::super::broker::{MetaDataBrokerError, MockMetaDataBroker};
    use super::super::core::{FailureDetector, ParFailureDetector};
    use super::*;
    use crate::common::cluster::{
        DBName, MigrationMeta, Node, RangeList, ReplMeta, Role, SlotRange, SlotRangeTag,
    };
    use crate::common::config::ClusterConfig;
    use crate::protocol::{
        Array, BinSafeStr, OptionalMulti, RedisClient, RedisClientError, Resp, RespVec,
    };
    use futures::{future, stream, StreamExt};
    use std::convert::TryFrom;
    use std::pin::Pin;
    use std::sync::Arc;
    use tokio;

    const NODE1: &'static str = "127.0.0.1:7000";
    const NODE2: &'static str = "127.0.0.1:7001";

    #[derive(Debug)]
    struct DummyClient {
        address: String,
    }

    impl RedisClient for DummyClient {
        fn execute<'s>(
            &'s mut self,
            _command: OptionalMulti<Vec<BinSafeStr>>,
        ) -> Pin<
            Box<dyn Future<Output = Result<OptionalMulti<RespVec>, RedisClientError>> + Send + 's>,
        > {
            if self.address == NODE1 {
                // only works for single command
                Box::pin(future::ok(OptionalMulti::Single(
                    Resp::Arr(Array::Nil).into(),
                )))
            } else {
                Box::pin(future::err(RedisClientError::InvalidReply))
            }
        }
    }

    struct DummyClientFactory;

    impl RedisClientFactory for DummyClientFactory {
        type Client = DummyClient;

        fn create_client(
            &self,
            address: String,
        ) -> Pin<Box<dyn Future<Output = Result<Self::Client, RedisClientError>> + Send>> {
            Box::pin(future::ok(DummyClient { address }))
        }
    }

    #[tokio::test]
    async fn test_proxy_retriever() {
        let mut mock_broker = MockMetaDataBroker::new();
        let addresses: Vec<String> = vec!["host1:port1", "host2:port2"]
            .into_iter()
            .map(|s| s.to_string())
            .collect();
        let addresses_clone = addresses.clone();
        mock_broker
            .expect_get_host_addresses()
            .returning(move || Box::pin(stream::iter(addresses_clone.clone().into_iter().map(Ok))));
        let broker = Arc::new(mock_broker);
        let retriever = BrokerProxiesRetriever::new(broker);
        let addrs: Vec<Result<String, CoordinateError>> =
            retriever.retrieve_proxies().collect().await;
        let addrs: Vec<String> = addrs.into_iter().map(|r| r.unwrap()).collect();
        assert_eq!(addrs, addresses);
    }

    #[tokio::test]
    async fn test_ordered_proxy_retriever() {
        let mut mock_broker = MockMetaDataBroker::new();
        let addresses: Vec<String> = vec![
            "host1:port1",
            "host2:port2",
            "host3:port3",
            "host4:port4",
            "host5:port5",
        ]
        .into_iter()
        .map(|s| s.to_string())
        .collect();

        mock_broker.expect_get_cluster_names().returning(move || {
            Box::pin(stream::iter(
                vec![
                    DBName::from("dybdb").unwrap(),
                    DBName::from("notfound").unwrap(),
                ]
                .into_iter()
                .map(Ok),
            ))
        });

        let mgr_meta = MigrationMeta {
            epoch: 5299,
            src_proxy_address: "host1:port1".to_string(),
            src_node_address: "redis1:port1".to_string(),
            dst_proxy_address: "host3:port3".to_string(),
            dst_node_address: "redis3:port3".to_string(),
        };
        let nodes = vec![
            Node::new(
                "redis1:port1".to_string(),
                "host1:port1".to_string(),
                DBName::from("dybdb").unwrap(),
                vec![SlotRange {
                    range_list: RangeList::try_from("1 0-233").unwrap(),
                    tag: SlotRangeTag::Migrating(mgr_meta.clone()),
                }],
                ReplMeta::new(Role::Master, Vec::new()),
            ),
            Node::new(
                "redis2:port2".to_string(),
                "host2:port2".to_string(),
                DBName::from("dybdb").unwrap(),
                vec![SlotRange {
                    range_list: RangeList::try_from("1 666-6699").unwrap(),
                    tag: SlotRangeTag::None,
                }],
                ReplMeta::new(Role::Master, Vec::new()),
            ),
            Node::new(
                "redis3:port3".to_string(),
                "host3:port3".to_string(),
                DBName::from("dybdb").unwrap(),
                vec![SlotRange {
                    range_list: RangeList::try_from("1 0-233").unwrap(),
                    tag: SlotRangeTag::Importing(mgr_meta),
                }],
                ReplMeta::new(Role::Master, Vec::new()),
            ),
            Node::new(
                "redis4:port4".to_string(),
                "host4:port4".to_string(),
                DBName::from("dybdb").unwrap(),
                vec![],
                ReplMeta::new(Role::Master, Vec::new()),
            ),
        ];
        mock_broker
            .expect_get_cluster()
            .returning(move |cluster_name| {
                let cluster = match cluster_name.to_string().as_str() {
                    "dybdb" => Some(Cluster::new(
                        cluster_name,
                        5299,
                        nodes.clone(),
                        ClusterConfig::default(),
                    )),
                    "notfound" | _ => None,
                };
                Box::pin(future::ok(cluster))
            });
        mock_broker
            .expect_get_host_addresses()
            .returning(move || Box::pin(stream::iter(addresses.clone().into_iter().map(Ok))));

        let broker = Arc::new(mock_broker);

        let retriever = BrokerOrderedProxiesRetriever::new(broker);
        let addrs: Vec<Result<String, CoordinateError>> =
            retriever.retrieve_proxies().collect().await;
        let addrs: Vec<String> = addrs.into_iter().map(|r| r.unwrap()).collect();
        assert_eq!(addrs.len(), 5);
        assert_eq!(addrs[0], "host3:port3"); // importing tag
        assert_eq!(addrs[1], "host4:port4"); // empty slots

        // migrating and normal slots could be random.
        if addrs[2] == "host1:port1" {
            assert_eq!(addrs[3], "host2:port2");
        } else {
            assert_eq!(addrs[2], "host2:port2");
            assert_eq!(addrs[3], "host1:port1");
        }
        // free nodes
        assert_eq!(addrs[4], "host5:port5");
    }

    #[tokio::test]
    async fn test_failure_detector() {
        let mut mock_broker = MockMetaDataBroker::new();

        let addresses: Vec<String> = vec![NODE1, NODE2]
            .into_iter()
            .map(|s| s.to_string())
            .collect();
        let addresses_clone = addresses.clone();
        mock_broker
            .expect_get_host_addresses()
            .returning(move || Box::pin(stream::iter(addresses_clone.clone().into_iter().map(Ok))));

        let checker = PingFailureDetector::new(Arc::new(DummyClientFactory {}));
        let res = checker.check(NODE1.to_string()).await;
        assert!(res.is_ok());
        assert!(res.unwrap().is_none());

        let res = checker.check(NODE2.to_string()).await;
        assert!(res.is_ok());
        assert_eq!(res.unwrap().unwrap(), NODE2);
    }

    #[tokio::test]
    async fn test_reporter() {
        let mut mock_broker = MockMetaDataBroker::new();
        mock_broker
            .expect_add_failure()
            .withf(|address: &String, _| address == NODE2)
            .times(1)
            .returning(|_, _| Box::pin(future::ok(())));

        let broker = Arc::new(mock_broker);
        let reporter = BrokerFailureReporter::new("test_id".to_string(), broker.clone());
        let res = reporter.report(NODE2.to_string()).await;
        assert!(res.is_ok());
    }

    // Integrate together
    #[tokio::test]
    async fn test_seq_failure_detector() {
        let mut mock_broker = MockMetaDataBroker::new();

        let addresses: Vec<String> = vec![NODE1, NODE2]
            .into_iter()
            .map(|s| s.to_string())
            .collect();
        let addresses_clone = addresses.clone();
        mock_broker
            .expect_get_host_addresses()
            .returning(move || Box::pin(stream::iter(addresses_clone.clone().into_iter().map(Ok))));
        mock_broker
            .expect_add_failure()
            .withf(|address: &String, _| address == NODE2)
            .times(1)
            .returning(|_, _| Box::pin(future::ok(())));

        let broker = Arc::new(mock_broker);
        let retriever = BrokerProxiesRetriever::new(broker.clone());
        let checker = PingFailureDetector::new(Arc::new(DummyClientFactory {}));
        let reporter = BrokerFailureReporter::new("test_id".to_string(), broker.clone());
        let detector = ParFailureDetector::new(retriever, checker, reporter);

        let res = detector.run().into_future().await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_detector_partial_error() {
        let mut mock_broker = MockMetaDataBroker::new();

        mock_broker.expect_get_host_addresses().returning(move || {
            let results = vec![
                Err(MetaDataBrokerError::InvalidReply),
                Ok(NODE1.to_string()),
                Ok(NODE2.to_string()),
            ];
            Box::pin(stream::iter(results))
        });
        mock_broker
            .expect_add_failure()
            .withf(|address: &String, _| address == NODE2)
            .times(1)
            .returning(|_, _| Box::pin(future::ok(())));

        let broker = Arc::new(mock_broker);
        let retriever = BrokerProxiesRetriever::new(broker.clone());
        let checker = PingFailureDetector::new(Arc::new(DummyClientFactory {}));
        let reporter = BrokerFailureReporter::new("test_id".to_string(), broker.clone());
        let detector = ParFailureDetector::new(retriever, checker, reporter);

        let res = detector.run().into_future().await;
        assert!(res.is_err());
    }
}
