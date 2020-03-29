use super::broker::MetaDataBroker;
use super::core::{CoordinateError, ProxyMetaRetriever, ProxyMetaSender};
use crate::common::cluster::{ClusterName, Proxy, Role, SlotRange};
use crate::common::proto::{ClusterConfigMap, ClusterMapFlags, ProxyClusterMap, ProxyClusterMeta};
use crate::common::response::{OK_REPLY, OLD_EPOCH_REPLY};
use crate::protocol::{RedisClient, RedisClientFactory, Resp};
use crate::replication::replicator::{encode_repl_meta, MasterMeta, ReplicaMeta, ReplicatorMeta};
use futures::{Future, TryFutureExt};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

pub struct ProxyMetaRespSender<F: RedisClientFactory> {
    client_factory: Arc<F>,
}

impl<F: RedisClientFactory> ProxyMetaRespSender<F> {
    pub fn new(client_factory: Arc<F>) -> Self {
        Self { client_factory }
    }
}

impl<F: RedisClientFactory> ProxyMetaRespSender<F> {
    async fn send_meta_impl(&self, proxy: Proxy) -> Result<(), CoordinateError> {
        let mut client = self
            .client_factory
            .create_client(proxy.get_address().to_string())
            .await
            .map_err(CoordinateError::Redis)?;
        let proxy_with_only_masters = filter_proxy_masters(proxy.clone());
        send_meta(
            &mut client,
            "SETREPL".to_string(),
            generate_repl_meta_cmd_args(proxy, ClusterMapFlags { force: false }),
        )
        .await?;
        send_meta(
            &mut client,
            "SETCLUSTER".to_string(),
            generate_proxy_meta_cmd_args(ClusterMapFlags { force: false }, proxy_with_only_masters),
        )
        .await?;
        Ok(())
    }
}

impl<F: RedisClientFactory> ProxyMetaSender for ProxyMetaRespSender<F> {
    fn send_meta<'s>(
        &'s self,
        proxy: Proxy,
    ) -> Pin<Box<dyn Future<Output = Result<(), CoordinateError>> + Send + 's>> {
        Box::pin(self.send_meta_impl(proxy))
    }
}

fn filter_proxy_masters(proxy: Proxy) -> Proxy {
    let address = proxy.get_address().to_string();
    let epoch = proxy.get_epoch();
    let free_nodes = proxy.get_free_nodes().to_vec();
    let peers = proxy.get_peers().to_vec();
    let clusters_config = proxy.get_clusters_config().clone();
    let masters = proxy
        .into_nodes()
        .into_iter()
        .filter(|node| node.get_role() == Role::Master)
        .collect();

    Proxy::new(address, epoch, masters, free_nodes, peers, clusters_config)
}

pub struct BrokerMetaRetriever<B: MetaDataBroker> {
    broker: Arc<B>,
}

impl<B: MetaDataBroker> BrokerMetaRetriever<B> {
    pub fn new(broker: Arc<B>) -> Self {
        Self { broker }
    }
}

impl<B: MetaDataBroker> ProxyMetaRetriever for BrokerMetaRetriever<B> {
    fn get_proxy_meta<'s>(
        &'s self,
        address: String,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Proxy>, CoordinateError>> + Send + 's>> {
        Box::pin(
            self.broker
                .get_proxy(address)
                .map_err(CoordinateError::MetaData),
        )
    }
}

fn generate_proxy_meta_cmd_args(flags: ClusterMapFlags, proxy: Proxy) -> Vec<String> {
    let epoch = proxy.get_epoch();
    let clusters_config = ClusterConfigMap::new(proxy.get_clusters_config().clone());

    let mut cluster_map: HashMap<ClusterName, HashMap<String, Vec<SlotRange>>> = HashMap::new();

    for peer_proxy in proxy.get_peers().iter() {
        let clusters = cluster_map
            .entry(peer_proxy.cluster_name.clone())
            .or_insert_with(HashMap::new);
        clusters.insert(peer_proxy.proxy_address.clone(), peer_proxy.slots.clone());
    }
    let peer = ProxyClusterMap::new(cluster_map);

    let mut cluster_map: HashMap<ClusterName, HashMap<String, Vec<SlotRange>>> = HashMap::new();

    for node in proxy.into_nodes() {
        let clusters = cluster_map
            .entry(node.get_cluster_name().clone())
            .or_insert_with(HashMap::new);
        clusters.insert(node.get_address().to_string(), node.into_slots().clone());
    }
    let local = ProxyClusterMap::new(cluster_map);

    let proxy_cluster_meta = ProxyClusterMeta::new(epoch, flags, local, peer, clusters_config);
    proxy_cluster_meta.to_args()
}

// sub_command should be SETCLUSTER, SETREPL
async fn send_meta<C: RedisClient>(
    client: &mut C,
    sub_command: String,
    args: Vec<String>,
) -> Result<(), CoordinateError> {
    trace!("sending meta {} {:?}", sub_command, args);
    let mut cmd = vec!["UMCTL".to_string(), sub_command.clone()];
    cmd.extend(args);
    let resp = client
        .execute_single(cmd.into_iter().map(String::into_bytes).collect())
        .await
        .map_err(|e| {
            error!("failed to send meta data of proxy {:?}", e);
            CoordinateError::Redis(e)
        })?;
    match resp {
        Resp::Error(err_str) => {
            if err_str == OLD_EPOCH_REPLY.as_bytes() {
                Ok(())
            } else {
                error!("failed to send meta, invalid reply {:?}", err_str);
                Err(CoordinateError::InvalidReply)
            }
        }
        Resp::Simple(s) => {
            if s != OK_REPLY.as_bytes() {
                warn!("unexpected reply: {:?}", s);
            }
            Ok(())
        }
        reply => {
            debug!("Successfully set meta {} {:?}", sub_command, reply);
            Ok(())
        }
    }
}

fn generate_repl_meta_cmd_args(proxy: Proxy, flags: ClusterMapFlags) -> Vec<String> {
    let epoch = proxy.get_epoch();

    let mut masters = Vec::new();
    let mut replicas = Vec::new();

    for free_node in proxy.get_free_nodes().iter() {
        // For free nodes we use empty cluster name.
        masters.push(MasterMeta {
            cluster_name: ClusterName::new(),
            master_node_address: free_node.clone(),
            replicas: Vec::new(),
        })
    }

    for node in proxy.into_nodes().into_iter() {
        let role = node.get_role();
        let meta = node.get_repl_meta();
        let cluster_name = node.get_cluster_name().clone();
        match role {
            Role::Master => {
                // For importing nodes in 0.1 migration protocol,
                // the role is controlled by the migration progress.
                // And the role cannot be affected by replicator set in this place.
                // But this has been changed in 0.2 migration protocol.
                // if node.get_slots().iter().any(|sr| sr.tag.is_importing()) {
                //     continue;
                // }

                let master_node_address = node.get_address().to_string();
                let replicas = meta.get_peers().to_vec();
                let master_meta = MasterMeta {
                    cluster_name,
                    master_node_address,
                    replicas,
                };
                masters.push(master_meta);
            }
            Role::Replica => {
                let replica_node_address = node.get_address().to_string();
                let masters = meta.get_peers().to_vec();
                let replica_meta = ReplicaMeta {
                    cluster_name,
                    replica_node_address,
                    masters,
                };
                replicas.push(replica_meta);
            }
        }
    }

    let repl_meta = ReplicatorMeta {
        epoch,
        flags,
        masters,
        replicas,
    };

    encode_repl_meta(repl_meta)
}

#[cfg(test)]
mod tests {
    use super::super::broker::{MetaDataBrokerError, MockMetaDataBroker};
    use super::super::core::{ProxyMetaRespSynchronizer, ProxyMetaSynchronizer};
    use super::super::detector::BrokerProxiesRetriever;
    use super::*;
    use crate::common::cluster::{Node, RangeList, ReplMeta, ReplPeer, SlotRange, SlotRangeTag};
    use crate::common::config::ClusterConfig;
    use crate::protocol::{BinSafeStr, DummyRedisClientFactory, MockRedisClient, Resp};
    use futures::{stream, StreamExt};
    use std::convert::TryFrom;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio;

    fn gen_testing_proxy(role: Role) -> Proxy {
        let cluste_name = ClusterName::try_from("mycluster").unwrap();
        let slot_range = SlotRange {
            range_list: RangeList::try_from("1 233-666").unwrap(),
            tag: SlotRangeTag::None,
        };
        let repl = ReplMeta::new(
            role,
            vec![ReplPeer {
                node_address: "127.0.0.1:7002".to_string(),
                proxy_address: "127.0.0.1:6001".to_string(),
            }],
        );
        let nodes = vec![Node::new(
            "127.0.0.1:7001".to_string(),
            "127.0.0.1:6000".to_string(),
            cluste_name.clone(),
            vec![slot_range],
            repl,
        )];
        let mut clusters_config = HashMap::new();
        clusters_config.insert(cluste_name, ClusterConfig::default());
        Proxy::new(
            "127.0.0.1:6000".to_string(),
            7799,
            nodes,
            vec![],
            vec![],
            clusters_config,
        )
    }

    fn gen_master_args() -> Vec<String> {
        vec![
            "7799",
            "NOFLAG",
            "master",
            "mycluster",
            "127.0.0.1:7001",
            "1",
            "127.0.0.1:7002",
            "127.0.0.1:6001",
        ]
        .into_iter()
        .map(|s| s.to_string())
        .collect()
    }

    fn gen_replica_args() -> Vec<String> {
        vec![
            "7799",
            "FORCE",
            "replica",
            "mycluster",
            "127.0.0.1:7001",
            "1",
            "127.0.0.1:7002",
            "127.0.0.1:6001",
        ]
        .into_iter()
        .map(|s| s.to_string())
        .collect()
    }

    fn gen_set_cluster_args() -> Vec<String> {
        vec![
            "7799",
            "NOFLAG",
            "mycluster",
            "127.0.0.1:7001",
            "1",
            "233-666",
        ]
        .into_iter()
        .map(|s| s.to_string())
        .collect()
    }

    #[test]
    fn test_master_generate_repl_meta_cmd_args() {
        let proxy = gen_testing_proxy(Role::Master);
        let args = generate_repl_meta_cmd_args(proxy, ClusterMapFlags { force: false });
        assert_eq!(args, gen_master_args())
    }

    #[test]
    fn test_replica_generate_repl_meta_cmd_args() {
        let proxy = gen_testing_proxy(Role::Replica);
        let args = generate_repl_meta_cmd_args(proxy, ClusterMapFlags { force: true });
        assert_eq!(args, gen_replica_args())
    }

    #[tokio::test]
    async fn test_send_meta() {
        let mut mock_client = MockRedisClient::new();
        let cmd = vec![
            b"UMCTL".to_vec(),
            b"SETCLUSTER".to_vec(),
            b"test_args".to_vec(),
        ];
        mock_client
            .expect_execute_single()
            .withf(move |command: &Vec<BinSafeStr>| command.eq(&cmd))
            .times(1)
            .returning(|_| Box::pin(async { Ok(Resp::Simple(b"ok".to_vec())) }));
        let res = send_meta(
            &mut mock_client,
            "SETCLUSTER".to_string(),
            vec!["test_args".to_string()],
        )
        .await;
        assert!(res.is_ok());
    }

    fn create_client_func() -> impl RedisClient {
        let call_times = Arc::new(AtomicUsize::new(0));

        let mut mock_client = MockRedisClient::new();

        let mut set_repl_cmd = vec![b"UMCTL".to_vec(), b"SETREPL".to_vec()];
        set_repl_cmd.append(
            &mut gen_master_args()
                .into_iter()
                .map(|s| s.into_bytes())
                .collect(),
        );
        let mut set_cluster_cmd = vec![b"UMCTL".to_vec(), b"SETCLUSTER".to_vec()];
        set_cluster_cmd.append(
            &mut gen_set_cluster_args()
                .into_iter()
                .map(|s| s.into_bytes())
                .collect(),
        );
        set_cluster_cmd.push(b"CONFIG".to_vec());

        mock_client
            .expect_execute_single()
            .withf(move |command: &Vec<BinSafeStr>| {
                if call_times.load(Ordering::SeqCst) == 0 {
                    call_times.fetch_add(1, Ordering::SeqCst);
                    command.eq(&set_repl_cmd)
                } else {
                    // Ignore the config part
                    let cmd = command.get(0..set_cluster_cmd.len()).unwrap().to_vec();
                    cmd.eq(&set_cluster_cmd)
                }
            })
            .times(2)
            .returning(|_| Box::pin(async { Ok(Resp::Simple(b"ok".to_vec())) }));

        mock_client
    }

    #[tokio::test]
    async fn test_meta_resp_sender() {
        let client_factory = DummyRedisClientFactory::new(create_client_func);
        let sender = ProxyMetaRespSender::new(Arc::new(client_factory));
        let proxy = gen_testing_proxy(Role::Master);
        let res = sender.send_meta(proxy).await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_meta_retriever() {
        let proxy_addr = "127.0.0.1:6000";
        let mut mock_broker = MockMetaDataBroker::new();

        mock_broker
            .expect_get_proxy()
            .withf(move |addr| addr == proxy_addr)
            .returning(move |_| {
                let proxy = gen_testing_proxy(Role::Master);
                Box::pin(async { Ok(Some(proxy)) })
            });

        let retriever = BrokerMetaRetriever::new(Arc::new(mock_broker));
        let res = retriever.get_proxy_meta(proxy_addr.to_string()).await;
        assert!(res.is_ok());
        let opt = res.unwrap();
        assert!(opt.is_some());
        let proxy = opt.unwrap();
        assert_eq!(proxy, gen_testing_proxy(Role::Master))
    }

    // Integrate together
    #[tokio::test]
    async fn test_meta_sync() {
        let mut mock_broker = MockMetaDataBroker::new();
        let proxy_addr = gen_testing_proxy(Role::Master).get_address().to_string();
        let proxy_addr2 = proxy_addr.clone();
        let not_exist_proxy = "127.0.0.1:99999".to_string();
        let not_exist_proxy2 = not_exist_proxy.clone();

        mock_broker.expect_get_proxy_addresses().returning(move || {
            let results = vec![
                Err(MetaDataBrokerError::InvalidReply),
                Ok(not_exist_proxy2.clone()),
                Ok(proxy_addr2.clone()),
            ];
            Box::pin(stream::iter(results))
        });
        mock_broker
            .expect_get_proxy()
            .withf(move |addr| addr == &proxy_addr)
            .times(1)
            .returning(move |_| {
                let proxy = gen_testing_proxy(Role::Master);
                Box::pin(async { Ok(Some(proxy)) })
            });
        mock_broker
            .expect_get_proxy()
            .withf(move |addr| addr == &not_exist_proxy)
            .times(1)
            .returning(move |_| Box::pin(async { Ok(None) }));

        let mock_broker = Arc::new(mock_broker);

        let proxies_retriever = BrokerProxiesRetriever::new(mock_broker.clone());
        let meta_retriever = BrokerMetaRetriever::new(mock_broker);
        let client_factory = DummyRedisClientFactory::new(create_client_func);
        let sender = ProxyMetaRespSender::new(Arc::new(client_factory));

        let sync = ProxyMetaRespSynchronizer::new(proxies_retriever, meta_retriever, sender);
        let results: Vec<_> = sync.run().collect().await;
        assert_eq!(results.len(), 1);
        assert!(results[0].is_err());
    }
}
