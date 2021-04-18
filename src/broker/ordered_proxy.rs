#[cfg(test)]
mod tests {
    use super::super::store::{MetaStore, MetaStoreError};
    use super::super::utils::tests::{check_cluster_and_proxy, check_cluster_slots};
    use crate::common::cluster::{Cluster, ClusterName, Role};
    use crate::common::config::ClusterConfig;
    use std::collections::HashSet;
    use std::convert::TryFrom;

    const CLUSTER_NAME: &'static str = "testcluster";

    fn add_testing_proxies(store: &mut MetaStore, host_num: usize) {
        for host_index in 0..host_num {
            let proxy_address = format!("127.0.0.{}:70{:02}", host_index, host_index);
            let node_addresses = [
                format!("127.0.0.{}:60{:02}", host_index, host_index * 2),
                format!("127.0.0.{}:60{:02}", host_index, host_index * 2 + 1),
            ];
            store
                .add_proxy(proxy_address, node_addresses, None, Some(host_index))
                .unwrap();
        }
    }

    fn check_cluster_proxy_order(cluster: &Cluster) {
        for (i, node) in cluster.get_nodes().iter().enumerate() {
            let proxy_index = i / 2;
            let address = node.get_proxy_address();
            let expected_address = format!("127.0.0.{}:70{:02}", proxy_index, proxy_index);
            assert_eq!(address, expected_address);
        }
    }

    // The following tests are ported from the `store` test module
    // for `enable_ordered_proxy` mode.

    #[test]
    fn test_add_and_remove_proxy() {
        let migration_limit = 0;

        let mut store = MetaStore::new(true);
        let proxy_address = "127.0.0.1:7000";
        let nodes = ["127.0.0.1:6000".to_string(), "127.0.0.1:6001".to_string()];

        let err = store
            .add_proxy(proxy_address.to_string(), nodes.clone(), None, None)
            .unwrap_err();
        assert_eq!(err, MetaStoreError::MissingIndex);

        store
            .add_proxy(proxy_address.to_string(), nodes.clone(), None, Some(1))
            .unwrap();
        assert_eq!(store.get_global_epoch(), 1);
        assert_eq!(store.all_proxies.len(), 1);
        let resource = store.all_proxies.get(proxy_address).unwrap();
        assert_eq!(resource.proxy_address, proxy_address);
        assert_eq!(resource.node_addresses, nodes);
        assert_eq!(resource.index, 1);

        assert_eq!(store.get_proxies(), vec![proxy_address.to_string()]);

        let proxy = store
            .get_proxy_by_address(proxy_address, migration_limit)
            .unwrap();
        assert_eq!(proxy.get_address(), proxy_address);
        assert_eq!(proxy.get_epoch(), 1);
        assert_eq!(proxy.get_nodes().len(), 0);
        assert_eq!(proxy.get_peers().len(), 0);
        assert_eq!(proxy.get_free_nodes().len(), 2);

        store.remove_proxy(proxy_address.to_string()).unwrap();
    }

    #[test]
    fn test_add_and_remove_cluster() {
        let migration_limit = 0;

        let mut store = MetaStore::new(true);
        add_testing_proxies(&mut store, 4);
        let proxies: Vec<_> = store
            .get_proxies()
            .into_iter()
            .filter_map(|proxy_address| store.get_proxy_by_address(&proxy_address, migration_limit))
            .collect();
        let original_free_node_num: usize = proxies
            .iter()
            .map(|proxy| proxy.get_free_nodes().len())
            .sum();

        let epoch1 = store.get_global_epoch();

        check_cluster_and_proxy(&store);
        let cluster_name = CLUSTER_NAME.to_string();
        store.add_cluster(cluster_name.clone(), 4, ClusterConfig::default()).unwrap();
        let epoch2 = store.get_global_epoch();
        assert!(epoch1 < epoch2);
        check_cluster_and_proxy(&store);
        let cluster = store
            .get_cluster_by_name(CLUSTER_NAME, migration_limit)
            .unwrap();
        check_cluster_proxy_order(&cluster);

        let names: Vec<String> = store
            .get_cluster_names()
            .into_iter()
            .map(|cluster_name| cluster_name.to_string())
            .collect();
        assert_eq!(names, vec![cluster_name.clone()]);

        let cluster = store
            .get_cluster_by_name(&cluster_name, migration_limit)
            .unwrap();
        assert_eq!(cluster.get_nodes().len(), 4);
        check_cluster_proxy_order(&cluster);
        let cluster_store = store
            .clusters
            .get(&ClusterName::try_from(cluster_name.as_str()).unwrap())
            .unwrap();
        assert_eq!(cluster_store.get_node_number(), 4);
        assert_eq!(cluster_store.get_node_number_with_slots(), 4);
        assert_eq!(cluster_store.is_migrating(), false);

        check_cluster_slots(cluster.clone(), 4);

        let proxies: Vec<_> = store
            .get_proxies()
            .into_iter()
            .filter_map(|proxy_address| store.get_proxy_by_address(&proxy_address, migration_limit))
            .collect();
        let free_node_num: usize = proxies
            .iter()
            .map(|proxy| proxy.get_free_nodes().len())
            .sum();
        assert_eq!(free_node_num, original_free_node_num - 4);

        let another_cluster = "another_cluster".to_string();
        let err = store.add_cluster(another_cluster.clone(), 4, ClusterConfig::default()).unwrap_err();
        assert_eq!(err, MetaStoreError::OneClusterAlreadyExisted);
        check_cluster_and_proxy(&store);

        for node in cluster.get_nodes() {
            let proxy = store
                .get_proxy_by_address(&node.get_proxy_address(), migration_limit)
                .unwrap();
            assert_eq!(proxy.get_free_nodes().len(), 0);
            assert_eq!(proxy.get_nodes().len(), 2);
            let proxy_port = node
                .get_proxy_address()
                .split(':')
                .nth(1)
                .unwrap()
                .parse::<usize>()
                .unwrap();
            let node_port = node
                .get_address()
                .split(':')
                .nth(1)
                .unwrap()
                .parse::<usize>()
                .unwrap();
            assert_eq!(proxy_port - 7000, (node_port - 6000) / 2);
        }

        let node_addresses_set: HashSet<String> = cluster
            .get_nodes()
            .iter()
            .map(|node| node.get_address().to_string())
            .collect();
        assert_eq!(node_addresses_set.len(), cluster.get_nodes().len());
        let proy_addresses_set: HashSet<String> = cluster
            .get_nodes()
            .iter()
            .map(|node| node.get_proxy_address().to_string())
            .collect();
        assert_eq!(proy_addresses_set.len() * 2, cluster.get_nodes().len());

        store.remove_cluster(cluster_name.clone()).unwrap();
        let epoch3 = store.get_global_epoch();
        assert!(epoch2 < epoch3);
        check_cluster_and_proxy(&store);

        let proxies: Vec<_> = store
            .get_proxies()
            .into_iter()
            .filter_map(|proxy_address| store.get_proxy_by_address(&proxy_address, migration_limit))
            .collect();
        let free_node_num: usize = proxies
            .iter()
            .map(|proxy| proxy.get_free_nodes().len())
            .sum();
        assert_eq!(free_node_num, original_free_node_num);
    }

    #[test]
    fn test_failures() {
        let migration_limit = 0;

        let mut store = MetaStore::new(true);
        const ALL_PROXIES: usize = 8;
        add_testing_proxies(&mut store, ALL_PROXIES);
        assert_eq!(store.get_free_proxies().len(), ALL_PROXIES);

        let original_proxy_num = store.get_proxies().len();
        let failed_address = "127.0.0.2:7002";
        assert!(store
            .get_proxy_by_address(failed_address, migration_limit)
            .is_some());
        let epoch1 = store.get_global_epoch();

        store.add_failure(failed_address.to_string(), "reporter_id".to_string());
        let epoch2 = store.get_global_epoch();
        assert!(epoch1 < epoch2);
        let proxy_num = store.get_proxies().len();
        assert_eq!(proxy_num, original_proxy_num);
        assert_eq!(store.get_free_proxies().len(), ALL_PROXIES - 1);

        let failure_quorum: u64 = 1;
        assert_eq!(
            store.get_failures(chrono::Duration::max_value(), failure_quorum),
            vec![failed_address.to_string()],
        );
        let failure_quorum: u64 = 2;
        assert!(store
            .get_failures(chrono::Duration::max_value(), failure_quorum)
            .is_empty());
        store.remove_proxy(failed_address.to_string()).unwrap();
        let epoch3 = store.get_global_epoch();
        assert!(epoch2 < epoch3);

        let cluster_name = CLUSTER_NAME.to_string();
        let err = store.add_cluster(cluster_name.clone(), 8, ClusterConfig::default()).unwrap_err();
        assert_eq!(err, MetaStoreError::ProxyResourceOutOfOrder);
        assert_eq!(store.get_free_proxies().len(), ALL_PROXIES - 1);

        store.add_cluster(cluster_name.clone(), 4, ClusterConfig::default()).unwrap();
        assert_eq!(store.get_free_proxies().len(), ALL_PROXIES - 3);
        let epoch4 = store.get_global_epoch();
        assert!(epoch3 < epoch4);

        let cluster = store
            .get_cluster_by_name(&cluster_name, migration_limit)
            .unwrap();
        check_cluster_slots(cluster.clone(), 4);
        check_cluster_proxy_order(&cluster);

        let failed_proxy_index = 0;
        let (failed_proxy_address, peer_proxy_address) = cluster
            .get_nodes()
            .get(failed_proxy_index)
            .map(|node| {
                (
                    node.get_proxy_address().to_string(),
                    node.get_repl_meta().get_peers()[0].proxy_address.clone(),
                )
            })
            .unwrap();
        store.add_failure(failed_proxy_address.clone(), "reporter_id".to_string());
        assert_eq!(store.get_free_proxies().len(), ALL_PROXIES - 3);
        let epoch5 = store.get_global_epoch();
        assert!(epoch4 < epoch5);

        let proxy_num = store.get_proxies().len();
        assert_eq!(proxy_num, original_proxy_num - 1);
        let failure_quorum = 1;
        assert_eq!(
            store.get_failures(chrono::Duration::max_value(), failure_quorum),
            vec![failed_proxy_address.clone()]
        );

        let new_proxy = store
            .replace_failed_proxy(failed_proxy_address.clone(), migration_limit)
            .unwrap();
        assert_eq!(new_proxy, None);
        let epoch6 = store.get_global_epoch();
        assert!(epoch5 < epoch6);

        let cluster = store
            .get_cluster_by_name(&cluster_name, migration_limit)
            .unwrap();
        assert_eq!(
            cluster
                .get_nodes()
                .iter()
                .filter(|node| node.get_proxy_address() == &failed_proxy_address)
                .count(),
            2
        );
        for node in cluster.get_nodes().iter() {
            if node.get_proxy_address() == peer_proxy_address {
                assert_eq!(node.get_role(), Role::Master);
            } else if node.get_proxy_address() == failed_proxy_address {
                assert_eq!(node.get_role(), Role::Replica);
            }
        }
        check_cluster_proxy_order(&cluster);

        // Recover proxy
        let nodes = store
            .all_proxies
            .get(&failed_proxy_address)
            .unwrap()
            .node_addresses
            .clone();
        let err = store
            .add_proxy(
                failed_proxy_address.clone(),
                nodes,
                None,
                Some(failed_proxy_index),
            )
            .unwrap_err();
        assert_eq!(err, MetaStoreError::AlreadyExisted);
        let failure_quorum = 1;
        assert_eq!(
            store
                .get_failures(chrono::Duration::max_value(), failure_quorum)
                .len(),
            0
        );
        let epoch7 = store.get_global_epoch();
        assert!(epoch6 < epoch7);
        check_cluster_and_proxy(&store);
    }

    #[test]
    fn test_add_and_delete_free_nodes() {
        let migration_limit = 0;

        let mut store = MetaStore::new(true);
        let all_proxy_num = 4;
        add_testing_proxies(&mut store, all_proxy_num);
        assert_eq!(store.get_free_proxies().len(), all_proxy_num);

        let cluster_name = CLUSTER_NAME.to_string();

        store.add_cluster(cluster_name.clone(), 4, ClusterConfig::default()).unwrap();
        let epoch1 = store.get_global_epoch();
        assert_eq!(store.get_free_proxies().len(), all_proxy_num - 2);
        assert_eq!(
            store
                .get_cluster_by_name(&cluster_name, migration_limit)
                .unwrap()
                .get_nodes()
                .len(),
            4
        );
        check_cluster_and_proxy(&store);
        let cluster = store
            .get_cluster_by_name(CLUSTER_NAME, migration_limit)
            .unwrap();
        check_cluster_proxy_order(&cluster);

        store.auto_add_nodes(cluster_name.clone(), 4).unwrap();
        let epoch2 = store.get_global_epoch();
        assert!(epoch1 < epoch2);
        assert_eq!(store.get_free_proxies().len(), all_proxy_num - 4);
        assert_eq!(
            store
                .get_cluster_by_name(&cluster_name, migration_limit)
                .unwrap()
                .get_nodes()
                .len(),
            8
        );
        check_cluster_and_proxy(&store);
        let cluster = store
            .get_cluster_by_name(CLUSTER_NAME, migration_limit)
            .unwrap();
        check_cluster_proxy_order(&cluster);

        store.auto_delete_free_nodes(cluster_name.clone()).unwrap();
        let epoch3 = store.get_global_epoch();
        assert!(epoch2 < epoch3);
        assert_eq!(store.get_free_proxies().len(), all_proxy_num - 2);
        assert_eq!(
            store
                .get_cluster_by_name(&cluster_name, migration_limit)
                .unwrap()
                .get_nodes()
                .len(),
            4
        );
        check_cluster_and_proxy(&store);
        let cluster = store
            .get_cluster_by_name(CLUSTER_NAME, migration_limit)
            .unwrap();
        check_cluster_proxy_order(&cluster);
    }
}
