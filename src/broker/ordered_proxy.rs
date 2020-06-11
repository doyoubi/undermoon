#[cfg(test)]
mod tests {
    use super::super::store::{MetaStore, MetaStoreError};
    use super::super::utils::tests::{check_cluster_and_proxy, check_cluster_slots};
    use crate::common::cluster::ClusterName;
    use std::collections::HashSet;
    use std::convert::TryFrom;

    fn add_testing_proxies(store: &mut MetaStore, host_num: usize) {
        for host_index in 0..=host_num {
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

    #[test]
    fn test_add_and_remove_proxy() {
        let migration_limit = 0;

        let mut store = MetaStore::new(true);
        let proxy_address = "127.0.0.1:7000";
        let nodes = ["127.0.0.1:6000".to_string(), "127.0.0.1:6001".to_string()];

        let err = store
            .add_proxy(proxy_address.to_string(), nodes.clone(), None, None)
            .unwrap_err();
        assert!(matches!(err, MetaStoreError::MissingIndex));

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
        let cluster_name = "testcluster".to_string();
        store.add_cluster(cluster_name.clone(), 4).unwrap();
        let epoch2 = store.get_global_epoch();
        assert!(epoch1 < epoch2);
        check_cluster_and_proxy(&store);

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
        let err = store.add_cluster(another_cluster.clone(), 4).unwrap_err();
        assert!(matches!(err, MetaStoreError::OneClusterAlreadyExisted));
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
}
