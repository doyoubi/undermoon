#[cfg(test)]
pub mod tests {
    use super::super::store::MetaStore;
    use crate::common::cluster::{Cluster, Role, SlotRangeTag};
    use crate::common::utils::SLOT_NUM;

    pub fn add_testing_proxies(store: &mut MetaStore, host_num: usize, proxy_per_host: usize) {
        for host_index in 1..=host_num {
            for i in 1..=proxy_per_host {
                let proxy_address = format!("127.0.0.{}:70{:02}", host_index, i);
                let node_addresses = [
                    format!("127.0.0.{}:60{:02}", host_index, i * 2),
                    format!("127.0.0.{}:60{:02}", host_index, i * 2 + 1),
                ];
                let index = host_index * proxy_per_host + i;
                store
                    .add_proxy(proxy_address, node_addresses, None, Some(index))
                    .unwrap();
            }
        }
    }

    pub fn check_cluster_and_proxy(store: &MetaStore) {
        for cluster in store.clusters.values() {
            for chunk in cluster.chunks.iter() {
                for proxy_address in chunk.proxy_addresses.iter() {
                    let proxy = store.all_proxies.get(proxy_address).unwrap();
                    assert_eq!(proxy.cluster.as_ref().unwrap(), &cluster.name);
                }
            }
        }
        for proxy in store.all_proxies.values() {
            let cluster_name = match proxy.cluster.as_ref() {
                Some(name) => name,
                None => continue,
            };
            let cluster = store.clusters.get(cluster_name).unwrap();
            assert!(cluster.chunks.iter().any(|chunk| chunk
                .proxy_addresses
                .iter()
                .any(|addr| addr == &proxy.proxy_address)));
        }

        assert!(store.check().is_ok());
    }

    pub fn check_cluster_slots(cluster: Cluster, node_num: usize) {
        assert_eq!(cluster.get_nodes().len(), node_num);
        let master_num = cluster.get_nodes().len() / 2;
        let average_slots_num = SLOT_NUM / master_num;

        let mut visited = Vec::with_capacity(SLOT_NUM);
        for _ in 0..SLOT_NUM {
            visited.push(false);
        }

        for node in cluster.get_nodes() {
            let slots = node.get_slots();
            if node.get_role() == Role::Master {
                assert_eq!(slots.len(), 1);
                assert_eq!(slots[0].tag, SlotRangeTag::None);
                let slots_num = slots[0].get_range_list().get_slots_num();
                let delta = slots_num.checked_sub(average_slots_num).unwrap();
                assert!(delta <= 1);

                for range in slots[0].get_range_list().get_ranges().iter() {
                    for i in range.start()..=range.end() {
                        assert!(!visited.get(i).unwrap());
                        *visited.get_mut(i).unwrap() = true;
                    }
                }

                let mut sorted_range_list = slots[0].get_range_list().clone();
                sorted_range_list.compact();
                assert_eq!(&sorted_range_list, slots[0].get_range_list());
            } else {
                assert!(slots.is_empty());
            }
        }
        for v in visited.iter() {
            assert!(*v);
        }

        let mut last_node_slot_num = usize::max_value();
        for node in cluster.get_nodes() {
            if node.get_role() == Role::Replica {
                continue;
            }
            let curr_num = node
                .get_slots()
                .iter()
                .map(|slots| slots.get_range_list().get_slots_num())
                .sum();
            assert!(last_node_slot_num >= curr_num);
            last_node_slot_num = curr_num;
        }
    }
}
