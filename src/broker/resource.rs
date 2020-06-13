use super::store::{MetaStore, MetaStoreError};
use std::collections::HashMap;

pub struct ResourceChecker {
    store: MetaStore,
}

impl ResourceChecker {
    pub fn new(store: MetaStore) -> Self {
        Self { store }
    }

    // Returns hosts that can't recover because of not having enough resources.
    pub fn check_failure_tolerance(
        &self,
        migration_limit: u64,
    ) -> Result<Vec<String>, MetaStoreError> {
        // host => proxy addresses
        let mut proxy_map = HashMap::new();
        for (proxy_address, proxy_resource) in self.store.all_proxies.iter() {
            proxy_map
                .entry(proxy_resource.host.clone())
                .or_insert_with(Vec::new)
                .push(proxy_address.clone());
        }

        let mut hosts = vec![];
        for (host, proxy_addresses) in proxy_map.iter() {
            let enough_resource =
                self.check_failure_tolerance_for_one_host(proxy_addresses, migration_limit)?;
            if !enough_resource {
                hosts.push(host.clone());
            }
        }

        Ok(hosts)
    }

    fn check_failure_tolerance_for_one_host(
        &self,
        proxy_addresses: &[String],
        migration_limit: u64,
    ) -> Result<bool, MetaStoreError> {
        let mut store = self.store.clone();
        for proxy_address in proxy_addresses.iter() {
            match store.replace_failed_proxy(proxy_address.clone(), migration_limit) {
                Ok(_) => (),
                Err(MetaStoreError::NoAvailableResource) => return Ok(false),
                Err(err) => {
                    error!("ResourceChecker failed to replace failed proxy: {}", err);
                    return Err(err);
                }
            }
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::super::utils::tests::add_testing_proxies;
    use super::*;

    #[test]
    fn test_no_cluster() {
        let mut store = MetaStore::new(false);
        add_testing_proxies(&mut store, 4, 2);

        let checker = ResourceChecker::new(store);
        let res = checker.check_failure_tolerance(2);
        let proxies = res.unwrap();
        assert!(proxies.is_empty());
    }

    #[test]
    fn test_enough_resources() {
        let mut store = MetaStore::new(false);
        add_testing_proxies(&mut store, 4, 2);
        store.add_cluster("test_cluster".to_string(), 12).unwrap();

        let checker = ResourceChecker::new(store);
        let res = checker.check_failure_tolerance(2);
        let proxies = res.unwrap();
        assert!(proxies.is_empty());
    }

    #[test]
    fn test_no_enough_resource() {
        let mut store = MetaStore::new(false);
        add_testing_proxies(&mut store, 4, 2);
        store.add_cluster("test_cluster".to_string(), 16).unwrap();

        let checker = ResourceChecker::new(store);
        let res = checker.check_failure_tolerance(2);
        let proxies = res.unwrap();
        assert!(!proxies.is_empty());
    }
}
