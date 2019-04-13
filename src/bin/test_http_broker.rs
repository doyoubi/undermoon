extern crate futures;
extern crate reqwest;
extern crate tokio;
extern crate undermoon;
#[macro_use]
extern crate log;
extern crate env_logger;

use futures::future::join_all;
use futures::stream::Stream;
use futures::Future;
use reqwest::r#async as request_async;
use undermoon::coordinator::broker::MetaDataBroker;
use undermoon::coordinator::http_meta_broker::HttpMetaBroker;

fn main() {
    env_logger::init();

    let proxy_address = "127.0.0.1:7799";
    let client = request_async::ClientBuilder::new().build().unwrap();
    let broker = HttpMetaBroker::new(proxy_address.to_string(), client);
    let fut = join_all(vec![
        test_get_cluster_names(broker.clone()),
        test_get_cluster(broker.clone()),
        test_get_host_addresses(broker.clone()),
        test_get_host(broker.clone()),
        test_add_failure(broker.clone()),
    ])
    .map(|_| info!("Tests completed"));
    tokio::run(fut);
}

fn test_get_cluster_names<B: MetaDataBroker>(
    broker: B,
) -> Box<dyn Future<Item = (), Error = ()> + Send> {
    let cluster_names = broker.get_cluster_names().collect();
    Box::new(
        cluster_names
            .map(|names| info!("names: {:?}", names))
            .map_err(|e| error!("failed to get names: {:?}", e)),
    )
}

fn test_get_cluster<B: MetaDataBroker>(broker: B) -> Box<dyn Future<Item = (), Error = ()> + Send> {
    Box::new(
        broker
            .get_cluster("clustername1".to_string())
            .map(|cluster| info!("cluster: {:?}", cluster))
            .map_err(|e| error!("failed to get cluster: {:?}", e)),
    )
}

fn test_get_host_addresses<B: MetaDataBroker>(
    broker: B,
) -> Box<dyn Future<Item = (), Error = ()> + Send> {
    let addresses = broker.get_host_addresses().collect();
    Box::new(
        addresses
            .map(|address| info!("addresses: {:?}", address))
            .map_err(|e| error!("failed to get addresses: {:?}", e)),
    )
}

fn test_get_host<B: MetaDataBroker>(broker: B) -> Box<dyn Future<Item = (), Error = ()> + Send> {
    Box::new(
        broker
            .get_host("127.0.0.1:5299".to_string())
            .map(|host| info!("host: {:?}", host))
            .map_err(|e| error!("failed to get host: {:?}", e)),
    )
}

fn test_add_failure<B: MetaDataBroker>(broker: B) -> Box<dyn Future<Item = (), Error = ()> + Send> {
    Box::new(
        broker
            .add_failure("127.0.0.1:5299".to_string(), "test_report_id".to_string())
            .map(|()| info!("Successfully add failure"))
            .map_err(|e| error!("failed to add failure: {:?}", e)),
    )
}
