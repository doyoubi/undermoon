extern crate reqwest;
extern crate futures;
extern crate tokio;
extern crate undermoon;

use futures::Future;
use futures::future::join_all;
use futures::stream::Stream;
use reqwest::async;
use undermoon::coordinator::http_broker::HttpMetaBroker;
use undermoon::coordinator::broker::MetaDataBroker;

fn main() {
    let proxy_address = "127.0.0.1:7799";
    let client = async::ClientBuilder::new().build().unwrap();
    let broker = HttpMetaBroker::new(proxy_address.to_string(), client);
    let fut = join_all(vec![
        test_get_cluster_names(broker.clone()),
        test_get_cluster(broker.clone()),
    ]).map(|_| ());
    tokio::run(fut);
}

fn test_get_cluster_names<B: MetaDataBroker>(broker: B) -> Box<dyn Future<Item = (), Error = ()> + Send> {
    let cluster_names = broker.get_cluster_names().collect();
    Box::new(
        cluster_names
        .map(|names| println!("names: {:?}", names))
        .map_err(|e| println!("failed to get names: {:?}", e))
    )
}

fn test_get_cluster<B: MetaDataBroker>(broker: B) -> Box<dyn Future<Item = (), Error = ()> + Send> {
    Box::new(
        broker.get_cluster("clustername1".to_string())
        .map(|cluster| {println!("cluster: {:?}", cluster)})
        .map_err(|e| println!("failed to get cluster: {:?}", e))
    )
}
