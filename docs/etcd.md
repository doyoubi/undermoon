# Etcd Data Design

# Path
```
/configurable_prefix/hosts/<host>/port/<port> => {
    "cluster": <string>,
    "slots": <string>,
}

/configurable_prefix/hosts/<host>/epoch => <epoch>

/configurable_prefix/clusters/<name>/nodes/<node_address> => {
    "slots": <string>,
}

/configurable_prefix/clusters/<name>/epoch => <epoch>

/configurable_prefix/coordinators/<address>/report_id/<report_id> => <report_id>

/configurable_prefix/failures/<address>/<report_id>
```

# Operations
- Get all proxy addresses
- Get nodes and their slots for each proxy address
- Get all cluster names
- Get nodes and slots for each cluster
- Create a node with slots for a cluster

# Implementation Details
The peer setting requests relies on the meta data of hosts. The epoch of peer data is the largest epoch of the corresponding clusters.
so all the cluster epochs should be generated from the same global epoch to ensure peer information could be updated for the proxies.

Since we don't have a good etcd client or gRPC library in Rust, we use a http service written in Golang to proxy the requests.
