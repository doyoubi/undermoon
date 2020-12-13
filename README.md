![undermoon logo](docs/undermoon-logo.svg)

# Undermoon [![Build Status](https://travis-ci.com/doyoubi/undermoon.svg?branch=master)](https://travis-ci.com/doyoubi/undermoon)
`Undermoon` is a self-managed Redis clustering system based on **Redis Cluster Protocol** supporting:

- Horizontal scalability and high availability
- Cluster management through HTTP API
- Automatic failover for both master and replica
- Fast scaling
- Both cluster-mode clients and non-cluster-mode clients.
- String value compression

Any storage system implementing redis protocol could also somehow work with undermoon,
such as [KeyDB](https://github.com/JohnSully/KeyDB).

For more in-depth explanation of Redis Cluster Protocol and how Undermoon implement it,
please refer to [Redis Cluster Protocol](./docs/redis_cluster_protocol.md).

## Architecture
![architecture](docs/architecture.svg)
##### Metadata Storage
Metadata storage stores all the metadata of the whole `undermoon` cluster,
including existing Redis instances, proxies, and exposed Redis clusters.
Now it's an in-memory storage server called `Memory Broker`.
When using [undermoon-operator](https://github.com/doyoubi/undermoon-operator),
this `Memory Broker` will change to use `ConfigMap` to store the data.

##### Coordinator
Coordinator will synchronize the metadata between broker and server proxy.
It also actively checks the liveness of server proxy and initiates failover.

##### Storage Cluster
The storage cluster consists of server proxies and Redis instances.
It serves just like the official Redis Cluster to the applications.
A Redis Cluster Proxy could be added between it and applications
so that applications don't need to upgrade their Redis clients to smart clients.

###### Chunk
Chunk is the smallest building block of every single exposed Redis Cluster.
Each chunk consists of 4 Redis instances and 2 server proxies evenly distributed in two different physical machines.
So the node number of each Redis cluster will be the multiples of 4 with half masters and half replicas.

The design of chunk makes it very easy to build a cluster with a good topology for **workload balancing**.

## Getting Started
### Run Undermoon in Kubernetes
Using [undermoon-operator](https://github.com/doyoubi/undermoon-operator)
is the easiest way to create Redis clusters if you have Kubernetes.

```
helm install my-undermoon-operator undermoon-operator-<x.x.x>.tgz

helm install \
    --set 'cluster.clusterName=my-cluster-name' \
    --set 'cluster.chunkNumber=2' \
    --set 'cluster.maxMemory=2048' \
    --set 'cluster.port=5299' \
    my-cluster \
    -n my-namespace \
    undermoon-cluster-<x.x.x>.tgz
```

See the `README.md` of [undermoon-operator](https://github.com/doyoubi/undermoon-operator)
for how to use it.

### Run Undermoon Using Docker Compose
See [docker compose example](./docs/docker_compose_example.md).

### Setup Undermoon Manually
Or you can set them up without docker following this docs: [setting up undermoon manually](docs/set_up_manually.md).

## Development
`undermoon` tries to avoid `unsafe` and some calls that could crash.
It uses a [customized linter](https://github.com/doyoubi/mylint-rs) to scan all the codes except test modules.

Install linters:
```
$ make install-linters
```

Then run the following commands before commit your codes:
```
$ make lint
$ make test
```

See more in the [development guide](./docs/development.md).

## Documentation
- [Redis Cluster Protocol and Server Proxy](./docs/redis_cluster_protocol.md)
- [Chunk](./docs/chunk.md)
- [Slot Migration](./docs/slots_migration.md)
- [Memory Broker Replica](./docs/mem_broker_replica.md)
- [Configure to support non-cluster-mode clients](./docs/active_redirection.md)
- [Command Table](./docs/command_table.md)
- [Performance](./docs/performance.md)
- [Best Practice](./docs/best_practice.md)
- [Broker External Storage](./docs/broker_external_storage.md)

## API
- [Proxy UMCTL command](./docs/meta_command.md)
- [HTTP Broker API](./docs/broker_http_api.md)
- [Memory Broker API](./docs/memory_broker_api.md)
