# Undermoon
Aims to provide a server-side Redis proxy implementing Redis Cluster Protocol.

# Architecture
![architecture](docs/architecture.svg)

# Broker
Since rust does not have a good etcd v3 client. We use a [proxy service](https://github.com/doyoubi/overmoon) written in Golang as a broker.

# Initialize Server-side Proxy
```
> ./undermoon  # runs on port 5299 and forward commands to 127.0.0.1:6379
> redis-cli -p 5299
127.0.0.1:5299> umctl setdb 1 noflags mydb 127.0.0.1:6379 0-8000
127.0.0.1:5299> umctl setpeer 1 noflags mydb 127.0.0.1:7000 8001-16383
127.0.0.1:5299> auth mydb
OK
127.0.0.1:5299> cluster nodes
mydb________________127.0.0.1:5299______ 127.0.0.1:5299 master - 0 0 1 connected 0-8000
mydb________________127.0.0.1:7000______ 127.0.0.1:7000 master - 0 0 1 connected 8001-16383
127.0.0.1:5299> get a
(error) MOVED 15495 127.0.0.1:7000
127.0.0.1:5299> set b 1
OK
```

## TODO
- ~~Basic proxy implementation~~ (done)
- Backend connection pool
- ~~Slot map and cluster map~~ (done)
- ~~Implement AUTH command to select database~~ (done)
- ~~Implement meta data manipulation api~~ (done)
- Implement coordinator (in progress)
- Implement Redis Replication Protocol
- Support slot migration via replication
- Optimize RESP parsing
- Implement CLUSTER SLOTS
- Implement commands to get proxy meta
- Track spawned futures
- Simple script to push configuration to proxy for demonstration
- Batch syscall
- ~~Add configuration~~ (done)
