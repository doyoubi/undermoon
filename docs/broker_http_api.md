# Broker HTTP API

All the payload of request and response should be in JSON format
and use the HTTP 200 to indicate success or failure.

HTTP Broker should at least implement the following apis to work with Coordinator:

##### (1) GET /api/v2/clusters/names?offset=<int>&limit=<int>
Get all the cluster names.
`offset` starts from zero.
```
Response:
{
    "names": ["cluster_name1", ...],
}
```

##### (2) GET /api/v2/clusters/meta/<cluster_name>
Get the meta data of <cluster_name>.
```
Response:
If the cluster exists:
{
    "cluster": {
        "name": "cluster_name1",
        "epoch": 1,
        "nodes": [{
            "address": "redis9:6379",
            "proxy_address": "server_proxy5:6005",
            "cluster_name": "mycluster",
            "slots": [{
                "range_list": [[0, 8191]],
                "tag": "None"
            }],
            "repl": {
                "role": "master",
                "peers": [{
                    "node_address": "redis2:6379",
                    "proxy_address": "server_proxy1:6001"
                }]
            }
          }, ...],
        "config": {
            "compression_strategy": "disabled"
        }
    }
}

If not:
{ "cluster": null }
```

##### (3) GET /api/v2/proxies/addresses?offset=<int>&limit=<int>
Get all the server-side proxy addresses.
`offset` starts from zero.
```
Response:
{
    "addresses": ["server_proxy_address1", ...],
}
```

##### (4) GET /api/v2/proxies/meta/<server_proxy_address>
Get the meta data of <server_proxy_address>
```
Response:
If the proxy exists:
{
    "proxy": {
        "address": "server_proxy_address1",
        "epoch": 1,
        "nodes": [{
            "address": "127.0.0.1:7001",
            "proxy_address": "127.0.0.1:6001",
            "cluster_name": "cluster_name1",
            "repl": {
                "role": "master",
                "peers": [{
                    "node_address": "127.0.0.1:7002",
                    "proxy_address": "127.0.0.1:7003",
                }...]
            },
            "slots": [{
                "range_list": [[0, 5000]],
                "tag": "None"
            }, ...]
        }, ...],
        "free_nodes": ["127.0.0.1:7004"],  // For free proxies
        "peers": [{
            "proxy_address": "127.0.0.1:6001",
            "cluster_name": "cluster_name1",
            "slots": [{
                "range_list": [[0, 5000]],
                "tag": "None"
            }, ...]
        }, ...],
        "clusters_config": {
            "cluster_name1": {
                "compression_strategy": "disabled"
            }
        }
    }
}
If not:
{ "proxy": null }
```

##### (5) POST /api/v2/failures/<server_proxy_address>/<reporter_id>
Report a suspected failure and tag it use a unique <reporter_id> for every Coordinator.
In the memory broker implementation, if `enable_ordered_proxy` is on,
this API won't do anything and return 200.
```
Response:
empty payload
```

##### (6) GET /api/v2/failures
Get all the failures reported by coordinator but not committed to be failed yet.
It's used by coordinator and you probably need to use (9) instead.
```
Response:
{
    "addresses": ["server_proxy_address1", ...],
}
```

##### (7) POST /api/v2/proxies/failover/<server_proxy_address>
Try to do the failover for the specified proxy.
In the memory broker implementation, if `enable_ordered_proxy` is on,
this API will always return 409.
```
Request:
empty payload

Response:
If success:
Proxy is being used by a cluster.
{
    "proxy": {
        "address": "server_proxy_address1",
        "epoch": 1,
        "nodes": [{
            "address": "127.0.0.1:7001",
            "proxy_address": "127.0.0.1:6001",
            "cluster_name": "cluster_name1",
            "repl": {
                "role": "master",
                "peers": [{
                    "node_address": "127.0.0.1:7002",
                    "proxy_address": "127.0.0.1:7003",
                }...]
            },
            "slots": [{
                "range_list": [[0, 5000]],
                "tag": "None"
            }, ...]
        }, ...]
    }
}

Proxy is not in use:
{
    "proxy": null
}

If not:
HTTP 409
```

##### (8) PUT /api/v2/clusters/migrations
Try to commit the migration.
The memory broker implementation also cleans up the free nodes after the migration is done.
```
Request:
{
    "cluster_name": "mydb",
    "slot_range": {
        "range_list": [[0, 5000]],
        "tag": {
            "Migrating": {
                "epoch": 233,
                "src_proxy_address": "127.0.0.1:7000",
                "src_node_address": "127.0.0.1:7001",
                "dst_proxy_address": "127.0.0.2:7000",
                "dst_node_address": "127.0.0.2:7001"
            }
        }
    }
}

Response:
{
    "addresses": ["server_proxy_address1", ...],
}
```

##### (9) GET /api/v2/proxies/failed/addresses
Get all the failed proxies.
```
Response:
{
    "addresses": ["server_proxy_address1", ...],
}
```
