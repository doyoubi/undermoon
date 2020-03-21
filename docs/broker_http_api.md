# Broker HTTP API

All the payload of request and response should be in JSON format
and use the HTTP 200 to indicate success or failure.

HTTP Broker should at least implement the following apis to work with Coordinator:

##### (1) GET /api/v2/clusters/names
Get all the cluster names.
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
                "start": 0,
                "end": 5000,
                "tag": "None"
            }, ...]
        }, ...],
        "config": {
            "compression_strategy": "disabled"
        }
    }
}

If not:
{ "cluster": null }
```

##### (3) GET /api/v2/proxies/addresses
Get all the server-side proxy addresses.
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
                "start": 0,
                "end": 5000,
                "tag": "None"
            }, ...]
        }, ...],
        "free_nodes": ["127.0.0.1:7004"],
        "peers": [{
            "proxy_address": "127.0.0.1:6001",
            "cluster_name": "cluster_name1",
            "slots": [{
                "start": 0,
                "end": 5000,
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
```
Response:
empty payload
```

##### (6) GET /api/v2/failures
Get all the failures.
```
Response:
{
    "addresses": ["server_proxy_address1", ...],
}
```

##### (7) POST /api/v2/proxies/failover/<server_proxy_address>
Try to do the failover for the specified proxy.
```
Request:
empty payload

Response:
If success:
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
                "start": 0,
                "end": 5000,
                "tag": "None"
            }, ...]
        }, ...]
    }
}
If not:
HTTP 409
```

##### (8) PUT /api/v2/clusters/migrations
Try to commit the migration
```
Request:
{
    "cluster_name": "mydb",
    "slot_range": {
        "start": 0,
        "end": 5000,
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
