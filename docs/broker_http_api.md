# Broker HTTP API

All the payload of request and response should be in JSON format
and use the HTTP 200 to indicate success or failure.

HTTP Broker should at least implement the following apis to work with Coordinator:

##### (1) GET /api/clusters/names
Get all the cluster names.
```
Response:
{
    "names": ["cluster_name1", ...],
}
```

##### (2) GET /api/clusters/<cluster_name>/meta
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
        }, ...]
    }
}

If not:
{ "cluster": null }
```

##### (3) GET /api/hosts/addresses
Get all the server-side proxy addresses.
```
Response:
{
    "addresses": ["server_proxy_address1", ...],
}
```

##### (4) GET /api/hosts/addresses/<server_proxy_address>
Get the meta data of <server_proxy_address>
```
Response:
If the host(or proxy) exists:
{
    "host": {
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
{ "host": null }
```

##### (5) POST /api/failures/<server_proxy_address>/<reporter_id>
Report a suspected failure and tag it use a unique <reporter_id> for every Coordinator.
```
Response:
empty payload
```

##### (6) GET /api/failures
Get all the failures.
```
Response:
{
    "addresses": ["server_proxy_address1", ...],
}
```

##### (7) PUT /api/clusters/nodes
Try to do the failover for the specified node.
```
Request:
{
    "cluster_epoch": 1,
    "node": {
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
    }
}

Response:
empty payload
```

##### (7) PUT /api/clusters/migrations
Try to do the migration
```
Request:
{
    "db_name": "mydb",
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
