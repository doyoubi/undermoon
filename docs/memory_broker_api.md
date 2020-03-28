# Memory Broker API
Memory Broker API is a superset of [Broker HTTP API](./broker_http_api.md).
It includes the following additional APIs.

#### (1) Get the version of undermoon
`GET` /api/v2/version

##### Success
```
HTTP 200

0.3.0
```

#### (2) Get inner metadata
This is not a stable API and should only be used for debugging.

`GET` /api/v2/metadata
##### Success
```
HTTP 200

{
  "global_epoch": 0,
  "clusters": {},
  "all_proxies": {},
  "failed_proxies": [],
  "failures": {}
}
```

#### (3) Create cluster
`POST` /api/v2/clusters/meta/<cluster_name>

##### Request
```json
{
    "node_number": 8,
}
```
- `cluster_name`
  - 0 < length <= 30
  - only contains alphabetic and numeric ascii or '@', '-', '_'
- `node_number` should be the multiples of `4`.  

##### Success
```
HTTP 200
```

##### Error
```
HTTP 400 { "error": "INVALID_CLUSTER_NAME" }
HTTP 400 { "error": "INVALID_NODE_NUMBER" }
HTTP 409 { "error": "ALREADY_EXISTED" }
HTTP 409 { "error": "NO_AVAILABLE_RESOURCE" }
```

#### (4) Delete cluster
`DELETE` /api/v2/clusters/meta/<cluster_name>

##### Success
```
HTTP 200
```

##### Error
```
HTTP 400 { "error": "INVALID_CLUSTER_NAME" }
HTTP 404 { "error": "CLUSTER_NOT_FOUND" }
```

#### (5) Add nodes to cluster
`PATCH` /clusters/nodes/<cluster_name>

##### Request
```json
{
    "node_number": 8
}
```
- `node_number` should be the multiples of `4`.
  
##### Success
```
HTTP 200
```

##### Error
```
HTTP 400 { "error": "INVALID_CLUSTER_NAME" }
HTTP 400 { "error": "INVALID_NODE_NUMBER" }
HTTP 404 { "error": "CLUSTER_NOT_FOUND" }
HTTP 409 { "error": "ALREADY_EXISTED" }
HTTP 409 { "error": "NO_AVAILABLE_RESOURCE" }
HTTP 409 { "error": "MIGRATION_RUNNING" }
```

#### (6) Delete Unused nodes in a cluster
`DELETE` /clusters/free_nodes/<cluster_name>

##### Success
```
HTTP 200
```

##### Error
```
HTTP 400 { "error": "INVALID_CLUSTER_NAME" }
HTTP 404 { "error": "CLUSTER_NOT_FOUND" }
HTTP 409 { "error": "FREE_NODE_NOT_FOUND" }
HTTP 409 { "error": "MIGRATION_RUNNING" }
```

#### (7) Start migration
`POST` /clusters/migrations/<cluster_name>
  
##### Success
```
HTTP 200
```

##### Error
```
HTTP 400 { "error": "INVALID_CLUSTER_NAME" }
HTTP 404 { "error": "CLUSTER_NOT_FOUND" }
HTTP 409 { "error": "SLOTS_ALREADY_EVEN" }
HTTP 409 { "error": "MIGRATION_RUNNING" }
```

#### (8) Change cluster config
`PATCH` /clusters/config/<cluster_name>

##### Request
```
{
    "compression_strategy": "disabled" | "set_get_only" | "allow_all"
}
```

##### Success
```
HTTP 200
```

##### Error
```
HTTP 400 { "error": "INVALID_CLUSTER_NAME" }
HTTP 404 { "error": "CLUSTER_NOT_FOUND" }
HTTP 409 {
    "error": "INVALID_CONFIG",
    "key": "compression_strategy",
    "value": "xxxx",
    "message": "xxxx"
}
```

#### (9) Add proxy
`POST` /proxies/meta

##### Request
```
{
    "proxy_address": "127.0.0.1:7000",
    "nodes": ["127.0.0.1:6000", "127.0.0.1:6001"],
    "host": "127.0.0.1" | null
}
```

##### Success
```
HTTP 200
```

##### Error
```
HTTP 400 { "error": "INVALID_PROXY_ADDRESS" }
HTTP 409 { "error": "ALREADY_EXISTED" }
```

#### (10) Delete proxy
`DELETE` /proxies/meta/{proxy_address}

##### Success
```
HTTP 200
```

##### Error
```
HTTP 404 { "error": "PROXY_NOT_FOUND" }
HTTP 409 { "error": "IN_USE" }
```
