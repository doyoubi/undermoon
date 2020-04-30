# Memory Broker API
Memory Broker API is a superset of [Broker HTTP API](./broker_http_api.md).
It includes the following additional APIs.

#### Get the version of undermoon
`GET` /api/v2/version

##### Success
```
HTTP 200

0.3.0
```

#### Get inner metadata
This is not a stable API and should only be used for debugging.

`GET` /api/v2/metadata
##### Success
```
HTTP 200

{
  "version": "mem-broker-0.1",
  "global_epoch": 0,
  "clusters": {},
  "all_proxies": {},
  "failed_proxies": [],
  "failures": {}
}
```

#### Restore metadata
Restore all the metadata.

`PUT` /api/v2/metadata
##### Request
```
{
  "version": "mem-broker-0.1",
  "global_epoch": 0,
  "clusters": {},
  "all_proxies": {},
  "failed_proxies": [],
  "failures": {}
}
```

##### Success
```
HTTP 200
```

##### Error
```
HTTP 409 { "error": "INVALID_META_VERSION" }
```

#### Create cluster
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

#### Delete cluster
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

#### Add nodes to cluster
`PATCH` /api/v2/clusters/nodes/<cluster_name>

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

#### Add nodes to cluster if needed
This API is idempotent compared to the previous one.
`PUT` /api/v2/clusters/nodes/<cluster_name>

##### Request
```json
{
    "cluster_node_number": 8
}
```
- `node_number` should be the multiples of `4`.

##### Success
```
HTTP 200
```

##### Error
```
HTTP 409 { "error": "NODE_NUM_ALREADY_ENOUGH }
HTTP 400 { "error": "INVALID_CLUSTER_NAME" }
HTTP 400 { "error": "INVALID_NODE_NUMBER" }
HTTP 404 { "error": "CLUSTER_NOT_FOUND" }
HTTP 409 { "error": "ALREADY_EXISTED" }
HTTP 409 { "error": "NO_AVAILABLE_RESOURCE" }
HTTP 409 { "error": "MIGRATION_RUNNING" }
```

#### Delete Unused nodes in a cluster
`DELETE` /api/v2/clusters/free_nodes/<cluster_name>

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

#### Start migration for scaling out
Note that you need to call `Add nodes to cluster` beforehand.

`POST` /api/v2/clusters/migrations/expand/<cluster_name>

##### Success
```
HTTP 200
```

##### Error
```
HTTP 400 { "error": "INVALID_CLUSTER_NAME" }
HTTP 404 { "error": "CLUSTER_NOT_FOUND" }
HTTP 409 { "error": "FreeNodeFound" }
HTTP 409 { "error": "MIGRATION_RUNNING" }
```

#### Start migration for scaling down
Note that this will not delete the nodes.
You still need to call the `Delete Unused nodes in a cluster` API after migration is done.

`POST` /api/v2/clusters/migrations/shrink/<cluster_name>/<new_cluster_nodes_number>
  
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

#### Change cluster config
`PATCH` /api/v2/clusters/config/<cluster_name>

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

#### Add proxy
`POST` /api/v2/proxies/meta

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

#### Delete proxy
`DELETE` /api/v2/proxies/meta/{proxy_address}

##### Success
```
HTTP 200
```

##### Error
```
HTTP 404 { "error": "PROXY_NOT_FOUND" }
HTTP 409 { "error": "IN_USE" }
```

#### Balance Masters
`PUT` /api/v2/clusters/balance/<cluster_name>

##### Success
```
HTTP 200
```

##### Error
```
HTTP 400 { "error": "INVALID_CLUSTER_NAME" }
HTTP 404 { "error": "CLUSTER_NOT_FOUND" }
```

#### Force to bump all epoch
Update all the epoch to the specified new epoch.
This should only be used when metadata is stale after failover
to make the metadata be able synchronized to server proxies again.
`PUT` /api/v2/epoch/<new_epoch>

##### Success
```
HTTP 200
```

##### Error
```
HTTP 409 { "error": "EPOCH_SMALLER_THAN_CURRENT" }
```

#### Check enough resources for failures
`POST` /api/v2/resources/failures/check

Empty `hosts_cannot_fail` means we still have enough resources for handling failures.

If `hosts_cannot_fail` is not empty, we should add more server proxies.

##### Success
```
HTTP 200
{
    "hosts_cannot_fail": ["host1", "host2", ...],
}
```
