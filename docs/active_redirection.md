# Active Redirection Mode
When server proxies run in `active redirection` mode,
they will expose themselves in a single Redis client protocol.
Clients don't need to support [Redis Cluster Protocol](./redis_cluster_protocol.md).

## Enable Active Redirection
Inside the server proxy config file`server-proxy.toml`,
set `active_redirection` to `true`,
or use environment variable `UNDERMOON_ACTIVE_REDIRECTION=true`.

## How it works?
When `active redirection` mode is enabled,
a server proxy will automatically redirect the requests to other server proxies.
Then if needed, other server proxies will keep redirecting the requests
until they find the owner or exceed maximum redirection limit
set by `max_redirections` in server proxy config file.
