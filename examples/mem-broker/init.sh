#!/usr/bin/env bash

API_VERSION='v3'

# Add proxy to mem-broker
curl -XPOST -H 'Content-Type: application/json' "http://localhost:7799/api/${API_VERSION}/proxies/meta" -d '{"proxy_address": "server_proxy1:6001", "nodes": ["server_proxy1:7001", "server_proxy1:7002"]}'
curl -XPOST -H 'Content-Type: application/json' "http://localhost:7799/api/${API_VERSION}/proxies/meta" -d '{"proxy_address": "server_proxy2:6002", "nodes": ["server_proxy2:7003", "server_proxy2:7004"]}'
curl -XPOST -H 'Content-Type: application/json' "http://localhost:7799/api/${API_VERSION}/proxies/meta" -d '{"proxy_address": "server_proxy3:6003", "nodes": ["server_proxy3:7005", "server_proxy3:7006"]}'
curl -XPOST -H 'Content-Type: application/json' "http://localhost:7799/api/${API_VERSION}/proxies/meta" -d '{"proxy_address": "server_proxy4:6004", "nodes": ["server_proxy4:7007", "server_proxy4:7008"]}'
curl -XPOST -H 'Content-Type: application/json' "http://localhost:7799/api/${API_VERSION}/proxies/meta" -d '{"proxy_address": "server_proxy5:6005", "nodes": ["server_proxy5:7009", "server_proxy5:7010"]}'
curl -XPOST -H 'Content-Type: application/json' "http://localhost:7799/api/${API_VERSION}/proxies/meta" -d '{"proxy_address": "server_proxy6:6006", "nodes": ["server_proxy6:7011", "server_proxy6:7012"]}'
