#!/usr/bin/env bash

API_VERSION='v2'

# Add proxy to mem-broker
curl -XPOST -H 'Content-Type: application/json' "http://localhost:7799/api/${API_VERSION}/proxies/meta" -d '{"proxy_address": "server_proxy1:6001", "nodes": ["redis1:6379", "redis2:6379"]}'
curl -XPOST -H 'Content-Type: application/json' "http://localhost:7799/api/${API_VERSION}/proxies/meta" -d '{"proxy_address": "server_proxy2:6002", "nodes": ["redis3:6379", "redis4:6379"]}'
curl -XPOST -H 'Content-Type: application/json' "http://localhost:7799/api/${API_VERSION}/proxies/meta" -d '{"proxy_address": "server_proxy3:6003", "nodes": ["redis5:6379", "redis6:6379"]}'
curl -XPOST -H 'Content-Type: application/json' "http://localhost:7799/api/${API_VERSION}/proxies/meta" -d '{"proxy_address": "server_proxy4:6004", "nodes": ["redis7:6379", "redis8:6379"]}'
curl -XPOST -H 'Content-Type: application/json' "http://localhost:7799/api/${API_VERSION}/proxies/meta" -d '{"proxy_address": "server_proxy5:6005", "nodes": ["redis9:6379", "redis10:6379"]}'
curl -XPOST -H 'Content-Type: application/json' "http://localhost:7799/api/${API_VERSION}/proxies/meta" -d '{"proxy_address": "server_proxy6:6006", "nodes": ["redis11:6379", "redis12:6379"]}'
