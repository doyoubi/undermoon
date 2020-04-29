| COMMAND | SUPPORTED | DESCRIPTION |
|---|---|---|
| append | True |  |
| asking | True | This is an no-op. It only returns OK. |
| auth | False | This command is reserved for future use. |
| bgrewriteaof | False |  |
| bgsave | False |  |
| bitcount | True |  |
| bitfield | True |  |
| bitop | False |  |
| bitpos | True |  |
| blpop | True | User MUST specify timeout. |
| brpop | True | User MUST specify timeout. |
| brpoplpush | True | User MUST specify timeout. |
| bzpopmax | False |  |
| bzpopmin | False |  |
| client | False |  |
| cluster | True | Only support the following sub commands: NODES, SLOTS, KEYSLOT. |
| command | False |  |
| config | True |  |
| dbsize | False |  |
| debug | False |  |
| decr | True |  |
| decrby | True |  |
| del | True |  |
| discard | False |  |
| dump | True |  |
| echo | True |  |
| eval | True | All the keys should be in the same slot. |
| evalsha | False |  |
| exec | False |  |
| exists | True |  |
| expire | True |  |
| expireat | True |  |
| flushall | False |  |
| flushdb | False |  |
| geoadd | True |  |
| geodist | True |  |
| geohash | True |  |
| geopos | True |  |
| georadius | True |  |
| georadius_ro | True |  |
| georadiusbymember | True |  |
| georadiusbymember_ro | True |  |
| get | True |  |
| getbit | True |  |
| getrange | True |  |
| getset | True |  |
| hdel | True |  |
| hexists | True |  |
| hget | True |  |
| hgetall | True |  |
| hincrby | True |  |
| hincrbyfloat | True |  |
| hkeys | True |  |
| hlen | True |  |
| hmget | True |  |
| hmset | True |  |
| host: | False |  |
| hscan | True |  |
| hset | True |  |
| hsetnx | True |  |
| hstrlen | True |  |
| hvals | True |  |
| incr | True |  |
| incrby | True |  |
| incrbyfloat | True |  |
| info | True |  |
| keys | False |  |
| lastsave | False |  |
| latency | False |  |
| lindex | True |  |
| linsert | True |  |
| llen | True |  |
| lolwut | False |  |
| lpop | True |  |
| lpush | True |  |
| lpushx | True |  |
| lrange | True |  |
| lrem | True |  |
| lset | True |  |
| ltrim | True |  |
| memory | False |  |
| mget | True |  |
| migrate | False |  |
| module | False |  |
| monitor | False |  |
| move | False |  |
| mset | True |  |
| msetnx | False |  |
| multi | False |  |
| object | False |  |
| persist | True |  |
| pexpire | True |  |
| pexpireat | True |  |
| pfadd | True |  |
| pfcount | True | All the keys should be in the same slot. |
| pfdebug | False |  |
| pfmerge | True | All the keys should be in the same slot. |
| pfselftest | False |  |
| ping | True |  |
| post | False |  |
| psetex | True |  |
| psubscribe | False |  |
| psync | False |  |
| pttl | True |  |
| publish | False |  |
| pubsub | False |  |
| punsubscribe | False |  |
| randomkey | False |  |
| readonly | False |  |
| readwrite | False |  |
| rename | True | All the keys should be in the same slot. |
| renamenx | False | All the keys should be in the same slot. |
| replconf | False |  |
| replicaof | False |  |
| restore | True |  |
| restore-asking | False |  |
| role | False |  |
| rpop | True |  |
| rpoplpush | True | All the keys should be in the same slot. |
| rpush | True |  |
| rpushx | True |  |
| sadd | True |  |
| save | False |  |
| scan | False |  |
| scard | True |  |
| script | False |  |
| sdiff | True | All the keys should be in the same slot. |
| sdiffstore | True | All the keys should be in the same slot. |
| select | False |  |
| set | True |  |
| setbit | True |  |
| setex | True |  |
| setnx | True |  |
| setrange | True |  |
| shutdown | False |  |
| sinter | True | All the keys should be in the same slot. |
| sinterstore | True | All the keys should be in the same slot. |
| sismember | True |  |
| slaveof | False |  |
| slowlog | False |  |
| smembers | True |  |
| smove | True | All the keys should be in the same slot. |
| sort | True |  |
| spop | True |  |
| srandmember | True |  |
| srem | True |  |
| sscan | True |  |
| strlen | True |  |
| subscribe | False |  |
| substr | False |  |
| sunion | True | All the keys should be in the same slot. |
| sunionstore | False | All the keys should be in the same slot. |
| swapdb | False |  |
| sync | False |  |
| time | False |  |
| touch | True | All the keys should be in the same slot. |
| ttl | True |  |
| type | True |  |
| unlink | True | All the keys should be in the same slot. |
| unsubscribe | False |  |
| unwatch | False |  |
| wait | False |  |
| watch | False |  |
| xack | True |  |
| xadd | True |  |
| xclaim | True |  |
| xdel | True |  |
| xgroup | False |  |
| xinfo | False |  |
| xlen | True |  |
| xpending | True |  |
| xrange | True |  |
| xread | False |  |
| xreadgroup | False |  |
| xrevrange | True |  |
| xsetid | False |  |
| xtrim | True |  |
| zadd | True |  |
| zcard | True |  |
| zcount | True |  |
| zincrby | True |  |
| zinterstore | True | All the keys should be in the same slot. |
| zlexcount | True |  |
| zpopmax | True |  |
| zpopmin | True |  |
| zrange | True |  |
| zrangebylex | True |  |
| zrangebyscore | True |  |
| zrank | True |  |
| zrem | True |  |
| zremrangebylex | True |  |
| zremrangebyrank | True |  |
| zremrangebyscore | True |  |
| zrevrange | True |  |
| zrevrangebylex | True |  |
| zrevrangebyscore | True |  |
| zrevrank | True |  |
| zscan | True |  |
| zscore | True |  |
| zunionstore | True | All the keys should be in the same slot. |