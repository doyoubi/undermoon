# Best Practice

#### Consider Multi-threaded Redis First
Now both Redis 6 and [KeyDB](https://github.com/JohnSully/KeyDB) support multi-threaded.
They should be your first choice before looking into the distributed Redis solution,
because any existing cluster solution is not trivial to maintain or it would be expensive
if you use enterprise cloud.

If your best machine still can't store all the data or support your high throughput,
then you can take your time on the cluster solution.

#### Use Small Redis
Each Redis instance should be specified a `max_memory` not larger than 8G.
`2G` is a good `max_memory` for each instance as it will not have great impact on the network during replication.
And it would be much easier to scale for small Redis.

`Undermoon` is designed to help you to maintain clusters with hundreds of nodes.

#### Trigger Migration with Cautions
Running migration could decrease the max throughput and increase the latency.
Try to employ a good capacity planning strategy and only trigger migration when the throughput is low.

#### Prefer Pipeline to Multi-key Commands
Multi-key commands are much harder to optimize for the proxy. Use pipeline instead of multi-key commands for better performance.
