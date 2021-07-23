# Slots Migration

Goals:
- Simple
- Fast

The migration process is based on the following Redis commands:
- SCAN
- DUMP
- PTTL
- RESTORE
- DEL

The SCAN command has a great property that
it can guarantee that all the keys set before the first SCAN command will finally be returned,
for multiple times sometimes though. We can perform a 3 stage migration to mimic the replication.

- Wait for all the commands to be finished by Redis.
- Redirect all the read and write operation to destination Redis.
  The destination Redis will need to dump the data of the key from the source Redis
  before processing the commands if the key does not exist.
- Start the scanning and forward the data to peer Redis.

## Detailed Steps
- The migrating proxy check whether the importing proxy
  has also received the migration task by a `PreCheck` command.
- The migrating proxy blocks all the newly added commands to `Queue`,
  and wait for the existing commands to be finished.
- The migrating proxy send a `TmpSwitch` command to importing proxy.
  Upon receiving this command,
  the import proxy start to process the keys inside the importing slot ranges.
  When the command returns, the migrating proxy release all the commands inside `Queue` and redirect them to the importing proxy.
- The migrating proxy use `SCAN`, `PTTL`, `DUMP`, `RESTORE`, `DEL`
  to forward all the data inside the migrating slot ranges to peer importing Redis.
  The `RESTORE` does not set the `REPLACE` flag.
- When the importing proxy processes commands, no matter read or write operation, it will first
  - If the command will not delete the key, get the `key lock`,
    - Send `EXISTS` and the processed command to local importing Redis,
      if `EXISTS` returns true, forward the command to the local importing Redis.
    - If `EXISTS` returns false,
      send `DUMP` and `PTTL`to migrating Redis to get the data,
      and `RESTORE` the data and forward the command to local Redis.
      Then finally forward the command to the local importing Redis.
  - If the command can possibly delete the key,
    get the `key lock` and
    send `UMSYNC` to the migrating proxy to let the migrating proxy
    use `DUMP`, `PTTL`, `RESTORE`, `DEL` to transfer the key to the importing proxy.
    Then finally forward the command to the local importing Redis
- When the migrating proxy finishes the scanning,
  it proposes the `CommitSwitch` to the importing proxy.
  Then the importing proxy will only need to process the command in local Redis.
- Notify `coordinator` and wait for the final commit by `UMCTL SETCLUSTER`.

## Why it's designed in this way.
The overall migration process is based on the following command `SCAN`, `PTTL`, `DUMP`, `RESTORE`, `DELETE`.
Only the `RESTORE` command is sent to importing server proxy, so for better performance,
this scanning and transferring should be executed in the migrating server proxy.

Since the scanning and transferring occupy a lot of CPU resource on both server proxy and Redis,
other workload should better be processed on the importing proxy.
So at the very beginning we transferred all the slots directly to the importing proxy.

At this point, the importing proxy still only have a small part of the data.
When it needs to process the commands on the newly added slots,
it needs to use `PTTL`, `DUMP`, `RESTORE` to pull the data from the migrating server proxy
before processing the requests.
It also need to send `DELETE` to remove the key.

Note that for any commands that won't delete the key,
it would still be correct to `RESTORE` multiple times for the same key since it's idempotent.
So just letting the importing proxy to pull the data won't cause any inconsistency.

But for those commands that could possibly remove the keys such as `DEL`, `EXPIRE`, `LPOP`,
just letting importing proxy pull the data could result in the following cases:
- The key get deleted
- There's another `RESTORE` command recovers the key.

So when pulling the data it needs to be isolated from
- Other `RESTORE` commands in the importing proxy.
- `SCAN` and `RESTORE` in the migrating proxy.

Thus we need key lock in the importing proxy,
and need the migrating proxy help us to send the data instead of pulling from the importing proxy
so that the operation for this key could only be processed in a sequential way.

## The Performance.
As a result, during the migration, the workload for the migrating and importing proxies is quite balanced.
The migrating proxy uses 130% of the CPU and the importing proxy uses 80% of the CPU.

And it only took **less a minute** to migrate 1G data.

In the test, during the migration while benchmarking,
the throughput reduces from 50k to 28k and gradually increases to 40k.
This is because the `SCAN`, `DUMP`, `RESTORE` costs a lot of throughput
in Redis in both the migrating and importing proxy.
But once the key get migrated to the importing server proxy,
it only need to send an additional `EXISTS` command before the request.

After committing the migration, the throughput will double.
