# Slots Migration

Goals:
- Simple
- Fast

The migration process is based on the following Redis commands:
- SCAN
- DUMP
- PTTL
- RESTORE

The SCAN command has a great property that
it can guarantee that all the keys set before the first SCAN command will finally be returned,
for multiple times sometimes though. We can perform a 3 stage migration to mimic the replication.

- Wait for all the commands to be finished by Redis.
- Redirect all the read and write operation to destination Redis.
  The destination Redis will need to dump the data of the key from the source Redis
  before processing the commands.
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
- The migrating proxy use `SCAN`, `PTTL`, `DUMP`, `RESTORE`
  to forward all the data inside the migrating slot ranges to peer importing Redis.
  The `RESTORE` does not set the `REPLACE` flag.
- When the importing proxy processes commands, no matter read or write operation, it will first
  - Send `EXISTS` and the processed command to local importing Redis,
    if `EXISTS` returns true, forward the command to the local importing Redis.
  - Send `DUMP` and `PTTL`to migrating Redis to get the data.
  - Apply the data and processed command to local Redis.
  - Forward the command to the local importing Redis
- When the migrating proxy finishes the scanning,
  it propose the `CommitSwitch` to the importing proxy.
  Then the importing proxy will only need to process the command in local Redis.
- Notify `coordinator` and wait for the final commit by `UMCTL SETCLUSTER`.
