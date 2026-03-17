# Redis Cluster, Sentinel, and Replication

## Cluster Mode

### Hash Slots

Redis Cluster uses 16,384 hash slots distributed across master nodes.

**Algorithm:** `HASH_SLOT = CRC16(key) mod 16384`
- CRC16 variant: XMODEM (Poly: 1021, Init: 0000)
- 14 out of 16 CRC16 output bits are used

### Hash Tags

Keys containing `{...}` pattern: only the substring between `{` and `}` is hashed.

```
{user:1000}:name  → hashes "user:1000"
{user:1000}:email → hashes "user:1000" (same slot)
```

This enables multi-key operations on keys that would otherwise land in different slots. Overusing hash tags can cause uneven distribution.

### Cross-Slot Errors

Multi-key commands fail with `CROSSSLOT Keys in request don't hash to the same slot` when keys span different slots. Solutions: hash tags, application-level grouping, or `CLUSTER KEYSLOT` to diagnose.

### CLUSTER Commands

**CLUSTER INFO** — vital cluster parameters: slots assigned, state (ok/pfail/fail), known nodes count.

**CLUSTER NODES** — per-node state in text format:
```
<node-id> <ip>:<port> <flags> <last-ping-sent> <last-pong-received> <config-epoch> <link-state> <slots>
```
Example:
```
d1861060fe6a... 127.0.0.1:6379 myself,master - 0 1318428930 1 connected 0-1364
3886e65cc906... 127.0.0.1:6380 master - 1318428930 1318428931 2 connected 1365-2729
```

**CLUSTER SLOTS** (deprecated) — slot ranges with master/replica addresses:
```
1) 1) (integer) 5461
   2) (integer) 10922
   3) 1) "127.0.0.1"     ← master
      2) (integer) 7001
   4) 1) "127.0.0.1"     ← replica
      2) (integer) 7004
```

**CLUSTER SHARDS** (replaces SLOTS) — more efficient representation with `slots` and `nodes` fields per shard.

**CLUSTER MYID** — returns the node's 160-bit hex identifier.

### MOVED Redirect

Permanent slot redirection:
```
-MOVED <slot> <endpoint>:<port>
```
Example: `-MOVED 3999 127.0.0.1:6381`

- Client should reissue query to specified node
- Client **should** update its slot-to-node mapping cache
- Empty endpoint means same host, different port

### ASK Redirect

Temporary redirection during slot migration:
```
-ASK <slot> <endpoint>:<port>
```

Key differences from MOVED:
- Applies **only** to the next query
- Client must NOT update slot mapping
- Client must send `ASKING` command before the redirected query
- `ASKING` sets a one-time flag allowing the node to serve an IMPORTING slot

### Slot Migration

1. Destination: `CLUSTER SETSLOT <slot> IMPORTING <source-id>`
2. Source: `CLUSTER SETSLOT <slot> MIGRATING <dest-id>`
3. Move keys: `CLUSTER GETKEYSINSLOT` → `MIGRATE`
4. Finalize: `CLUSTER SETSLOT <slot> NODE <dest-id>` on both nodes

**MIGRATING state:** existing keys processed normally, non-existent keys → ASK redirect
**IMPORTING state:** only serves queries preceded by ASKING command

### Client Topology Discovery

- Use `CLUSTER SHARDS` (preferred) or `CLUSTER SLOTS`/`CLUSTER NODES`
- Cache topology in-memory, refresh on MOVED/ASK redirections
- Refresh on `CLUSTERDOWN` error or persistent timeouts
- Use exponential backoff with jitter for retries

### Failure Detection

**PFAIL (Possible Failure):**
- Local flag — node unreachable for > NODE_TIMEOUT
- Not propagated cluster-wide

**FAIL:**
- Majority of masters report PFAIL within NODE_TIMEOUT * 2
- Propagated via FAIL messages
- Triggers replica promotion

### CLUSTERDOWN Error

`-CLUSTERDOWN The cluster is down` — returned when cluster has uncovered hash slots. Default behavior: entire cluster stops accepting queries if any slot is uncovered. Override with `cluster-require-full-coverage no`.

### configEpoch

Logical clock for the cluster. Higher epoch wins in configuration conflicts. Replicas increment epoch during election. If node is a replica, configEpoch reflects last known master's epoch.

---

## Sentinel Mode

### Overview

Redis Sentinel provides high availability for non-clustered Redis:
- **Monitoring** — continuously checks master and replica health
- **Automatic failover** — promotes replica when master fails
- **Configuration provider** — clients discover current master via Sentinel
- **Notification** — alerts when issues occur

Default port: 26379.

### Configuration

```
sentinel monitor mymaster 127.0.0.1 6379 2
sentinel down-after-milliseconds mymaster 60000
sentinel failover-timeout mymaster 180000
sentinel parallel-syncs mymaster 1
```

Minimum 3 Sentinel instances in independent failure domains.

### SENTINEL Commands

**Essential:**
- `SENTINEL MASTERS` — list all monitored masters
- `SENTINEL MASTER <name>` — master state and info
- `SENTINEL REPLICAS <name>` — list replicas
- `SENTINEL SENTINELS <name>` — list other Sentinels
- `SENTINEL GET-MASTER-ADDR-BY-NAME <name>` — current master (ip, port)
- `SENTINEL FAILOVER <name>` — force failover
- `SENTINEL CKQUORUM <name>` — check quorum reachability
- `SENTINEL MYID` — this Sentinel's ID

**Configuration:**
- `SENTINEL MONITOR <name> <ip> <port> <quorum>`
- `SENTINEL REMOVE <name>`
- `SENTINEL SET <name> <option> <value>`
- `SENTINEL CONFIG GET/SET`
- `SENTINEL FLUSHCONFIG` — rewrite config to disk
- `SENTINEL RESET` — refresh replica list

### Failure Detection: SDOWN and ODOWN

**SDOWN (Subjectively Down):**
- Local decision by single Sentinel
- No valid PING reply for `down-after-milliseconds`
- Valid replies: `+PONG`, `-LOADING`, `-MASTERDOWN`

**ODOWN (Objectively Down):**
- Consensus: quorum Sentinels agree about SDOWN
- Only applies to masters (replicas only reach SDOWN)
- Triggers failover attempt

### Quorum vs Majority

- **Quorum**: Sentinels needed to detect failure (mark ODOWN)
- **Majority**: Sentinels needed to authorize failover

Example with 5 Sentinels, quorum=2:
- 2 Sentinels agree master is down → ODOWN
- 3+ Sentinels must authorize → failover proceeds

### Failover Process

1. Quorum Sentinels agree master is unreachable → ODOWN
2. One Sentinel requests majority authorization
3. Select best replica: disconnection time → replica priority → replication offset → run ID
4. Send `REPLICAOF NO ONE` to selected replica
5. Reconfigure other replicas to follow new master
6. Broadcast new configuration via Pub/Sub

### Replica Selection Priority

1. Lowest `replica-priority` (0 = never promote)
2. Most replication offset processed
3. Smallest lexicographic run ID (tiebreaker)

### Authentication

Redis 6+: `sentinel auth-user <name> <username>` + `sentinel auth-pass <name> <password>`
Pre-6: `sentinel auth-pass <name> <password>`

---

## Replication

### Three Mechanisms

1. **Command stream**: master sends continuous command stream when well-connected
2. **Partial resynchronization**: on disconnection, sync only missed commands
3. **Full resynchronization**: complete dataset copy when partial sync impossible

### Full Synchronization

1. Master creates background RDB snapshot
2. Master buffers all new writes during snapshot
3. Master transfers RDB file to replica
4. Replica saves to disk, loads into memory
5. Master sends all buffered commands

### PSYNC Command

```
PSYNC <replication-id> <offset>
```

- **Replication ID**: pseudo-random string marking dataset history
- **Replication offset**: increments for every byte produced
- **Replication backlog**: circular buffer enabling partial resyncs

If backlog contains requested offset → partial sync. Otherwise → full sync.

### PSYNC2 (Redis 4.0+)

After failover, promoted replica remembers old master's replication ID and offset. Can perform partial resync with other replicas of the old master without requiring full sync.

Each master maintains:
- **Main ID**: current replication history
- **Secondary ID**: previous master's ID (for failovers)

### REPLCONF Command

Internal command for configuring replication:
- `REPLCONF listening-port <port>` — replica's port
- `REPLCONF ip-address <ip>` — replica's IP
- `REPLCONF capa <eof|psync2|rdb-channel-repl>` — capabilities
- `REPLCONF ack <offset> [fack <aofofs>]` — acknowledge processed offset

### WAIT Command (Synchronous Replication)

```
WAIT <num-replicas> <timeout>
```

- Blocks until N replicas acknowledge the write
- Returns number of replicas that acknowledged within timeout
- Does **not** guarantee data safety during failover
- Implementation: groups WAIT callers per event loop iteration, sends `REPLCONF GETACK` to replication stream

### REPLICAOF Command

- `REPLICAOF <host> <port>` — become replica of specified master
- `REPLICAOF NO ONE` — promote to master (keeps existing data)
- Switching masters: stops old replication, starts new, discards old dataset

### Key Characteristics

- **Non-blocking**: master continues handling queries during sync
- **Asynchronous**: master doesn't wait for per-command acknowledgment
- **Auto-reconnection**: replicas automatically reconnect on breaks
- **Cascading**: replicas can have sub-replicas (Redis 4.0+: all receive identical streams)

### INFO Replication Fields

**Always present:**
`role`, `master_replid`, `master_replid2`, `master_repl_offset`, `second_repl_offset`, `repl_backlog_active`, `repl_backlog_size`, `repl_backlog_first_byte_offset`, `repl_backlog_histlen`, `connected_slaves`, `master_failover_state`

**Replica-only:**
`master_host`, `master_port`, `master_link_status` (up/down), `master_last_io_seconds_ago`, `master_sync_in_progress`, `slave_read_repl_offset`, `slave_repl_offset`, `slave_priority`, `slave_read_only`, `replica_announced`

**During sync:**
`master_sync_total_bytes`, `master_sync_read_bytes`, `master_sync_left_bytes`, `master_sync_perc`, `master_sync_last_io_seconds_ago`

**Link down:**
`master_link_down_since_seconds`

---

## Implications for RedisBox Node Simulator

### Cluster Simulation Priorities
- Hash slot calculation (CRC16 mod 16384) — must be exact
- Hash tag parsing — `{...}` extraction
- MOVED/ASK redirects — correct format and behavior
- CLUSTER INFO/NODES/SHARDS responses
- Cross-slot error detection
- Slot migration state machine (MIGRATING/IMPORTING)

### Sentinel Simulation Priorities
- SENTINEL commands (MASTER, MASTERS, REPLICAS, GET-MASTER-ADDR-BY-NAME)
- SDOWN/ODOWN state machine
- Failover trigger and replica selection

### Replication Simulation Priorities
- REPLICAOF command handling
- INFO replication section
- WAIT command (with simulated acknowledgment)
- PSYNC handshake (for clients that expect it)

---

[← Back to Node Simulator Research](README.md)
