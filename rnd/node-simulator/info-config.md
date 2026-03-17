# Redis INFO and CONFIG Commands

## INFO Command

Returns server statistics in `key:value` format with sections marked by `# SectionName` headers. Lines terminated by `\r\n`.

### Usage

```
INFO                     # all default sections
INFO server              # specific section
INFO memory clients      # multiple sections
INFO everything          # all sections including optional
```

### Sections and Fields

#### Server
- `redis_version` — version number
- `redis_git_sha1` — Git commit SHA
- `redis_build_id` — build identifier
- `redis_mode` — standalone, sentinel, or cluster
- `os` — operating system
- `arch_bits` — 32 or 64
- `process_id` — PID
- `uptime_in_seconds`, `uptime_in_days`
- `tcp_port` — listen port
- `executable` — path to binary
- `config_file` — path to config

#### Clients
- `connected_clients` — active client connections
- `blocked_clients` — clients in blocking commands
- `tracking_clients` — clients using CLIENT TRACKING
- `pubsub_clients` — clients in pub/sub mode
- `watching_clients` — clients with WATCH keys

#### Memory
- `used_memory` — total bytes allocated
- `used_memory_human` — human-readable (e.g., "2.5M")
- `used_memory_rss` — resident set size (physical RAM)
- `used_memory_peak` — peak since startup
- `maxmemory` — configured limit (0 = unlimited)
- `maxmemory_human`
- `used_memory_overhead` — internal structures
- `used_memory_dataset` — actual data
- `mem_fragmentation_ratio` — RSS / used_memory
- `mem_fragmentation_bytes`

#### Persistence
- `loading` — 1 if RDB load in progress
- `rdb_changes_since_last_save`
- `rdb_bgsave_in_progress`
- `rdb_last_save_time` — Unix timestamp
- `rdb_last_bgsave_status` — "ok" or "err"
- `rdb_last_bgsave_time_sec`
- `aof_enabled` — 0 or 1
- `aof_rewrite_in_progress`
- `aof_current_size` — bytes
- `aof_base_size` — size after last rewrite
- `aof_last_bgrewrite_status`

#### Stats
- `total_connections_received` — total since startup
- `total_commands_processed`
- `instantaneous_ops_per_sec`
- `total_net_input_bytes`, `total_net_output_bytes`
- `keyspace_hits`, `keyspace_misses`
- `instantaneous_input_kbps`, `instantaneous_output_kbps`
- `evicted_keys` — removed due to maxmemory
- `expired_keys` — removed due to expiration

#### Replication
- `role` — "master", "slave", or "sentinel"
- `connected_slaves`
- `master_replid` — replication ID for partial resync
- `master_replid2` — second replication ID (failover)
- `master_repl_offset` — current offset
- `repl_backlog_active`, `repl_backlog_size`
- `repl_backlog_first_byte_offset`, `repl_backlog_histlen`

#### CPU
- `used_cpu_sys`, `used_cpu_user` — seconds
- `used_cpu_sys_children`, `used_cpu_user_children`

#### Commandstats
Format: `cmdstat_<COMMAND>: calls=X,usec=Y,usec_per_call=Z,rejected_calls=A,failed_calls=B`

#### Keyspace
Format: `db<N>: keys=123,expires=45,avg_ttl=5000`

#### Other Sections
- **Cluster**: `cluster_enabled`, `cluster_state`, `cluster_slots_assigned`, etc.
- **Modules**: module name and version for each loaded module
- **Latencystats**: command latency percentiles (p50, p99, p99.9)
- **Errorstats**: count of each error type

### Simulator Requirements

A simulator must provide at minimum:
- `connected_clients` (accurate count)
- Memory calculations (used_memory)
- Keyspace stats (keys, expires, avg_ttl per database)
- Per-command stats (if commandstats queried)
- Uptime tracking
- Version info matching claimed Redis version
- Role and replication info

---

## CONFIG Command

### CONFIG GET

```
CONFIG GET parameter [parameter ...]    # Redis 7.0+: multiple params
CONFIG GET *                            # all parameters
CONFIG GET *max*                        # glob patterns
```

Returns array of key-value pairs (RESP2) or map (RESP3).

### CONFIG SET

```
CONFIG SET parameter value
```

Changes take effect immediately. Does **not** modify the config file (runtime-only).

### CONFIG REWRITE

Updates `redis.conf` to match current running configuration. Preserves comments and structure, appends new non-default parameters.

### CONFIG RESETSTAT

Resets all statistics counters (commandstats, keyspace stats, connection stats, errors).

### Important Configuration Parameters

#### Memory
- `maxmemory` — max RAM for data (0 = unlimited on 64-bit)
- `maxmemory-policy` — eviction policy: `noeviction`, `allkeys-lru`, `allkeys-lfu`, `volatile-lru`, `volatile-lfu`, `volatile-ttl`, `volatile-random`, `allkeys-random`
- `maxmemory-samples` — sample size for eviction (default 5)

#### Persistence
- `save` — RDB schedule (e.g., `save 900 1`)
- `appendonly` — enable/disable AOF
- `appendfsync` — `always`, `everysec`, `no`
- `dir` — directory for RDB/AOF
- `dbfilename` — RDB filename

#### Connection
- `timeout` — client idle timeout (0 = disabled)
- `tcp-keepalive` — keepalive interval
- `databases` — number of databases (default 16)

#### Network
- `port` — TCP port (default 6379)
- `bind` — IP addresses to bind to
- `tcp-backlog` — pending connections backlog

#### Performance
- `slowlog-log-slower-than` — microseconds threshold (default 10000)
- `slowlog-max-len` — max entries (default 128)
- `loglevel` — debug, verbose, notice, warning

#### Data Structure Encoding
- `hash-max-listpack-entries`, `hash-max-listpack-value`
- `set-max-intset-entries`
- `zset-max-listpack-entries`
- `list-max-listpack-size`
- `stream-node-max-bytes`

### Read-Only vs Runtime-Writable

Most parameters are writable at runtime. Notable exceptions:
- `requirepass` — must be set at startup
- `io-threads` — startup only
- Other internal startup configuration

---

[← Back to Node Simulator Research](README.md)
