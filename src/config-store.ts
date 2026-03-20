/**
 * Redis configuration store.
 *
 * Holds all CONFIG-accessible parameters with their default values,
 * supports glob-pattern lookup and validated set.
 */

import { matchGlob } from './engine/glob-pattern.ts';

// ---------------------------------------------------------------------------
// Config parameter definitions
// ---------------------------------------------------------------------------

type ConfigValidator = (value: string) => boolean;

interface ConfigParam {
  defaultValue: string;
  validate?: ConfigValidator;
}

const isYesNo: ConfigValidator = (v) => v === 'yes' || v === 'no';
const isNonNegInt: ConfigValidator = (v) => /^\d+$/.test(v);
const isInt: ConfigValidator = (v) => /^-?\d+$/.test(v);
const isMemory: ConfigValidator = (v) =>
  /^\d+$/.test(v) || /^\d+[kmgKMG][bB]?$/.test(v);

function isOneOf(...values: string[]): ConfigValidator {
  return (v) => values.includes(v);
}

// ---------------------------------------------------------------------------
// Default config map — mirrors Redis 7.2 defaults
// ---------------------------------------------------------------------------

function buildDefaults(): Map<string, ConfigParam> {
  const m = new Map<string, ConfigParam>();

  function add(
    key: string,
    defaultValue: string,
    validate?: ConfigValidator
  ): void {
    m.set(key, { defaultValue, validate });
  }

  // --- Network ---
  add('bind', '127.0.0.1 -::1');
  add('bind-source-addr', '');
  add('protected-mode', 'yes', isYesNo);
  add('port', '6379', isNonNegInt);
  add('tcp-backlog', '511', isNonNegInt);
  add('unixsocket', '');
  add('unixsocketperm', '0');
  add('timeout', '0', isNonNegInt);
  add('tcp-keepalive', '300', isNonNegInt);

  // --- TLS ---
  add('tls-port', '0', isNonNegInt);
  add('tls-cert-file', '');
  add('tls-key-file', '');
  add('tls-ca-cert-file', '');
  add('tls-ca-cert-dir', '');
  add('tls-auth-clients', 'yes', isOneOf('yes', 'no', 'optional'));
  add('tls-replication', 'no', isYesNo);
  add('tls-cluster', 'no', isYesNo);

  // --- General ---
  add('daemonize', 'no', isYesNo);
  add('supervised', 'no', isOneOf('no', 'upstart', 'systemd', 'auto'));
  add('pidfile', '');
  add('loglevel', 'notice', isOneOf('debug', 'verbose', 'notice', 'warning'));
  add('logfile', '');
  add('databases', '16', isNonNegInt);
  add('always-show-logo', 'no', isYesNo);
  add('set-proc-title', 'yes', isYesNo);
  add('proc-title-template', '{title} {laddr} {server-mode}');
  add('locale-collate', '');

  // --- Snapshotting ---
  add('save', '3600 1 300 100 60 10000');
  add('stop-writes-on-bgsave-error', 'yes', isYesNo);
  add('rdbcompression', 'yes', isYesNo);
  add('rdbchecksum', 'yes', isYesNo);
  add('sanitize-dump-payload', 'no', isOneOf('no', 'yes', 'clients'));
  add('dbfilename', 'dump.rdb');
  add('rdb-del-sync-files', 'no', isYesNo);
  add('dir', './');

  // --- Replication ---
  add('replicaof', '');
  add('masterauth', '');
  add('masteruser', '');
  add('replica-serve-stale-data', 'yes', isYesNo);
  add('replica-read-only', 'yes', isYesNo);
  add('repl-diskless-sync', 'yes', isYesNo);
  add('repl-diskless-sync-delay', '5', isNonNegInt);
  add('repl-diskless-sync-max-replicas', '0', isNonNegInt);
  add(
    'repl-diskless-load',
    'disabled',
    isOneOf('disabled', 'on-empty-db', 'swapdb')
  );
  add('repl-ping-replica-period', '10', isNonNegInt);
  add('repl-timeout', '60', isNonNegInt);
  add('repl-disable-tcp-nodelay', 'no', isYesNo);
  add('repl-backlog-size', '1048576', isMemory);
  add('repl-backlog-ttl', '3600', isNonNegInt);
  add('replica-priority', '100', isNonNegInt);
  add('replica-announced', 'yes', isYesNo);
  add('min-replicas-to-write', '0', isNonNegInt);
  add('min-replicas-max-lag', '10', isNonNegInt);

  // --- Security ---
  add('requirepass', '');
  add('rename-command', '');
  add('aclfile', '');
  add('acllog-max-len', '128', isNonNegInt);

  // --- Clients ---
  add('maxclients', '10000', isNonNegInt);

  // --- Memory ---
  add('maxmemory', '0', isMemory);
  add(
    'maxmemory-policy',
    'noeviction',
    isOneOf(
      'volatile-lru',
      'allkeys-lru',
      'volatile-lfu',
      'allkeys-lfu',
      'volatile-random',
      'allkeys-random',
      'volatile-ttl',
      'noeviction'
    )
  );
  add('maxmemory-samples', '5', isNonNegInt);
  add('maxmemory-eviction-tenacity', '10', isNonNegInt);
  add('replica-ignore-maxmemory', 'yes', isYesNo);
  add('active-expire-enabled', 'yes', isYesNo);
  add('active-expire-effort', '1', isNonNegInt);

  // --- Lazy freeing ---
  add('lazyfree-lazy-eviction', 'no', isYesNo);
  add('lazyfree-lazy-expire', 'no', isYesNo);
  add('lazyfree-lazy-server-del', 'no', isYesNo);
  add('lazyfree-lazy-user-del', 'no', isYesNo);
  add('lazyfree-lazy-user-flush', 'no', isYesNo);

  // --- Threaded I/O ---
  add('io-threads', '1', isNonNegInt);
  add('io-threads-do-reads', 'no', isYesNo);

  // --- AOF ---
  add('appendonly', 'no', isYesNo);
  add('appendfilename', 'appendonly.aof');
  add('appenddirname', 'appendonlydir');
  add('appendfsync', 'everysec', isOneOf('always', 'everysec', 'no'));
  add('no-appendfsync-on-rewrite', 'no', isYesNo);
  add('auto-aof-rewrite-percentage', '100', isNonNegInt);
  add('auto-aof-rewrite-min-size', '67108864', isMemory);
  add('aof-load-truncated', 'yes', isYesNo);
  add('aof-use-rdb-preamble', 'yes', isYesNo);
  add('aof-timestamp-enabled', 'no', isYesNo);

  // --- Slow log ---
  add('slowlog-log-slower-than', '10000', isInt);
  add('slowlog-max-len', '128', isNonNegInt);

  // --- Latency monitor ---
  add('latency-monitor-threshold', '0', isNonNegInt);

  // --- Keyspace notifications ---
  add('notify-keyspace-events', '');

  // --- Data structure encoding thresholds ---
  add('hash-max-listpack-entries', '128', isNonNegInt);
  add('hash-max-listpack-value', '64', isNonNegInt);
  add('hash-max-ziplist-entries', '128', isNonNegInt);
  add('hash-max-ziplist-value', '64', isNonNegInt);
  add('list-max-listpack-size', '-2', isInt);
  add('list-max-ziplist-size', '-2', isInt);
  add('list-compress-depth', '0', isNonNegInt);
  add('set-max-intset-entries', '512', isNonNegInt);
  add('set-max-listpack-entries', '128', isNonNegInt);
  add('set-max-listpack-value', '64', isNonNegInt);
  add('zset-max-listpack-entries', '128', isNonNegInt);
  add('zset-max-listpack-value', '64', isNonNegInt);
  add('zset-max-ziplist-entries', '128', isNonNegInt);
  add('zset-max-ziplist-value', '64', isNonNegInt);

  // --- HyperLogLog ---
  add('hll-sparse-max-bytes', '3000', isNonNegInt);

  // --- Streams ---
  add('stream-node-max-bytes', '4096', isNonNegInt);
  add('stream-node-max-entries', '100', isNonNegInt);

  // --- Active rehashing ---
  add('activerehashing', 'yes', isYesNo);

  // --- Misc ---
  add('hz', '10', isNonNegInt);
  add('dynamic-hz', 'yes', isYesNo);
  add('active-defrag-enabled', 'no', isYesNo);
  add('active-defrag-threshold-lower', '10', isNonNegInt);
  add('active-defrag-threshold-upper', '100', isNonNegInt);
  add('active-defrag-cycle-min', '1', isNonNegInt);
  add('active-defrag-cycle-max', '25', isNonNegInt);
  add('active-defrag-max-scan-fields', '1000', isNonNegInt);
  add('jemalloc-bg-thread', 'yes', isYesNo);
  add('crash-log-enabled', 'yes', isYesNo);
  add('use-exit-on-panic', 'no', isYesNo);
  add('disable-thp', 'no', isYesNo);
  add('cluster-enabled', 'no', isYesNo);
  add('cluster-config-file', 'nodes.conf');
  add('cluster-node-timeout', '15000', isNonNegInt);
  add('cluster-allow-reads-when-down', 'no', isYesNo);
  add('lua-time-limit', '5000', isNonNegInt);
  add('busy-reply-threshold', '5000', isNonNegInt);
  add('latency-tracking', 'yes', isYesNo);
  add('latency-tracking-info-percentiles', '50 99 99.9');
  add('proto-max-bulk-len', '536870912', isNonNegInt);
  add('tracking-table-max-keys', '0', isNonNegInt);
  add('lfu-log-factor', '10', isNonNegInt);
  add('lfu-decay-time', '1', isNonNegInt);
  return m;
}

// ---------------------------------------------------------------------------
// ConfigStore
// ---------------------------------------------------------------------------

export type ConfigChangeListener = (
  changes: readonly { key: string; value: string; oldValue: string }[]
) => void;

export class ConfigStore {
  private readonly params: Map<string, ConfigParam>;
  private readonly values: Map<string, string>;
  private readonly listeners: ConfigChangeListener[] = [];

  constructor() {
    this.params = buildDefaults();
    this.values = new Map<string, string>();
    // Initialize values from defaults
    for (const [key, param] of this.params) {
      this.values.set(key, param.defaultValue);
    }
  }

  /**
   * Register a listener that is called after CONFIG SET succeeds.
   * Returns an unsubscribe function.
   */
  onChange(listener: ConfigChangeListener): () => void {
    this.listeners.push(listener);
    return () => {
      const idx = this.listeners.indexOf(listener);
      if (idx >= 0) this.listeners.splice(idx, 1);
    };
  }

  private notify(
    changes: { key: string; value: string; oldValue: string }[]
  ): void {
    for (const listener of this.listeners) {
      listener(changes);
    }
  }

  /**
   * Get all config key-value pairs matching a glob pattern.
   * Returns a flat array: [key1, val1, key2, val2, ...] — like Redis.
   */
  get(pattern: string): string[] {
    const lowerPattern = pattern.toLowerCase();
    const result: string[] = [];

    for (const [key, value] of this.values) {
      if (matchGlob(lowerPattern, key)) {
        result.push(key, value);
      }
    }

    return result;
  }

  /**
   * Get all config key-value pairs matching multiple patterns.
   * Deduplicates keys that match more than one pattern.
   */
  getMulti(patterns: string[]): string[] {
    const seen = new Set<string>();
    const result: string[] = [];

    for (const pattern of patterns) {
      const lowerPattern = pattern.toLowerCase();
      for (const [key, value] of this.values) {
        if (!seen.has(key) && matchGlob(lowerPattern, key)) {
          seen.add(key);
          result.push(key, value);
        }
      }
    }

    return result;
  }

  /**
   * Set a config parameter. Returns null on success, or an error message string.
   */
  set(key: string, value: string): string | null {
    const lowerKey = key.toLowerCase();
    const param = this.params.get(lowerKey);

    if (!param) {
      return `ERR Unsupported CONFIG parameter: ${key}`;
    }

    if (param.validate && !param.validate(value)) {
      return `ERR Invalid argument '${value}' for CONFIG SET '${lowerKey}'`;
    }

    const oldValue = this.values.get(lowerKey) ?? '';
    this.values.set(lowerKey, value);
    if (oldValue !== value) {
      this.notify([{ key: lowerKey, value, oldValue }]);
    }
    return null;
  }

  /**
   * Set multiple config parameters atomically.
   * Returns null on success, or the first error message.
   * On error, no changes are applied (all-or-nothing).
   */
  setMulti(pairs: [string, string][]): string | null {
    // Validate all first
    for (const [key, value] of pairs) {
      const lowerKey = key.toLowerCase();
      const param = this.params.get(lowerKey);

      if (!param) {
        return `ERR Unsupported CONFIG parameter: ${key}`;
      }

      if (param.validate && !param.validate(value)) {
        return `ERR Invalid argument '${value}' for CONFIG SET '${lowerKey}'`;
      }
    }

    // Apply all and collect changes
    const changes: { key: string; value: string; oldValue: string }[] = [];
    for (const [key, value] of pairs) {
      const lowerKey = key.toLowerCase();
      const oldValue = this.values.get(lowerKey) ?? '';
      this.values.set(lowerKey, value);
      if (oldValue !== value) {
        changes.push({ key: lowerKey, value, oldValue });
      }
    }

    if (changes.length > 0) {
      this.notify(changes);
    }
    return null;
  }

  /**
   * Reset statistics counters.
   * In a full engine this would reset keyspace_hits, etc.
   * Currently a no-op placeholder that returns OK.
   */
  resetStat(): void {
    // Stats counters will be reset here when the engine tracks them
  }

  /**
   * Reset all config values to their defaults.
   */
  resetToDefaults(): void {
    for (const [key, param] of this.params) {
      this.values.set(key, param.defaultValue);
    }
  }
}
