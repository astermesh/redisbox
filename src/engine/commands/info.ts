import type { CommandContext, Reply } from '../types.ts';
import { bulkReply } from '../types.ts';
import type { CommandSpec } from '../command-table.ts';

// ---------------------------------------------------------------------------
// Section names
// ---------------------------------------------------------------------------

const KNOWN_SECTIONS = new Set([
  'server',
  'clients',
  'memory',
  'stats',
  'replication',
  'cpu',
  'modules',
  'commandstats',
  'errorstats',
  'cluster',
  'keyspace',
  'all',
  'everything',
  'default',
]);

const DEFAULT_SECTIONS = [
  'server',
  'clients',
  'memory',
  'stats',
  'replication',
  'cpu',
  'modules',
  'cluster',
  'keyspace',
] as const;

const ALL_SECTIONS = [
  'server',
  'clients',
  'memory',
  'stats',
  'replication',
  'cpu',
  'modules',
  'commandstats',
  'errorstats',
  'cluster',
  'keyspace',
] as const;

// ---------------------------------------------------------------------------
// Section builders
// ---------------------------------------------------------------------------

function serverSection(ctx: CommandContext): string {
  const uptimeMs = ctx.engine.clock() - ctx.engine.startTime;
  const uptimeSec = Math.floor(uptimeMs / 1000);
  const uptimeDays = Math.floor(uptimeSec / 86400);
  const port = ctx.config ? (ctx.config.get('port')[1] ?? '6379') : '6379';
  const hz = ctx.config ? (ctx.config.get('hz')[1] ?? '10') : '10';
  const configuredHz = ctx.config
    ? (ctx.config.get('dynamic-hz')[1] ?? 'yes')
    : 'yes';

  return [
    '# Server',
    'redis_version:7.2.0',
    'redis_git_sha1:00000000',
    'redis_git_dirty:0',
    'redis_build_id:0',
    'redis_mode:standalone',
    `os:${getOsString()}`,
    `arch_bits:${getArchBits()}`,
    'monotonic_clock:POSIX clock_gettime',
    'multiplexing_api:epoll',
    'gcc_version:0.0.0',
    'process_id:0',
    'run_id:0000000000000000000000000000000000000000',
    `tcp_port:${port}`,
    `server_time_usec:${ctx.engine.clock() * 1000}`,
    `uptime_in_seconds:${uptimeSec}`,
    `uptime_in_days:${uptimeDays}`,
    `hz:${hz}`,
    `configured_hz:${configuredHz}`,
    'lru_clock:0',
    'executable:',
    'config_file:',
    'io_threads_active:0',
    'listener0:name=tcp,bind-addr=127.0.0.1,bind-addr-actual=127.0.0.1,port=6379,type=tcp',
  ].join('\r\n');
}

function clientsSection(ctx: CommandContext): string {
  const connectedClients = ctx.clientStore ? ctx.clientStore.size : 1;
  const maxclients = ctx.config
    ? (ctx.config.get('maxclients')[1] ?? '10000')
    : '10000';
  const blockedClients = ctx.engine.blocking.blockedCount;

  return [
    '# Clients',
    `connected_clients:${connectedClients}`,
    'cluster_connections:0',
    `maxclients:${maxclients}`,
    'client_recent_max_input_buffer:0',
    'client_recent_max_output_buffer:0',
    'total_clients_connected_including_replicas:0',
    `blocked_clients:${blockedClients}`,
    'tracking_clients:0',
    'clients_in_timeout_table:0',
    `total_blocking_clients:${blockedClients}`,
    'total_blocking_clients_on_nokey:0',
  ].join('\r\n');
}

function formatHumanBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes}B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(2)}K`;
  if (bytes < 1024 * 1024 * 1024)
    return `${(bytes / (1024 * 1024)).toFixed(2)}M`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)}G`;
}

function memorySection(ctx: CommandContext): string {
  const usedMemory = ctx.engine.usedMemory();
  const usedMemoryHuman = formatHumanBytes(usedMemory);
  const maxmemory = ctx.config ? (ctx.config.get('maxmemory')[1] ?? '0') : '0';
  const maxmemoryPolicy = ctx.config
    ? (ctx.config.get('maxmemory-policy')[1] ?? 'noeviction')
    : 'noeviction';
  const maxmemoryNum = parseInt(maxmemory, 10) || 0;
  const maxmemoryHuman = formatHumanBytes(maxmemoryNum);

  return [
    '# Memory',
    `used_memory:${usedMemory}`,
    `used_memory_human:${usedMemoryHuman}`,
    `used_memory_rss:${usedMemory}`,
    `used_memory_rss_human:${usedMemoryHuman}`,
    `used_memory_peak:${usedMemory}`,
    `used_memory_peak_human:${usedMemoryHuman}`,
    'used_memory_peak_perc:100.00%',
    'used_memory_overhead:0',
    'used_memory_startup:0',
    `used_memory_dataset:${usedMemory}`,
    'used_memory_dataset_perc:0.00%',
    'allocator_allocated:0',
    'allocator_active:0',
    'allocator_resident:0',
    'total_system_memory:0',
    'total_system_memory_human:0B',
    'used_memory_lua:0',
    'used_memory_vm_eval:0',
    'used_memory_lua_human:0B',
    'used_memory_scripts_eval:0',
    'used_memory_vm_functions:0',
    'used_memory_vm_total:0',
    'used_memory_vm_total_human:0B',
    'used_memory_functions:0',
    'used_memory_scripts:0',
    'used_memory_scripts_human:0B',
    'number_of_cached_scripts:0',
    'number_of_functions:0',
    'number_of_libraries:0',
    `maxmemory:${maxmemory}`,
    `maxmemory_human:${maxmemoryHuman}`,
    `maxmemory_policy:${maxmemoryPolicy}`,
    'allocator_frag_ratio:0.00',
    'allocator_frag_bytes:0',
    'allocator_rss_ratio:0.00',
    'allocator_rss_bytes:0',
    'rss_overhead_ratio:0.00',
    'rss_overhead_bytes:0',
    'mem_fragmentation_ratio:0.00',
    'mem_fragmentation_bytes:0',
    'mem_not_counted_for_evict:0',
    'mem_replication_backlog:0',
    'mem_total_replication_buffers:0',
    'mem_clients_slaves:0',
    'mem_clients_normal:0',
    'mem_cluster_links:0',
    'mem_aof_buffer:0',
    'mem_allocator:libc',
    'active_defrag_running:0',
    'lazyfree_pending_objects:0',
    'lazyfreed_objects:0',
  ].join('\r\n');
}

function statsSection(ctx: CommandContext): string {
  const pubsubChannels = ctx.engine.pubsub.totalChannels;

  return [
    '# Stats',
    'total_connections_received:0',
    'total_commands_processed:0',
    'instantaneous_ops_per_sec:0',
    'total_net_input_bytes:0',
    'total_net_output_bytes:0',
    'total_net_repl_input_bytes:0',
    'total_net_repl_output_bytes:0',
    'instantaneous_input_kbps:0.00',
    'instantaneous_output_kbps:0.00',
    'instantaneous_input_repl_kbps:0.00',
    'instantaneous_output_repl_kbps:0.00',
    'rejected_connections:0',
    'sync_full:0',
    'sync_partial_ok:0',
    'sync_partial_err:0',
    'expired_keys:0',
    'expired_stale_perc:0.00',
    'expired_time_cap_reached_count:0',
    'expire_cycle_cpu_milliseconds:0',
    'evicted_keys:0',
    'evicted_clients:0',
    'total_keys_fetched:0',
    'keyspace_hits:0',
    'keyspace_misses:0',
    `pubsub_channels:${pubsubChannels}`,
    'pubsub_patterns:0',
    'pubsub_shardchannels:0',
    'latest_fork_usec:0',
    'total_forks:0',
    'migrate_cached_sockets:0',
    'slave_expires_tracked_keys:0',
    'active_defrag_hits:0',
    'active_defrag_misses:0',
    'active_defrag_key_hits:0',
    'active_defrag_key_misses:0',
    'tracking_total_keys:0',
    'tracking_total_items:0',
    'tracking_total_prefixes:0',
    'unexpected_error_replies:0',
    'total_error_replies:0',
    'dump_payload_sanitizations:0',
    'total_reads_processed:0',
    'total_writes_processed:0',
    'io_threaded_reads_processed:0',
    'io_threaded_writes_processed:0',
    'reply_buffer_shrinks:0',
    'reply_buffer_expands:0',
    'current_cow_peak:0',
    'current_cow_size:0',
    'current_cow_size_age:0',
    'current_save_keys_processed:0',
    'current_save_keys_total:0',
  ].join('\r\n');
}

function replicationSection(): string {
  return [
    '# Replication',
    'role:master',
    'connected_slaves:0',
    'master_failover_state:no-failover',
    'master_replid:0000000000000000000000000000000000000000',
    'master_replid2:0000000000000000000000000000000000000000',
    'master_repl_offset:0',
    'second_repl_offset:-1',
    'repl_backlog_active:0',
    'repl_backlog_size:1048576',
    'repl_backlog_first_byte_offset:0',
    'repl_backlog_histlen:0',
  ].join('\r\n');
}

function cpuSection(): string {
  return [
    '# CPU',
    'used_cpu_sys:0.000000',
    'used_cpu_user:0.000000',
    'used_cpu_sys_children:0.000000',
    'used_cpu_user_children:0.000000',
    'used_cpu_sys_main_thread:0.000000',
    'used_cpu_user_main_thread:0.000000',
  ].join('\r\n');
}

function modulesSection(): string {
  return '# Modules';
}

function commandstatsSection(): string {
  return '# Commandstats';
}

function errorstatsSection(): string {
  return '# Errorstats';
}

function clusterSection(): string {
  return ['# Cluster', 'cluster_enabled:0'].join('\r\n');
}

function keyspaceSection(ctx: CommandContext): string {
  const lines: string[] = ['# Keyspace'];
  const now = ctx.engine.clock();

  for (let i = 0; i < ctx.engine.databases.length; i++) {
    const db = ctx.engine.databases[i];
    if (!db || db.size === 0) continue;

    const keys = db.size;
    const expires = db.expirySize;
    let avgTtl = 0;

    if (expires > 0) {
      let totalTtl = 0;
      let count = 0;
      for (const [, expiryMs] of db.expiryEntries()) {
        const remaining = expiryMs - now;
        if (remaining > 0) {
          totalTtl += remaining;
          count++;
        }
      }
      if (count > 0) {
        avgTtl = Math.round(totalTtl / count);
      }
    }

    lines.push(`db${i}:keys=${keys},expires=${expires},avg_ttl=${avgTtl}`);
  }

  return lines.join('\r\n');
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function getOsString(): string {
  return 'Linux 0.0.0 x86_64';
}

function getArchBits(): number {
  return 64;
}

type SectionBuilder = (ctx: CommandContext) => string;

const SECTION_BUILDERS: Record<string, SectionBuilder> = {
  server: serverSection,
  clients: clientsSection,
  memory: memorySection,
  stats: statsSection,
  replication: replicationSection,
  cpu: cpuSection,
  modules: modulesSection,
  commandstats: commandstatsSection,
  errorstats: errorstatsSection,
  cluster: clusterSection,
  keyspace: keyspaceSection,
};

function buildSections(
  ctx: CommandContext,
  sections: readonly string[]
): string {
  const parts: string[] = [];
  for (const section of sections) {
    const builder = SECTION_BUILDERS[section];
    if (builder) {
      parts.push(builder(ctx));
    }
  }
  return parts.join('\r\n\r\n') + '\r\n';
}

// ---------------------------------------------------------------------------
// INFO command
// ---------------------------------------------------------------------------

export function info(ctx: CommandContext, args: string[]): Reply {
  if (args.length === 0) {
    return bulkReply(buildSections(ctx, DEFAULT_SECTIONS));
  }

  const sections: string[] = [];
  for (const arg of args) {
    const lower = arg.toLowerCase();
    if (KNOWN_SECTIONS.has(lower)) {
      expandSection(lower, sections);
    }
    // Unknown sections are silently ignored (matches Redis behavior)
  }

  if (sections.length === 0) {
    // All sections were unknown — return empty bulk string
    return bulkReply('');
  }

  return bulkReply(buildSections(ctx, dedupe(sections)));
}

function expandSection(section: string, out: string[]): void {
  if (section === 'default') {
    out.push(...DEFAULT_SECTIONS);
  } else if (section === 'all') {
    out.push(...ALL_SECTIONS);
  } else if (section === 'everything') {
    out.push(...ALL_SECTIONS);
  } else {
    out.push(section);
  }
}

function dedupe(sections: string[]): string[] {
  const seen = new Set<string>();
  const result: string[] = [];
  for (const s of sections) {
    if (!seen.has(s)) {
      seen.add(s);
      result.push(s);
    }
  }
  return result;
}

export const specs: CommandSpec[] = [
  {
    name: 'info',
    handler: (ctx, args) => info(ctx, args),
    arity: -1,
    flags: ['loading', 'stale'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@slow', '@dangerous'],
  },
];
