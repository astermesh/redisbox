import { describe, it, expect } from 'vitest';
import { info } from './info.ts';
import { RedisEngine } from '../engine.ts';
import { ConfigStore } from '../../config-store.ts';
import { ClientStateStore } from '../../server/client-state.ts';
import type { CommandContext } from '../types.ts';
import type { Reply } from '../types.ts';

function createCtx(opts?: {
  time?: number;
  config?: ConfigStore;
  clientStore?: ClientStateStore;
}): CommandContext {
  const now = opts?.time ?? 10000;
  const engine = new RedisEngine({ clock: () => now });
  return {
    db: engine.db(0),
    engine,
    config: opts?.config ?? new ConfigStore(),
    clientStore: opts?.clientStore,
  };
}

function getBulkValue(reply: Reply): string {
  expect(reply.kind).toBe('bulk');
  if (reply.kind === 'bulk') {
    expect(reply.value).not.toBeNull();
    return reply.value ?? '';
  }
  throw new Error('Expected bulk reply');
}

describe('INFO', () => {
  describe('no arguments (default)', () => {
    it('returns bulk string reply', () => {
      const ctx = createCtx();
      const result = info(ctx, []);
      expect(result.kind).toBe('bulk');
    });

    it('includes default sections', () => {
      const ctx = createCtx();
      const text = getBulkValue(info(ctx, []));
      expect(text).toContain('# Server');
      expect(text).toContain('# Clients');
      expect(text).toContain('# Memory');
      expect(text).toContain('# Stats');
      expect(text).toContain('# Replication');
      expect(text).toContain('# CPU');
      expect(text).toContain('# Modules');
      expect(text).toContain('# Cluster');
      expect(text).toContain('# Keyspace');
    });

    it('does not include commandstats or errorstats in default', () => {
      const ctx = createCtx();
      const text = getBulkValue(info(ctx, []));
      expect(text).not.toContain('# Commandstats');
      expect(text).not.toContain('# Errorstats');
    });
  });

  describe('single section argument', () => {
    it('returns only the requested section', () => {
      const ctx = createCtx();
      const text = getBulkValue(info(ctx, ['server']));
      expect(text).toContain('# Server');
      expect(text).not.toContain('# Clients');
      expect(text).not.toContain('# Memory');
    });

    it('is case-insensitive', () => {
      const ctx = createCtx();
      const text = getBulkValue(info(ctx, ['SERVER']));
      expect(text).toContain('# Server');
    });

    it('returns empty string for unknown section', () => {
      const ctx = createCtx();
      const result = info(ctx, ['nonexistent']);
      expect(result).toEqual({ kind: 'bulk', value: '' });
    });
  });

  describe('server section', () => {
    it('contains redis_version', () => {
      const ctx = createCtx();
      const text = getBulkValue(info(ctx, ['server']));
      expect(text).toContain('redis_version:7.2.0');
    });

    it('contains redis_mode:standalone', () => {
      const ctx = createCtx();
      const text = getBulkValue(info(ctx, ['server']));
      expect(text).toContain('redis_mode:standalone');
    });

    it('contains tcp_port from config', () => {
      const config = new ConfigStore();
      config.set('port', '6380');
      const ctx = createCtx({ config });
      const text = getBulkValue(info(ctx, ['server']));
      expect(text).toContain('tcp_port:6380');
    });

    it('contains uptime_in_seconds', () => {
      const ctx = createCtx();
      const text = getBulkValue(info(ctx, ['server']));
      expect(text).toContain('uptime_in_seconds:');
    });

    it('contains uptime_in_days', () => {
      const ctx = createCtx();
      const text = getBulkValue(info(ctx, ['server']));
      expect(text).toContain('uptime_in_days:');
    });
  });

  describe('clients section', () => {
    it('contains connected_clients', () => {
      const ctx = createCtx();
      const text = getBulkValue(info(ctx, ['clients']));
      expect(text).toContain('connected_clients:');
    });

    it('reports correct number of connected clients', () => {
      const clientStore = new ClientStateStore();
      clientStore.create(1, 1000);
      clientStore.create(2, 1000);
      clientStore.create(3, 1000);
      const ctx = createCtx({ clientStore });
      const text = getBulkValue(info(ctx, ['clients']));
      expect(text).toContain('connected_clients:3');
    });

    it('defaults to 1 when no client store', () => {
      const engine = new RedisEngine({ clock: () => 1000 });
      const ctx: CommandContext = { db: engine.db(0), engine };
      const text = getBulkValue(info(ctx, ['clients']));
      expect(text).toContain('connected_clients:1');
    });
  });

  describe('memory section', () => {
    it('contains used_memory', () => {
      const ctx = createCtx();
      const text = getBulkValue(info(ctx, ['memory']));
      expect(text).toContain('used_memory:');
    });

    it('contains maxmemory_policy', () => {
      const ctx = createCtx();
      const text = getBulkValue(info(ctx, ['memory']));
      expect(text).toContain('maxmemory_policy:noeviction');
    });
  });

  describe('replication section', () => {
    it('contains role:master', () => {
      const ctx = createCtx();
      const text = getBulkValue(info(ctx, ['replication']));
      expect(text).toContain('role:master');
    });

    it('contains connected_slaves:0', () => {
      const ctx = createCtx();
      const text = getBulkValue(info(ctx, ['replication']));
      expect(text).toContain('connected_slaves:0');
    });
  });

  describe('cluster section', () => {
    it('contains cluster_enabled:0', () => {
      const ctx = createCtx();
      const text = getBulkValue(info(ctx, ['cluster']));
      expect(text).toContain('cluster_enabled:0');
    });
  });

  describe('keyspace section', () => {
    it('returns empty keyspace when no keys', () => {
      const ctx = createCtx();
      const text = getBulkValue(info(ctx, ['keyspace']));
      expect(text).toBe('# Keyspace\r\n');
    });

    it('reports keys count for non-empty databases', () => {
      const ctx = createCtx();
      ctx.db.set('key1', 'string', 'raw', 'value1');
      ctx.db.set('key2', 'string', 'raw', 'value2');
      const text = getBulkValue(info(ctx, ['keyspace']));
      expect(text).toContain('db0:keys=2,expires=0,avg_ttl=0');
    });

    it('reports expires count', () => {
      const ctx = createCtx({ time: 1000 });
      ctx.db.set('key1', 'string', 'raw', 'value1');
      ctx.db.set('key2', 'string', 'raw', 'value2');
      ctx.db.setExpiry('key1', 5000);
      const text = getBulkValue(info(ctx, ['keyspace']));
      expect(text).toContain('db0:keys=2,expires=1,avg_ttl=');
    });

    it('calculates avg_ttl correctly', () => {
      const now = 1000;
      const engine = new RedisEngine({ clock: () => now });
      const ctx: CommandContext = {
        db: engine.db(0),
        engine,
      };
      ctx.db.set('key1', 'string', 'raw', 'value1');
      ctx.db.set('key2', 'string', 'raw', 'value2');
      ctx.db.setExpiry('key1', 3000); // remaining: 2000ms
      ctx.db.setExpiry('key2', 5000); // remaining: 4000ms
      const text = getBulkValue(info(ctx, ['keyspace']));
      // avg_ttl = (2000 + 4000) / 2 = 3000
      expect(text).toContain('db0:keys=2,expires=2,avg_ttl=3000');
    });

    it('reports multiple databases', () => {
      const ctx = createCtx();
      ctx.db.set('key1', 'string', 'raw', 'value1');
      const db3 = ctx.engine.db(3);
      db3.set('key2', 'string', 'raw', 'value2');
      const text = getBulkValue(info(ctx, ['keyspace']));
      expect(text).toContain('db0:keys=1,expires=0,avg_ttl=0');
      expect(text).toContain('db3:keys=1,expires=0,avg_ttl=0');
    });

    it('does not list empty databases', () => {
      const ctx = createCtx();
      ctx.db.set('key1', 'string', 'raw', 'value1');
      const text = getBulkValue(info(ctx, ['keyspace']));
      expect(text).not.toContain('db1:');
      expect(text).not.toContain('db2:');
    });
  });

  describe('all section', () => {
    it('includes commandstats and errorstats', () => {
      const ctx = createCtx();
      const text = getBulkValue(info(ctx, ['all']));
      expect(text).toContain('# Server');
      expect(text).toContain('# Clients');
      expect(text).toContain('# Memory');
      expect(text).toContain('# Commandstats');
      expect(text).toContain('# Errorstats');
      expect(text).toContain('# Keyspace');
    });
  });

  describe('everything section', () => {
    it('includes all sections including commandstats and errorstats', () => {
      const ctx = createCtx();
      const text = getBulkValue(info(ctx, ['everything']));
      expect(text).toContain('# Server');
      expect(text).toContain('# Commandstats');
      expect(text).toContain('# Errorstats');
      expect(text).toContain('# Keyspace');
    });
  });

  describe('default section', () => {
    it('same as no arguments', () => {
      const ctx = createCtx();
      const defaultText = getBulkValue(info(ctx, ['default']));
      const noArgText = getBulkValue(info(ctx, []));
      expect(defaultText).toBe(noArgText);
    });
  });

  describe('output format', () => {
    it('uses CRLF line endings', () => {
      const ctx = createCtx();
      const text = getBulkValue(info(ctx, ['cluster']));
      expect(text).toContain('\r\n');
      expect(text).not.toMatch(/[^\r]\n/);
    });

    it('sections separated by blank lines (CRLF CRLF)', () => {
      const ctx = createCtx();
      const text = getBulkValue(info(ctx, []));
      expect(text).toContain('\r\n\r\n');
    });

    it('key:value format within sections', () => {
      const ctx = createCtx();
      const text = getBulkValue(info(ctx, ['cluster']));
      expect(text).toMatch(/cluster_enabled:\d/);
    });
  });

  describe('cpu section', () => {
    it('contains cpu fields', () => {
      const ctx = createCtx();
      const text = getBulkValue(info(ctx, ['cpu']));
      expect(text).toContain('# CPU');
      expect(text).toContain('used_cpu_sys:');
      expect(text).toContain('used_cpu_user:');
    });
  });

  describe('modules section', () => {
    it('contains header only', () => {
      const ctx = createCtx();
      const text = getBulkValue(info(ctx, ['modules']));
      expect(text).toContain('# Modules');
    });
  });

  describe('stats section', () => {
    it('contains stats fields', () => {
      const ctx = createCtx();
      const text = getBulkValue(info(ctx, ['stats']));
      expect(text).toContain('# Stats');
      expect(text).toContain('total_connections_received:');
      expect(text).toContain('keyspace_hits:');
      expect(text).toContain('keyspace_misses:');
    });
  });
});
