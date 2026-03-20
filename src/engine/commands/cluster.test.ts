import { describe, it, expect } from 'vitest';
import { RedisEngine } from '../engine.ts';
import type { CommandContext } from '../types.ts';
import * as cluster from './cluster.ts';

function createCtx(): CommandContext {
  const engine = new RedisEngine({ clock: () => 1000 });
  return {
    db: engine.db(0),
    engine,
  };
}

describe('CLUSTER INFO', () => {
  it('returns bulk string with cluster_enabled:0', () => {
    const reply = cluster.clusterInfo();
    expect(reply.kind).toBe('bulk');
    expect((reply as { value: string }).value).toContain('cluster_enabled:0');
  });

  it('contains cluster_state:ok', () => {
    const reply = cluster.clusterInfo();
    expect((reply as { value: string }).value).toContain('cluster_state:ok');
  });

  it('contains all required fields', () => {
    const reply = cluster.clusterInfo();
    const text = (reply as { value: string }).value;
    const fields = [
      'cluster_enabled',
      'cluster_state',
      'cluster_slots_assigned',
      'cluster_slots_ok',
      'cluster_slots_pfail',
      'cluster_slots_fail',
      'cluster_known_nodes',
      'cluster_size',
      'cluster_current_epoch',
      'cluster_my_epoch',
      'cluster_stats_messages_sent',
      'cluster_stats_messages_received',
      'total_cluster_links_buffer_limit_exceeded',
    ];
    for (const field of fields) {
      expect(text).toContain(field);
    }
  });
});

describe('CLUSTER MYID', () => {
  it('returns a 40-character hex node ID', () => {
    const reply = cluster.clusterMyid();
    expect(reply.kind).toBe('bulk');
    const value = (reply as { value: string }).value;
    expect(value).toHaveLength(40);
    expect(value).toMatch(/^[0-9a-f]{40}$/);
  });

  it('returns consistent ID across calls', () => {
    const r1 = cluster.clusterMyid();
    const r2 = cluster.clusterMyid();
    expect(r1).toEqual(r2);
  });
});

describe('CLUSTER KEYSLOT', () => {
  it('returns integer reply for a key', () => {
    const reply = cluster.clusterKeyslot(['foo']);
    expect(reply.kind).toBe('integer');
  });

  it('returns slot in range 0-16383', () => {
    const reply = cluster.clusterKeyslot(['test']);
    const slot = (reply as { value: number }).value;
    expect(slot).toBeGreaterThanOrEqual(0);
    expect(slot).toBeLessThanOrEqual(16383);
  });

  // Known Redis CRC16 hash slot values
  it('computes correct slot for "foo"', () => {
    // Redis: CLUSTER KEYSLOT foo => 12182
    const reply = cluster.clusterKeyslot(['foo']);
    expect((reply as { value: number }).value).toBe(12182);
  });

  it('computes correct slot for "bar"', () => {
    // Redis: CLUSTER KEYSLOT bar => 5061
    const reply = cluster.clusterKeyslot(['bar']);
    expect((reply as { value: number }).value).toBe(5061);
  });

  it('computes correct slot for "hello"', () => {
    // Redis: CLUSTER KEYSLOT hello => 866
    const reply = cluster.clusterKeyslot(['hello']);
    expect((reply as { value: number }).value).toBe(866);
  });

  it('computes correct slot for empty string', () => {
    // Redis: CLUSTER KEYSLOT "" => 0
    const reply = cluster.clusterKeyslot(['']);
    expect((reply as { value: number }).value).toBe(0);
  });

  it('handles hash tags - {user}.info', () => {
    // The hash tag is "user", so should match CLUSTER KEYSLOT "user"
    const withTag = cluster.clusterKeyslot(['{user}.info']);
    const plain = cluster.clusterKeyslot(['user']);
    expect(withTag).toEqual(plain);
  });

  it('handles hash tags - {user}.session', () => {
    const withTag = cluster.clusterKeyslot(['{user}.session']);
    const plain = cluster.clusterKeyslot(['user']);
    expect(withTag).toEqual(plain);
  });

  it('ignores empty hash tag {}', () => {
    // {} means no valid hash tag — entire key is used
    const reply = cluster.clusterKeyslot(['{}key']);
    const plain = cluster.clusterKeyslot(['{}key']);
    expect(reply).toEqual(plain);
    // Should NOT equal keyslot of empty string
    const empty = cluster.clusterKeyslot(['']);
    expect(reply).not.toEqual(empty);
  });

  it('uses first valid hash tag only', () => {
    // {a}{b} — hash tag is "a"
    const reply = cluster.clusterKeyslot(['{a}{b}']);
    const justA = cluster.clusterKeyslot(['a']);
    expect(reply).toEqual(justA);
  });

  it('returns error without key argument', () => {
    const reply = cluster.clusterKeyslot([]);
    expect(reply.kind).toBe('error');
  });

  it('computes correct slot for "123456789"', () => {
    // Redis: CLUSTER KEYSLOT 123456789 => 12739
    const reply = cluster.clusterKeyslot(['123456789']);
    expect((reply as { value: number }).value).toBe(12739);
  });
});

describe('CLUSTER NODES', () => {
  it('returns bulk string with node info', () => {
    const reply = cluster.clusterNodes();
    expect(reply.kind).toBe('bulk');
    const value = (reply as { value: string }).value;
    expect(value).toContain('myself,master');
    expect(value).toContain('connected');
    expect(value).toContain('0-16383');
  });
});

describe('CLUSTER SLOTS', () => {
  it('returns empty array', () => {
    const reply = cluster.clusterSlots();
    expect(reply).toEqual({ kind: 'array', value: [] });
  });
});

describe('CLUSTER SHARDS', () => {
  it('returns empty array', () => {
    const reply = cluster.clusterShards();
    expect(reply).toEqual({ kind: 'array', value: [] });
  });
});

describe('CLUSTER COUNTKEYSINSLOT', () => {
  it('returns 0 for empty database', () => {
    const ctx = createCtx();
    const reply = cluster.clusterCountkeysinslot(ctx, ['0']);
    expect(reply).toEqual({ kind: 'integer', value: 0 });
  });

  it('counts keys in the correct slot', () => {
    const ctx = createCtx();
    // "foo" is in slot 12182
    ctx.db.set('foo', 'string', 'raw', 'bar');
    const reply = cluster.clusterCountkeysinslot(ctx, ['12182']);
    expect((reply as { value: number }).value).toBe(1);
  });

  it('returns 0 for slot with no keys', () => {
    const ctx = createCtx();
    ctx.db.set('foo', 'string', 'raw', 'bar');
    const reply = cluster.clusterCountkeysinslot(ctx, ['0']);
    expect((reply as { value: number }).value).toBe(0);
  });

  it('returns error for invalid slot', () => {
    const ctx = createCtx();
    const reply = cluster.clusterCountkeysinslot(ctx, ['16384']);
    expect(reply.kind).toBe('error');
  });

  it('returns error for negative slot', () => {
    const ctx = createCtx();
    const reply = cluster.clusterCountkeysinslot(ctx, ['-1']);
    expect(reply.kind).toBe('error');
  });

  it('returns error for non-integer slot', () => {
    const ctx = createCtx();
    const reply = cluster.clusterCountkeysinslot(ctx, ['abc']);
    expect(reply.kind).toBe('error');
  });
});

describe('CLUSTER GETKEYSINSLOT', () => {
  it('returns empty array for empty database', () => {
    const ctx = createCtx();
    const reply = cluster.clusterGetkeysinslot(ctx, ['0', '10']);
    expect(reply).toEqual({ kind: 'array', value: [] });
  });

  it('returns keys in the correct slot', () => {
    const ctx = createCtx();
    ctx.db.set('foo', 'string', 'raw', 'bar');
    const reply = cluster.clusterGetkeysinslot(ctx, ['12182', '10']);
    expect(reply.kind).toBe('array');
    const arr = (reply as { value: { value: string }[] }).value;
    expect(arr).toHaveLength(1);
    expect(arr[0]?.value).toBe('foo');
  });

  it('respects count limit', () => {
    const ctx = createCtx();
    // Add multiple keys that hash to the same slot
    ctx.db.set('foo', 'string', 'raw', 'v1');
    const reply = cluster.clusterGetkeysinslot(ctx, ['12182', '0']);
    expect(reply).toEqual({ kind: 'array', value: [] });
  });

  it('returns error for invalid slot', () => {
    const ctx = createCtx();
    const reply = cluster.clusterGetkeysinslot(ctx, ['16384', '10']);
    expect(reply.kind).toBe('error');
  });

  it('returns error for missing count', () => {
    const ctx = createCtx();
    const reply = cluster.clusterGetkeysinslot(ctx, ['0']);
    expect(reply.kind).toBe('error');
  });
});

describe('CLUSTER RESET', () => {
  it('returns OK with no arguments', () => {
    const reply = cluster.clusterReset([]);
    expect(reply).toEqual({ kind: 'status', value: 'OK' });
  });

  it('returns OK with SOFT', () => {
    const reply = cluster.clusterReset(['SOFT']);
    expect(reply).toEqual({ kind: 'status', value: 'OK' });
  });

  it('returns OK with HARD', () => {
    const reply = cluster.clusterReset(['HARD']);
    expect(reply).toEqual({ kind: 'status', value: 'OK' });
  });

  it('is case insensitive', () => {
    const reply = cluster.clusterReset(['soft']);
    expect(reply).toEqual({ kind: 'status', value: 'OK' });
  });

  it('returns error for invalid mode', () => {
    const reply = cluster.clusterReset(['INVALID']);
    expect(reply.kind).toBe('error');
  });
});

describe('CLUSTER HELP', () => {
  it('returns array of help lines', () => {
    const reply = cluster.clusterHelp();
    expect(reply.kind).toBe('array');
    const arr = (reply as { value: { value: string }[] }).value;
    expect(arr.length).toBeGreaterThan(0);
    expect(arr[0]?.value).toContain('CLUSTER');
  });
});

describe('CLUSTER main dispatch', () => {
  it('dispatches to INFO', () => {
    const ctx = createCtx();
    const reply = cluster.cluster(ctx, ['INFO']);
    expect(reply.kind).toBe('bulk');
    expect((reply as { value: string }).value).toContain('cluster_enabled:0');
  });

  it('dispatches case insensitively', () => {
    const ctx = createCtx();
    const reply = cluster.cluster(ctx, ['info']);
    expect(reply.kind).toBe('bulk');
  });

  it('returns error for unknown subcommand', () => {
    const ctx = createCtx();
    const reply = cluster.cluster(ctx, ['UNKNOWN']);
    expect(reply.kind).toBe('error');
    expect((reply as { message: string }).message).toContain(
      'unknown subcommand'
    );
  });

  it('returns error for empty args', () => {
    const ctx = createCtx();
    const reply = cluster.cluster(ctx, []);
    expect(reply.kind).toBe('error');
  });

  it('dispatches to KEYSLOT', () => {
    const ctx = createCtx();
    const reply = cluster.cluster(ctx, ['KEYSLOT', 'foo']);
    expect(reply.kind).toBe('integer');
    expect((reply as { value: number }).value).toBe(12182);
  });

  it('dispatches stub subcommands', () => {
    const ctx = createCtx();
    expect(
      cluster.cluster(ctx, ['SETSLOT', '0', 'IMPORTING', 'nodeid'])
    ).toEqual({
      kind: 'status',
      value: 'OK',
    });
    expect(cluster.cluster(ctx, ['ADDSLOTS', '0'])).toEqual({
      kind: 'status',
      value: 'OK',
    });
    expect(cluster.cluster(ctx, ['FAILOVER'])).toEqual({
      kind: 'status',
      value: 'OK',
    });
    expect(cluster.cluster(ctx, ['LINKS'])).toEqual({
      kind: 'array',
      value: [],
    });
  });
});

describe('keySlot', () => {
  it('is exported for use by other modules', () => {
    expect(typeof cluster.keySlot).toBe('function');
  });

  it('returns consistent results', () => {
    expect(cluster.keySlot('test')).toBe(cluster.keySlot('test'));
  });

  it('handles binary-safe key names', () => {
    const slot = cluster.keySlot('key with spaces');
    expect(slot).toBeGreaterThanOrEqual(0);
    expect(slot).toBeLessThanOrEqual(16383);
  });
});
