import { describe, it, expect } from 'vitest';
import { RedisEngine } from '../engine.ts';
import type { CommandContext } from '../types.ts';
import * as repl from './replication.ts';

function createCtx(): CommandContext {
  const engine = new RedisEngine({ clock: () => 1000 });
  return {
    db: engine.db(0),
    engine,
  };
}

describe('REPLICAOF', () => {
  it('returns OK for NO ONE', () => {
    const reply = repl.replicaof(['NO', 'ONE']);
    expect(reply).toEqual({ kind: 'status', value: 'OK' });
  });

  it('is case insensitive for NO ONE', () => {
    const reply = repl.replicaof(['no', 'one']);
    expect(reply).toEqual({ kind: 'status', value: 'OK' });
  });

  it('returns OK for host port', () => {
    const reply = repl.replicaof(['127.0.0.1', '6379']);
    expect(reply).toEqual({ kind: 'status', value: 'OK' });
  });

  it('returns error for non-integer port', () => {
    const reply = repl.replicaof(['127.0.0.1', 'abc']);
    expect(reply.kind).toBe('error');
  });

  it('returns error for wrong number of arguments (too few)', () => {
    const reply = repl.replicaof(['127.0.0.1']);
    expect(reply.kind).toBe('error');
  });

  it('returns error for wrong number of arguments (too many)', () => {
    const reply = repl.replicaof(['127.0.0.1', '6379', 'extra']);
    expect(reply.kind).toBe('error');
  });

  it('returns error for empty args', () => {
    const reply = repl.replicaof([]);
    expect(reply.kind).toBe('error');
  });
});

describe('SLAVEOF', () => {
  it('returns OK for NO ONE', () => {
    const reply = repl.slaveof(['NO', 'ONE']);
    expect(reply).toEqual({ kind: 'status', value: 'OK' });
  });

  it('returns OK for host port', () => {
    const reply = repl.slaveof(['127.0.0.1', '6379']);
    expect(reply).toEqual({ kind: 'status', value: 'OK' });
  });

  it('behaves identically to REPLICAOF', () => {
    expect(repl.slaveof(['NO', 'ONE'])).toEqual(repl.replicaof(['NO', 'ONE']));
    expect(repl.slaveof(['127.0.0.1', '6379'])).toEqual(
      repl.replicaof(['127.0.0.1', '6379'])
    );
  });
});

describe('REPLCONF', () => {
  it('returns OK for any arguments', () => {
    const reply = repl.replconf(['LISTENING-PORT', '6380']);
    expect(reply).toEqual({ kind: 'status', value: 'OK' });
  });

  it('returns OK for ACK', () => {
    const reply = repl.replconf(['ACK', '0']);
    expect(reply).toEqual({ kind: 'status', value: 'OK' });
  });

  it('returns OK for CAPA', () => {
    const reply = repl.replconf(['CAPA', 'eof', 'CAPA', 'psync2']);
    expect(reply).toEqual({ kind: 'status', value: 'OK' });
  });

  it('returns OK for GETACK', () => {
    const reply = repl.replconf(['GETACK', '*']);
    expect(reply).toEqual({ kind: 'status', value: 'OK' });
  });
});

describe('PSYNC', () => {
  it('returns FULLRESYNC response', () => {
    const reply = repl.psync(['?', '-1']);
    expect(reply.kind).toBe('status');
    const value = (reply as { value: string }).value;
    expect(value).toMatch(/^FULLRESYNC [0-9a-f]{40} 0$/);
  });

  it('returns FULLRESYNC for any replication ID', () => {
    const reply = repl.psync(['abc123', '100']);
    expect(reply.kind).toBe('status');
    const value = (reply as { value: string }).value;
    expect(value).toMatch(/^FULLRESYNC [0-9a-f]{40} 0$/);
  });
});

describe('WAIT', () => {
  it('returns 0 replicas', () => {
    const ctx = createCtx();
    const reply = repl.wait(ctx, ['1', '0']);
    expect(reply).toEqual({ kind: 'integer', value: 0 });
  });

  it('returns 0 regardless of numreplicas requested', () => {
    const ctx = createCtx();
    const reply = repl.wait(ctx, ['10', '5000']);
    expect(reply).toEqual({ kind: 'integer', value: 0 });
  });

  it('returns error for wrong number of arguments', () => {
    const ctx = createCtx();
    const reply = repl.wait(ctx, ['1']);
    expect(reply.kind).toBe('error');
  });

  it('returns error for non-integer numreplicas', () => {
    const ctx = createCtx();
    const reply = repl.wait(ctx, ['abc', '0']);
    expect(reply.kind).toBe('error');
  });

  it('returns error for non-integer timeout', () => {
    const ctx = createCtx();
    const reply = repl.wait(ctx, ['1', 'abc']);
    expect(reply.kind).toBe('error');
  });

  it('returns error for negative timeout', () => {
    const ctx = createCtx();
    const reply = repl.wait(ctx, ['1', '-1']);
    expect(reply.kind).toBe('error');
  });
});

describe('WAITAOF', () => {
  it('returns [0, 0]', () => {
    const ctx = createCtx();
    const reply = repl.waitaof(ctx, ['0', '0', '0']);
    expect(reply).toEqual({
      kind: 'array',
      value: [
        { kind: 'integer', value: 0 },
        { kind: 'integer', value: 0 },
      ],
    });
  });

  it('returns [0, 0] regardless of arguments', () => {
    const ctx = createCtx();
    const reply = repl.waitaof(ctx, ['1', '1', '5000']);
    expect(reply).toEqual({
      kind: 'array',
      value: [
        { kind: 'integer', value: 0 },
        { kind: 'integer', value: 0 },
      ],
    });
  });

  it('returns error for wrong number of arguments', () => {
    const ctx = createCtx();
    const reply = repl.waitaof(ctx, ['0', '0']);
    expect(reply.kind).toBe('error');
  });

  it('returns error for non-integer arguments', () => {
    const ctx = createCtx();
    const reply = repl.waitaof(ctx, ['abc', '0', '0']);
    expect(reply.kind).toBe('error');
  });
});

describe('specs', () => {
  it('exports all replication command specs', () => {
    const names = repl.specs.map((s) => s.name);
    expect(names).toContain('replicaof');
    expect(names).toContain('slaveof');
    expect(names).toContain('replconf');
    expect(names).toContain('psync');
    expect(names).toContain('wait');
    expect(names).toContain('waitaof');
  });

  it('REPLICAOF has arity 3', () => {
    const spec = repl.specs.find((s) => s.name === 'replicaof');
    expect(spec?.arity).toBe(3);
  });

  it('SLAVEOF has arity 3', () => {
    const spec = repl.specs.find((s) => s.name === 'slaveof');
    expect(spec?.arity).toBe(3);
  });

  it('WAIT has arity 3', () => {
    const spec = repl.specs.find((s) => s.name === 'wait');
    expect(spec?.arity).toBe(3);
  });

  it('WAITAOF has arity 4', () => {
    const spec = repl.specs.find((s) => s.name === 'waitaof');
    expect(spec?.arity).toBe(4);
  });

  it('REPLCONF has variable arity', () => {
    const spec = repl.specs.find((s) => s.name === 'replconf');
    expect(spec?.arity).toBe(-1);
  });

  it('PSYNC has arity 3', () => {
    const spec = repl.specs.find((s) => s.name === 'psync');
    expect(spec?.arity).toBe(3);
  });
});
