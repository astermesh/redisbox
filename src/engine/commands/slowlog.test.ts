import { describe, it, expect } from 'vitest';
import { RedisEngine } from '../engine.ts';
import type { CommandContext } from '../types.ts';
import {
  slowlogGet,
  slowlogLen,
  slowlogReset,
  slowlogHelp,
} from './slowlog.ts';
import { specs } from './slowlog.ts';

function createCtx(): CommandContext {
  const engine = new RedisEngine({ clock: () => 1000 });
  return { db: engine.db(0), engine };
}

// The main SLOWLOG handler
function slowlogDispatch(
  ctx: CommandContext,
  args: string[]
): ReturnType<(typeof specs)[0]['handler']> {
  const spec = specs[0];
  if (!spec) throw new Error('SLOWLOG spec not found');
  return spec.handler(ctx, args);
}

describe('SLOWLOG GET', () => {
  it('returns empty array when no entries', () => {
    const ctx = createCtx();
    const reply = slowlogGet(ctx, []);
    expect(reply).toEqual({ kind: 'array', value: [] });
  });

  it('returns entries after recording', () => {
    const ctx = createCtx();
    ctx.engine.slowlog.record(
      15000,
      10000,
      128,
      1609459200,
      ['SET', 'k', 'v'],
      '127.0.0.1:6379',
      'app'
    );

    const reply = slowlogGet(ctx, []);
    expect(reply.kind).toBe('array');
    if (reply.kind !== 'array') return;

    expect(reply.value).toHaveLength(1);
    const entry = reply.value[0];
    if (!entry || entry.kind !== 'array') return;

    // id
    expect(entry.value[0]).toEqual({ kind: 'integer', value: 0 });
    // timestamp
    expect(entry.value[1]).toEqual({ kind: 'integer', value: 1609459200 });
    // duration
    expect(entry.value[2]).toEqual({ kind: 'integer', value: 15000 });
    // args
    expect(entry.value[3]).toEqual({
      kind: 'array',
      value: [
        { kind: 'bulk', value: 'SET' },
        { kind: 'bulk', value: 'k' },
        { kind: 'bulk', value: 'v' },
      ],
    });
    // client addr
    expect(entry.value[4]).toEqual({ kind: 'bulk', value: '127.0.0.1:6379' });
    // client name
    expect(entry.value[5]).toEqual({ kind: 'bulk', value: 'app' });
  });

  it('defaults to 10 entries without count', () => {
    const ctx = createCtx();
    for (let i = 0; i < 20; i++) {
      ctx.engine.slowlog.record(
        15000,
        10000,
        128,
        1000,
        ['CMD', String(i)],
        '',
        ''
      );
    }

    const reply = slowlogGet(ctx, []);
    expect(reply.kind).toBe('array');
    if (reply.kind !== 'array') return;
    expect(reply.value).toHaveLength(10);
  });

  it('respects count parameter', () => {
    const ctx = createCtx();
    for (let i = 0; i < 5; i++) {
      ctx.engine.slowlog.record(
        15000,
        10000,
        128,
        1000,
        ['CMD', String(i)],
        '',
        ''
      );
    }

    const reply = slowlogGet(ctx, ['2']);
    expect(reply.kind).toBe('array');
    if (reply.kind !== 'array') return;
    expect(reply.value).toHaveLength(2);
  });

  it('returns error for non-integer count', () => {
    const ctx = createCtx();
    const reply = slowlogGet(ctx, ['abc']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'count should be greater than or equal to -1',
    });
  });

  it('returns error for count less than -1', () => {
    const ctx = createCtx();
    const reply = slowlogGet(ctx, ['-2']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'count should be greater than or equal to -1',
    });
  });

  it('returns all entries for count -1', () => {
    const ctx = createCtx();
    for (let i = 0; i < 15; i++) {
      ctx.engine.slowlog.record(15000, 10000, 128, 1000, ['CMD'], '', '');
    }
    const reply = slowlogGet(ctx, ['-1']);
    expect(reply.kind).toBe('array');
    if (reply.kind !== 'array') return;
    expect(reply.value).toHaveLength(15);
  });

  it('returns empty for count 0', () => {
    const ctx = createCtx();
    ctx.engine.slowlog.record(15000, 10000, 128, 1000, ['CMD'], '', '');
    const reply = slowlogGet(ctx, ['0']);
    expect(reply).toEqual({ kind: 'array', value: [] });
  });
});

describe('SLOWLOG LEN', () => {
  it('returns 0 when empty', () => {
    const ctx = createCtx();
    expect(slowlogLen(ctx)).toEqual({ kind: 'integer', value: 0 });
  });

  it('returns count after recording', () => {
    const ctx = createCtx();
    ctx.engine.slowlog.record(15000, 10000, 128, 1000, ['A'], '', '');
    ctx.engine.slowlog.record(15000, 10000, 128, 1000, ['B'], '', '');
    expect(slowlogLen(ctx)).toEqual({ kind: 'integer', value: 2 });
  });
});

describe('SLOWLOG RESET', () => {
  it('clears entries and returns OK', () => {
    const ctx = createCtx();
    ctx.engine.slowlog.record(15000, 10000, 128, 1000, ['A'], '', '');
    expect(slowlogReset(ctx)).toEqual({ kind: 'status', value: 'OK' });
    expect(slowlogLen(ctx)).toEqual({ kind: 'integer', value: 0 });
  });
});

describe('SLOWLOG HELP', () => {
  it('returns array of bulk strings', () => {
    const reply = slowlogHelp();
    expect(reply.kind).toBe('array');
    if (reply.kind !== 'array') return;
    expect(reply.value.length).toBeGreaterThan(0);
    for (const line of reply.value) {
      expect(line.kind).toBe('bulk');
    }
  });

  it('mentions default count of 10', () => {
    const reply = slowlogHelp();
    if (reply.kind !== 'array') return;
    const text = reply.value
      .map((v) => (v.kind === 'bulk' ? v.value : ''))
      .join('\n');
    expect(text).toContain('default: 10');
  });
});

describe('SLOWLOG specs', () => {
  it('exports SLOWLOG spec with subcommands', () => {
    expect(specs).toHaveLength(1);
    const spec = specs[0];
    if (!spec) return;
    expect(spec.name).toBe('SLOWLOG');
    expect(spec.arity).toBe(-2);
    expect(spec.subcommands).toBeDefined();

    const subs = spec.subcommands ?? [];
    expect(subs.length).toBe(4);

    const subNames = subs.map((s) => s.name);
    expect(subNames).toContain('GET');
    expect(subNames).toContain('LEN');
    expect(subNames).toContain('RESET');
    expect(subNames).toContain('HELP');
  });
});

describe('SLOWLOG dispatcher', () => {
  it('dispatches to unknown subcommand error', () => {
    const ctx = createCtx();
    const reply = slowlogDispatch(ctx, ['UNKNOWN']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message:
        "unknown subcommand or wrong number of arguments for 'slowlog|UNKNOWN' command",
    });
  });

  it('dispatches GET subcommand', () => {
    const ctx = createCtx();
    const reply = slowlogDispatch(ctx, ['GET']);
    expect(reply).toEqual({ kind: 'array', value: [] });
  });

  it('dispatches LEN subcommand', () => {
    const ctx = createCtx();
    const reply = slowlogDispatch(ctx, ['LEN']);
    expect(reply).toEqual({ kind: 'integer', value: 0 });
  });

  it('dispatches RESET subcommand', () => {
    const ctx = createCtx();
    const reply = slowlogDispatch(ctx, ['RESET']);
    expect(reply).toEqual({ kind: 'status', value: 'OK' });
  });

  it('dispatches HELP subcommand', () => {
    const ctx = createCtx();
    const reply = slowlogDispatch(ctx, ['HELP']);
    expect(reply.kind).toBe('array');
  });

  it('is case-insensitive for subcommands', () => {
    const ctx = createCtx();
    expect(slowlogDispatch(ctx, ['get']).kind).toBe('array');
    expect(slowlogDispatch(ctx, ['len']).kind).toBe('integer');
    expect(slowlogDispatch(ctx, ['reset']).kind).toBe('status');
  });

  it('returns error with no subcommand', () => {
    const ctx = createCtx();
    const reply = slowlogDispatch(ctx, []);
    expect(reply.kind).toBe('error');
  });
});
