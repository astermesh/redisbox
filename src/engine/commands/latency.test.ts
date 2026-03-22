import { describe, it, expect } from 'vitest';
import { RedisEngine } from '../engine.ts';
import type { CommandContext } from '../types.ts';
import {
  latencyLatest,
  latencyHistory,
  latencyReset,
  latencyGraph,
  latencyDoctor,
  latencyHelp,
} from './latency.ts';
import { specs } from './latency.ts';

function createCtx(): CommandContext {
  const engine = new RedisEngine({ clock: () => 1000 });
  return { db: engine.db(0), engine };
}

function latencyDispatch(
  ctx: CommandContext,
  args: string[]
): ReturnType<(typeof specs)[0]['handler']> {
  const spec = specs[0];
  if (!spec) throw new Error('LATENCY spec not found');
  return spec.handler(ctx, args);
}

// Helper to seed latency data
function seedLatency(
  ctx: CommandContext,
  event: string,
  entries: { latency: number; timestamp: number }[]
): void {
  for (const e of entries) {
    ctx.engine.latency.record(event, e.latency, 1, e.timestamp);
  }
}

describe('LATENCY LATEST', () => {
  it('returns empty array when no events recorded', () => {
    const ctx = createCtx();
    const reply = latencyLatest(ctx);
    expect(reply).toEqual({ kind: 'array', value: [] });
  });

  it('returns latest sample for each event', () => {
    const ctx = createCtx();
    seedLatency(ctx, 'command', [
      { latency: 100, timestamp: 1000 },
      { latency: 500, timestamp: 1001 },
      { latency: 200, timestamp: 1002 },
    ]);

    const reply = latencyLatest(ctx);
    expect(reply.kind).toBe('array');
    if (reply.kind !== 'array') return;

    expect(reply.value).toHaveLength(1);
    const entry = reply.value[0];
    if (!entry || entry.kind !== 'array') return;

    // [event-name, timestamp, latest-latency, all-time-max]
    expect(entry.value[0]).toEqual({ kind: 'bulk', value: 'command' });
    expect(entry.value[1]).toEqual({ kind: 'integer', value: 1002 });
    expect(entry.value[2]).toEqual({ kind: 'integer', value: 200 });
    expect(entry.value[3]).toEqual({ kind: 'integer', value: 500 });
  });

  it('returns multiple events', () => {
    const ctx = createCtx();
    seedLatency(ctx, 'command', [{ latency: 100, timestamp: 1000 }]);
    seedLatency(ctx, 'fast-command', [{ latency: 200, timestamp: 1001 }]);

    const reply = latencyLatest(ctx);
    expect(reply.kind).toBe('array');
    if (reply.kind !== 'array') return;
    expect(reply.value).toHaveLength(2);
  });
});

describe('LATENCY HISTORY', () => {
  it('returns error when no event argument', () => {
    const ctx = createCtx();
    const reply = latencyHistory(ctx, []);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message:
        "unknown subcommand or wrong number of arguments for 'latency|HISTORY' command",
    });
  });

  it('returns empty array for unknown event', () => {
    const ctx = createCtx();
    const reply = latencyHistory(ctx, ['unknown']);
    expect(reply).toEqual({ kind: 'array', value: [] });
  });

  it('returns timestamp-latency pairs', () => {
    const ctx = createCtx();
    seedLatency(ctx, 'command', [
      { latency: 100, timestamp: 1000 },
      { latency: 200, timestamp: 1001 },
    ]);

    const reply = latencyHistory(ctx, ['command']);
    expect(reply.kind).toBe('array');
    if (reply.kind !== 'array') return;

    expect(reply.value).toHaveLength(2);

    const first = reply.value[0];
    if (!first || first.kind !== 'array') return;
    expect(first.value[0]).toEqual({ kind: 'integer', value: 1000 });
    expect(first.value[1]).toEqual({ kind: 'integer', value: 100 });

    const second = reply.value[1];
    if (!second || second.kind !== 'array') return;
    expect(second.value[0]).toEqual({ kind: 'integer', value: 1001 });
    expect(second.value[1]).toEqual({ kind: 'integer', value: 200 });
  });
});

describe('LATENCY RESET', () => {
  it('returns 0 when no events exist', () => {
    const ctx = createCtx();
    const reply = latencyReset(ctx, []);
    expect(reply).toEqual({ kind: 'integer', value: 0 });
  });

  it('resets all events when called with no args', () => {
    const ctx = createCtx();
    seedLatency(ctx, 'command', [{ latency: 100, timestamp: 1000 }]);
    seedLatency(ctx, 'fast-command', [{ latency: 200, timestamp: 1001 }]);

    const reply = latencyReset(ctx, []);
    expect(reply).toEqual({ kind: 'integer', value: 2 });
    expect(latencyLatest(ctx)).toEqual({ kind: 'array', value: [] });
  });

  it('resets only specified events', () => {
    const ctx = createCtx();
    seedLatency(ctx, 'command', [{ latency: 100, timestamp: 1000 }]);
    seedLatency(ctx, 'fast-command', [{ latency: 200, timestamp: 1001 }]);

    const reply = latencyReset(ctx, ['command']);
    expect(reply).toEqual({ kind: 'integer', value: 1 });

    const latest = latencyLatest(ctx);
    expect(latest.kind).toBe('array');
    if (latest.kind !== 'array') return;
    expect(latest.value).toHaveLength(1);
  });

  it('returns 0 for non-existent events', () => {
    const ctx = createCtx();
    const reply = latencyReset(ctx, ['nonexistent']);
    expect(reply).toEqual({ kind: 'integer', value: 0 });
  });
});

describe('LATENCY GRAPH', () => {
  it('returns error when no event argument', () => {
    const ctx = createCtx();
    const reply = latencyGraph(ctx, []);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message:
        "unknown subcommand or wrong number of arguments for 'latency|GRAPH' command",
    });
  });

  it('returns event name with dash for empty event', () => {
    const ctx = createCtx();
    const reply = latencyGraph(ctx, ['command']);
    expect(reply).toEqual({ kind: 'bulk', value: 'command - ' });
  });

  it('returns graph with samples', () => {
    const ctx = createCtx();
    seedLatency(ctx, 'command', [
      { latency: 100, timestamp: 1000 },
      { latency: 200, timestamp: 1001 },
      { latency: 150, timestamp: 1002 },
    ]);

    const reply = latencyGraph(ctx, ['command']);
    expect(reply.kind).toBe('bulk');
    if (reply.kind !== 'bulk') return;
    expect(reply.value).toBeTruthy();
    // Should contain the event name and high/low info
    expect(reply.value).toContain('command');
    expect(reply.value).toContain('high 200 ms');
    expect(reply.value).toContain('low 100 ms');
    // Should contain # characters for the graph bars
    expect(reply.value).toContain('#');
  });
});

describe('LATENCY DOCTOR', () => {
  it('returns no-data message when no events', () => {
    const ctx = createCtx();
    const reply = latencyDoctor(ctx);
    expect(reply.kind).toBe('bulk');
    if (reply.kind !== 'bulk') return;
    expect(reply.value).toContain('no latency reports');
    expect(reply.value).toContain('latency-monitor-threshold');
  });

  it('returns analysis with event data', () => {
    const ctx = createCtx();
    seedLatency(ctx, 'command', [{ latency: 100, timestamp: 1000 }]);
    seedLatency(ctx, 'fast-command', [{ latency: 200, timestamp: 1001 }]);

    const reply = latencyDoctor(ctx);
    expect(reply.kind).toBe('bulk');
    if (reply.kind !== 'bulk') return;
    expect(reply.value).toContain('command');
    expect(reply.value).toContain('fast-command');
    expect(reply.value).toContain('100 ms');
    expect(reply.value).toContain('200 ms');
  });
});

describe('LATENCY HELP', () => {
  it('returns array of bulk strings', () => {
    const reply = latencyHelp();
    expect(reply.kind).toBe('array');
    if (reply.kind !== 'array') return;
    expect(reply.value.length).toBeGreaterThan(0);
    for (const line of reply.value) {
      expect(line.kind).toBe('bulk');
    }
  });

  it('mentions all subcommands', () => {
    const reply = latencyHelp();
    if (reply.kind !== 'array') return;
    const text = reply.value
      .map((v) => (v.kind === 'bulk' ? v.value : ''))
      .join('\n');
    expect(text).toContain('LATEST');
    expect(text).toContain('HISTORY');
    expect(text).toContain('RESET');
    expect(text).toContain('GRAPH');
    expect(text).toContain('DOCTOR');
    expect(text).toContain('HELP');
  });
});

describe('LATENCY specs', () => {
  it('exports LATENCY spec with subcommands', () => {
    expect(specs).toHaveLength(1);
    const spec = specs[0];
    if (!spec) return;
    expect(spec.name).toBe('LATENCY');
    expect(spec.arity).toBe(-2);
    expect(spec.subcommands).toBeDefined();

    const subs = spec.subcommands ?? [];
    expect(subs.length).toBe(6);

    const subNames = subs.map((s) => s.name);
    expect(subNames).toContain('LATEST');
    expect(subNames).toContain('HISTORY');
    expect(subNames).toContain('RESET');
    expect(subNames).toContain('GRAPH');
    expect(subNames).toContain('DOCTOR');
    expect(subNames).toContain('HELP');
  });
});

describe('LATENCY dispatcher', () => {
  it('dispatches to unknown subcommand error', () => {
    const ctx = createCtx();
    const reply = latencyDispatch(ctx, ['UNKNOWN']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message:
        "unknown subcommand or wrong number of arguments for 'latency|UNKNOWN' command",
    });
  });

  it('returns error with no subcommand', () => {
    const ctx = createCtx();
    const reply = latencyDispatch(ctx, []);
    expect(reply.kind).toBe('error');
  });

  it('dispatches LATEST subcommand', () => {
    const ctx = createCtx();
    const reply = latencyDispatch(ctx, ['LATEST']);
    expect(reply).toEqual({ kind: 'array', value: [] });
  });

  it('dispatches HISTORY subcommand', () => {
    const ctx = createCtx();
    seedLatency(ctx, 'command', [{ latency: 100, timestamp: 1000 }]);
    const reply = latencyDispatch(ctx, ['HISTORY', 'command']);
    expect(reply.kind).toBe('array');
  });

  it('dispatches RESET subcommand', () => {
    const ctx = createCtx();
    const reply = latencyDispatch(ctx, ['RESET']);
    expect(reply).toEqual({ kind: 'integer', value: 0 });
  });

  it('dispatches GRAPH subcommand', () => {
    const ctx = createCtx();
    const reply = latencyDispatch(ctx, ['GRAPH', 'command']);
    expect(reply.kind).toBe('bulk');
  });

  it('dispatches DOCTOR subcommand', () => {
    const ctx = createCtx();
    const reply = latencyDispatch(ctx, ['DOCTOR']);
    expect(reply.kind).toBe('bulk');
  });

  it('dispatches HELP subcommand', () => {
    const ctx = createCtx();
    const reply = latencyDispatch(ctx, ['HELP']);
    expect(reply.kind).toBe('array');
  });

  it('is case-insensitive for subcommands', () => {
    const ctx = createCtx();
    expect(latencyDispatch(ctx, ['latest']).kind).toBe('array');
    expect(latencyDispatch(ctx, ['Latest']).kind).toBe('array');
    expect(latencyDispatch(ctx, ['reset']).kind).toBe('integer');
    expect(latencyDispatch(ctx, ['doctor']).kind).toBe('bulk');
  });
});
