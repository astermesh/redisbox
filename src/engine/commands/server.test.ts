import { describe, it, expect } from 'vitest';
import { RedisEngine } from '../engine.ts';
import type { Database } from '../database.ts';
import { RedisStream } from '../stream.ts';
import * as cmd from './server.ts';
import type { Reply } from '../types.ts';

function createDb(time = 1000): {
  db: Database;
  engine: RedisEngine;
  setTime: (t: number) => void;
  clock: () => number;
} {
  let now = time;
  const clock = () => now;
  const engine = new RedisEngine({
    clock,
    rng: () => 0.5,
  });
  return {
    db: engine.db(0),
    engine,
    setTime: (t: number) => {
      now = t;
    },
    clock,
  };
}

// ---------------------------------------------------------------------------
// TIME
// ---------------------------------------------------------------------------

describe('TIME', () => {
  it('returns array of two bulk strings [seconds, microseconds]', () => {
    // clock returns 1_500_123 ms → 1500 seconds, 123000 microseconds
    const { clock } = createDb(1_500_123);
    const reply = cmd.time(clock);
    expect(reply).toEqual({
      kind: 'array',
      value: [
        { kind: 'bulk', value: '1500' },
        { kind: 'bulk', value: '123000' },
      ],
    });
  });

  it('returns 0 microseconds for exact second boundary', () => {
    const { clock } = createDb(2_000_000);
    const reply = cmd.time(clock);
    expect(reply).toEqual({
      kind: 'array',
      value: [
        { kind: 'bulk', value: '2000' },
        { kind: 'bulk', value: '0' },
      ],
    });
  });

  it('handles epoch zero', () => {
    const { clock } = createDb(0);
    const reply = cmd.time(clock);
    expect(reply).toEqual({
      kind: 'array',
      value: [
        { kind: 'bulk', value: '0' },
        { kind: 'bulk', value: '0' },
      ],
    });
  });

  it('handles large timestamps', () => {
    // 1700000000000 ms = 1700000000 seconds
    const { clock } = createDb(1_700_000_000_456);
    const reply = cmd.time(clock);
    expect(reply).toEqual({
      kind: 'array',
      value: [
        { kind: 'bulk', value: '1700000000' },
        { kind: 'bulk', value: '456000' },
      ],
    });
  });
});

// ---------------------------------------------------------------------------
// DEBUG OBJECT
// ---------------------------------------------------------------------------

describe('DEBUG OBJECT', () => {
  it('returns debug info for a string key', () => {
    const { db, clock } = createDb(1000);
    db.set('mykey', 'string', 'embstr', 'hello');
    const reply = cmd.debugObject(db, clock, ['mykey']);
    expect(reply.kind).toBe('status');
    if (reply.kind === 'status') {
      expect(reply.value).toContain('Value at:');
      expect(reply.value).toContain('refcount:1');
      expect(reply.value).toContain('encoding:embstr');
      expect(reply.value).toContain('serializedlength:');
      expect(reply.value).toContain('lru_seconds_idle:');
      expect(reply.value).toContain('type:string');
    }
  });

  it('returns error for missing key', () => {
    const { db, clock } = createDb();
    const reply = cmd.debugObject(db, clock, ['missing']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'no such key',
    });
  });

  it('reports correct encoding for different types', () => {
    const { db, clock } = createDb(1000);
    db.set('h', 'hash', 'listpack', new Map([['f', 'v']]));
    const reply = cmd.debugObject(db, clock, ['h']);
    expect(reply.kind).toBe('status');
    if (reply.kind === 'status') {
      expect(reply.value).toContain('encoding:listpack');
      expect(reply.value).toContain('type:hash');
    }
  });

  it('reports correct info for stream type', () => {
    const { db, clock } = createDb(1000);
    const stream = new RedisStream();
    stream.addEntry({ ms: 1, seq: 0 }, [['field', 'value']]);
    db.set('s', 'stream', 'stream', stream);
    const reply = cmd.debugObject(db, clock, ['s']);
    expect(reply.kind).toBe('status');
    if (reply.kind === 'status') {
      expect(reply.value).toContain('type:stream');
      expect(reply.value).toContain('encoding:stream');
      expect(reply.value).toContain('serializedlength:');
      // serializedlength should be > 1 (not falling to default)
      const match = reply.value.match(/serializedlength:(\d+)/);
      expect(Number(match?.[1])).toBeGreaterThan(1);
    }
  });

  it('reports idle time in seconds', () => {
    const { db, setTime } = createDb(1000);
    db.set('k', 'string', 'raw', 'v');
    setTime(6000);
    // debugObject uses getWithoutTouch, so we use a clock that returns 6000
    const reply = cmd.debugObject(db, () => 6000, ['k']);
    expect(reply.kind).toBe('status');
    if (reply.kind === 'status') {
      expect(reply.value).toContain('lru_seconds_idle:5');
    }
  });
});

// ---------------------------------------------------------------------------
// DEBUG SLEEP
// ---------------------------------------------------------------------------

describe('DEBUG SLEEP', () => {
  it('returns OK (stub — does not actually block)', () => {
    const reply = cmd.debugSleep();
    expect(reply).toEqual({ kind: 'status', value: 'OK' });
  });
});

// ---------------------------------------------------------------------------
// DEBUG SET-ACTIVE-EXPIRE
// ---------------------------------------------------------------------------

describe('DEBUG SET-ACTIVE-EXPIRE', () => {
  it('returns OK (stub — does not affect active expiration)', () => {
    const reply = cmd.debugSetActiveExpire();
    expect(reply).toEqual({ kind: 'status', value: 'OK' });
  });
});

// ---------------------------------------------------------------------------
// DEBUG HELP
// ---------------------------------------------------------------------------

describe('DEBUG HELP', () => {
  it('returns array of help strings', () => {
    const reply = cmd.debugHelp();
    expect(reply.kind).toBe('array');
    if (reply.kind === 'array') {
      expect(reply.value.length).toBeGreaterThan(0);
      expect(reply.value.every((r: Reply) => r.kind === 'bulk')).toBe(true);
    }
  });
});

// ---------------------------------------------------------------------------
// DEBUG dispatcher
// ---------------------------------------------------------------------------

describe('DEBUG (dispatcher)', () => {
  it('dispatches to OBJECT subcommand', () => {
    const { db, clock } = createDb(1000);
    db.set('k', 'string', 'raw', 'v');
    const reply = cmd.debug(db, clock, ['OBJECT', 'k']);
    expect(reply.kind).toBe('status');
  });

  it('dispatches to SLEEP subcommand', () => {
    const reply = cmd.debug(null as unknown as Database, () => 0, [
      'SLEEP',
      '0',
    ]);
    expect(reply).toEqual({ kind: 'status', value: 'OK' });
  });

  it('dispatches to SET-ACTIVE-EXPIRE subcommand', () => {
    const reply = cmd.debug(null as unknown as Database, () => 0, [
      'SET-ACTIVE-EXPIRE',
      '1',
    ]);
    expect(reply).toEqual({ kind: 'status', value: 'OK' });
  });

  it('dispatches to HELP subcommand', () => {
    const reply = cmd.debug(null as unknown as Database, () => 0, ['HELP']);
    expect(reply.kind).toBe('array');
  });

  it('returns error for unknown subcommand', () => {
    const reply = cmd.debug(null as unknown as Database, () => 0, [
      'UNKNOWN',
      'x',
    ]);
    expect(reply.kind).toBe('error');
  });

  it('returns error with no args', () => {
    const reply = cmd.debug(null as unknown as Database, () => 0, []);
    expect(reply.kind).toBe('error');
  });

  it('is case-insensitive for subcommands', () => {
    const { db, clock } = createDb(1000);
    db.set('k', 'string', 'raw', 'v');
    expect(cmd.debug(db, clock, ['object', 'k']).kind).toBe('status');
    expect(cmd.debug(db, clock, ['Object', 'k']).kind).toBe('status');
  });
});

// ---------------------------------------------------------------------------
// MONITOR
// ---------------------------------------------------------------------------

describe('MONITOR', () => {
  it('returns OK status', () => {
    const reply = cmd.monitor();
    expect(reply).toEqual({ kind: 'status', value: 'OK' });
  });
});
