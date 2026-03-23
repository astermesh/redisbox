import { describe, it, expect } from 'vitest';
import { RedisEngine } from '../../engine.ts';
import type { Reply } from '../../types.ts';
import * as stream from './index.ts';

function createDb(time = 1000) {
  let now = time;
  const engine = new RedisEngine({
    clock: () => now,
    rng: () => 0.5,
  });
  return {
    db: engine.db(0),
    engine,
    setTime: (t: number) => {
      now = t;
    },
    getTime: () => now,
  };
}

/** Seed a stream with 5 entries at 1000-0 … 5000-0 */
function seedStream(
  db: ReturnType<typeof createDb>['db'],
  clock: { setTime: (t: number) => void; getTime: () => number }
) {
  for (let i = 1; i <= 5; i++) {
    clock.setTime(i * 1000);
    stream.xadd(db, clock.getTime(), ['s', '*', 'k', String(i)]);
  }
}

function entryReply(id: string, fields: [string, string][]): Reply {
  return {
    kind: 'array',
    value: [
      { kind: 'bulk', value: id },
      {
        kind: 'array',
        value: fields.flatMap(([f, v]) => [
          { kind: 'bulk', value: f },
          { kind: 'bulk', value: v },
        ]),
      },
    ],
  };
}

// ─── XRANGE ─────────────────────────────────────────────────────────────

describe('XRANGE', () => {
  it('returns all entries with - +', () => {
    const ctx = createDb(0);
    seedStream(ctx.db, ctx);
    const reply = stream.xrange(ctx.db, ['s', '-', '+']);
    expect(reply).toEqual({
      kind: 'array',
      value: [
        entryReply('1000-0', [['k', '1']]),
        entryReply('2000-0', [['k', '2']]),
        entryReply('3000-0', [['k', '3']]),
        entryReply('4000-0', [['k', '4']]),
        entryReply('5000-0', [['k', '5']]),
      ],
    });
  });

  it('returns subset by ID range', () => {
    const ctx = createDb(0);
    seedStream(ctx.db, ctx);
    const reply = stream.xrange(ctx.db, ['s', '2000-0', '4000-0']);
    expect(reply).toEqual({
      kind: 'array',
      value: [
        entryReply('2000-0', [['k', '2']]),
        entryReply('3000-0', [['k', '3']]),
        entryReply('4000-0', [['k', '4']]),
      ],
    });
  });

  it('respects COUNT', () => {
    const ctx = createDb(0);
    seedStream(ctx.db, ctx);
    const reply = stream.xrange(ctx.db, ['s', '-', '+', 'COUNT', '2']);
    expect(reply).toEqual({
      kind: 'array',
      value: [
        entryReply('1000-0', [['k', '1']]),
        entryReply('2000-0', [['k', '2']]),
      ],
    });
  });

  it('returns empty array for non-existent key', () => {
    const { db } = createDb();
    const reply = stream.xrange(db, ['missing', '-', '+']);
    expect(reply).toEqual({ kind: 'array', value: [] });
  });

  it('returns WRONGTYPE for non-stream key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'hello');
    const reply = stream.xrange(db, ['k', '-', '+']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'WRONGTYPE',
      message: 'Operation against a key holding the wrong kind of value',
    });
  });

  it('returns empty array when start > end', () => {
    const ctx = createDb(0);
    seedStream(ctx.db, ctx);
    const reply = stream.xrange(ctx.db, ['s', '5000-0', '1000-0']);
    expect(reply).toEqual({ kind: 'array', value: [] });
  });

  it('handles incomplete IDs (ms only) for start', () => {
    const ctx = createDb(0);
    seedStream(ctx.db, ctx);
    const reply = stream.xrange(ctx.db, ['s', '3000', '5000']);
    expect(reply).toEqual({
      kind: 'array',
      value: [
        entryReply('3000-0', [['k', '3']]),
        entryReply('4000-0', [['k', '4']]),
        entryReply('5000-0', [['k', '5']]),
      ],
    });
  });

  it('returns error for invalid ID', () => {
    const { db } = createDb();
    const reply = stream.xrange(db, ['s', 'abc', '+']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'Invalid stream ID specified as stream command argument',
    });
  });

  it('handles entries with multiple fields', () => {
    const { db, getTime } = createDb(1000);
    stream.xadd(db, getTime(), ['s', '*', 'a', '1', 'b', '2']);
    const reply = stream.xrange(db, ['s', '-', '+']);
    expect(reply).toEqual({
      kind: 'array',
      value: [
        entryReply('1000-0', [
          ['a', '1'],
          ['b', '2'],
        ]),
      ],
    });
  });

  it('COUNT 0 returns empty array', () => {
    const ctx = createDb(0);
    seedStream(ctx.db, ctx);
    const reply = stream.xrange(ctx.db, ['s', '-', '+', 'COUNT', '0']);
    expect(reply).toEqual({ kind: 'array', value: [] });
  });

  it('rejects non-integer COUNT', () => {
    const ctx = createDb(0);
    seedStream(ctx.db, ctx);
    const reply = stream.xrange(ctx.db, ['s', '-', '+', 'COUNT', 'abc']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'value is not an integer or out of range',
    });
  });

  it('rejects negative COUNT', () => {
    const ctx = createDb(0);
    seedStream(ctx.db, ctx);
    const reply = stream.xrange(ctx.db, ['s', '-', '+', 'COUNT', '-1']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'value is not an integer or out of range',
    });
  });

  // ─── Exclusive ranges ( prefix ────────────────────────────────────

  it('exclusive start with ( prefix', () => {
    const ctx = createDb(0);
    seedStream(ctx.db, ctx);
    const reply = stream.xrange(ctx.db, ['s', '(2000-0', '+']);
    expect(reply).toEqual({
      kind: 'array',
      value: [
        entryReply('3000-0', [['k', '3']]),
        entryReply('4000-0', [['k', '4']]),
        entryReply('5000-0', [['k', '5']]),
      ],
    });
  });

  it('exclusive end with ( prefix', () => {
    const ctx = createDb(0);
    seedStream(ctx.db, ctx);
    const reply = stream.xrange(ctx.db, ['s', '-', '(4000-0']);
    expect(reply).toEqual({
      kind: 'array',
      value: [
        entryReply('1000-0', [['k', '1']]),
        entryReply('2000-0', [['k', '2']]),
        entryReply('3000-0', [['k', '3']]),
      ],
    });
  });

  it('exclusive both start and end with ( prefix', () => {
    const ctx = createDb(0);
    seedStream(ctx.db, ctx);
    const reply = stream.xrange(ctx.db, ['s', '(1000-0', '(5000-0']);
    expect(reply).toEqual({
      kind: 'array',
      value: [
        entryReply('2000-0', [['k', '2']]),
        entryReply('3000-0', [['k', '3']]),
        entryReply('4000-0', [['k', '4']]),
      ],
    });
  });

  it('exclusive with incomplete ID (ms only)', () => {
    const ctx = createDb(0);
    seedStream(ctx.db, ctx);
    // (2000 as start → parses as 2000-0, then increments to 2000-1
    const reply = stream.xrange(ctx.db, ['s', '(2000', '+']);
    expect(reply).toEqual({
      kind: 'array',
      value: [
        entryReply('3000-0', [['k', '3']]),
        entryReply('4000-0', [['k', '4']]),
        entryReply('5000-0', [['k', '5']]),
      ],
    });
  });

  it('exclusive range with invalid ID returns error', () => {
    const { db } = createDb();
    const reply = stream.xrange(db, ['s', '(abc', '+']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'Invalid stream ID specified as stream command argument',
    });
  });
});

// ─── XREVRANGE ──────────────────────────────────────────────────────────

describe('XREVRANGE', () => {
  it('returns all entries in reverse with + -', () => {
    const ctx = createDb(0);
    seedStream(ctx.db, ctx);
    const reply = stream.xrevrange(ctx.db, ['s', '+', '-']);
    expect(reply).toEqual({
      kind: 'array',
      value: [
        entryReply('5000-0', [['k', '5']]),
        entryReply('4000-0', [['k', '4']]),
        entryReply('3000-0', [['k', '3']]),
        entryReply('2000-0', [['k', '2']]),
        entryReply('1000-0', [['k', '1']]),
      ],
    });
  });

  it('returns subset by ID range in reverse', () => {
    const ctx = createDb(0);
    seedStream(ctx.db, ctx);
    const reply = stream.xrevrange(ctx.db, ['s', '4000-0', '2000-0']);
    expect(reply).toEqual({
      kind: 'array',
      value: [
        entryReply('4000-0', [['k', '4']]),
        entryReply('3000-0', [['k', '3']]),
        entryReply('2000-0', [['k', '2']]),
      ],
    });
  });

  it('respects COUNT', () => {
    const ctx = createDb(0);
    seedStream(ctx.db, ctx);
    const reply = stream.xrevrange(ctx.db, ['s', '+', '-', 'COUNT', '2']);
    expect(reply).toEqual({
      kind: 'array',
      value: [
        entryReply('5000-0', [['k', '5']]),
        entryReply('4000-0', [['k', '4']]),
      ],
    });
  });

  it('returns empty array for non-existent key', () => {
    const { db } = createDb();
    const reply = stream.xrevrange(db, ['missing', '+', '-']);
    expect(reply).toEqual({ kind: 'array', value: [] });
  });

  it('returns WRONGTYPE for non-stream key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'hello');
    const reply = stream.xrevrange(db, ['k', '+', '-']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'WRONGTYPE',
      message: 'Operation against a key holding the wrong kind of value',
    });
  });

  it('returns empty when end > start (reversed args)', () => {
    const ctx = createDb(0);
    seedStream(ctx.db, ctx);
    const reply = stream.xrevrange(ctx.db, ['s', '1000-0', '5000-0']);
    expect(reply).toEqual({ kind: 'array', value: [] });
  });

  // ─── Exclusive ranges ( prefix ────────────────────────────────────

  it('exclusive end (lower bound) with ( prefix', () => {
    const ctx = createDb(0);
    seedStream(ctx.db, ctx);
    // XREVRANGE s + (2000-0 → entries > 2000-0 in reverse
    const reply = stream.xrevrange(ctx.db, ['s', '+', '(2000-0']);
    expect(reply).toEqual({
      kind: 'array',
      value: [
        entryReply('5000-0', [['k', '5']]),
        entryReply('4000-0', [['k', '4']]),
        entryReply('3000-0', [['k', '3']]),
      ],
    });
  });

  it('exclusive start (upper bound) with ( prefix', () => {
    const ctx = createDb(0);
    seedStream(ctx.db, ctx);
    // XREVRANGE s (4000-0 - → entries < 4000-0 in reverse
    const reply = stream.xrevrange(ctx.db, ['s', '(4000-0', '-']);
    expect(reply).toEqual({
      kind: 'array',
      value: [
        entryReply('3000-0', [['k', '3']]),
        entryReply('2000-0', [['k', '2']]),
        entryReply('1000-0', [['k', '1']]),
      ],
    });
  });
});

// ─── XREAD ──────────────────────────────────────────────────────────────

describe('XREAD', () => {
  it('reads entries after given ID from single stream', () => {
    const ctx = createDb(0);
    seedStream(ctx.db, ctx);
    const reply = stream.xread(ctx.db, ['STREAMS', 's', '2000-0']);
    expect(reply).toEqual({
      kind: 'array',
      value: [
        {
          kind: 'array',
          value: [
            { kind: 'bulk', value: 's' },
            {
              kind: 'array',
              value: [
                entryReply('3000-0', [['k', '3']]),
                entryReply('4000-0', [['k', '4']]),
                entryReply('5000-0', [['k', '5']]),
              ],
            },
          ],
        },
      ],
    });
  });

  it('reads from multiple streams', () => {
    const ctx = createDb(0);
    ctx.setTime(1000);
    stream.xadd(ctx.db, ctx.getTime(), ['a', '*', 'x', '1']);
    ctx.setTime(2000);
    stream.xadd(ctx.db, ctx.getTime(), ['a', '*', 'x', '2']);
    ctx.setTime(1000);
    stream.xadd(ctx.db, ctx.getTime(), ['b', '*', 'y', '10']);

    const reply = stream.xread(ctx.db, ['STREAMS', 'a', 'b', '0-0', '0-0']);
    expect(reply).toEqual({
      kind: 'array',
      value: [
        {
          kind: 'array',
          value: [
            { kind: 'bulk', value: 'a' },
            {
              kind: 'array',
              value: [
                entryReply('1000-0', [['x', '1']]),
                entryReply('2000-0', [['x', '2']]),
              ],
            },
          ],
        },
        {
          kind: 'array',
          value: [
            { kind: 'bulk', value: 'b' },
            {
              kind: 'array',
              value: [entryReply('1000-0', [['y', '10']])],
            },
          ],
        },
      ],
    });
  });

  it('respects COUNT', () => {
    const ctx = createDb(0);
    seedStream(ctx.db, ctx);
    const reply = stream.xread(ctx.db, ['COUNT', '2', 'STREAMS', 's', '0-0']);
    expect(reply).toEqual({
      kind: 'array',
      value: [
        {
          kind: 'array',
          value: [
            { kind: 'bulk', value: 's' },
            {
              kind: 'array',
              value: [
                entryReply('1000-0', [['k', '1']]),
                entryReply('2000-0', [['k', '2']]),
              ],
            },
          ],
        },
      ],
    });
  });

  it('returns nil-array when no entries found', () => {
    const ctx = createDb(0);
    seedStream(ctx.db, ctx);
    const reply = stream.xread(ctx.db, ['STREAMS', 's', '5000-0']);
    expect(reply).toEqual({ kind: 'nil-array' });
  });

  it('returns nil-array for non-existent stream', () => {
    const { db } = createDb();
    const reply = stream.xread(db, ['STREAMS', 'missing', '0-0']);
    expect(reply).toEqual({ kind: 'nil-array' });
  });

  it('returns WRONGTYPE for non-stream key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'hello');
    const reply = stream.xread(db, ['STREAMS', 'k', '0-0']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'WRONGTYPE',
      message: 'Operation against a key holding the wrong kind of value',
    });
  });

  it('skips non-existent streams in multi-stream read', () => {
    const ctx = createDb(1000);
    stream.xadd(ctx.db, ctx.getTime(), ['a', '*', 'x', '1']);

    const reply = stream.xread(ctx.db, [
      'STREAMS',
      'a',
      'missing',
      '0-0',
      '0-0',
    ]);
    expect(reply).toEqual({
      kind: 'array',
      value: [
        {
          kind: 'array',
          value: [
            { kind: 'bulk', value: 'a' },
            {
              kind: 'array',
              value: [entryReply('1000-0', [['x', '1']])],
            },
          ],
        },
      ],
    });
  });

  it('returns syntax error when STREAMS keyword missing', () => {
    const { db } = createDb();
    const reply = stream.xread(db, ['s', '0-0']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'syntax error',
    });
  });

  it('returns error for unbalanced streams/ids', () => {
    const { db } = createDb();
    const reply = stream.xread(db, ['STREAMS', 'a', 'b', '0-0']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message:
        "Unbalanced 'xread' list of streams: for each stream key an ID or '$' must be specified.",
    });
  });

  it('returns error when STREAMS keyword missing and args unbalanced', () => {
    const { db } = createDb();
    const reply = stream.xread(db, ['STREAMS']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message:
        "Unbalanced 'xread' list of streams: for each stream key an ID or '$' must be specified.",
    });
  });

  it('$ ID returns nil-array (non-blocking)', () => {
    const ctx = createDb(0);
    seedStream(ctx.db, ctx);
    const reply = stream.xread(ctx.db, ['STREAMS', 's', '$']);
    expect(reply).toEqual({ kind: 'nil-array' });
  });

  it('BLOCK option is accepted (non-blocking returns immediately)', () => {
    const ctx = createDb(0);
    seedStream(ctx.db, ctx);
    const reply = stream.xread(ctx.db, [
      'COUNT',
      '1',
      'BLOCK',
      '0',
      'STREAMS',
      's',
      '0-0',
    ]);
    expect(reply).toEqual({
      kind: 'array',
      value: [
        {
          kind: 'array',
          value: [
            { kind: 'bulk', value: 's' },
            {
              kind: 'array',
              value: [entryReply('1000-0', [['k', '1']])],
            },
          ],
        },
      ],
    });
  });

  it('reads entries with ID 0-0 (gets all)', () => {
    const ctx = createDb(0);
    seedStream(ctx.db, ctx);
    const reply = stream.xread(ctx.db, ['STREAMS', 's', '0-0']);
    const arr = reply as { kind: 'array'; value: Reply[] };
    const streamEntries = (arr.value[0] as { kind: 'array'; value: Reply[] })
      .value[1] as { kind: 'array'; value: Reply[] };
    expect(streamEntries.value).toHaveLength(5);
  });
});
