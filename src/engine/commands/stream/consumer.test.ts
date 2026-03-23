import { describe, it, expect } from 'vitest';
import { RedisEngine } from '../../engine.ts';
import type { Reply } from '../../types.ts';
import { RedisStream } from '../../stream.ts';
import type { Database } from '../../database.ts';
import * as stream from './index.ts';

function getStreamHelper(
  db: Database,
  key: string
): { stream: RedisStream | null } {
  const entry = db.get(key);
  if (!entry || entry.type !== 'stream') return { stream: null };
  return { stream: entry.value as RedisStream };
}

function getGroup(
  s: RedisStream | null,
  groupName: string
): import('../../stream.ts').ConsumerGroup {
  if (!s) throw new Error('stream is null');
  const group = s.getGroup(groupName);
  if (!group) throw new Error(`group ${groupName} not found`);
  return group;
}

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

const xgroupSpec = stream.specs.find((s) => s.name === 'xgroup');

function execXgroup(ctx: ReturnType<typeof createDb>, args: string[]): Reply {
  if (!xgroupSpec) throw new Error('xgroup spec not found');
  return xgroupSpec.handler({ db: ctx.db, engine: ctx.engine }, args);
}

function execXreadgroup(
  ctx: ReturnType<typeof createDb>,
  args: string[]
): Reply {
  const spec = stream.specs.find((s) => s.name === 'xreadgroup');
  if (!spec) throw new Error('xreadgroup spec not found');
  return spec.handler({ db: ctx.db, engine: ctx.engine }, args);
}

function setupGroupWithEntries(time = 1000) {
  const ctx = createDb(time);
  // Add 5 entries: 1000-0 through 5000-0
  for (let i = 1; i <= 5; i++) {
    ctx.setTime(i * 1000);
    stream.xadd(ctx.db, ctx.getTime(), ['s', '*', 'k', String(i)]);
  }
  // Create group starting at 0 (all entries are new)
  execXgroup(ctx, ['CREATE', 's', 'g1', '0']);
  return ctx;
}

function execXack(ctx: ReturnType<typeof createDb>, args: string[]): Reply {
  const spec = stream.specs.find((s) => s.name === 'xack');
  if (!spec) throw new Error('xack spec not found');
  return spec.handler({ db: ctx.db, engine: ctx.engine }, args);
}

function execXpending(ctx: ReturnType<typeof createDb>, args: string[]): Reply {
  const spec = stream.specs.find((s) => s.name === 'xpending');
  if (!spec) throw new Error('xpending spec not found');
  return spec.handler({ db: ctx.db, engine: ctx.engine }, args);
}

function execXdel(ctx: ReturnType<typeof createDb>, args: string[]): Reply {
  return stream.xdel(ctx.db, args);
}

function execXclaim(ctx: ReturnType<typeof createDb>, args: string[]): Reply {
  const spec = stream.specs.find((s) => s.name === 'xclaim');
  if (!spec) throw new Error('xclaim spec not found');
  return spec.handler({ db: ctx.db, engine: ctx.engine }, args);
}

function setupClaimScenario() {
  const ctx = createDb(1000);
  // Add 5 entries
  for (let i = 1; i <= 5; i++) {
    ctx.setTime(i * 1000);
    stream.xadd(ctx.db, ctx.getTime(), ['s', '*', 'k', String(i)]);
  }
  // Create group at 0
  execXgroup(ctx, ['CREATE', 's', 'g1', '0']);
  // Alice reads all 5 entries
  ctx.setTime(10000);
  execXreadgroup(ctx, ['GROUP', 'g1', 'alice', 'STREAMS', 's', '>']);
  return ctx;
}

function execXautoclaim(
  ctx: ReturnType<typeof createDb>,
  args: string[]
): Reply {
  const spec = stream.specs.find((s) => s.name === 'xautoclaim');
  if (!spec) throw new Error('xautoclaim spec not found');
  return spec.handler({ db: ctx.db, engine: ctx.engine }, args);
}

// ─── XREADGROUP ──────────────────────────────────────────────────────

describe('XREADGROUP', () => {
  it('reads new messages with > ID', () => {
    const ctx = setupGroupWithEntries();
    const reply = execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'STREAMS',
      's',
      '>',
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
              value: [
                entryReply('1000-0', [['k', '1']]),
                entryReply('2000-0', [['k', '2']]),
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

  it('returns nil-array when no new messages with >', () => {
    const ctx = setupGroupWithEntries();
    // Read all
    execXreadgroup(ctx, ['GROUP', 'g1', 'alice', 'STREAMS', 's', '>']);
    // Try reading again — no new entries
    const reply = execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'STREAMS',
      's',
      '>',
    ]);
    expect(reply).toEqual({ kind: 'nil-array' });
  });

  it('respects COUNT with >', () => {
    const ctx = setupGroupWithEntries();
    const reply = execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'COUNT',
      '2',
      'STREAMS',
      's',
      '>',
    ]);
    // Should only get first 2 entries
    const arr = reply as { kind: 'array'; value: Reply[] };
    const streamArr = (arr.value[0] as { kind: 'array'; value: Reply[] })
      .value[1] as { kind: 'array'; value: Reply[] };
    expect(streamArr.value.length).toBe(2);
  });

  it('adds messages to PEL when reading with >', () => {
    const ctx = setupGroupWithEntries();
    execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'COUNT',
      '2',
      'STREAMS',
      's',
      '>',
    ]);
    // Now read pending for alice with specific ID 0-0
    const reply = execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'STREAMS',
      's',
      '0-0',
    ]);
    const arr = reply as { kind: 'array'; value: Reply[] };
    const streamArr = (arr.value[0] as { kind: 'array'; value: Reply[] })
      .value[1] as { kind: 'array'; value: Reply[] };
    expect(streamArr.value.length).toBe(2);
  });

  it('reads pending entries when using specific ID (not >)', () => {
    const ctx = setupGroupWithEntries();
    // Read 3 entries to add to PEL
    execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'COUNT',
      '3',
      'STREAMS',
      's',
      '>',
    ]);
    // Read pending from start
    const reply = execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
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
              value: [
                entryReply('1000-0', [['k', '1']]),
                entryReply('2000-0', [['k', '2']]),
                entryReply('3000-0', [['k', '3']]),
              ],
            },
          ],
        },
      ],
    });
  });

  it('returns empty array for pending when consumer has none', () => {
    const ctx = setupGroupWithEntries();
    // alice has no pending entries
    const reply = execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'STREAMS',
      's',
      '0-0',
    ]);
    // Should return stream with empty array (not nil-array)
    expect(reply).toEqual({
      kind: 'array',
      value: [
        {
          kind: 'array',
          value: [
            { kind: 'bulk', value: 's' },
            { kind: 'array', value: [] },
          ],
        },
      ],
    });
  });

  it('auto-creates consumer on read', () => {
    const ctx = setupGroupWithEntries();
    execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'newconsumer',
      'COUNT',
      '1',
      'STREAMS',
      's',
      '>',
    ]);
    const { stream: s } = getStreamHelper(ctx.db, 's');
    const group = getGroup(s, 'g1');
    expect(group.consumers.has('newconsumer')).toBe(true);
  });

  it('increments delivery count on re-read of pending', () => {
    const ctx = setupGroupWithEntries();
    execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'COUNT',
      '1',
      'STREAMS',
      's',
      '>',
    ]);
    // Read pending — delivery count increments (matches real Redis)
    execXreadgroup(ctx, ['GROUP', 'g1', 'alice', 'STREAMS', 's', '0-0']);
    const { stream: s } = getStreamHelper(ctx.db, 's');
    const group = getGroup(s, 'g1');
    const pe = group.pel.get('1000-0');
    expect(pe).toBeDefined();
    expect(pe?.deliveryCount).toBe(2);
  });

  it('returns NOGROUP for non-existent group', () => {
    const ctx = setupGroupWithEntries();
    const reply = execXreadgroup(ctx, [
      'GROUP',
      'nogroup',
      'alice',
      'STREAMS',
      's',
      '>',
    ]);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'NOGROUP',
      message:
        "No such key 's' or consumer group 'nogroup' in XREADGROUP with GROUP option",
    });
  });

  it('returns WRONGTYPE for non-stream key', () => {
    const ctx = createDb(1000);
    ctx.db.set('str', 'string', 'raw', 'hello');
    const reply = execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'STREAMS',
      'str',
      '>',
    ]);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'WRONGTYPE',
      message: 'Operation against a key holding the wrong kind of value',
    });
  });

  it('returns NOGROUP for non-existent key', () => {
    const ctx = createDb(1000);
    const reply = execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'STREAMS',
      'nokey',
      '>',
    ]);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'NOGROUP',
      message:
        "No such key 'nokey' or consumer group 'g1' in XREADGROUP with GROUP option",
    });
  });

  it('accepts NOACK option (entries not added to PEL)', () => {
    const ctx = setupGroupWithEntries();
    execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'COUNT',
      '2',
      'NOACK',
      'STREAMS',
      's',
      '>',
    ]);
    // With NOACK, entries should NOT be in PEL
    const { stream: s } = getStreamHelper(ctx.db, 's');
    const group = getGroup(s, 'g1');
    expect(group.pel.size).toBe(0);
  });

  it('updates lastDeliveredId after reading with >', () => {
    const ctx = setupGroupWithEntries();
    execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'COUNT',
      '2',
      'STREAMS',
      's',
      '>',
    ]);
    const { stream: s } = getStreamHelper(ctx.db, 's');
    const group = getGroup(s, 'g1');
    expect(group.lastDeliveredId).toEqual({ ms: 2000, seq: 0 });
  });

  it('handles multiple streams', () => {
    const ctx = createDb(1000);
    stream.xadd(ctx.db, ctx.getTime(), ['s1', '*', 'a', '1']);
    stream.xadd(ctx.db, ctx.getTime(), ['s2', '*', 'b', '2']);
    execXgroup(ctx, ['CREATE', 's1', 'g1', '0']);
    execXgroup(ctx, ['CREATE', 's2', 'g1', '0']);
    const reply = execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'STREAMS',
      's1',
      's2',
      '>',
      '>',
    ]);
    const arr = reply as { kind: 'array'; value: Reply[] };
    expect(arr.value.length).toBe(2);
  });

  it('requires GROUP keyword', () => {
    const ctx = setupGroupWithEntries();
    const reply = execXreadgroup(ctx, [
      'NOTGROUP',
      'g1',
      'alice',
      'STREAMS',
      's',
      '>',
    ]);
    expect(reply.kind).toBe('error');
  });

  it('returns error for $ as ID', () => {
    const ctx = setupGroupWithEntries();
    const reply = execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'STREAMS',
      's',
      '$',
    ]);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message:
        'The $ ID is meaningless in the context of XREADGROUP: you want to read the history of this consumer by specifying a proper ID, or use the > ID to get new messages. The $ ID would just return an empty result set.',
    });
  });

  it('updates delivery time and count when re-reading pending', () => {
    const ctx = setupGroupWithEntries();
    ctx.setTime(10000);
    execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'COUNT',
      '2',
      'STREAMS',
      's',
      '>',
    ]);
    // Advance time and re-read pending
    ctx.setTime(20000);
    execXreadgroup(ctx, ['GROUP', 'g1', 'alice', 'STREAMS', 's', '0-0']);
    const { stream: s } = getStreamHelper(ctx.db, 's');
    const group = getGroup(s, 'g1');
    const pe = group.pel.get('1000-0');
    expect(pe).toBeDefined();
    expect(pe?.deliveryCount).toBe(2);
    expect(pe?.deliveryTime).toBe(20000);
  });

  it('returns null fields for pending entries that were trimmed', () => {
    const ctx = createDb(1000);
    // Add entries
    stream.xadd(ctx.db, ctx.getTime(), ['s', '*', 'k', '1']);
    ctx.setTime(2000);
    stream.xadd(ctx.db, ctx.getTime(), ['s', '*', 'k', '2']);
    ctx.setTime(3000);
    stream.xadd(ctx.db, ctx.getTime(), ['s', '*', 'k', '3']);
    // Create group starting at 0
    execXgroup(ctx, ['CREATE', 's', 'g1', '0']);
    // Read all 3 entries
    execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'COUNT',
      '3',
      'STREAMS',
      's',
      '>',
    ]);
    // Trim the stream (removes first entry)
    ctx.setTime(4000);
    stream.xadd(ctx.db, ctx.getTime(), ['s', 'MAXLEN', '2', '*', 'k', '4']);
    // Re-read pending — trimmed entry 1000-0 should return [id, null]
    const reply = execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'STREAMS',
      's',
      '0-0',
    ]);
    const arr = reply as { kind: 'array'; value: Reply[] };
    const entries = (arr.value[0] as { kind: 'array'; value: Reply[] })
      .value[1] as { kind: 'array'; value: Reply[] };
    // First entry was trimmed — should have null fields
    expect(entries.value[0]).toEqual({
      kind: 'array',
      value: [
        { kind: 'bulk', value: '1000-0' },
        { kind: 'bulk', value: null },
      ],
    });
    // Remaining entries should be normal
    expect(entries.value.length).toBe(3);
  });

  it('group starting at $ only reads entries added after group creation', () => {
    const ctx = createDb(1000);
    stream.xadd(ctx.db, ctx.getTime(), ['s', '*', 'k', '1']);
    ctx.setTime(2000);
    stream.xadd(ctx.db, ctx.getTime(), ['s', '*', 'k', '2']);
    execXgroup(ctx, ['CREATE', 's', 'g1', '$']);
    // No new messages since group was created at $
    const reply = execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'STREAMS',
      's',
      '>',
    ]);
    expect(reply).toEqual({ kind: 'nil-array' });
    // Now add a new entry
    ctx.setTime(3000);
    stream.xadd(ctx.db, ctx.getTime(), ['s', '*', 'k', '3']);
    const reply2 = execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'STREAMS',
      's',
      '>',
    ]);
    const arr = reply2 as { kind: 'array'; value: Reply[] };
    const streamArr = (arr.value[0] as { kind: 'array'; value: Reply[] })
      .value[1] as { kind: 'array'; value: Reply[] };
    expect(streamArr.value.length).toBe(1);
  });
});

// ─── XACK ────────────────────────────────────────────────────────────

describe('XACK', () => {
  it('acknowledges a single message', () => {
    const ctx = setupGroupWithEntries();
    execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'COUNT',
      '3',
      'STREAMS',
      's',
      '>',
    ]);
    const reply = execXack(ctx, ['s', 'g1', '1000-0']);
    expect(reply).toEqual({ kind: 'integer', value: 1 });
  });

  it('removes acknowledged entry from group PEL', () => {
    const ctx = setupGroupWithEntries();
    execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'COUNT',
      '3',
      'STREAMS',
      's',
      '>',
    ]);
    execXack(ctx, ['s', 'g1', '1000-0']);
    const { stream: s } = getStreamHelper(ctx.db, 's');
    const group = getGroup(s, 'g1');
    expect(group.pel.has('1000-0')).toBe(false);
    expect(group.pel.size).toBe(2);
  });

  it('removes acknowledged entry from consumer PEL', () => {
    const ctx = setupGroupWithEntries();
    execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'COUNT',
      '3',
      'STREAMS',
      's',
      '>',
    ]);
    execXack(ctx, ['s', 'g1', '1000-0']);
    const { stream: s } = getStreamHelper(ctx.db, 's');
    const group = getGroup(s, 'g1');
    const consumer = group.consumers.get('alice');
    expect(consumer).toBeDefined();
    expect(consumer?.pending.has('1000-0')).toBe(false);
    expect(consumer?.pending.size).toBe(2);
  });

  it('acknowledges multiple messages at once', () => {
    const ctx = setupGroupWithEntries();
    execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'COUNT',
      '3',
      'STREAMS',
      's',
      '>',
    ]);
    const reply = execXack(ctx, ['s', 'g1', '1000-0', '2000-0', '3000-0']);
    expect(reply).toEqual({ kind: 'integer', value: 3 });
  });

  it('returns 0 for IDs not in PEL', () => {
    const ctx = setupGroupWithEntries();
    execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'COUNT',
      '1',
      'STREAMS',
      's',
      '>',
    ]);
    const reply = execXack(ctx, ['s', 'g1', '9999-0']);
    expect(reply).toEqual({ kind: 'integer', value: 0 });
  });

  it('counts only IDs that were actually in PEL', () => {
    const ctx = setupGroupWithEntries();
    execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'COUNT',
      '2',
      'STREAMS',
      's',
      '>',
    ]);
    // 1000-0 is in PEL, 9999-0 is not
    const reply = execXack(ctx, ['s', 'g1', '1000-0', '9999-0']);
    expect(reply).toEqual({ kind: 'integer', value: 1 });
  });

  it('returns 0 for non-existent group', () => {
    const ctx = setupGroupWithEntries();
    const reply = execXack(ctx, ['s', 'nogroup', '1000-0']);
    expect(reply).toEqual({ kind: 'integer', value: 0 });
  });

  it('returns 0 for non-existent key', () => {
    const ctx = createDb(1000);
    const reply = execXack(ctx, ['nokey', 'g1', '1000-0']);
    expect(reply).toEqual({ kind: 'integer', value: 0 });
  });

  it('returns WRONGTYPE for non-stream key', () => {
    const ctx = createDb(1000);
    ctx.db.set('str', 'string', 'raw', 'hello');
    const reply = execXack(ctx, ['str', 'g1', '1000-0']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'WRONGTYPE',
      message: 'Operation against a key holding the wrong kind of value',
    });
  });

  it('returns error for invalid stream ID', () => {
    const ctx = setupGroupWithEntries();
    const reply = execXack(ctx, ['s', 'g1', 'invalid']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'Invalid stream ID specified as stream command argument',
    });
  });
});

// ─── XPENDING ────────────────────────────────────────────────────────

describe('XPENDING', () => {
  it('returns summary form with pending entries', () => {
    const ctx = setupGroupWithEntries();
    ctx.setTime(10000);
    execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'COUNT',
      '3',
      'STREAMS',
      's',
      '>',
    ]);
    const reply = execXpending(ctx, ['s', 'g1']);
    expect(reply).toEqual({
      kind: 'array',
      value: [
        { kind: 'integer', value: 3 },
        { kind: 'bulk', value: '1000-0' },
        { kind: 'bulk', value: '3000-0' },
        {
          kind: 'array',
          value: [
            {
              kind: 'array',
              value: [
                { kind: 'bulk', value: 'alice' },
                { kind: 'bulk', value: '3' },
              ],
            },
          ],
        },
      ],
    });
  });

  it('returns summary with zero pending', () => {
    const ctx = setupGroupWithEntries();
    const reply = execXpending(ctx, ['s', 'g1']);
    expect(reply).toEqual({
      kind: 'array',
      value: [
        { kind: 'integer', value: 0 },
        { kind: 'bulk', value: null },
        { kind: 'bulk', value: null },
        { kind: 'nil-array' },
      ],
    });
  });

  it('returns summary with multiple consumers', () => {
    const ctx = setupGroupWithEntries();
    ctx.setTime(10000);
    execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'COUNT',
      '2',
      'STREAMS',
      's',
      '>',
    ]);
    execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'bob',
      'COUNT',
      '1',
      'STREAMS',
      's',
      '>',
    ]);
    const reply = execXpending(ctx, ['s', 'g1']);
    const arr = reply as { kind: 'array'; value: Reply[] };
    expect(arr.value[0]).toEqual({ kind: 'integer', value: 3 });
    expect(arr.value[1]).toEqual({ kind: 'bulk', value: '1000-0' });
    expect(arr.value[2]).toEqual({ kind: 'bulk', value: '3000-0' });
    // Consumer list
    const consumers = (arr.value[3] as { kind: 'array'; value: Reply[] }).value;
    expect(consumers.length).toBe(2);
  });

  it('returns detail form with range', () => {
    const ctx = setupGroupWithEntries();
    ctx.setTime(10000);
    execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'COUNT',
      '3',
      'STREAMS',
      's',
      '>',
    ]);
    const reply = execXpending(ctx, ['s', 'g1', '-', '+', '10']);
    const arr = reply as { kind: 'array'; value: Reply[] };
    expect(arr.value.length).toBe(3);
    // Each entry: [id, consumer, idle-time, delivery-count]
    const entry = arr.value[0] as { kind: 'array'; value: Reply[] };
    expect(entry.value[0]).toEqual({ kind: 'bulk', value: '1000-0' });
    expect(entry.value[1]).toEqual({ kind: 'bulk', value: 'alice' });
    expect(entry.value[2]).toEqual({ kind: 'integer', value: 0 });
    expect(entry.value[3]).toEqual({ kind: 'integer', value: 1 });
  });

  it('detail form respects COUNT', () => {
    const ctx = setupGroupWithEntries();
    ctx.setTime(10000);
    execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'COUNT',
      '5',
      'STREAMS',
      's',
      '>',
    ]);
    const reply = execXpending(ctx, ['s', 'g1', '-', '+', '2']);
    const arr = reply as { kind: 'array'; value: Reply[] };
    expect(arr.value.length).toBe(2);
  });

  it('detail form filters by consumer', () => {
    const ctx = setupGroupWithEntries();
    ctx.setTime(10000);
    execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'COUNT',
      '2',
      'STREAMS',
      's',
      '>',
    ]);
    execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'bob',
      'COUNT',
      '2',
      'STREAMS',
      's',
      '>',
    ]);
    const reply = execXpending(ctx, ['s', 'g1', '-', '+', '10', 'bob']);
    const arr = reply as { kind: 'array'; value: Reply[] };
    expect(arr.value.length).toBe(2);
    // All entries should be bob's
    for (const entry of arr.value) {
      const e = entry as { kind: 'array'; value: Reply[] };
      expect(e.value[1]).toEqual({ kind: 'bulk', value: 'bob' });
    }
  });

  it('detail form filters by IDLE time (Redis 6.2+)', () => {
    const ctx = setupGroupWithEntries();
    ctx.setTime(10000);
    execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'COUNT',
      '3',
      'STREAMS',
      's',
      '>',
    ]);
    // Advance time so idle > 5000
    ctx.setTime(16000);
    const reply = execXpending(ctx, [
      's',
      'g1',
      'IDLE',
      '5000',
      '-',
      '+',
      '10',
    ]);
    const arr = reply as { kind: 'array'; value: Reply[] };
    // All 3 entries should match since they were delivered at t=10000, now t=16000 (idle=6000)
    expect(arr.value.length).toBe(3);
  });

  it('detail form IDLE filters out non-idle entries', () => {
    const ctx = setupGroupWithEntries();
    ctx.setTime(10000);
    execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'COUNT',
      '3',
      'STREAMS',
      's',
      '>',
    ]);
    // Only 1ms later — none should be idle for > 5000ms
    ctx.setTime(10001);
    const reply = execXpending(ctx, [
      's',
      'g1',
      'IDLE',
      '5000',
      '-',
      '+',
      '10',
    ]);
    const arr = reply as { kind: 'array'; value: Reply[] };
    expect(arr.value.length).toBe(0);
  });

  it('returns NOGROUP for non-existent group', () => {
    const ctx = setupGroupWithEntries();
    const reply = execXpending(ctx, ['s', 'nogroup']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'NOGROUP',
      message: "No such key 's' or consumer group 'nogroup'",
    });
  });

  it('returns NOGROUP for non-existent key', () => {
    const ctx = createDb(1000);
    const reply = execXpending(ctx, ['nokey', 'g1']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'NOGROUP',
      message: "No such key 'nokey' or consumer group 'g1'",
    });
  });

  it('returns WRONGTYPE for non-stream key', () => {
    const ctx = createDb(1000);
    ctx.db.set('str', 'string', 'raw', 'hello');
    const reply = execXpending(ctx, ['str', 'g1']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'WRONGTYPE',
      message: 'Operation against a key holding the wrong kind of value',
    });
  });

  it('returns empty detail for empty range', () => {
    const ctx = setupGroupWithEntries();
    ctx.setTime(10000);
    execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'COUNT',
      '3',
      'STREAMS',
      's',
      '>',
    ]);
    // Range that excludes all entries
    const reply = execXpending(ctx, ['s', 'g1', '9000-0', '9999-0', '10']);
    const arr = reply as { kind: 'array'; value: Reply[] };
    expect(arr.value.length).toBe(0);
  });
});

// ─── XCLAIM ──────────────────────────────────────────────────────────

describe('XCLAIM', () => {
  it('transfers ownership of pending entry', () => {
    const ctx = setupClaimScenario();
    ctx.setTime(20000);
    const reply = execXclaim(ctx, ['s', 'g1', 'bob', '0', '1000-0']);
    const arr = reply as { kind: 'array'; value: Reply[] };
    expect(arr.value.length).toBe(1);
    // Should return the claimed entry
    expect(arr.value[0]).toEqual(entryReply('1000-0', [['k', '1']]));
  });

  it('transfers multiple entries', () => {
    const ctx = setupClaimScenario();
    ctx.setTime(20000);
    const reply = execXclaim(ctx, [
      's',
      'g1',
      'bob',
      '0',
      '1000-0',
      '2000-0',
      '3000-0',
    ]);
    const arr = reply as { kind: 'array'; value: Reply[] };
    expect(arr.value.length).toBe(3);
  });

  it('ignores IDs not in PEL (without FORCE)', () => {
    const ctx = setupClaimScenario();
    ctx.setTime(20000);
    const reply = execXclaim(ctx, [
      's',
      'g1',
      'bob',
      '0',
      '9999-0', // not in PEL
    ]);
    const arr = reply as { kind: 'array'; value: Reply[] };
    expect(arr.value.length).toBe(0);
  });

  it('returns JUSTID — only IDs, not full entries', () => {
    const ctx = setupClaimScenario();
    ctx.setTime(20000);
    const reply = execXclaim(ctx, [
      's',
      'g1',
      'bob',
      '0',
      '1000-0',
      '2000-0',
      'JUSTID',
    ]);
    const arr = reply as { kind: 'array'; value: Reply[] };
    expect(arr.value.length).toBe(2);
    expect(arr.value[0]).toEqual({ kind: 'bulk', value: '1000-0' });
    expect(arr.value[1]).toEqual({ kind: 'bulk', value: '2000-0' });
  });

  it('updates delivery count on claim', () => {
    const ctx = setupClaimScenario();
    ctx.setTime(20000);
    execXclaim(ctx, ['s', 'g1', 'bob', '0', '1000-0']);
    // Check via XPENDING detail
    const pending = execXpending(ctx, ['s', 'g1', '-', '+', '10', 'bob']);
    const arr = pending as { kind: 'array'; value: Reply[] };
    expect(arr.value.length).toBe(1);
    const entry = arr.value[0] as { kind: 'array'; value: Reply[] };
    // delivery count should be 2 (1 original + 1 claim)
    expect(entry.value[3]).toEqual({ kind: 'integer', value: 2 });
  });

  it('respects RETRYCOUNT option', () => {
    const ctx = setupClaimScenario();
    ctx.setTime(20000);
    execXclaim(ctx, ['s', 'g1', 'bob', '0', '1000-0', 'RETRYCOUNT', '5']);
    const pending = execXpending(ctx, ['s', 'g1', '-', '+', '10', 'bob']);
    const arr = pending as { kind: 'array'; value: Reply[] };
    const entry = arr.value[0] as { kind: 'array'; value: Reply[] };
    expect(entry.value[3]).toEqual({ kind: 'integer', value: 5 });
  });

  it('respects IDLE option — sets idle time', () => {
    const ctx = setupClaimScenario();
    ctx.setTime(20000);
    execXclaim(ctx, ['s', 'g1', 'bob', '0', '1000-0', 'IDLE', '5000']);
    // Delivery time should be 20000-5000=15000, idle=20000-15000=5000
    const pending = execXpending(ctx, ['s', 'g1', '-', '+', '10', 'bob']);
    const arr = pending as { kind: 'array'; value: Reply[] };
    const entry = arr.value[0] as { kind: 'array'; value: Reply[] };
    expect(entry.value[2]).toEqual({ kind: 'integer', value: 5000 });
  });

  it('FORCE claims entry not in PEL if it exists in stream', () => {
    const ctx = createDb(1000);
    stream.xadd(ctx.db, ctx.getTime(), ['s', '*', 'k', '1']);
    execXgroup(ctx, ['CREATE', 's', 'g1', '0']);
    // Don't read with XREADGROUP, so nothing in PEL
    const reply = execXclaim(ctx, ['s', 'g1', 'bob', '0', '1000-0', 'FORCE']);
    const arr = reply as { kind: 'array'; value: Reply[] };
    expect(arr.value.length).toBe(1);
  });

  it('returns NOGROUP for non-existing group', () => {
    const ctx = createDb(1000);
    stream.xadd(ctx.db, ctx.getTime(), ['s', '*', 'k', '1']);
    const reply = execXclaim(ctx, ['s', 'nogroup', 'bob', '0', '1000-0']);
    expect(reply.kind).toBe('error');
  });

  it('returns error for non-existing key', () => {
    const ctx = createDb(1000);
    const reply = execXclaim(ctx, ['nokey', 'g1', 'bob', '0', '1000-0']);
    expect(reply.kind).toBe('error');
  });

  it('skips entries whose idle time is below min-idle-time', () => {
    const ctx = setupClaimScenario();
    // Only 1ms after delivery (at 10000) — idle time is 1ms
    ctx.setTime(10001);
    const reply = execXclaim(ctx, [
      's',
      'g1',
      'bob',
      '5000', // min-idle-time=5000ms
      '1000-0',
      '2000-0',
      '3000-0',
    ]);
    const arr = reply as { kind: 'array'; value: Reply[] };
    // No entries should be claimed — idle time is only 1ms
    expect(arr.value.length).toBe(0);
  });

  it('claims only entries that exceed min-idle-time', () => {
    const ctx = createDb(1000);
    // Add 2 entries
    stream.xadd(ctx.db, ctx.getTime(), ['s', '*', 'k', '1']);
    ctx.setTime(2000);
    stream.xadd(ctx.db, ctx.getTime(), ['s', '*', 'k', '2']);
    execXgroup(ctx, ['CREATE', 's', 'g1', '0']);
    // Alice reads at time 5000
    ctx.setTime(5000);
    execXreadgroup(ctx, ['GROUP', 'g1', 'alice', 'STREAMS', 's', '>']);
    // At time 8000 (idle=3000ms), claim with min-idle=2000 — both should be claimed
    ctx.setTime(8000);
    const reply = execXclaim(ctx, [
      's',
      'g1',
      'bob',
      '2000',
      '1000-0',
      '2000-0',
    ]);
    const arr = reply as { kind: 'array'; value: Reply[] };
    expect(arr.value.length).toBe(2);
  });

  it('removes deleted entries from PEL instead of returning them', () => {
    const ctx = setupClaimScenario();
    // Delete entry 2000-0
    execXdel(ctx, ['s', '2000-0']);
    ctx.setTime(20000);
    const reply = execXclaim(ctx, [
      's',
      'g1',
      'bob',
      '0',
      '1000-0',
      '2000-0',
      '3000-0',
    ]);
    const arr = reply as { kind: 'array'; value: Reply[] };
    // Should return 2 entries (1000-0, 3000-0), not 3
    // Deleted entry 2000-0 is removed from PEL silently
    expect(arr.value.length).toBe(2);
    // Verify 2000-0 was removed from PEL
    const s = getStreamHelper(ctx.db, 's').stream as RedisStream;
    const group = getGroup(s, 'g1');
    expect(group.pel.has('2000-0')).toBe(false);
  });
});

// ─── XAUTOCLAIM ──────────────────────────────────────────────────────

describe('XAUTOCLAIM', () => {
  it('claims idle pending entries', () => {
    const ctx = setupClaimScenario();
    // Advance time so entries are idle > 5000ms
    ctx.setTime(20000);
    const reply = execXautoclaim(ctx, [
      's',
      'g1',
      'bob',
      '5000', // min-idle-time
      '0-0', // start
    ]);
    const arr = reply as { kind: 'array'; value: Reply[] };
    // [cursor, claimed-entries, deleted-ids]
    expect(arr.value.length).toBe(3);
    const cursor = arr.value[0] as { kind: 'bulk'; value: string };
    expect(cursor.value).toBe('0-0'); // no more entries
    const claimed = arr.value[1] as { kind: 'array'; value: Reply[] };
    expect(claimed.value.length).toBe(5); // all 5 entries
    const deletedIds = arr.value[2] as { kind: 'array'; value: Reply[] };
    expect(deletedIds.value.length).toBe(0);
  });

  it('returns 0-0 cursor when all entries claimed', () => {
    const ctx = setupClaimScenario();
    ctx.setTime(20000);
    const reply = execXautoclaim(ctx, ['s', 'g1', 'bob', '5000', '0-0']);
    const arr = reply as { kind: 'array'; value: Reply[] };
    const cursor = arr.value[0] as { kind: 'bulk'; value: string };
    expect(cursor.value).toBe('0-0');
  });

  it('respects COUNT limit and returns non-zero cursor', () => {
    const ctx = setupClaimScenario();
    ctx.setTime(20000);
    const reply = execXautoclaim(ctx, [
      's',
      'g1',
      'bob',
      '5000',
      '0-0',
      'COUNT',
      '2',
    ]);
    const arr = reply as { kind: 'array'; value: Reply[] };
    const cursor = arr.value[0] as { kind: 'bulk'; value: string };
    // Cursor should point to next entry after the 2 claimed
    expect(cursor.value).not.toBe('0-0');
    const claimed = arr.value[1] as { kind: 'array'; value: Reply[] };
    expect(claimed.value.length).toBe(2);
  });

  it('filters by start ID', () => {
    const ctx = setupClaimScenario();
    ctx.setTime(20000);
    const reply = execXautoclaim(ctx, [
      's',
      'g1',
      'bob',
      '5000',
      '3000-0', // start from 3000-0
    ]);
    const arr = reply as { kind: 'array'; value: Reply[] };
    const claimed = arr.value[1] as { kind: 'array'; value: Reply[] };
    expect(claimed.value.length).toBe(3); // 3000-0, 4000-0, 5000-0
  });

  it('skips entries that are not idle enough', () => {
    const ctx = setupClaimScenario();
    // Only 1ms later — nothing is idle for > 5000ms
    ctx.setTime(10001);
    const reply = execXautoclaim(ctx, ['s', 'g1', 'bob', '5000', '0-0']);
    const arr = reply as { kind: 'array'; value: Reply[] };
    const claimed = arr.value[1] as { kind: 'array'; value: Reply[] };
    expect(claimed.value.length).toBe(0);
  });

  it('reports deleted entries in third array element', () => {
    const ctx = setupClaimScenario();
    // Delete an entry that alice has pending
    execXdel(ctx, ['s', '2000-0']);
    ctx.setTime(20000);
    const reply = execXautoclaim(ctx, ['s', 'g1', 'bob', '5000', '0-0']);
    const arr = reply as { kind: 'array'; value: Reply[] };
    const claimed = arr.value[1] as { kind: 'array'; value: Reply[] };
    const deletedIds = arr.value[2] as { kind: 'array'; value: Reply[] };
    // 4 entries claimed (1,3,4,5), 1 deleted (2)
    expect(claimed.value.length).toBe(4);
    expect(deletedIds.value.length).toBe(1);
    expect(deletedIds.value[0]).toEqual({ kind: 'bulk', value: '2000-0' });
  });

  it('JUSTID returns only IDs', () => {
    const ctx = setupClaimScenario();
    ctx.setTime(20000);
    const reply = execXautoclaim(ctx, [
      's',
      'g1',
      'bob',
      '5000',
      '0-0',
      'JUSTID',
    ]);
    const arr = reply as { kind: 'array'; value: Reply[] };
    const claimed = arr.value[1] as { kind: 'array'; value: Reply[] };
    // Should be bulk strings, not arrays
    expect(claimed.value[0]).toEqual({ kind: 'bulk', value: '1000-0' });
  });

  it('returns NOGROUP for non-existing group', () => {
    const ctx = createDb(1000);
    stream.xadd(ctx.db, ctx.getTime(), ['s', '*', 'k', '1']);
    const reply = execXautoclaim(ctx, ['s', 'nogroup', 'bob', '0', '0-0']);
    expect(reply.kind).toBe('error');
  });

  it('deleted entries consume COUNT budget and cursor advances correctly', () => {
    const ctx = setupClaimScenario();
    // Delete entries 1000-0 and 2000-0
    execXdel(ctx, ['s', '1000-0']);
    execXdel(ctx, ['s', '2000-0']);
    ctx.setTime(20000);
    // COUNT=2 — should process 2 entries (both deleted), cursor advances past them
    const reply = execXautoclaim(ctx, [
      's',
      'g1',
      'bob',
      '5000',
      '0-0',
      'COUNT',
      '2',
    ]);
    const arr = reply as { kind: 'array'; value: Reply[] };
    const cursor = arr.value[0] as { kind: 'bulk'; value: string };
    const claimed = arr.value[1] as { kind: 'array'; value: Reply[] };
    const deletedIds = arr.value[2] as { kind: 'array'; value: Reply[] };
    // 0 claimed, 2 deleted
    expect(claimed.value.length).toBe(0);
    expect(deletedIds.value.length).toBe(2);
    // Cursor should point to next entry (3000-0), not 0-0
    expect(cursor.value).toBe('3000-0');
  });

  it('cursor points to next entry after last scanned, not last claimed', () => {
    const ctx = setupClaimScenario();
    ctx.setTime(20000);
    // COUNT=3 claims 3 entries (1000-0, 2000-0, 3000-0), cursor -> 4000-0
    const reply = execXautoclaim(ctx, [
      's',
      'g1',
      'bob',
      '5000',
      '0-0',
      'COUNT',
      '3',
    ]);
    const arr = reply as { kind: 'array'; value: Reply[] };
    const cursor = arr.value[0] as { kind: 'bulk'; value: string };
    expect(cursor.value).toBe('4000-0');
  });
});
