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

function execXinfo(ctx: ReturnType<typeof createDb>, args: string[]): Reply {
  const spec = stream.specs.find((s) => s.name === 'xinfo');
  if (!spec) throw new Error('xinfo spec not found');
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

function findField(arr: Reply[], fieldName: string): Reply | undefined {
  const values = (arr as unknown as { kind: 'array'; value: Reply[] }).kind
    ? (arr as unknown as { kind: 'array'; value: Reply[] }).value
    : arr;
  for (let i = 0; i < values.length - 1; i++) {
    const item = values[i] as { kind: string; value: string };
    if (item.kind === 'bulk' && item.value === fieldName) {
      return values[i + 1];
    }
  }
  return undefined;
}

describe('XINFO STREAM', () => {
  it('returns stream metadata', () => {
    const ctx = createDb(1000);
    for (let i = 1; i <= 3; i++) {
      ctx.setTime(i * 1000);
      stream.xadd(ctx.db, ctx.getTime(), ['s', '*', 'k', String(i)]);
    }
    const reply = execXinfo(ctx, ['STREAM', 's']);
    const arr = reply as { kind: 'array'; value: Reply[] };

    const length = findField(arr.value, 'length');
    expect(length).toEqual({ kind: 'integer', value: 3 });

    const lastId = findField(arr.value, 'last-generated-id');
    expect(lastId).toEqual({ kind: 'bulk', value: '3000-0' });

    const entriesAdded = findField(arr.value, 'entries-added');
    expect(entriesAdded).toEqual({ kind: 'integer', value: 3 });

    const groups = findField(arr.value, 'groups');
    expect(groups).toEqual({ kind: 'integer', value: 0 });

    const firstEntry = findField(arr.value, 'first-entry');
    expect(firstEntry).toEqual(entryReply('1000-0', [['k', '1']]));

    const lastEntry = findField(arr.value, 'last-entry');
    expect(lastEntry).toEqual(entryReply('3000-0', [['k', '3']]));
  });

  it('returns error for non-existing key', () => {
    const ctx = createDb(1000);
    const reply = execXinfo(ctx, ['STREAM', 'nokey']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'no such key',
    });
  });

  it('returns WRONGTYPE for non-stream key', () => {
    const ctx = createDb(1000);
    ctx.db.set('str', 'string', 'raw', 'hello');
    const reply = execXinfo(ctx, ['STREAM', 'str']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'WRONGTYPE',
      message: 'Operation against a key holding the wrong kind of value',
    });
  });

  it('FULL returns entries and group details', () => {
    const ctx = createDb(1000);
    for (let i = 1; i <= 3; i++) {
      ctx.setTime(i * 1000);
      stream.xadd(ctx.db, ctx.getTime(), ['s', '*', 'k', String(i)]);
    }
    execXgroup(ctx, ['CREATE', 's', 'g1', '0']);

    const reply = execXinfo(ctx, ['STREAM', 's', 'FULL']);
    const arr = reply as { kind: 'array'; value: Reply[] };

    const entries = findField(arr.value, 'entries');
    const entriesArr = entries as { kind: 'array'; value: Reply[] };
    expect(entriesArr.value.length).toBe(3);

    const groups = findField(arr.value, 'groups');
    const groupsArr = groups as { kind: 'array'; value: Reply[] };
    expect(groupsArr.value.length).toBe(1);
  });

  it('recorded-first-entry-id tracks first entry and updates after trim', () => {
    const ctx = createDb(1000);
    for (let i = 1; i <= 5; i++) {
      ctx.setTime(i * 1000);
      stream.xadd(ctx.db, ctx.getTime(), ['s', '*', 'k', String(i)]);
    }
    // Before trim, recorded-first-entry-id should be the first entry
    let reply = execXinfo(ctx, ['STREAM', 's']);
    let arr = reply as { kind: 'array'; value: Reply[] };
    let recordedFirst = findField(arr.value, 'recorded-first-entry-id');
    expect(recordedFirst).toEqual({ kind: 'bulk', value: '1000-0' });

    // Trim by MAXLEN to keep 3 entries (removes 1000-0, 2000-0)
    const xtrimSpec = stream.specs.find((s) => s.name === 'xtrim');
    if (xtrimSpec) {
      xtrimSpec.handler({ db: ctx.db, engine: ctx.engine }, [
        's',
        'MAXLEN',
        '3',
      ]);
    }

    // After trim, recorded-first-entry-id should be updated to 3000-0
    reply = execXinfo(ctx, ['STREAM', 's']);
    arr = reply as { kind: 'array'; value: Reply[] };
    recordedFirst = findField(arr.value, 'recorded-first-entry-id');
    expect(recordedFirst).toEqual({ kind: 'bulk', value: '3000-0' });
  });

  it('FULL COUNT limits entries', () => {
    const ctx = createDb(1000);
    for (let i = 1; i <= 5; i++) {
      ctx.setTime(i * 1000);
      stream.xadd(ctx.db, ctx.getTime(), ['s', '*', 'k', String(i)]);
    }

    const reply = execXinfo(ctx, ['STREAM', 's', 'FULL', 'COUNT', '2']);
    const arr = reply as { kind: 'array'; value: Reply[] };
    const entries = findField(arr.value, 'entries');
    const entriesArr = entries as { kind: 'array'; value: Reply[] };
    expect(entriesArr.value.length).toBe(2);
  });
});

describe('XINFO GROUPS', () => {
  it('returns group list', () => {
    const ctx = createDb(1000);
    stream.xadd(ctx.db, ctx.getTime(), ['s', '*', 'k', '1']);
    execXgroup(ctx, ['CREATE', 's', 'g1', '0']);
    execXgroup(ctx, ['CREATE', 's', 'g2', '$']);

    const reply = execXinfo(ctx, ['GROUPS', 's']);
    const arr = reply as { kind: 'array'; value: Reply[] };
    expect(arr.value.length).toBe(2);

    const g1 = arr.value[0] as { kind: 'array'; value: Reply[] };
    const name = findField(g1.value, 'name');
    expect(name).toEqual({ kind: 'bulk', value: 'g1' });
  });

  it('returns empty array for stream with no groups', () => {
    const ctx = createDb(1000);
    stream.xadd(ctx.db, ctx.getTime(), ['s', '*', 'k', '1']);
    const reply = execXinfo(ctx, ['GROUPS', 's']);
    expect(reply).toEqual({ kind: 'array', value: [] });
  });

  it('shows pending count and consumer count', () => {
    const ctx = setupClaimScenario();
    const reply = execXinfo(ctx, ['GROUPS', 's']);
    const arr = reply as { kind: 'array'; value: Reply[] };
    const g1 = arr.value[0] as { kind: 'array'; value: Reply[] };

    const consumers = findField(g1.value, 'consumers');
    expect(consumers).toEqual({ kind: 'integer', value: 1 }); // alice

    const pending = findField(g1.value, 'pending');
    expect(pending).toEqual({ kind: 'integer', value: 5 });
  });

  it('returns error for non-existing key', () => {
    const ctx = createDb(1000);
    const reply = execXinfo(ctx, ['GROUPS', 'nokey']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'no such key',
    });
  });
});

describe('XINFO CONSUMERS', () => {
  it('returns consumer list with idle times', () => {
    const ctx = setupClaimScenario();
    ctx.setTime(15000);
    const reply = execXinfo(ctx, ['CONSUMERS', 's', 'g1']);
    const arr = reply as { kind: 'array'; value: Reply[] };
    expect(arr.value.length).toBe(1); // alice

    const alice = arr.value[0] as { kind: 'array'; value: Reply[] };
    const name = findField(alice.value, 'name');
    expect(name).toEqual({ kind: 'bulk', value: 'alice' });

    const pending = findField(alice.value, 'pending');
    expect(pending).toEqual({ kind: 'integer', value: 5 });

    const idle = findField(alice.value, 'idle');
    expect(idle).toEqual({ kind: 'integer', value: 5000 });
  });

  it('returns NOGROUP for non-existing group', () => {
    const ctx = createDb(1000);
    stream.xadd(ctx.db, ctx.getTime(), ['s', '*', 'k', '1']);
    const reply = execXinfo(ctx, ['CONSUMERS', 's', 'nogroup']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'NOGROUP',
      message: "No such consumer group 'nogroup' for key name 's'",
    });
  });

  it('returns error for non-existing key', () => {
    const ctx = createDb(1000);
    const reply = execXinfo(ctx, ['CONSUMERS', 'nokey', 'g1']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'no such key',
    });
  });

  it('returns empty array when no consumers exist', () => {
    const ctx = createDb(1000);
    stream.xadd(ctx.db, ctx.getTime(), ['s', '*', 'k', '1']);
    execXgroup(ctx, ['CREATE', 's', 'g1', '0']);
    const reply = execXinfo(ctx, ['CONSUMERS', 's', 'g1']);
    expect(reply).toEqual({ kind: 'array', value: [] });
  });

  it('returns error for unknown subcommand', () => {
    const ctx = createDb(1000);
    const reply = execXinfo(ctx, ['UNKNOWN', 's']);
    expect(reply.kind).toBe('error');
  });

  it('idle and inactive differ after XREADGROUP with no new entries', () => {
    const ctx = createDb(1000);
    stream.xadd(ctx.db, ctx.getTime(), ['s', '*', 'k', '1']);
    execXgroup(ctx, ['CREATE', 's', 'g1', '0']);
    // Alice reads at time 2000 — entries delivered, activeTime=2000
    ctx.setTime(2000);
    execXreadgroup(ctx, ['GROUP', 'g1', 'alice', 'STREAMS', 's', '>']);
    // Alice reads again at time 5000 with no new entries — seenTime=5000, activeTime=2000
    ctx.setTime(5000);
    execXreadgroup(ctx, ['GROUP', 'g1', 'alice', 'STREAMS', 's', '>']);
    // Check at time 6000
    ctx.setTime(6000);
    const reply = execXinfo(ctx, ['CONSUMERS', 's', 'g1']);
    const arr = reply as { kind: 'array'; value: Reply[] };
    const alice = arr.value[0] as { kind: 'array'; value: Reply[] };
    const idle = findField(alice.value, 'idle') as {
      kind: 'integer';
      value: number;
    };
    const inactive = findField(alice.value, 'inactive') as {
      kind: 'integer';
      value: number;
    };
    // idle = 6000 - 5000 = 1000 (last interaction)
    expect(idle.value).toBe(1000);
    // inactive = 6000 - 2000 = 4000 (last successful delivery)
    expect(inactive.value).toBe(4000);
  });
});

describe('XINFO HELP', () => {
  it('returns help text', () => {
    const ctx = createDb(1000);
    const reply = execXinfo(ctx, ['HELP']);
    expect(reply.kind).toBe('array');
    const arr = reply as { kind: 'array'; value: Reply[] };
    expect(arr.value.length).toBeGreaterThan(0);
  });
});
