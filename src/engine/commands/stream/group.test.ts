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

function seedStream(
  db: ReturnType<typeof createDb>['db'],
  clock: { setTime: (t: number) => void; getTime: () => number }
) {
  for (let i = 1; i <= 5; i++) {
    clock.setTime(i * 1000);
    stream.xadd(db, clock.getTime(), ['s', '*', 'k', String(i)]);
  }
}

const xgroupSpec = stream.specs.find((s) => s.name === 'xgroup');

function execXgroup(ctx: ReturnType<typeof createDb>, args: string[]): Reply {
  if (!xgroupSpec) throw new Error('xgroup spec not found');
  return xgroupSpec.handler({ db: ctx.db, engine: ctx.engine }, args);
}

describe('XGROUP CREATE', () => {
  it('creates a consumer group with $ ID', () => {
    const ctx = createDb(1000);
    seedStream(ctx.db, ctx);
    const reply = execXgroup(ctx, ['CREATE', 's', 'mygroup', '$']);
    expect(reply).toEqual({ kind: 'status', value: 'OK' });
  });

  it('creates a consumer group with 0 ID', () => {
    const ctx = createDb(1000);
    seedStream(ctx.db, ctx);
    const reply = execXgroup(ctx, ['CREATE', 's', 'g1', '0']);
    expect(reply).toEqual({ kind: 'status', value: 'OK' });
  });

  it('creates a consumer group with explicit ID', () => {
    const ctx = createDb(1000);
    seedStream(ctx.db, ctx);
    const reply = execXgroup(ctx, ['CREATE', 's', 'g1', '1000-2']);
    expect(reply).toEqual({ kind: 'status', value: 'OK' });
  });

  it('returns BUSYGROUP if group already exists', () => {
    const ctx = createDb(1000);
    seedStream(ctx.db, ctx);
    execXgroup(ctx, ['CREATE', 's', 'mygroup', '$']);
    const reply = execXgroup(ctx, ['CREATE', 's', 'mygroup', '$']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'BUSYGROUP',
      message: 'Consumer Group name already exists',
    });
  });

  it('returns error if key does not exist without MKSTREAM', () => {
    const ctx = createDb(1000);
    const reply = execXgroup(ctx, ['CREATE', 'nokey', 'g1', '$']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message:
        'The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.',
    });
  });

  it('creates stream with MKSTREAM when key does not exist', () => {
    const ctx = createDb(1000);
    const reply = execXgroup(ctx, [
      'CREATE',
      'newstream',
      'g1',
      '$',
      'MKSTREAM',
    ]);
    expect(reply).toEqual({ kind: 'status', value: 'OK' });
    const entry = ctx.db.get('newstream');
    expect(entry).toBeDefined();
    expect(entry?.type).toBe('stream');
  });

  it('returns WRONGTYPE for non-stream key', () => {
    const ctx = createDb(1000);
    ctx.db.set('str', 'string', 'raw', 'hello');
    const reply = execXgroup(ctx, ['CREATE', 'str', 'g1', '$']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'WRONGTYPE',
      message: 'Operation against a key holding the wrong kind of value',
    });
  });

  it('returns error for invalid stream ID', () => {
    const ctx = createDb(1000);
    seedStream(ctx.db, ctx);
    const reply = execXgroup(ctx, ['CREATE', 's', 'g1', 'invalid']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'Invalid stream ID specified as stream command argument',
    });
  });

  it('supports ENTRIESREAD option', () => {
    const ctx = createDb(1000);
    seedStream(ctx.db, ctx);
    const reply = execXgroup(ctx, [
      'CREATE',
      's',
      'g1',
      '0',
      'ENTRIESREAD',
      '5',
    ]);
    expect(reply).toEqual({ kind: 'status', value: 'OK' });
  });

  it('allows multiple groups on same stream', () => {
    const ctx = createDb(1000);
    seedStream(ctx.db, ctx);
    const r1 = execXgroup(ctx, ['CREATE', 's', 'g1', '$']);
    const r2 = execXgroup(ctx, ['CREATE', 's', 'g2', '0']);
    expect(r1).toEqual({ kind: 'status', value: 'OK' });
    expect(r2).toEqual({ kind: 'status', value: 'OK' });
  });
});

describe('XGROUP SETID', () => {
  it('sets last-delivered-id to $', () => {
    const ctx = createDb(1000);
    seedStream(ctx.db, ctx);
    execXgroup(ctx, ['CREATE', 's', 'g1', '0']);
    const reply = execXgroup(ctx, ['SETID', 's', 'g1', '$']);
    expect(reply).toEqual({ kind: 'status', value: 'OK' });
  });

  it('sets last-delivered-id to explicit ID', () => {
    const ctx = createDb(1000);
    seedStream(ctx.db, ctx);
    execXgroup(ctx, ['CREATE', 's', 'g1', '0']);
    const reply = execXgroup(ctx, ['SETID', 's', 'g1', '1000-2']);
    expect(reply).toEqual({ kind: 'status', value: 'OK' });
  });

  it('returns NOGROUP error if group does not exist', () => {
    const ctx = createDb(1000);
    seedStream(ctx.db, ctx);
    const reply = execXgroup(ctx, ['SETID', 's', 'nogroup', '$']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'NOGROUP',
      message: "No such consumer group 'nogroup' for key name 's'",
    });
  });

  it('returns error if key does not exist', () => {
    const ctx = createDb(1000);
    const reply = execXgroup(ctx, ['SETID', 'nokey', 'g1', '$']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message:
        'The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.',
    });
  });

  it('returns error for invalid stream ID', () => {
    const ctx = createDb(1000);
    seedStream(ctx.db, ctx);
    execXgroup(ctx, ['CREATE', 's', 'g1', '0']);
    const reply = execXgroup(ctx, ['SETID', 's', 'g1', 'bad']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'Invalid stream ID specified as stream command argument',
    });
  });
});

describe('XGROUP DESTROY', () => {
  it('destroys an existing group and returns 1', () => {
    const ctx = createDb(1000);
    seedStream(ctx.db, ctx);
    execXgroup(ctx, ['CREATE', 's', 'g1', '$']);
    const reply = execXgroup(ctx, ['DESTROY', 's', 'g1']);
    expect(reply).toEqual({ kind: 'integer', value: 1 });
  });

  it('returns 0 for non-existent group', () => {
    const ctx = createDb(1000);
    seedStream(ctx.db, ctx);
    const reply = execXgroup(ctx, ['DESTROY', 's', 'nogroup']);
    expect(reply).toEqual({ kind: 'integer', value: 0 });
  });

  it('returns error if key does not exist', () => {
    const ctx = createDb(1000);
    const reply = execXgroup(ctx, ['DESTROY', 'nokey', 'g1']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message:
        'The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.',
    });
  });

  it('group cannot be accessed after destroy', () => {
    const ctx = createDb(1000);
    seedStream(ctx.db, ctx);
    execXgroup(ctx, ['CREATE', 's', 'g1', '$']);
    execXgroup(ctx, ['DESTROY', 's', 'g1']);
    const reply = execXgroup(ctx, ['CREATECONSUMER', 's', 'g1', 'alice']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'NOGROUP',
      message: "No such consumer group 'g1' for key name 's'",
    });
  });
});

describe('XGROUP CREATECONSUMER', () => {
  it('creates a new consumer and returns 1', () => {
    const ctx = createDb(1000);
    seedStream(ctx.db, ctx);
    execXgroup(ctx, ['CREATE', 's', 'g1', '$']);
    const reply = execXgroup(ctx, ['CREATECONSUMER', 's', 'g1', 'alice']);
    expect(reply).toEqual({ kind: 'integer', value: 1 });
  });

  it('returns 0 if consumer already exists', () => {
    const ctx = createDb(1000);
    seedStream(ctx.db, ctx);
    execXgroup(ctx, ['CREATE', 's', 'g1', '$']);
    execXgroup(ctx, ['CREATECONSUMER', 's', 'g1', 'alice']);
    const reply = execXgroup(ctx, ['CREATECONSUMER', 's', 'g1', 'alice']);
    expect(reply).toEqual({ kind: 'integer', value: 0 });
  });

  it('returns NOGROUP error if group does not exist', () => {
    const ctx = createDb(1000);
    seedStream(ctx.db, ctx);
    const reply = execXgroup(ctx, ['CREATECONSUMER', 's', 'nogroup', 'alice']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'NOGROUP',
      message: "No such consumer group 'nogroup' for key name 's'",
    });
  });

  it('returns error if key does not exist', () => {
    const ctx = createDb(1000);
    const reply = execXgroup(ctx, ['CREATECONSUMER', 'nokey', 'g1', 'alice']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message:
        'The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.',
    });
  });

  it('can create multiple consumers in same group', () => {
    const ctx = createDb(1000);
    seedStream(ctx.db, ctx);
    execXgroup(ctx, ['CREATE', 's', 'g1', '$']);
    const r1 = execXgroup(ctx, ['CREATECONSUMER', 's', 'g1', 'alice']);
    const r2 = execXgroup(ctx, ['CREATECONSUMER', 's', 'g1', 'bob']);
    expect(r1).toEqual({ kind: 'integer', value: 1 });
    expect(r2).toEqual({ kind: 'integer', value: 1 });
  });
});

describe('XGROUP DELCONSUMER', () => {
  it('deletes a consumer with no pending entries and returns 0', () => {
    const ctx = createDb(1000);
    seedStream(ctx.db, ctx);
    execXgroup(ctx, ['CREATE', 's', 'g1', '$']);
    execXgroup(ctx, ['CREATECONSUMER', 's', 'g1', 'alice']);
    const reply = execXgroup(ctx, ['DELCONSUMER', 's', 'g1', 'alice']);
    expect(reply).toEqual({ kind: 'integer', value: 0 });
  });

  it('returns 0 for non-existent consumer', () => {
    const ctx = createDb(1000);
    seedStream(ctx.db, ctx);
    execXgroup(ctx, ['CREATE', 's', 'g1', '$']);
    const reply = execXgroup(ctx, ['DELCONSUMER', 's', 'g1', 'nobody']);
    expect(reply).toEqual({ kind: 'integer', value: 0 });
  });

  it('returns NOGROUP error if group does not exist', () => {
    const ctx = createDb(1000);
    seedStream(ctx.db, ctx);
    const reply = execXgroup(ctx, ['DELCONSUMER', 's', 'nogroup', 'alice']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'NOGROUP',
      message: "No such consumer group 'nogroup' for key name 's'",
    });
  });

  it('returns error if key does not exist', () => {
    const ctx = createDb(1000);
    const reply = execXgroup(ctx, ['DELCONSUMER', 'nokey', 'g1', 'alice']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message:
        'The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.',
    });
  });

  it('consumer is gone after deletion', () => {
    const ctx = createDb(1000);
    seedStream(ctx.db, ctx);
    execXgroup(ctx, ['CREATE', 's', 'g1', '$']);
    execXgroup(ctx, ['CREATECONSUMER', 's', 'g1', 'alice']);
    execXgroup(ctx, ['DELCONSUMER', 's', 'g1', 'alice']);
    const reply = execXgroup(ctx, ['CREATECONSUMER', 's', 'g1', 'alice']);
    expect(reply).toEqual({ kind: 'integer', value: 1 });
  });
});

describe('XGROUP edge cases', () => {
  it('returns error for unknown subcommand', () => {
    const ctx = createDb(1000);
    seedStream(ctx.db, ctx);
    const reply = execXgroup(ctx, ['BADCMD', 's', 'g1']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message:
        "unknown subcommand or wrong number of arguments for 'xgroup|BADCMD' command",
    });
  });

  it('returns error with no args', () => {
    const ctx = createDb(1000);
    const reply = execXgroup(ctx, []);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: "wrong number of arguments for 'xgroup' command",
    });
  });

  it('handles case-insensitive subcommands', () => {
    const ctx = createDb(1000);
    seedStream(ctx.db, ctx);
    const r1 = execXgroup(ctx, ['create', 's', 'g1', '$']);
    expect(r1).toEqual({ kind: 'status', value: 'OK' });
    const r2 = execXgroup(ctx, ['destroy', 's', 'g1']);
    expect(r2).toEqual({ kind: 'integer', value: 1 });
  });

  it('XGROUP CREATE rejects non-integer ENTRIESREAD', () => {
    const ctx = createDb(1000);
    seedStream(ctx.db, ctx);
    const reply = execXgroup(ctx, [
      'CREATE',
      's',
      'g1',
      '0',
      'ENTRIESREAD',
      'abc',
    ]);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'value is not an integer or out of range',
    });
  });

  it('XGROUP CREATE rejects negative ENTRIESREAD', () => {
    const ctx = createDb(1000);
    seedStream(ctx.db, ctx);
    const reply = execXgroup(ctx, [
      'CREATE',
      's',
      'g1',
      '0',
      'ENTRIESREAD',
      '-1',
    ]);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'value is not an integer or out of range',
    });
  });

  it('XGROUP CREATE rejects ENTRIESREAD without value', () => {
    const ctx = createDb(1000);
    seedStream(ctx.db, ctx);
    const reply = execXgroup(ctx, ['CREATE', 's', 'g1', '0', 'ENTRIESREAD']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'syntax error',
    });
  });

  it('XGROUP SETID with ENTRIESREAD option', () => {
    const ctx = createDb(1000);
    seedStream(ctx.db, ctx);
    execXgroup(ctx, ['CREATE', 's', 'g1', '0']);
    const reply = execXgroup(ctx, [
      'SETID',
      's',
      'g1',
      '$',
      'ENTRIESREAD',
      '3',
    ]);
    expect(reply).toEqual({ kind: 'status', value: 'OK' });
  });

  it('XGROUP SETID rejects invalid ENTRIESREAD', () => {
    const ctx = createDb(1000);
    seedStream(ctx.db, ctx);
    execXgroup(ctx, ['CREATE', 's', 'g1', '0']);
    const reply = execXgroup(ctx, [
      'SETID',
      's',
      'g1',
      '$',
      'ENTRIESREAD',
      'bad',
    ]);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'value is not an integer or out of range',
    });
  });

  it('XGROUP CREATE with MKSTREAM and ENTRIESREAD together', () => {
    const ctx = createDb(1000);
    const reply = execXgroup(ctx, [
      'CREATE',
      'newkey',
      'g1',
      '$',
      'MKSTREAM',
      'ENTRIESREAD',
      '0',
    ]);
    expect(reply).toEqual({ kind: 'status', value: 'OK' });
    expect(ctx.db.get('newkey')?.type).toBe('stream');
  });

  it('XGROUP DESTROY returns WRONGTYPE for non-stream key', () => {
    const ctx = createDb(1000);
    ctx.db.set('str', 'string', 'raw', 'hello');
    const reply = execXgroup(ctx, ['DESTROY', 'str', 'g1']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'WRONGTYPE',
      message: 'Operation against a key holding the wrong kind of value',
    });
  });

  it('XGROUP DELCONSUMER returns WRONGTYPE for non-stream key', () => {
    const ctx = createDb(1000);
    ctx.db.set('str', 'string', 'raw', 'hello');
    const reply = execXgroup(ctx, ['DELCONSUMER', 'str', 'g1', 'alice']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'WRONGTYPE',
      message: 'Operation against a key holding the wrong kind of value',
    });
  });

  it('XGROUP CREATECONSUMER returns WRONGTYPE for non-stream key', () => {
    const ctx = createDb(1000);
    ctx.db.set('str', 'string', 'raw', 'hello');
    const reply = execXgroup(ctx, ['CREATECONSUMER', 'str', 'g1', 'alice']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'WRONGTYPE',
      message: 'Operation against a key holding the wrong kind of value',
    });
  });

  it('XGROUP SETID returns WRONGTYPE for non-stream key', () => {
    const ctx = createDb(1000);
    ctx.db.set('str', 'string', 'raw', 'hello');
    const reply = execXgroup(ctx, ['SETID', 'str', 'g1', '$']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'WRONGTYPE',
      message: 'Operation against a key holding the wrong kind of value',
    });
  });
});
