import { describe, it, expect, beforeEach } from 'vitest';
import { CommandTable } from './command-table.ts';
import type { CommandDefinition, CommandHandler } from './command-table.ts';
import { createCommandTable } from './command-registry.ts';
import { RedisEngine } from './engine.ts';
import type { Reply } from './types.ts';
import { statusReply } from './types.ts';

function stubHandler(): CommandHandler {
  return () => statusReply('OK');
}

function makeDef(
  overrides: Partial<CommandDefinition> = {}
): CommandDefinition {
  return {
    name: 'test',
    handler: stubHandler(),
    arity: 1,
    flags: new Set(),
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: new Set(),
    ...overrides,
  };
}

/** Get a command definition, failing the test if not found. */
function getDef(table: CommandTable, name: string): CommandDefinition {
  const def = table.get(name);
  expect(def).toBeDefined();
  return def as CommandDefinition;
}

function createCtx() {
  const engine = new RedisEngine({ clock: () => 1000, rng: () => 0.5 });
  const db = engine.db(0);
  return { engine, db };
}

describe('CommandTable', () => {
  let table: CommandTable;

  beforeEach(() => {
    table = new CommandTable();
  });

  describe('register and get', () => {
    it('registers and retrieves a command', () => {
      const def = makeDef({ name: 'ping' });
      table.register(def);
      expect(table.get('ping')).toBe(def);
    });

    it('lookup is case-insensitive', () => {
      const def = makeDef({ name: 'PING' });
      table.register(def);
      expect(table.get('ping')).toBe(def);
      expect(table.get('PING')).toBe(def);
      expect(table.get('Ping')).toBe(def);
    });

    it('has returns true for registered commands', () => {
      table.register(makeDef({ name: 'get' }));
      expect(table.has('get')).toBe(true);
      expect(table.has('GET')).toBe(true);
    });

    it('has returns false for unregistered commands', () => {
      expect(table.has('nonexistent')).toBe(false);
    });

    it('get returns undefined for unregistered commands', () => {
      expect(table.get('nonexistent')).toBeUndefined();
    });

    it('tracks size correctly', () => {
      expect(table.size).toBe(0);
      table.register(makeDef({ name: 'a' }));
      expect(table.size).toBe(1);
      table.register(makeDef({ name: 'b' }));
      expect(table.size).toBe(2);
    });

    it('overwrites command with same name', () => {
      const first = makeDef({ name: 'cmd', arity: 1 });
      const second = makeDef({ name: 'cmd', arity: 2 });
      table.register(first);
      table.register(second);
      expect(table.size).toBe(1);
      expect(table.get('cmd')?.arity).toBe(2);
    });

    it('all() iterates over all registered commands', () => {
      table.register(makeDef({ name: 'a' }));
      table.register(makeDef({ name: 'b' }));
      table.register(makeDef({ name: 'c' }));
      const names = Array.from(table.all()).map((d) => d.name);
      expect(names).toHaveLength(3);
      expect(names).toContain('a');
      expect(names).toContain('b');
      expect(names).toContain('c');
    });
  });

  describe('checkArity', () => {
    it('positive arity: exact match passes', () => {
      const def = makeDef({ name: 'get', arity: 2 });
      expect(table.checkArity(def, 2)).toBeNull();
    });

    it('positive arity: too few args fails', () => {
      const def = makeDef({ name: 'get', arity: 2 });
      const result = table.checkArity(def, 1);
      expect(result).not.toBeNull();
      expect(result?.kind).toBe('error');
      expect((result as { message: string }).message).toContain(
        "wrong number of arguments for 'get' command"
      );
    });

    it('positive arity: too many args fails', () => {
      const def = makeDef({ name: 'get', arity: 2 });
      const result = table.checkArity(def, 3);
      expect(result).not.toBeNull();
      expect(result?.kind).toBe('error');
    });

    it('negative arity: minimum met passes', () => {
      const def = makeDef({ name: 'del', arity: -2 });
      expect(table.checkArity(def, 2)).toBeNull();
      expect(table.checkArity(def, 5)).toBeNull();
    });

    it('negative arity: below minimum fails', () => {
      const def = makeDef({ name: 'del', arity: -2 });
      const result = table.checkArity(def, 1);
      expect(result).not.toBeNull();
      expect(result?.kind).toBe('error');
      expect((result as { message: string }).message).toContain(
        "wrong number of arguments for 'del' command"
      );
    });

    it('arity 1: only command name, no args', () => {
      const def = makeDef({ name: 'randomkey', arity: 1 });
      expect(table.checkArity(def, 1)).toBeNull();
      const result = table.checkArity(def, 2);
      expect(result).not.toBeNull();
    });

    it('error message uses lowercase command name', () => {
      const def = makeDef({ name: 'SET', arity: 3 });
      const result = table.checkArity(def, 1);
      expect(result).not.toBeNull();
      expect((result as { message: string }).message).toContain("'set'");
    });
  });

  describe('lookup', () => {
    it('returns definition for known command', () => {
      const def = makeDef({ name: 'ping' });
      table.register(def);
      const result = table.lookup('ping');
      expect('handler' in result).toBe(true);
      expect(result).toBe(def);
    });

    it('returns error reply for unknown command', () => {
      const result = table.lookup('badcmd') as Reply;
      expect(result.kind).toBe('error');
      expect((result as { message: string }).message).toContain(
        "unknown command 'badcmd'"
      );
    });

    it('unknown command error includes command name', () => {
      const result = table.lookup('FOOBAR') as Reply;
      expect(result.kind).toBe('error');
      expect((result as { message: string }).message).toContain('FOOBAR');
    });
  });
});

describe('CommandDefinition structure', () => {
  it('supports subcommands map', () => {
    const subDef = makeDef({ name: 'encoding', arity: 3 });
    const def = makeDef({
      name: 'object',
      subcommands: new Map([['encoding', subDef]]),
    });
    expect(def.subcommands?.get('encoding')).toBe(subDef);
    expect(def.subcommands?.size).toBe(1);
  });

  it('flags are stored as a Set', () => {
    const def = makeDef({
      name: 'set',
      flags: new Set(['write', 'denyoom']),
    });
    expect(def.flags.has('write')).toBe(true);
    expect(def.flags.has('denyoom')).toBe(true);
    expect(def.flags.has('readonly')).toBe(false);
  });

  it('categories are stored as a Set', () => {
    const def = makeDef({
      name: 'get',
      categories: new Set(['@string', '@read']),
    });
    expect(def.categories.has('@string')).toBe(true);
    expect(def.categories.has('@read')).toBe(true);
  });
});

describe('createCommandTable (registry)', () => {
  let table: CommandTable;

  beforeEach(() => {
    table = createCommandTable();
  });

  describe('all commands are registered', () => {
    const expectedCommands = [
      'ping',
      'echo',
      'quit',
      'reset',
      'hello',
      'auth',
      'select',
      'dbsize',
      'flushdb',
      'flushall',
      'swapdb',
      'client',
      'del',
      'unlink',
      'exists',
      'type',
      'rename',
      'renamenx',
      'persist',
      'randomkey',
      'touch',
      'copy',
      'object',
      'wait',
      'dump',
      'restore',
      'expire',
      'pexpire',
      'expireat',
      'pexpireat',
      'ttl',
      'pttl',
      'expiretime',
      'pexpiretime',
      'keys',
      'scan',
      'sort',
      'sort_ro',
      'get',
      'set',
      'mget',
      'mset',
      'msetnx',
      'append',
      'strlen',
      'setrange',
      'getrange',
      'substr',
      'getex',
      'getdel',
      'getset',
      'setnx',
      'setex',
      'psetex',
      'lcs',
      'incr',
      'decr',
      'incrby',
      'decrby',
      'incrbyfloat',
      'hset',
      'hget',
      'hmset',
      'hmget',
      'hgetall',
      'hdel',
      'hexists',
      'hlen',
      'hkeys',
      'hvals',
      'hsetnx',
      'hincrby',
      'hincrbyfloat',
      'hrandfield',
      'hscan',
      'hexpire',
      'hpexpire',
      'hexpireat',
      'hpexpireat',
      'httl',
      'hpttl',
      'hpersist',
      'hexpiretime',
      'hpexpiretime',
      'lpush',
      'rpush',
      'lpushx',
      'rpushx',
      'lpop',
      'rpop',
      'llen',
      'sadd',
      'srem',
      'sismember',
      'smismember',
      'smembers',
      'scard',
      'smove',
      'command',
    ];

    for (const name of expectedCommands) {
      it(`has '${name}' registered`, () => {
        expect(table.has(name)).toBe(true);
      });
    }

    it('has the expected total count', () => {
      expect(table.size).toBe(expectedCommands.length);
    });
  });

  describe('arity values match Redis', () => {
    const arityTests: [string, number][] = [
      ['ping', -1],
      ['echo', 2],
      ['quit', -1],
      ['reset', 1],
      ['hello', -1],
      ['auth', -2],
      ['select', 2],
      ['dbsize', 1],
      ['flushdb', -1],
      ['flushall', -1],
      ['swapdb', 3],
      ['client', -2],
      ['del', -2],
      ['unlink', -2],
      ['exists', -2],
      ['type', 2],
      ['rename', 3],
      ['renamenx', 3],
      ['persist', 2],
      ['randomkey', 1],
      ['touch', -2],
      ['copy', -3],
      ['object', -2],
      ['wait', 3],
      ['dump', 2],
      ['restore', -4],
      ['expire', -3],
      ['pexpire', -3],
      ['expireat', -3],
      ['pexpireat', -3],
      ['ttl', 2],
      ['pttl', 2],
      ['expiretime', 2],
      ['pexpiretime', 2],
      ['keys', 2],
      ['scan', -2],
      ['sort', -2],
      ['sort_ro', -2],
      ['get', 2],
      ['set', -3],
      ['mget', -2],
      ['mset', -3],
      ['msetnx', -3],
      ['append', 3],
      ['strlen', 2],
      ['setrange', 4],
      ['getrange', 4],
      ['substr', 4],
      ['getex', -2],
      ['getdel', 2],
      ['getset', 3],
      ['setnx', 3],
      ['setex', 4],
      ['psetex', 4],
      ['lcs', -3],
      ['incr', 2],
      ['decr', 2],
      ['incrby', 3],
      ['decrby', 3],
      ['incrbyfloat', 3],
      ['hset', -4],
      ['hget', 3],
      ['hmset', -4],
      ['hmget', -3],
      ['hgetall', 2],
      ['hdel', -3],
      ['hexists', 3],
      ['hlen', 2],
      ['hkeys', 2],
      ['hvals', 2],
      ['hsetnx', 4],
      ['hincrby', 4],
      ['hincrbyfloat', 4],
      ['hrandfield', -2],
      ['hscan', -3],
      ['hexpire', -6],
      ['hpexpire', -6],
      ['hexpireat', -6],
      ['hpexpireat', -6],
      ['httl', -5],
      ['hpttl', -5],
      ['hpersist', -5],
      ['hexpiretime', -5],
      ['hpexpiretime', -5],
      ['lpush', -3],
      ['rpush', -3],
      ['lpushx', -3],
      ['rpushx', -3],
      ['lpop', -2],
      ['rpop', -2],
      ['llen', 2],
    ];

    for (const [name, expectedArity] of arityTests) {
      it(`'${name}' has arity ${expectedArity}`, () => {
        expect(table.get(name)?.arity).toBe(expectedArity);
      });
    }
  });

  describe('flags are correct', () => {
    it('del is write', () => {
      const def = getDef(table, 'del');
      expect(def.flags.has('write')).toBe(true);
      expect(def.flags.has('readonly')).toBe(false);
    });

    it('exists is readonly and fast', () => {
      const def = getDef(table, 'exists');
      expect(def.flags.has('readonly')).toBe(true);
      expect(def.flags.has('fast')).toBe(true);
    });

    it('unlink is write and fast', () => {
      const def = getDef(table, 'unlink');
      expect(def.flags.has('write')).toBe(true);
      expect(def.flags.has('fast')).toBe(true);
    });

    it('sort is write and denyoom', () => {
      const def = getDef(table, 'sort');
      expect(def.flags.has('write')).toBe(true);
      expect(def.flags.has('denyoom')).toBe(true);
    });

    it('sort_ro is readonly', () => {
      const def = getDef(table, 'sort_ro');
      expect(def.flags.has('readonly')).toBe(true);
      expect(def.flags.has('write')).toBe(false);
    });

    it('ttl is readonly and fast', () => {
      const def = getDef(table, 'ttl');
      expect(def.flags.has('readonly')).toBe(true);
      expect(def.flags.has('fast')).toBe(true);
    });

    it('expire is write and fast', () => {
      const def = getDef(table, 'expire');
      expect(def.flags.has('write')).toBe(true);
      expect(def.flags.has('fast')).toBe(true);
    });

    it('get is readonly and fast', () => {
      const def = getDef(table, 'get');
      expect(def.flags.has('readonly')).toBe(true);
      expect(def.flags.has('fast')).toBe(true);
    });

    it('set is write and denyoom', () => {
      const def = getDef(table, 'set');
      expect(def.flags.has('write')).toBe(true);
      expect(def.flags.has('denyoom')).toBe(true);
    });

    it('ping is fast, stale, loading', () => {
      const def = getDef(table, 'ping');
      expect(def.flags.has('fast')).toBe(true);
      expect(def.flags.has('stale')).toBe(true);
      expect(def.flags.has('loading')).toBe(true);
    });

    it('echo is fast, stale, loading', () => {
      const def = getDef(table, 'echo');
      expect(def.flags.has('fast')).toBe(true);
      expect(def.flags.has('stale')).toBe(true);
      expect(def.flags.has('loading')).toBe(true);
    });

    it('quit is fast, noscript, stale, loading, noauth', () => {
      const def = getDef(table, 'quit');
      expect(def.flags.has('fast')).toBe(true);
      expect(def.flags.has('noscript')).toBe(true);
      expect(def.flags.has('stale')).toBe(true);
      expect(def.flags.has('loading')).toBe(true);
      expect(def.flags.has('noauth')).toBe(true);
    });

    it('reset is fast, noscript, loading, stale, noauth', () => {
      const def = getDef(table, 'reset');
      expect(def.flags.has('fast')).toBe(true);
      expect(def.flags.has('noscript')).toBe(true);
      expect(def.flags.has('loading')).toBe(true);
      expect(def.flags.has('stale')).toBe(true);
      expect(def.flags.has('noauth')).toBe(true);
    });
  });

  describe('key positions are correct', () => {
    it('del: keys from 1 to -1 step 1', () => {
      const def = getDef(table, 'del');
      expect(def.firstKey).toBe(1);
      expect(def.lastKey).toBe(-1);
      expect(def.keyStep).toBe(1);
    });

    it('rename: keys at positions 1 and 2', () => {
      const def = getDef(table, 'rename');
      expect(def.firstKey).toBe(1);
      expect(def.lastKey).toBe(2);
      expect(def.keyStep).toBe(1);
    });

    it('randomkey: no keys (firstKey=0)', () => {
      const def = getDef(table, 'randomkey');
      expect(def.firstKey).toBe(0);
      expect(def.lastKey).toBe(0);
      expect(def.keyStep).toBe(0);
    });

    it('scan: no keys (cursor-based)', () => {
      const def = getDef(table, 'scan');
      expect(def.firstKey).toBe(0);
    });

    it('type: single key at position 1', () => {
      const def = getDef(table, 'type');
      expect(def.firstKey).toBe(1);
      expect(def.lastKey).toBe(1);
      expect(def.keyStep).toBe(1);
    });
  });

  describe('categories are correct', () => {
    it('del has @keyspace and @write', () => {
      const def = getDef(table, 'del');
      expect(def.categories.has('@keyspace')).toBe(true);
      expect(def.categories.has('@write')).toBe(true);
    });

    it('exists has @keyspace and @read', () => {
      const def = getDef(table, 'exists');
      expect(def.categories.has('@keyspace')).toBe(true);
      expect(def.categories.has('@read')).toBe(true);
    });

    it('sort has @write', () => {
      const def = getDef(table, 'sort');
      expect(def.categories.has('@write')).toBe(true);
    });

    it('sort_ro has @read', () => {
      const def = getDef(table, 'sort_ro');
      expect(def.categories.has('@read')).toBe(true);
    });
  });

  describe('subcommands', () => {
    it('object has subcommands', () => {
      const def = getDef(table, 'object');
      expect(def.subcommands).toBeDefined();
      expect(def.subcommands?.size).toBe(5);
    });

    it('object subcommands include encoding, refcount, idletime, freq, help', () => {
      const def = getDef(table, 'object');
      const subs = def.subcommands;
      expect(subs?.has('encoding')).toBe(true);
      expect(subs?.has('refcount')).toBe(true);
      expect(subs?.has('idletime')).toBe(true);
      expect(subs?.has('freq')).toBe(true);
      expect(subs?.has('help')).toBe(true);
    });

    it('object|encoding subcommand has arity 3', () => {
      const def = getDef(table, 'object');
      const sub = def.subcommands?.get('encoding');
      expect(sub).toBeDefined();
      expect(sub?.arity).toBe(3);
      expect(sub?.name).toBe('encoding');
    });
  });

  describe('handler integration', () => {
    it('del handler executes correctly through command table', () => {
      const { engine, db } = createCtx();
      db.set('a', 'string', 'raw', '1');
      db.set('b', 'string', 'raw', '2');

      const def = getDef(table, 'del');
      const result = def.handler({ db, engine }, ['a', 'b', 'missing']);
      expect(result).toEqual({ kind: 'integer', value: 2 });
      expect(db.has('a')).toBe(false);
      expect(db.has('b')).toBe(false);
    });

    it('exists handler works through command table', () => {
      const { engine, db } = createCtx();
      db.set('x', 'string', 'raw', 'v');

      const def = getDef(table, 'exists');
      const result = def.handler({ db, engine }, ['x', 'missing']);
      expect(result).toEqual({ kind: 'integer', value: 1 });
    });

    it('type handler works through command table', () => {
      const { engine, db } = createCtx();
      db.set('k', 'list', 'quicklist', []);

      const def = getDef(table, 'type');
      const result = def.handler({ db, engine }, ['k']);
      expect(result).toEqual({ kind: 'status', value: 'list' });
    });

    it('randomkey handler works through command table', () => {
      const { engine, db } = createCtx();
      const def = getDef(table, 'randomkey');
      const result = def.handler({ db, engine }, []);
      expect(result).toEqual({ kind: 'bulk', value: null });
    });

    it('rename handler works through command table', () => {
      const { engine, db } = createCtx();
      db.set('src', 'string', 'raw', 'val');

      const def = getDef(table, 'rename');
      const result = def.handler({ db, engine }, ['src', 'dst']);
      expect(result).toEqual({ kind: 'status', value: 'OK' });
      expect(db.get('dst')?.value).toBe('val');
    });

    it('expire handler works through command table', () => {
      const { engine, db } = createCtx();
      db.set('k', 'string', 'raw', 'v');

      const def = getDef(table, 'expire');
      const result = def.handler({ db, engine }, ['k', '10']);
      expect(result).toEqual({ kind: 'integer', value: 1 });
      expect(db.getExpiry('k')).toBe(11000);
    });

    it('ttl handler works through command table', () => {
      const { engine, db } = createCtx();
      db.set('k', 'string', 'raw', 'v');
      db.setExpiry('k', 11000);

      const def = getDef(table, 'ttl');
      const result = def.handler({ db, engine }, ['k']);
      expect(result).toEqual({ kind: 'integer', value: 10 });
    });

    it('object handler dispatches subcommands through command table', () => {
      const { engine, db } = createCtx();
      db.set('k', 'string', 'embstr', 'hi');

      const def = getDef(table, 'object');
      const result = def.handler({ db, engine }, ['ENCODING', 'k']);
      expect(result).toEqual({ kind: 'bulk', value: 'embstr' });
    });

    it('copy handler accesses engine for cross-db copy', () => {
      const { engine, db } = createCtx();
      db.set('src', 'string', 'raw', 'hello');

      const def = getDef(table, 'copy');
      def.handler({ db, engine }, ['src', 'dst', 'DB', '1']);
      expect(engine.db(1).get('dst')?.value).toBe('hello');
    });

    it('sort handler works through command table', () => {
      const { engine, db } = createCtx();
      db.set('list', 'list', 'quicklist', ['3', '1', '2']);

      const def = getDef(table, 'sort');
      const result = def.handler({ db, engine }, ['list']);
      expect(result.kind).toBe('array');
    });

    it('keys handler works through command table', () => {
      const { engine, db } = createCtx();
      db.set('foo', 'string', 'raw', '1');
      db.set('bar', 'string', 'raw', '2');

      const def = getDef(table, 'keys');
      const result = def.handler({ db, engine }, ['*']);
      expect(result.kind).toBe('array');
    });

    it('scan handler works through command table', () => {
      const { engine, db } = createCtx();
      const def = getDef(table, 'scan');
      const result = def.handler({ db, engine }, ['0']);
      expect(result.kind).toBe('array');
    });

    it('wait handler returns 0', () => {
      const { engine, db } = createCtx();
      const def = getDef(table, 'wait');
      const result = def.handler({ db, engine }, []);
      expect(result).toEqual({ kind: 'integer', value: 0 });
    });

    it('dump handler returns error', () => {
      const { engine, db } = createCtx();
      const def = getDef(table, 'dump');
      expect(def.handler({ db, engine }, []).kind).toBe('error');
    });

    it('restore handler returns error', () => {
      const { engine, db } = createCtx();
      const def = getDef(table, 'restore');
      expect(def.handler({ db, engine }, []).kind).toBe('error');
    });

    it('get handler returns bulk string for existing key', () => {
      const { engine, db } = createCtx();
      db.set('k', 'string', 'raw', 'hello');
      const def = getDef(table, 'get');
      const result = def.handler({ db, engine }, ['k']);
      expect(result).toEqual({ kind: 'bulk', value: 'hello' });
    });

    it('get handler returns nil for missing key', () => {
      const { engine, db } = createCtx();
      const def = getDef(table, 'get');
      const result = def.handler({ db, engine }, ['missing']);
      expect(result).toEqual({ kind: 'bulk', value: null });
    });

    it('set handler stores value and returns OK', () => {
      const { engine, db } = createCtx();
      const def = getDef(table, 'set');
      const result = def.handler({ db, engine }, ['k', 'val']);
      expect(result).toEqual({ kind: 'status', value: 'OK' });
      expect(db.get('k')?.value).toBe('val');
    });

    it('set handler with EX sets expiry', () => {
      const { engine, db } = createCtx();
      const def = getDef(table, 'set');
      def.handler({ db, engine }, ['k', 'v', 'EX', '10']);
      expect(db.getExpiry('k')).toBe(11000);
    });
  });

  describe('arity validation with registered commands', () => {
    it('del: accepts 2+ args (command + keys)', () => {
      const def = getDef(table, 'del');
      expect(table.checkArity(def, 1)).not.toBeNull();
      expect(table.checkArity(def, 2)).toBeNull();
      expect(table.checkArity(def, 5)).toBeNull();
    });

    it('type: requires exactly 2 (command + key)', () => {
      const def = getDef(table, 'type');
      expect(table.checkArity(def, 1)).not.toBeNull();
      expect(table.checkArity(def, 2)).toBeNull();
      expect(table.checkArity(def, 3)).not.toBeNull();
    });

    it('rename: requires exactly 3', () => {
      const def = getDef(table, 'rename');
      expect(table.checkArity(def, 2)).not.toBeNull();
      expect(table.checkArity(def, 3)).toBeNull();
      expect(table.checkArity(def, 4)).not.toBeNull();
    });

    it('copy: requires 3+', () => {
      const def = getDef(table, 'copy');
      expect(table.checkArity(def, 2)).not.toBeNull();
      expect(table.checkArity(def, 3)).toBeNull();
      expect(table.checkArity(def, 6)).toBeNull();
    });

    it('randomkey: requires exactly 1 (just command)', () => {
      const def = getDef(table, 'randomkey');
      expect(table.checkArity(def, 1)).toBeNull();
      expect(table.checkArity(def, 2)).not.toBeNull();
    });

    it('wait: requires exactly 3', () => {
      const def = getDef(table, 'wait');
      expect(table.checkArity(def, 2)).not.toBeNull();
      expect(table.checkArity(def, 3)).toBeNull();
      expect(table.checkArity(def, 4)).not.toBeNull();
    });

    it('expire: requires 3+', () => {
      const def = getDef(table, 'expire');
      expect(table.checkArity(def, 2)).not.toBeNull();
      expect(table.checkArity(def, 3)).toBeNull();
      expect(table.checkArity(def, 5)).toBeNull();
    });

    it('restore: requires 4+', () => {
      const def = getDef(table, 'restore');
      expect(table.checkArity(def, 3)).not.toBeNull();
      expect(table.checkArity(def, 4)).toBeNull();
      expect(table.checkArity(def, 7)).toBeNull();
    });

    it('get: requires exactly 2', () => {
      const def = getDef(table, 'get');
      expect(table.checkArity(def, 1)).not.toBeNull();
      expect(table.checkArity(def, 2)).toBeNull();
      expect(table.checkArity(def, 3)).not.toBeNull();
    });

    it('set: requires 3+', () => {
      const def = getDef(table, 'set');
      expect(table.checkArity(def, 2)).not.toBeNull();
      expect(table.checkArity(def, 3)).toBeNull();
      expect(table.checkArity(def, 7)).toBeNull();
    });
  });

  describe('lookup with registered commands', () => {
    it('finds existing command case-insensitively', () => {
      const result = table.lookup('DEL');
      expect('handler' in result).toBe(true);
    });

    it('returns error for unknown command', () => {
      const result = table.lookup('NONEXISTENT') as Reply;
      expect(result.kind).toBe('error');
      expect((result as { message: string }).message).toContain(
        "unknown command 'NONEXISTENT'"
      );
    });
  });
});
