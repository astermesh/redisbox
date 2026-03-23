/**
 * Integration tests for keyspace event notifications.
 *
 * Verifies that command handlers emit correct keyspace notifications
 * matching real Redis behavior.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { RedisEngine } from './engine.ts';
import { ConfigStore } from '../config-store.ts';
import type { CommandContext, Reply } from './types.ts';
import { ClientState } from '../server/client-state.ts';
import type { CommandSpec } from './command-table.ts';

// Import all command specs
import { specs as genericSpecs } from './commands/generic.ts';
import { specs as stringSpecs } from './commands/string.ts';
import { specs as incrSpecs } from './commands/incr.ts';
import { specs as hashSpecs } from './commands/hash.ts';
import { specs as listSpecs } from './commands/list.ts';
import { specs as setSpecs } from './commands/set.ts';
import { specs as sortedSetSpecs } from './commands/sorted-set.ts';
import { specs as streamSpecs } from './commands/stream.ts';
import { specs as ttlSpecs } from './commands/ttl.ts';
import { specs as bitmapSpecs } from './commands/bitmap.ts';
import { specs as hyperloglogSpecs } from './commands/hyperloglog.ts';
import { specs as geoSpecs } from './commands/geo.ts';
import { specs as databaseSpecs } from './commands/database.ts';
import { specs as sortSpecs } from './commands/sort.ts';
import { specs as blockingListSpecs } from './commands/blocking-list.ts';
import { specs as blockingSortedSetSpecs } from './commands/blocking-sorted-set.ts';

interface CapturedMsg {
  clientId: number;
  reply: Reply;
}

/**
 * Extract channel and message from a pubsub reply.
 * Message replies look like: { kind: 'array', value: [{kind:'bulk',value:'message'}, {kind:'bulk',value:channel}, {kind:'bulk',value:message}] }
 * Pattern replies: { kind: 'array', value: [{kind:'bulk',value:'pmessage'}, {kind:'bulk',value:pattern}, {kind:'bulk',value:channel}, {kind:'bulk',value:message}] }
 */
function extractEvent(msg: CapturedMsg): {
  channel: string;
  message: string;
} | null {
  const reply = msg.reply;
  if (reply.kind !== 'array' || !Array.isArray(reply.value)) return null;
  const parts = reply.value as Reply[];
  const type =
    parts[0] && parts[0].kind === 'bulk' ? (parts[0].value as string) : '';
  if (type === 'message') {
    return {
      channel:
        parts[1] && parts[1].kind === 'bulk' ? (parts[1].value as string) : '',
      message:
        parts[2] && parts[2].kind === 'bulk' ? (parts[2].value as string) : '',
    };
  }
  if (type === 'pmessage') {
    return {
      channel:
        parts[2] && parts[2].kind === 'bulk' ? (parts[2].value as string) : '',
      message:
        parts[3] && parts[3].kind === 'bulk' ? (parts[3].value as string) : '',
    };
  }
  return null;
}

describe('keyspace event integration', () => {
  let engine: RedisEngine;
  let config: ConfigStore;
  let messages: CapturedMsg[];
  let ctx: CommandContext;
  const LISTENER_ID = 999;

  function findSpec(
    specs: CommandSpec[],
    name: string
  ): CommandSpec['handler'] {
    const spec = specs.find((s) => s.name === name);
    if (!spec) throw new Error(`Spec not found: ${name}`);
    return spec.handler;
  }

  function exec(handler: CommandSpec['handler'], args: string[]): Reply {
    return handler(ctx, args);
  }

  beforeEach(() => {
    engine = new RedisEngine({
      clock: () => 1000,
      rng: () => 0.5,
    });
    config = new ConfigStore();
    // Enable all notifications
    config.set('notify-keyspace-events', 'KEA');
    messages = [];
    engine.pubsub.setSender((clientId, reply) => {
      messages.push({ clientId, reply });
    });

    // Subscribe to all keyspace/keyevent channels via pattern
    engine.pubsub.psubscribe(LISTENER_ID, '__key*@*__:*');

    const client = new ClientState(1, 0);
    client.dbIndex = 0;
    ctx = {
      db: engine.db(0),
      engine,
      client,
      config,
      pubsub: engine.pubsub,
    };
  });

  function getEvents(): { channel: string; message: string }[] {
    return messages
      .map(extractEvent)
      .filter((e): e is { channel: string; message: string } => e !== null);
  }

  function expectEvent(event: string, key: string): void {
    const evts = getEvents();
    const found = evts.some(
      (e) =>
        (e.channel === `__keyspace@0__:${key}` && e.message === event) ||
        (e.channel === `__keyevent@0__:${event}` && e.message === key)
    );
    expect(
      found,
      `Expected event '${event}' for key '${key}'. Got: ${JSON.stringify(evts)}`
    ).toBe(true);
  }

  function expectNoEvents(): void {
    expect(getEvents()).toHaveLength(0);
  }

  function clearEvents(): void {
    messages.length = 0;
  }

  // --- Generic commands ---

  describe('generic commands', () => {
    it('DEL emits "del" for each deleted key', () => {
      ctx.db.set('k1', 'string', 'raw', 'v1');
      ctx.db.set('k2', 'string', 'raw', 'v2');
      clearEvents();

      exec(findSpec(genericSpecs, 'del'), ['k1', 'k2', 'k3']);

      expectEvent('del', 'k1');
      expectEvent('del', 'k2');
      // k3 didn't exist — no event for it
      const evts = getEvents();
      const k3events = evts.filter(
        (e) => e.message === 'k3' || e.channel.includes('k3')
      );
      expect(k3events).toHaveLength(0);
    });

    it('UNLINK emits "del" for each deleted key', () => {
      ctx.db.set('k1', 'string', 'raw', 'v1');
      clearEvents();

      exec(findSpec(genericSpecs, 'unlink'), ['k1']);
      expectEvent('del', 'k1');
    });

    it('RENAME emits "rename_from" and "rename_to"', () => {
      ctx.db.set('src', 'string', 'raw', 'v');
      clearEvents();

      exec(findSpec(genericSpecs, 'rename'), ['src', 'dst']);
      expectEvent('rename_from', 'src');
      expectEvent('rename_to', 'dst');
    });

    it('RENAMENX emits events only when successful', () => {
      ctx.db.set('src', 'string', 'raw', 'v');
      ctx.db.set('dst', 'string', 'raw', 'existing');
      clearEvents();

      exec(findSpec(genericSpecs, 'renamenx'), ['src', 'dst']);
      expectNoEvents();
    });

    it('PERSIST emits "persist" when successful', () => {
      ctx.db.set('k', 'string', 'raw', 'v');
      ctx.db.setExpiry('k', 5000);
      clearEvents();

      exec(findSpec(genericSpecs, 'persist'), ['k']);
      expectEvent('persist', 'k');
    });

    it('COPY emits "copy_to" on destination', () => {
      ctx.db.set('src', 'string', 'raw', 'v');
      clearEvents();

      exec(findSpec(genericSpecs, 'copy'), ['src', 'dst']);
      expectEvent('copy_to', 'dst');
    });
  });

  // --- String commands ---

  describe('string commands', () => {
    it('SET emits "set"', () => {
      exec(findSpec(stringSpecs, 'set'), ['k', 'v']);
      expectEvent('set', 'k');
    });

    it('SET with NX does not emit when key exists', () => {
      ctx.db.set('k', 'string', 'raw', 'old');
      clearEvents();

      exec(findSpec(stringSpecs, 'set'), ['k', 'v', 'NX']);
      expectNoEvents();
    });

    it('SETNX emits "set" when successful', () => {
      exec(findSpec(stringSpecs, 'setnx'), ['k', 'v']);
      expectEvent('set', 'k');
    });

    it('SETEX emits "set"', () => {
      exec(findSpec(stringSpecs, 'setex'), ['k', '10', 'v']);
      expectEvent('set', 'k');
    });

    it('PSETEX emits "set"', () => {
      exec(findSpec(stringSpecs, 'psetex'), ['k', '10000', 'v']);
      expectEvent('set', 'k');
    });

    it('MSET emits "set" for each key', () => {
      exec(findSpec(stringSpecs, 'mset'), ['k1', 'v1', 'k2', 'v2']);
      expectEvent('set', 'k1');
      expectEvent('set', 'k2');
    });

    it('MSETNX emits "set" for each key when successful', () => {
      exec(findSpec(stringSpecs, 'msetnx'), ['k1', 'v1', 'k2', 'v2']);
      expectEvent('set', 'k1');
      expectEvent('set', 'k2');
    });

    it('MSETNX does not emit when any key exists', () => {
      ctx.db.set('k1', 'string', 'raw', 'existing');
      clearEvents();

      exec(findSpec(stringSpecs, 'msetnx'), ['k1', 'v1', 'k2', 'v2']);
      expectNoEvents();
    });

    it('APPEND emits "append"', () => {
      exec(findSpec(stringSpecs, 'append'), ['k', 'hello']);
      expectEvent('append', 'k');
    });

    it('GETSET emits "set"', () => {
      exec(findSpec(stringSpecs, 'getset'), ['k', 'v']);
      expectEvent('set', 'k');
    });

    it('GETDEL emits "del" when key exists', () => {
      ctx.db.set('k', 'string', 'raw', 'v');
      clearEvents();

      exec(findSpec(stringSpecs, 'getdel'), ['k']);
      expectEvent('del', 'k');
    });

    it('GETDEL does not emit when key does not exist', () => {
      exec(findSpec(stringSpecs, 'getdel'), ['nonexistent']);
      expectNoEvents();
    });

    it('GETEX does not emit with no options (bare GETEX)', () => {
      ctx.db.set('k', 'string', 'raw', 'v');
      clearEvents();

      exec(findSpec(stringSpecs, 'getex'), ['k']);
      expectNoEvents();
    });

    it('GETEX emits "expire" with EX option', () => {
      ctx.db.set('k', 'string', 'raw', 'v');
      clearEvents();

      exec(findSpec(stringSpecs, 'getex'), ['k', 'EX', '10']);
      expectEvent('expire', 'k');
    });

    it('GETEX emits "expire" with PX option', () => {
      ctx.db.set('k', 'string', 'raw', 'v');
      clearEvents();

      exec(findSpec(stringSpecs, 'getex'), ['k', 'PX', '10000']);
      expectEvent('expire', 'k');
    });

    it('GETEX emits "persist" with PERSIST option', () => {
      ctx.db.set('k', 'string', 'raw', 'v');
      ctx.db.setExpiry('k', 5000);
      clearEvents();

      exec(findSpec(stringSpecs, 'getex'), ['k', 'PERSIST']);
      expectEvent('persist', 'k');
    });

    it('SETRANGE emits "setrange"', () => {
      ctx.db.set('k', 'string', 'raw', 'hello');
      clearEvents();

      exec(findSpec(stringSpecs, 'setrange'), ['k', '0', 'world']);
      expectEvent('setrange', 'k');
    });
  });

  // --- Incr commands ---

  describe('incr commands', () => {
    it('INCR emits "incrby"', () => {
      ctx.db.set('k', 'string', 'int', '10');
      clearEvents();

      exec(findSpec(incrSpecs, 'incr'), ['k']);
      expectEvent('incrby', 'k');
    });

    it('DECR emits "decrby"', () => {
      ctx.db.set('k', 'string', 'int', '10');
      clearEvents();

      exec(findSpec(incrSpecs, 'decr'), ['k']);
      expectEvent('decrby', 'k');
    });

    it('INCRBY emits "incrby"', () => {
      ctx.db.set('k', 'string', 'int', '10');
      clearEvents();

      exec(findSpec(incrSpecs, 'incrby'), ['k', '5']);
      expectEvent('incrby', 'k');
    });

    it('DECRBY emits "decrby"', () => {
      ctx.db.set('k', 'string', 'int', '10');
      clearEvents();

      exec(findSpec(incrSpecs, 'decrby'), ['k', '5']);
      expectEvent('decrby', 'k');
    });

    it('INCRBYFLOAT emits "incrbyfloat"', () => {
      ctx.db.set('k', 'string', 'raw', '10.5');
      clearEvents();

      exec(findSpec(incrSpecs, 'incrbyfloat'), ['k', '0.5']);
      expectEvent('incrbyfloat', 'k');
    });
  });

  // --- Hash commands ---

  describe('hash commands', () => {
    it('HSET emits "hset"', () => {
      exec(findSpec(hashSpecs, 'hset'), ['k', 'f', 'v']);
      expectEvent('hset', 'k');
    });

    it('HMSET emits "hset"', () => {
      exec(findSpec(hashSpecs, 'hmset'), ['k', 'f1', 'v1', 'f2', 'v2']);
      expectEvent('hset', 'k');
    });

    it('HSETNX emits "hset" when successful', () => {
      exec(findSpec(hashSpecs, 'hsetnx'), ['k', 'f', 'v']);
      expectEvent('hset', 'k');
    });

    it('HSETNX does not emit when field exists', () => {
      ctx.db.set('k', 'hash', 'listpack', new Map([['f', 'v']]));
      clearEvents();

      exec(findSpec(hashSpecs, 'hsetnx'), ['k', 'f', 'new']);
      expectNoEvents();
    });

    it('HDEL emits "hdel"', () => {
      ctx.db.set('k', 'hash', 'listpack', new Map([['f', 'v']]));
      clearEvents();

      exec(findSpec(hashSpecs, 'hdel'), ['k', 'f']);
      expectEvent('hdel', 'k');
    });

    it('HINCRBY emits "hincrby"', () => {
      ctx.db.set('k', 'hash', 'listpack', new Map([['f', '10']]));
      clearEvents();

      exec(findSpec(hashSpecs, 'hincrby'), ['k', 'f', '5']);
      expectEvent('hincrby', 'k');
    });

    it('HINCRBYFLOAT emits "hincrbyfloat"', () => {
      ctx.db.set('k', 'hash', 'listpack', new Map([['f', '10.5']]));
      clearEvents();

      exec(findSpec(hashSpecs, 'hincrbyfloat'), ['k', 'f', '0.5']);
      expectEvent('hincrbyfloat', 'k');
    });
  });

  // --- List commands ---

  describe('list commands', () => {
    it('LPUSH emits "lpush"', () => {
      exec(findSpec(listSpecs, 'lpush'), ['k', 'v1', 'v2']);
      expectEvent('lpush', 'k');
    });

    it('RPUSH emits "rpush"', () => {
      exec(findSpec(listSpecs, 'rpush'), ['k', 'v1', 'v2']);
      expectEvent('rpush', 'k');
    });

    it('LPUSHX emits "lpush" when key exists', () => {
      ctx.db.set('k', 'list', 'listpack', ['existing']);
      clearEvents();

      exec(findSpec(listSpecs, 'lpushx'), ['k', 'v']);
      expectEvent('lpush', 'k');
    });

    it('RPUSHX emits "rpush" when key exists', () => {
      ctx.db.set('k', 'list', 'listpack', ['existing']);
      clearEvents();

      exec(findSpec(listSpecs, 'rpushx'), ['k', 'v']);
      expectEvent('rpush', 'k');
    });

    it('LPOP emits "lpop"', () => {
      ctx.db.set('k', 'list', 'listpack', ['v1', 'v2']);
      clearEvents();

      exec(findSpec(listSpecs, 'lpop'), ['k']);
      expectEvent('lpop', 'k');
    });

    it('RPOP emits "rpop"', () => {
      ctx.db.set('k', 'list', 'listpack', ['v1', 'v2']);
      clearEvents();

      exec(findSpec(listSpecs, 'rpop'), ['k']);
      expectEvent('rpop', 'k');
    });

    it('LSET emits "lset"', () => {
      ctx.db.set('k', 'list', 'listpack', ['v1', 'v2']);
      clearEvents();

      exec(findSpec(listSpecs, 'lset'), ['k', '0', 'new']);
      expectEvent('lset', 'k');
    });

    it('LINSERT emits "linsert"', () => {
      ctx.db.set('k', 'list', 'listpack', ['v1']);
      clearEvents();

      exec(findSpec(listSpecs, 'linsert'), ['k', 'BEFORE', 'v1', 'v0']);
      expectEvent('linsert', 'k');
    });

    it('LREM emits "lrem"', () => {
      ctx.db.set('k', 'list', 'listpack', ['v1', 'v1', 'v2']);
      clearEvents();

      exec(findSpec(listSpecs, 'lrem'), ['k', '1', 'v1']);
      expectEvent('lrem', 'k');
    });

    it('LTRIM emits "ltrim"', () => {
      ctx.db.set('k', 'list', 'listpack', ['v1', 'v2', 'v3']);
      clearEvents();

      exec(findSpec(listSpecs, 'ltrim'), ['k', '0', '1']);
      expectEvent('ltrim', 'k');
    });

    it('LMOVE emits events for source and destination', () => {
      ctx.db.set('src', 'list', 'listpack', ['v1', 'v2']);
      clearEvents();

      exec(findSpec(listSpecs, 'lmove'), ['src', 'dst', 'RIGHT', 'LEFT']);
      expectEvent('rpop', 'src');
      expectEvent('lpush', 'dst');
    });

    it('RPOPLPUSH emits events', () => {
      ctx.db.set('src', 'list', 'listpack', ['v1', 'v2']);
      clearEvents();

      exec(findSpec(listSpecs, 'rpoplpush'), ['src', 'dst']);
      expectEvent('rpop', 'src');
      expectEvent('lpush', 'dst');
    });
  });

  // --- Set commands ---

  describe('set commands', () => {
    it('SADD emits "sadd"', () => {
      exec(findSpec(setSpecs, 'sadd'), ['k', 'm1', 'm2']);
      expectEvent('sadd', 'k');
    });

    it('SREM emits "srem"', () => {
      ctx.db.set('k', 'set', 'listpack', new Set(['m1', 'm2']));
      clearEvents();

      exec(findSpec(setSpecs, 'srem'), ['k', 'm1']);
      expectEvent('srem', 'k');
    });

    it('SPOP emits "spop"', () => {
      ctx.db.set('k', 'set', 'hashtable', new Set(['m1', 'm2']));
      clearEvents();

      exec(findSpec(setSpecs, 'spop'), ['k']);
      expectEvent('spop', 'k');
    });

    it('SMOVE emits "srem" on source and "sadd" on destination', () => {
      ctx.db.set('src', 'set', 'hashtable', new Set(['m1']));
      clearEvents();

      exec(findSpec(setSpecs, 'smove'), ['src', 'dst', 'm1']);
      expectEvent('srem', 'src');
      expectEvent('sadd', 'dst');
    });

    it('SUNIONSTORE emits "sunionstore"', () => {
      ctx.db.set('s1', 'set', 'hashtable', new Set(['a']));
      ctx.db.set('s2', 'set', 'hashtable', new Set(['b']));
      clearEvents();

      exec(findSpec(setSpecs, 'sunionstore'), ['dst', 's1', 's2']);
      expectEvent('sunionstore', 'dst');
    });

    it('SINTERSTORE emits "sinterstore"', () => {
      ctx.db.set('s1', 'set', 'hashtable', new Set(['a', 'b']));
      ctx.db.set('s2', 'set', 'hashtable', new Set(['b', 'c']));
      clearEvents();

      exec(findSpec(setSpecs, 'sinterstore'), ['dst', 's1', 's2']);
      expectEvent('sinterstore', 'dst');
    });

    it('SDIFFSTORE emits "sdiffstore"', () => {
      ctx.db.set('s1', 'set', 'hashtable', new Set(['a', 'b']));
      ctx.db.set('s2', 'set', 'hashtable', new Set(['b']));
      clearEvents();

      exec(findSpec(setSpecs, 'sdiffstore'), ['dst', 's1', 's2']);
      expectEvent('sdiffstore', 'dst');
    });
  });

  // --- Sorted set commands ---

  describe('sorted set commands', () => {
    it('ZADD emits "zadd"', () => {
      exec(findSpec(sortedSetSpecs, 'zadd'), ['k', '1', 'm1']);
      expectEvent('zadd', 'k');
    });

    it('ZREM emits "zrem"', () => {
      exec(findSpec(sortedSetSpecs, 'zadd'), ['k', '1', 'm1']);
      clearEvents();

      exec(findSpec(sortedSetSpecs, 'zrem'), ['k', 'm1']);
      expectEvent('zrem', 'k');
    });

    it('ZINCRBY emits "zincrby"', () => {
      exec(findSpec(sortedSetSpecs, 'zadd'), ['k', '1', 'm1']);
      clearEvents();

      exec(findSpec(sortedSetSpecs, 'zincrby'), ['k', '5', 'm1']);
      expectEvent('zincrby', 'k');
    });

    it('ZPOPMIN emits "zpopmin"', () => {
      exec(findSpec(sortedSetSpecs, 'zadd'), ['k', '1', 'm1', '2', 'm2']);
      clearEvents();

      exec(findSpec(sortedSetSpecs, 'zpopmin'), ['k']);
      expectEvent('zpopmin', 'k');
    });

    it('ZPOPMAX emits "zpopmax"', () => {
      exec(findSpec(sortedSetSpecs, 'zadd'), ['k', '1', 'm1', '2', 'm2']);
      clearEvents();

      exec(findSpec(sortedSetSpecs, 'zpopmax'), ['k']);
      expectEvent('zpopmax', 'k');
    });

    it('ZRANGESTORE emits "zrangestore"', () => {
      exec(findSpec(sortedSetSpecs, 'zadd'), ['src', '1', 'a', '2', 'b']);
      clearEvents();

      exec(findSpec(sortedSetSpecs, 'zrangestore'), ['dst', 'src', '0', '-1']);
      expectEvent('zrangestore', 'dst');
    });

    it('ZUNIONSTORE emits "zunionstore"', () => {
      exec(findSpec(sortedSetSpecs, 'zadd'), ['s1', '1', 'a']);
      clearEvents();

      exec(findSpec(sortedSetSpecs, 'zunionstore'), ['dst', '1', 's1']);
      expectEvent('zunionstore', 'dst');
    });

    it('ZINTERSTORE emits "zinterstore"', () => {
      exec(findSpec(sortedSetSpecs, 'zadd'), ['s1', '1', 'a']);
      clearEvents();

      exec(findSpec(sortedSetSpecs, 'zinterstore'), ['dst', '1', 's1']);
      expectEvent('zinterstore', 'dst');
    });

    it('ZDIFFSTORE emits "zdiffstore"', () => {
      exec(findSpec(sortedSetSpecs, 'zadd'), ['s1', '1', 'a']);
      clearEvents();

      exec(findSpec(sortedSetSpecs, 'zdiffstore'), ['dst', '1', 's1']);
      expectEvent('zdiffstore', 'dst');
    });
  });

  // --- Stream commands ---

  describe('stream commands', () => {
    it('XADD emits "xadd"', () => {
      exec(findSpec(streamSpecs, 'xadd'), ['k', '*', 'f', 'v']);
      expectEvent('xadd', 'k');
    });

    it('XGROUP CREATE emits "xgroup-create"', () => {
      exec(findSpec(streamSpecs, 'xadd'), ['k', '*', 'f', 'v']);
      clearEvents();

      exec(findSpec(streamSpecs, 'xgroup'), ['CREATE', 'k', 'grp', '$']);
      expectEvent('xgroup-create', 'k');
    });

    it('XGROUP DESTROY emits "xgroup-destroy"', () => {
      exec(findSpec(streamSpecs, 'xadd'), ['k', '*', 'f', 'v']);
      exec(findSpec(streamSpecs, 'xgroup'), ['CREATE', 'k', 'grp', '$']);
      clearEvents();

      exec(findSpec(streamSpecs, 'xgroup'), ['DESTROY', 'k', 'grp']);
      expectEvent('xgroup-destroy', 'k');
    });

    it('XGROUP SETID emits "xgroup-setid"', () => {
      exec(findSpec(streamSpecs, 'xadd'), ['k', '*', 'f', 'v']);
      exec(findSpec(streamSpecs, 'xgroup'), ['CREATE', 'k', 'grp', '$']);
      clearEvents();

      exec(findSpec(streamSpecs, 'xgroup'), ['SETID', 'k', 'grp', '0']);
      expectEvent('xgroup-setid', 'k');
    });

    it('XGROUP DELCONSUMER emits "xgroup-delconsumer"', () => {
      exec(findSpec(streamSpecs, 'xadd'), ['k', '*', 'f', 'v']);
      exec(findSpec(streamSpecs, 'xgroup'), ['CREATE', 'k', 'grp', '0']);
      exec(findSpec(streamSpecs, 'xgroup'), [
        'CREATECONSUMER',
        'k',
        'grp',
        'c1',
      ]);
      clearEvents();

      exec(findSpec(streamSpecs, 'xgroup'), ['DELCONSUMER', 'k', 'grp', 'c1']);
      expectEvent('xgroup-delconsumer', 'k');
    });

    it('XDEL emits "xdel" when entries deleted', () => {
      const addResult = exec(findSpec(streamSpecs, 'xadd'), [
        'k',
        '*',
        'f',
        'v',
      ]);
      const id =
        addResult.kind === 'bulk' ? (addResult.value as string) : '1-0';
      clearEvents();

      exec(findSpec(streamSpecs, 'xdel'), ['k', id]);
      expectEvent('xdel', 'k');
    });

    it('XTRIM emits "xtrim" when entries trimmed', () => {
      exec(findSpec(streamSpecs, 'xadd'), ['k', '*', 'f', 'v1']);
      exec(findSpec(streamSpecs, 'xadd'), ['k', '*', 'f', 'v2']);
      exec(findSpec(streamSpecs, 'xadd'), ['k', '*', 'f', 'v3']);
      clearEvents();

      exec(findSpec(streamSpecs, 'xtrim'), ['k', 'MAXLEN', '1']);
      expectEvent('xtrim', 'k');
    });

    it('XSETID emits "xsetid" on success', () => {
      exec(findSpec(streamSpecs, 'xadd'), ['k', '1-0', 'f', 'v']);
      clearEvents();

      exec(findSpec(streamSpecs, 'xsetid'), ['k', '99-0']);
      expectEvent('xsetid', 'k');
    });

    it('XADD with MAXLEN emits secondary "xtrim" when entries trimmed', () => {
      exec(findSpec(streamSpecs, 'xadd'), ['k', '*', 'f', 'v1']);
      exec(findSpec(streamSpecs, 'xadd'), ['k', '*', 'f', 'v2']);
      clearEvents();

      exec(findSpec(streamSpecs, 'xadd'), ['k', 'MAXLEN', '2', '*', 'f', 'v3']);
      expectEvent('xadd', 'k');
      expectEvent('xtrim', 'k');
    });

    it('XADD with MAXLEN does not emit "xtrim" when no entries trimmed', () => {
      exec(findSpec(streamSpecs, 'xadd'), ['k', '*', 'f', 'v1']);
      clearEvents();

      exec(findSpec(streamSpecs, 'xadd'), [
        'k',
        'MAXLEN',
        '10',
        '*',
        'f',
        'v2',
      ]);
      expectEvent('xadd', 'k');
      const evts = getEvents();
      const xtrimEvts = evts.filter(
        (e) => e.channel === '__keyevent@0__:xtrim' || e.message === 'xtrim'
      );
      expect(xtrimEvts).toHaveLength(0);
    });

    it('XGROUP CREATECONSUMER emits "xgroup-createconsumer"', () => {
      exec(findSpec(streamSpecs, 'xadd'), ['k', '*', 'f', 'v']);
      exec(findSpec(streamSpecs, 'xgroup'), ['CREATE', 'k', 'grp', '$']);
      clearEvents();

      exec(findSpec(streamSpecs, 'xgroup'), [
        'CREATECONSUMER',
        'k',
        'grp',
        'c1',
      ]);
      expectEvent('xgroup-createconsumer', 'k');
    });
  });

  // --- TTL commands ---

  describe('TTL commands', () => {
    it('EXPIRE emits "expire"', () => {
      ctx.db.set('k', 'string', 'raw', 'v');
      clearEvents();

      exec(findSpec(ttlSpecs, 'expire'), ['k', '10']);
      expectEvent('expire', 'k');
    });

    it('PEXPIRE emits "pexpire"', () => {
      ctx.db.set('k', 'string', 'raw', 'v');
      clearEvents();

      exec(findSpec(ttlSpecs, 'pexpire'), ['k', '10000']);
      expectEvent('pexpire', 'k');
    });

    it('EXPIREAT emits "expire"', () => {
      ctx.db.set('k', 'string', 'raw', 'v');
      clearEvents();

      exec(findSpec(ttlSpecs, 'expireat'), ['k', '9999999']);
      expectEvent('expire', 'k');
    });

    it('PEXPIREAT emits "pexpire"', () => {
      ctx.db.set('k', 'string', 'raw', 'v');
      clearEvents();

      exec(findSpec(ttlSpecs, 'pexpireat'), ['k', '9999999999']);
      expectEvent('pexpire', 'k');
    });

    it('EXPIRE does not emit when key does not exist', () => {
      exec(findSpec(ttlSpecs, 'expire'), ['nonexistent', '10']);
      expectNoEvents();
    });
  });

  // --- Bitmap commands ---

  describe('bitmap commands', () => {
    it('SETBIT emits "setbit"', () => {
      exec(findSpec(bitmapSpecs, 'setbit'), ['k', '7', '1']);
      expectEvent('setbit', 'k');
    });

    it('BITOP emits "set" on destination', () => {
      ctx.db.set('s1', 'string', 'raw', 'abc');
      clearEvents();

      exec(findSpec(bitmapSpecs, 'bitop'), ['AND', 'dst', 's1']);
      expectEvent('set', 'dst');
    });

    it('BITFIELD emits "setbit" when writing', () => {
      exec(findSpec(bitmapSpecs, 'bitfield'), ['k', 'SET', 'u8', '0', '255']);
      expectEvent('setbit', 'k');
    });
  });

  // --- HyperLogLog commands ---

  describe('hyperloglog commands', () => {
    it('PFADD emits "pfadd"', () => {
      exec(findSpec(hyperloglogSpecs, 'pfadd'), ['k', 'a', 'b']);
      expectEvent('pfadd', 'k');
    });

    it('PFMERGE emits "pfmerge" on destination', () => {
      exec(findSpec(hyperloglogSpecs, 'pfadd'), ['s1', 'a']);
      clearEvents();

      exec(findSpec(hyperloglogSpecs, 'pfmerge'), ['dst', 's1']);
      expectEvent('pfmerge', 'dst');
    });
  });

  // --- Geo commands ---

  describe('geo commands', () => {
    it('GEOADD emits "zadd" (uses sorted set)', () => {
      exec(findSpec(geoSpecs, 'geoadd'), [
        'k',
        '13.361389',
        '38.115556',
        'Palermo',
      ]);
      expectEvent('zadd', 'k');
    });

    it('GEOSEARCHSTORE emits "geosearchstore"', () => {
      exec(findSpec(geoSpecs, 'geoadd'), [
        'src',
        '13.361389',
        '38.115556',
        'Palermo',
      ]);
      clearEvents();

      exec(findSpec(geoSpecs, 'geosearchstore'), [
        'dst',
        'src',
        'FROMLONLAT',
        '15',
        '37',
        'BYRADIUS',
        '200',
        'km',
      ]);
      expectEvent('geosearchstore', 'dst');
    });
  });

  // --- Database commands ---

  describe('database commands', () => {
    it('FLUSHDB emits no per-key events (matches Redis)', () => {
      ctx.db.set('k1', 'string', 'raw', 'v');
      ctx.db.set('k2', 'string', 'raw', 'v');
      clearEvents();

      exec(findSpec(databaseSpecs, 'flushdb'), []);
      expectNoEvents();
    });

    it('SWAPDB does not emit keyspace events (matches Redis)', () => {
      exec(findSpec(databaseSpecs, 'swapdb'), ['0', '1']);
      expectNoEvents();
    });
  });

  // --- Sort commands ---

  describe('sort commands', () => {
    it('SORT with STORE emits "sortstore"', () => {
      ctx.db.set('src', 'list', 'listpack', ['3', '1', '2']);
      clearEvents();

      exec(findSpec(sortSpecs, 'sort'), ['src', 'STORE', 'dst']);
      expectEvent('sortstore', 'dst');
    });

    it('SORT without STORE does not emit', () => {
      ctx.db.set('src', 'list', 'listpack', ['3', '1', '2']);
      clearEvents();

      exec(findSpec(sortSpecs, 'sort'), ['src']);
      expectNoEvents();
    });
  });

  // --- Blocking list commands ---

  describe('blocking list commands (non-blocking path)', () => {
    it('BLPOP emits "lpop" when data available', () => {
      ctx.db.set('k', 'list', 'listpack', ['v1', 'v2']);
      clearEvents();

      exec(findSpec(blockingListSpecs, 'blpop'), ['k', '0']);
      expectEvent('lpop', 'k');
    });

    it('BRPOP emits "rpop" when data available', () => {
      ctx.db.set('k', 'list', 'listpack', ['v1', 'v2']);
      clearEvents();

      exec(findSpec(blockingListSpecs, 'brpop'), ['k', '0']);
      expectEvent('rpop', 'k');
    });

    it('BLMOVE emits events when data available', () => {
      ctx.db.set('src', 'list', 'listpack', ['v1', 'v2']);
      clearEvents();

      exec(findSpec(blockingListSpecs, 'blmove'), [
        'src',
        'dst',
        'LEFT',
        'RIGHT',
        '0',
      ]);
      expectEvent('lpop', 'src');
      expectEvent('rpush', 'dst');
    });
  });

  // --- Blocking sorted set commands ---

  describe('blocking sorted set commands (non-blocking path)', () => {
    it('BZPOPMIN emits "zpopmin" when data available', () => {
      exec(findSpec(sortedSetSpecs, 'zadd'), ['k', '1', 'm1']);
      clearEvents();

      exec(findSpec(blockingSortedSetSpecs, 'bzpopmin'), ['k', '0']);
      expectEvent('zpopmin', 'k');
    });

    it('BZPOPMAX emits "zpopmax" when data available', () => {
      exec(findSpec(sortedSetSpecs, 'zadd'), ['k', '1', 'm1']);
      clearEvents();

      exec(findSpec(blockingSortedSetSpecs, 'bzpopmax'), ['k', '0']);
      expectEvent('zpopmax', 'k');
    });
  });
});
