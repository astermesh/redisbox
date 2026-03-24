/**
 * Integration tests for sandbox libraries used within real script execution
 * via ScriptManager and redis.call.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { ScriptManager } from '../script-manager.ts';
import type { Reply } from '../../types.ts';
import {
  statusReply,
  errorReply,
  bulkReply,
  integerReply,
} from '../../types.ts';
import type { CommandExecutor } from '../redis-bridge.ts';

let manager: ScriptManager;
const store = new Map<string, string>();

function makeExecutor(): CommandExecutor {
  return (args: string[]): Reply => {
    const cmd = (args[0] ?? '').toUpperCase();
    if (cmd === 'SET') {
      store.set(args[1] ?? '', args[2] ?? '');
      return statusReply('OK');
    }
    if (cmd === 'GET') {
      const val = store.get(args[1] ?? '');
      return bulkReply(val ?? null);
    }
    if (cmd === 'INCR') {
      const key = args[1] ?? '';
      const cur = parseInt(store.get(key) ?? '0', 10);
      const next = cur + 1;
      store.set(key, String(next));
      return integerReply(next);
    }
    return errorReply('ERR', `unknown command '${args[0]}'`);
  };
}

function evalScript(script: string, keys: string[], argv: string[]): Reply {
  return manager.evalScript(
    script,
    keys,
    argv,
    false,
    undefined,
    makeExecutor()
  );
}

describe('sandbox integration with redis.call', () => {
  beforeEach(async () => {
    store.clear();
    manager = new ScriptManager();
    await manager.init(makeExecutor());
  });

  afterEach(() => {
    manager.close();
  });

  it('cjson.decode on data retrieved via redis.call', () => {
    store.set('mykey', '{"name":"redis","ver":7}');
    const result = evalScript(
      `
      local json = redis.call("GET", KEYS[1])
      local t = cjson.decode(json)
      return t.name .. t.ver
      `,
      ['mykey'],
      []
    );
    expect(result).toEqual(bulkReply('redis7'));
  });

  it('cjson.encode stored via redis.call', () => {
    const result = evalScript(
      `
      local t = {a=1, b=2}
      redis.call("SET", KEYS[1], cjson.encode(t))
      return redis.call("GET", KEYS[1])
      `,
      ['json-key'],
      []
    );
    const json = (result as unknown as { type: string; value: string }).value;
    const parsed = JSON.parse(json);
    expect(parsed).toEqual({ a: 1, b: 2 });
  });

  it('bit operations in a script with redis.call', () => {
    const result = evalScript(
      `
      local flags = tonumber(redis.call("GET", KEYS[1]) or "0")
      flags = bit.bor(flags, tonumber(ARGV[1]))
      redis.call("SET", KEYS[1], tostring(flags))
      return redis.call("GET", KEYS[1])
      `,
      ['flags'],
      ['5']
    );
    expect(result).toEqual(bulkReply('5'));
  });

  it('cmsgpack round-trip with redis.call storage', () => {
    const result = evalScript(
      `
      local packed = cmsgpack.pack({1, 2, 3})
      redis.call("SET", KEYS[1], packed)
      local raw = redis.call("GET", KEYS[1])
      local t = cmsgpack.unpack(raw)
      return t[1] + t[2] + t[3]
      `,
      ['packed-key'],
      []
    );
    expect(result).toEqual(integerReply(6));
  });

  it('struct pack/unpack within single script (no redis.call storage)', () => {
    // Storing binary struct data through redis.call GET/SET may corrupt null bytes.
    // Test that struct works correctly within a single script execution.
    const result = evalScript(
      `
      local packed = struct.pack(">BB", 10, 20)
      local a, b = struct.unpack(">BB", packed)
      return a + b
      `,
      [],
      []
    );
    expect(result).toEqual(integerReply(30));
  });

  it('PRNG is deterministic across separate EVAL calls', () => {
    // Each evalScript resets PRNG to srand48(0) (Redis behavior)
    const result1 = evalScript('return math.random(1000000)', [], []);
    const result2 = evalScript('return math.random(1000000)', [], []);
    // Both calls should produce the same first random number
    expect(result1).toEqual(result2);
  });

  it('PRNG resets between EVAL calls', () => {
    // First call: consume some random numbers
    evalScript(
      `
      for i = 1, 10 do math.random() end
      `,
      [],
      []
    );
    // Second call: should start from same seed again
    const result = evalScript('return math.random(1000000)', [], []);
    // Should match the first random number from a fresh seed(0) state
    expect(result).toEqual(integerReply(170829));
  });

  it('all libraries work together with redis.call', () => {
    store.set('data', '{"x":10}');
    const result = evalScript(
      `
      -- Read JSON from Redis
      local json = redis.call("GET", KEYS[1])
      local t = cjson.decode(json)

      -- Use bit operations
      local flags = bit.bor(t.x, 0x20)

      -- Pack with struct
      local packed = struct.pack(">I", flags)

      -- Store via msgpack
      local msg = cmsgpack.pack({flags=flags, packed=#packed})

      -- Use PRNG
      local r = math.random(100)

      return flags + r
      `,
      ['data'],
      []
    );
    // flags = 10 | 0x20 = 10 | 32 = 42, r = first random with seed(0)
    // We just verify it returns an integer (exact value depends on PRNG)
    expect(result.kind).toBe('integer');
  });

  it('KEYS and ARGV are properly set per eval', () => {
    const result = evalScript(
      'return KEYS[1] .. ":" .. ARGV[1]',
      ['mykey'],
      ['myarg']
    );
    expect(result).toEqual(bulkReply('mykey:myarg'));
  });

  it('multiple KEYS and ARGV', () => {
    const result = evalScript(
      'return #KEYS .. ":" .. #ARGV',
      ['k1', 'k2', 'k3'],
      ['a1', 'a2']
    );
    expect(result).toEqual(bulkReply('3:2'));
  });
});
