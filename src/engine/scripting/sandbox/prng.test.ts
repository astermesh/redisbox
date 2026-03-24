import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { WasmoonEngine } from '../wasmoon-engine.ts';
import { applySandbox } from './sandbox.ts';

describe('PRNG (redisLrand48)', () => {
  let engine: WasmoonEngine;

  beforeEach(async () => {
    engine = await WasmoonEngine.create();
    await applySandbox(engine);
  });

  afterEach(() => {
    if (!engine.closed) {
      engine.close();
    }
  });

  it('math.random returns a number', async () => {
    const result = await engine.execute('return type(math.random())');
    expect(result.values).toEqual(['number']);
  });

  it('math.random() returns value in [0,1)', async () => {
    const result = await engine.execute(`
      local ok = true
      for i = 1, 100 do
        local v = math.random()
        if v < 0 or v >= 1 then ok = false end
      end
      return ok
    `);
    expect(result.values).toEqual([true]);
  });

  it('math.random(n) returns value in [1,n]', async () => {
    const result = await engine.execute(`
      local ok = true
      for i = 1, 100 do
        local v = math.random(10)
        if v < 1 or v > 10 then ok = false end
      end
      return ok
    `);
    expect(result.values).toEqual([true]);
  });

  it('math.random(m,n) returns value in [m,n]', async () => {
    const result = await engine.execute(`
      local ok = true
      for i = 1, 100 do
        local v = math.random(5, 15)
        if v < 5 or v > 15 then ok = false end
      end
      return ok
    `);
    expect(result.values).toEqual([true]);
  });

  it('math.randomseed resets PRNG state', async () => {
    const result = await engine.execute(`
      math.randomseed(42)
      local a = math.random()
      math.randomseed(42)
      local b = math.random()
      return a == b
    `);
    expect(result.values).toEqual([true]);
  });

  it('produces deterministic sequence with default state', async () => {
    // Two separate engine instances with same init should produce same sequence
    const engine2 = await WasmoonEngine.create();
    await applySandbox(engine2);

    try {
      const result1 = await engine.execute(`
        math.randomseed(0)
        local vals = {}
        for i = 1, 5 do vals[i] = math.random(1000000) end
        return vals[1] .. "," .. vals[2] .. "," .. vals[3] .. "," .. vals[4] .. "," .. vals[5]
      `);
      const result2 = await engine2.execute(`
        math.randomseed(0)
        local vals = {}
        for i = 1, 5 do vals[i] = math.random(1000000) end
        return vals[1] .. "," .. vals[2] .. "," .. vals[3] .. "," .. vals[4] .. "," .. vals[5]
      `);
      expect(result1.values).toEqual(result2.values);
    } finally {
      engine2.close();
    }
  });

  it('produces Redis-compatible sequence with seed 0', async () => {
    // These values are verified against real Redis 7.x
    // redis> EVAL "math.randomseed(0); return math.random(1000000)" 0
    // The redisLrand48 with seed=0 has initial state {0x330E, 0x0000, 0x0000}
    // First call to lrand48 advances state and returns upper 31 bits
    const result = await engine.execute(`
      math.randomseed(0)
      local v1 = math.random(1000000)
      local v2 = math.random(1000000)
      local v3 = math.random(1000000)
      return v1 .. "," .. v2 .. "," .. v3
    `);
    // Verify against known Redis output
    // With seed 0: state = 0x00000000330E
    // After 1st advance: state = (0x5DEECE66D * 0x330E + 0xB) & 0xFFFFFFFFFFFF
    // = 0x130E94CC8B2 -> lrand48 result = state >> 17 = 0x130E94CC8B2 >> 17
    // Redis math.random(n) = 1 + floor(lrand48_result * n / (2^31))
    expect(result.values[0]).toBe('170829,749902,96372');
  });

  it('produces Redis-compatible sequence with default per-EVAL state', async () => {
    // Redis calls redisSrand48(0) before every EVAL for deterministic replication.
    // applySandbox initializes with srand48(0), so the default state matches.
    // This test verifies the sequence WITHOUT any explicit randomseed call.
    const result = await engine.execute(`
      local v1 = math.random(1000000)
      local v2 = math.random(1000000)
      local v3 = math.random(1000000)
      return v1 .. "," .. v2 .. "," .. v3
    `);
    // Same as seed=0 — matches Redis EVAL behavior
    expect(result.values[0]).toBe('170829,749902,96372');
  });

  // ---- argument validation ----

  describe('argument validation', () => {
    it('math.random(1) always returns 1', async () => {
      const result = await engine.execute(`
        local ok = true
        for i = 1, 50 do
          if math.random(1) ~= 1 then ok = false end
        end
        return ok
      `);
      expect(result.values).toEqual([true]);
    });

    it('math.random(n,n) always returns n', async () => {
      const result = await engine.execute(`
        local ok = true
        for i = 1, 50 do
          if math.random(7, 7) ~= 7 then ok = false end
        end
        return ok
      `);
      expect(result.values).toEqual([true]);
    });

    it('math.random(0) errors', async () => {
      await expect(engine.execute('return math.random(0)')).rejects.toThrow(
        /invalid argument/
      );
    });

    it('math.random(-1) errors', async () => {
      await expect(engine.execute('return math.random(-1)')).rejects.toThrow(
        /invalid argument/
      );
    });

    it('math.random(m,n) with m > n errors', async () => {
      await expect(engine.execute('return math.random(10, 5)')).rejects.toThrow(
        /invalid argument/
      );
    });
  });

  // ---- seed edge cases ----

  describe('seed edge cases', () => {
    it('different seeds produce different sequences', async () => {
      const result = await engine.execute(`
        math.randomseed(1)
        local a = math.random(1000000)
        math.randomseed(2)
        local b = math.random(1000000)
        return a ~= b
      `);
      expect(result.values).toEqual([true]);
    });

    it('negative seed is accepted', async () => {
      const result = await engine.execute(`
        math.randomseed(-1)
        local a = math.random(1000000)
        math.randomseed(-1)
        local b = math.random(1000000)
        return a == b
      `);
      expect(result.values).toEqual([true]);
    });

    it('large seed is accepted', async () => {
      const result = await engine.execute(`
        math.randomseed(2147483647)
        local a = math.random(1000000)
        math.randomseed(2147483647)
        local b = math.random(1000000)
        return a == b
      `);
      expect(result.values).toEqual([true]);
    });

    it('seed 42 produces deterministic sequence', async () => {
      const result = await engine.execute(`
        math.randomseed(42)
        local vals = {}
        for i = 1, 5 do vals[i] = math.random(1000000) end
        return vals[1] .. "," .. vals[2] .. "," .. vals[3] .. "," .. vals[4] .. "," .. vals[5]
      `);
      // Run twice to verify determinism
      const result2 = await engine.execute(`
        math.randomseed(42)
        local vals = {}
        for i = 1, 5 do vals[i] = math.random(1000000) end
        return vals[1] .. "," .. vals[2] .. "," .. vals[3] .. "," .. vals[4] .. "," .. vals[5]
      `);
      expect(result.values).toEqual(result2.values);
    });
  });

  // ---- math.random range coverage ----

  describe('range coverage', () => {
    it('math.random(2) produces both 1 and 2 over many calls', async () => {
      const result = await engine.execute(`
        local seen1, seen2 = false, false
        for i = 1, 200 do
          local v = math.random(2)
          if v == 1 then seen1 = true end
          if v == 2 then seen2 = true end
        end
        return seen1 and seen2
      `);
      expect(result.values).toEqual([true]);
    });

    it('math.random() returns integers for math.random(n)', async () => {
      const result = await engine.execute(`
        local ok = true
        for i = 1, 100 do
          local v = math.random(100)
          if v ~= math.floor(v) then ok = false end
        end
        return ok
      `);
      expect(result.values).toEqual([true]);
    });
  });
});
