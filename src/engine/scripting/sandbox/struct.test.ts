import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { WasmoonEngine } from '../wasmoon-engine.ts';
import { applySandbox } from './sandbox.ts';

describe('struct library', () => {
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

  it('is available', async () => {
    const result = await engine.execute('return type(struct)');
    expect(result.values).toEqual(['table']);
  });

  it('struct.size returns correct size for format', async () => {
    // "I" = unsigned int (4 bytes), "B" = unsigned byte (1 byte)
    const result = await engine.execute('return struct.size("BB")');
    expect(result.values).toEqual([2]);
  });

  it('round-trips unsigned bytes', async () => {
    const result = await engine.execute(`
      local packed = struct.pack("BB", 65, 66)
      local a, b = struct.unpack("BB", packed)
      return a + b
    `);
    expect(result.values).toEqual([131]);
  });

  it('round-trips unsigned 16-bit integer', async () => {
    const result = await engine.execute(`
      local packed = struct.pack(">H", 1000)
      return struct.unpack(">H", packed)
    `);
    expect(result.values).toEqual([1000]);
  });

  it('round-trips signed 32-bit integer', async () => {
    const result = await engine.execute(`
      local packed = struct.pack(">i", -12345)
      return struct.unpack(">i", packed)
    `);
    expect(result.values).toEqual([-12345]);
  });

  it('round-trips a zero-terminated string', async () => {
    const result = await engine.execute(`
      local packed = struct.pack(">s", "hello")
      return struct.unpack(">s", packed)
    `);
    // The s format packs a zero-terminated string (matching Redis struct library)
    expect(result.values).toEqual(['hello']);
  });

  it('packs and unpacks little-endian', async () => {
    const result = await engine.execute(`
      local packed = struct.pack("<H", 0x0102)
      return string.byte(packed, 1), string.byte(packed, 2)
    `);
    // Little-endian: low byte first
    expect(result.values).toEqual([0x02]);
  });

  it('packs and unpacks big-endian', async () => {
    const result = await engine.execute(`
      local packed = struct.pack(">H", 0x0102)
      return string.byte(packed, 1)
    `);
    // Big-endian: high byte first
    expect(result.values).toEqual([0x01]);
  });

  it('struct.unpack returns offset as last value', async () => {
    const result = await engine.execute(`
      local packed = struct.pack("BB", 1, 2)
      local a, b, pos = struct.unpack("BB", packed)
      return pos
    `);
    // After reading 2 bytes, position should be 3 (1-based)
    expect(result.values).toEqual([3]);
  });

  it('round-trips float', async () => {
    const result = await engine.execute(`
      local packed = struct.pack(">f", 3.14)
      local val = struct.unpack(">f", packed)
      return math.floor(val * 100)
    `);
    expect(result.values).toEqual([314]);
  });

  it('round-trips double', async () => {
    const result = await engine.execute(`
      local packed = struct.pack(">d", 3.14159265358979)
      local val = struct.unpack(">d", packed)
      return math.floor(val * 1000000)
    `);
    expect(result.values).toEqual([3141592]);
  });
});
