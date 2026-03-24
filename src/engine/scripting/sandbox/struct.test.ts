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

  // ---- all format specifiers ----

  describe('all format specifiers', () => {
    it('round-trips signed byte (b)', async () => {
      const result = await engine.execute(`
        local packed = struct.pack("b", -42)
        return struct.unpack("b", packed)
      `);
      expect(result.values).toEqual([-42]);
    });

    it('round-trips unsigned byte (B) at max (255)', async () => {
      const result = await engine.execute(`
        local packed = struct.pack("B", 255)
        return struct.unpack("B", packed)
      `);
      expect(result.values).toEqual([255]);
    });

    it('round-trips signed 16-bit (h)', async () => {
      const result = await engine.execute(`
        local packed = struct.pack(">h", -1000)
        return struct.unpack(">h", packed)
      `);
      expect(result.values).toEqual([-1000]);
    });

    it('round-trips unsigned 32-bit (I)', async () => {
      const result = await engine.execute(`
        local packed = struct.pack(">I", 3000000000)
        return struct.unpack(">I", packed)
      `);
      expect(result.values).toEqual([3000000000]);
    });

    it('round-trips signed 32-bit via l alias', async () => {
      const result = await engine.execute(`
        local packed = struct.pack(">l", -999999)
        return struct.unpack(">l", packed)
      `);
      expect(result.values).toEqual([-999999]);
    });

    it('round-trips unsigned 32-bit via L alias', async () => {
      const result = await engine.execute(`
        local packed = struct.pack(">L", 4000000000)
        return struct.unpack(">L", packed)
      `);
      expect(result.values).toEqual([4000000000]);
    });
  });

  // ---- integer boundary values ----

  describe('integer boundaries', () => {
    it('round-trips min int8 (-128)', async () => {
      const result = await engine.execute(`
        local packed = struct.pack("b", -128)
        return struct.unpack("b", packed)
      `);
      expect(result.values).toEqual([-128]);
    });

    it('round-trips max int8 (127)', async () => {
      const result = await engine.execute(`
        local packed = struct.pack("b", 127)
        return struct.unpack("b", packed)
      `);
      expect(result.values).toEqual([127]);
    });

    it('round-trips min uint8 (0)', async () => {
      const result = await engine.execute(`
        local packed = struct.pack("B", 0)
        return struct.unpack("B", packed)
      `);
      expect(result.values).toEqual([0]);
    });

    it('round-trips min int16 (-32768)', async () => {
      const result = await engine.execute(`
        local packed = struct.pack(">h", -32768)
        return struct.unpack(">h", packed)
      `);
      expect(result.values).toEqual([-32768]);
    });

    it('round-trips max int16 (32767)', async () => {
      const result = await engine.execute(`
        local packed = struct.pack(">h", 32767)
        return struct.unpack(">h", packed)
      `);
      expect(result.values).toEqual([32767]);
    });

    it('round-trips max uint16 (65535)', async () => {
      const result = await engine.execute(`
        local packed = struct.pack(">H", 65535)
        return struct.unpack(">H", packed)
      `);
      expect(result.values).toEqual([65535]);
    });

    it('round-trips min int32 (-2147483648)', async () => {
      const result = await engine.execute(`
        local packed = struct.pack(">i", -2147483648)
        return struct.unpack(">i", packed)
      `);
      expect(result.values).toEqual([-2147483648]);
    });

    it('round-trips max int32 (2147483647)', async () => {
      const result = await engine.execute(`
        local packed = struct.pack(">i", 2147483647)
        return struct.unpack(">i", packed)
      `);
      expect(result.values).toEqual([2147483647]);
    });
  });

  // ---- endianness combinations ----

  describe('endianness', () => {
    it('native endian (=) works like little-endian', async () => {
      const result = await engine.execute(`
        local packed = struct.pack("=H", 0x0102)
        return string.byte(packed, 1)
      `);
      // = is little-endian, so low byte first
      expect(result.values).toEqual([0x02]);
    });

    it('big-endian int32', async () => {
      const result = await engine.execute(`
        local packed = struct.pack(">i", 0x01020304)
        return string.byte(packed, 1), string.byte(packed, 2), string.byte(packed, 3), string.byte(packed, 4)
      `);
      expect(result.values).toEqual([0x01]);
    });

    it('little-endian int32', async () => {
      const result = await engine.execute(`
        local packed = struct.pack("<i", 0x01020304)
        return string.byte(packed, 1)
      `);
      expect(result.values).toEqual([0x04]);
    });

    it('big-endian float round-trips', async () => {
      const result = await engine.execute(`
        local packed = struct.pack(">f", 1.5)
        return struct.unpack(">f", packed)
      `);
      expect(result.values[0]).toBeCloseTo(1.5);
    });

    it('little-endian float round-trips', async () => {
      const result = await engine.execute(`
        local packed = struct.pack("<f", 1.5)
        return struct.unpack("<f", packed)
      `);
      expect(result.values[0]).toBeCloseTo(1.5);
    });

    it('little-endian double round-trips', async () => {
      const result = await engine.execute(`
        local packed = struct.pack("<d", 2.718281828)
        local val = struct.unpack("<d", packed)
        return math.floor(val * 1000000)
      `);
      expect(result.values).toEqual([2718281]);
    });
  });

  // ---- mixed format and position tracking ----

  describe('mixed formats', () => {
    it('packs and unpacks mixed format ">BHi"', async () => {
      const result = await engine.execute(`
        local packed = struct.pack(">BHi", 42, 1000, -5000)
        local a, b, c = struct.unpack(">BHi", packed)
        return a .. "," .. b .. "," .. c
      `);
      expect(result.values).toEqual(['42,1000,-5000']);
    });

    it('sequential unpack with position continuation', async () => {
      const result = await engine.execute(`
        local packed = struct.pack(">BH", 10, 2000)
        local a, pos1 = struct.unpack(">B", packed)
        local b, pos2 = struct.unpack(">H", packed, pos1)
        return a .. "," .. b .. "," .. pos2
      `);
      expect(result.values).toEqual(['10,2000,4']);
    });

    it('struct.size for mixed format', async () => {
      const result = await engine.execute('return struct.size(">BHid")');
      // B=1 + H=2 + i=4 + d=8 = 15
      expect(result.values).toEqual([15]);
    });

    it('struct.size returns 0 for format with s', async () => {
      const result = await engine.execute('return struct.size(">Bs")');
      expect(result.values).toEqual([0]);
    });
  });

  // ---- string edge cases ----

  describe('string edge cases', () => {
    it('round-trips empty string', async () => {
      const result = await engine.execute(`
        local packed = struct.pack(">s", "")
        return struct.unpack(">s", packed)
      `);
      expect(result.values).toEqual(['']);
    });

    it('round-trips string with spaces', async () => {
      const result = await engine.execute(`
        local packed = struct.pack(">s", "hello world")
        return struct.unpack(">s", packed)
      `);
      expect(result.values).toEqual(['hello world']);
    });

    it('format with spaces is valid', async () => {
      const result = await engine.execute(`
        local packed = struct.pack("> B H", 10, 500)
        local a, b = struct.unpack("> B H", packed)
        return a .. "," .. b
      `);
      expect(result.values).toEqual(['10,500']);
    });
  });
});
