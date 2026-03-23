/**
 * Lua sandbox and library shims for Redis-compatible scripting.
 *
 * Applies:
 * - Sandbox: removes dangerous globals (os, io, debug, require, loadfile, dofile, print, etc.)
 * - Read-only _G: prevents global variable creation via metatable
 * - bit library: LuaBitOp-compatible bitwise operations (pure Lua)
 * - cjson library: JSON encode (pure Lua) / decode (JS bridge with tagged encoding)
 * - cmsgpack library: MessagePack pack/unpack via JS bridge with tagged encoding
 * - struct library: C-struct packing via JS bridge with tagged encoding
 * - PRNG: replaces math.random/math.randomseed with Redis-compatible redisLrand48
 */

import type { LuaEngine } from './lua-engine.ts';

// ---- redisLrand48 PRNG (matches Redis rand.c) ----

const LRAND48_A = 0x5deece66dn;
const LRAND48_C = 0xbn;
const LRAND48_M = 1n << 48n;

/** 48-bit PRNG state */
let prngState = 0n;

/** REDIS_LRAND48_MAX = INT32_MAX = 2^31 - 1 */
const LRAND48_MAX = 2147483647;

/**
 * Set PRNG state from a seed value, matching Redis srand48 behavior.
 * Xi = {0x330E, seed_low16, seed_high16}
 */
function srand48(seed: number): void {
  const s = seed & 0xffffffff;
  const lo = s & 0xffff;
  const hi = (s >>> 16) & 0xffff;
  prngState = (BigInt(hi) << 32n) | (BigInt(lo) << 16n) | 0x330en;
}

/**
 * Advance PRNG and return upper 31 bits (matching Redis lrand48).
 */
function lrand48(): number {
  prngState = (LRAND48_A * prngState + LRAND48_C) % LRAND48_M;
  return Number(prngState >> 17n);
}

/**
 * Reset PRNG to Redis per-EVAL state: srand48(0).
 * Redis calls redisSrand48(0) before every EVAL for deterministic replication.
 */
function resetPrng(): void {
  srand48(0);
}

// ---- Tagged encoding for JS→Lua table transfer ----
// Uses the same pattern as redis-bridge.ts to avoid wasmoon's
// 0-indexed array proxy issues.

const TAG_ARRAY = 1;
const TAG_MAP = 2;
const TAG_NULL = 3;

function jsToTagged(value: unknown): unknown {
  if (value === null || value === undefined) {
    return { t: TAG_NULL };
  }
  if (
    typeof value === 'boolean' ||
    typeof value === 'number' ||
    typeof value === 'string'
  ) {
    return value;
  }
  if (Array.isArray(value)) {
    const obj: Record<string, unknown> = { t: TAG_ARRAY, n: value.length };
    for (let i = 0; i < value.length; i++) {
      obj[String(i)] = jsToTagged(value[i]);
    }
    return obj;
  }
  if (typeof value === 'object') {
    const entries = Object.entries(value as Record<string, unknown>);
    const obj: Record<string, unknown> = { t: TAG_MAP, n: entries.length };
    for (let i = 0; i < entries.length; i++) {
      const entry = entries[i];
      if (entry) {
        obj['k' + i] = entry[0];
        obj['v' + i] = jsToTagged(entry[1]);
      }
    }
    return obj;
  }
  return { t: TAG_NULL };
}

// ---- cmsgpack JS helpers ----

function msgpackEncode(value: unknown): number[] {
  const buf: number[] = [];
  writeValue(buf, value);
  return buf;
}

function writeValue(buf: number[], value: unknown): void {
  if (value === null || value === undefined) {
    buf.push(0xc0);
    return;
  }
  if (typeof value === 'boolean') {
    buf.push(value ? 0xc3 : 0xc2);
    return;
  }
  if (typeof value === 'number') {
    if (Number.isInteger(value)) {
      writeInteger(buf, value);
    } else {
      writeFloat64(buf, value);
    }
    return;
  }
  if (typeof value === 'string') {
    writeString(buf, value);
    return;
  }
  if (Array.isArray(value)) {
    writeArray(buf, value);
    return;
  }
  if (typeof value === 'object') {
    writeMap(buf, value as Record<string, unknown>);
    return;
  }
  buf.push(0xc0);
}

function writeInteger(buf: number[], n: number): void {
  if (n >= 0) {
    if (n <= 0x7f) {
      buf.push(n);
    } else if (n <= 0xff) {
      buf.push(0xcc, n);
    } else if (n <= 0xffff) {
      buf.push(0xcd, (n >> 8) & 0xff, n & 0xff);
    } else if (n <= 0xffffffff) {
      buf.push(
        0xce,
        (n >>> 24) & 0xff,
        (n >>> 16) & 0xff,
        (n >>> 8) & 0xff,
        n & 0xff
      );
    } else {
      writeFloat64(buf, n);
    }
  } else {
    if (n >= -32) {
      buf.push(n & 0xff);
    } else if (n >= -128) {
      buf.push(0xd0, n & 0xff);
    } else if (n >= -32768) {
      buf.push(0xd1, (n >> 8) & 0xff, n & 0xff);
    } else if (n >= -2147483648) {
      buf.push(
        0xd2,
        (n >> 24) & 0xff,
        (n >> 16) & 0xff,
        (n >> 8) & 0xff,
        n & 0xff
      );
    } else {
      writeFloat64(buf, n);
    }
  }
}

function writeFloat64(buf: number[], n: number): void {
  buf.push(0xcb);
  const view = new DataView(new ArrayBuffer(8));
  view.setFloat64(0, n);
  for (let i = 0; i < 8; i++) {
    buf.push(view.getUint8(i));
  }
}

function writeString(buf: number[], s: string): void {
  const encoded = new TextEncoder().encode(s);
  const len = encoded.length;
  if (len <= 31) {
    buf.push(0xa0 | len);
  } else if (len <= 0xff) {
    buf.push(0xd9, len);
  } else if (len <= 0xffff) {
    buf.push(0xda, (len >> 8) & 0xff, len & 0xff);
  } else {
    buf.push(
      0xdb,
      (len >>> 24) & 0xff,
      (len >>> 16) & 0xff,
      (len >>> 8) & 0xff,
      len & 0xff
    );
  }
  for (const b of encoded) {
    buf.push(b);
  }
}

function writeArray(buf: number[], arr: unknown[]): void {
  const len = arr.length;
  if (len <= 15) {
    buf.push(0x90 | len);
  } else if (len <= 0xffff) {
    buf.push(0xdc, (len >> 8) & 0xff, len & 0xff);
  } else {
    buf.push(
      0xdd,
      (len >>> 24) & 0xff,
      (len >>> 16) & 0xff,
      (len >>> 8) & 0xff,
      len & 0xff
    );
  }
  for (const item of arr) {
    writeValue(buf, item);
  }
}

function writeMap(buf: number[], obj: Record<string, unknown>): void {
  const keys = Object.keys(obj);
  const len = keys.length;
  if (len <= 15) {
    buf.push(0x80 | len);
  } else if (len <= 0xffff) {
    buf.push(0xde, (len >> 8) & 0xff, len & 0xff);
  } else {
    buf.push(
      0xdf,
      (len >>> 24) & 0xff,
      (len >>> 16) & 0xff,
      (len >>> 8) & 0xff,
      len & 0xff
    );
  }
  for (const key of keys) {
    writeValue(buf, key);
    writeValue(buf, obj[key]);
  }
}

// ---- msgpack decode ----

interface DecodeResult {
  value: unknown;
  offset: number;
}

function b(bytes: Uint8Array, i: number): number {
  return bytes[i] ?? 0;
}

function msgpackDecode(bytes: Uint8Array, offset: number): DecodeResult {
  const byte = b(bytes, offset);
  if (byte <= 0x7f) return { value: byte, offset: offset + 1 };
  if ((byte & 0xf0) === 0x80) return readMsgMap(bytes, offset + 1, byte & 0x0f);
  if ((byte & 0xf0) === 0x90) return readMsgArr(bytes, offset + 1, byte & 0x0f);
  if ((byte & 0xe0) === 0xa0) return readMsgStr(bytes, offset + 1, byte & 0x1f);
  if (byte >= 0xe0) return { value: byte - 256, offset: offset + 1 };

  switch (byte) {
    case 0xc0:
      return { value: null, offset: offset + 1 };
    case 0xc2:
      return { value: false, offset: offset + 1 };
    case 0xc3:
      return { value: true, offset: offset + 1 };
    case 0xcc:
      return { value: b(bytes, offset + 1), offset: offset + 2 };
    case 0xcd:
      return {
        value: (b(bytes, offset + 1) << 8) | b(bytes, offset + 2),
        offset: offset + 3,
      };
    case 0xce:
      return {
        value:
          ((b(bytes, offset + 1) << 24) |
            (b(bytes, offset + 2) << 16) |
            (b(bytes, offset + 3) << 8) |
            b(bytes, offset + 4)) >>>
          0,
        offset: offset + 5,
      };
    case 0xd0: {
      let v = b(bytes, offset + 1);
      if (v >= 128) v -= 256;
      return { value: v, offset: offset + 2 };
    }
    case 0xd1: {
      let v = (b(bytes, offset + 1) << 8) | b(bytes, offset + 2);
      if (v >= 32768) v -= 65536;
      return { value: v, offset: offset + 3 };
    }
    case 0xd2: {
      const v =
        (b(bytes, offset + 1) << 24) |
        (b(bytes, offset + 2) << 16) |
        (b(bytes, offset + 3) << 8) |
        b(bytes, offset + 4);
      return { value: v | 0, offset: offset + 5 };
    }
    case 0xca: {
      const view = new DataView(bytes.buffer, bytes.byteOffset + offset + 1, 4);
      return { value: view.getFloat32(0), offset: offset + 5 };
    }
    case 0xcb: {
      const view = new DataView(bytes.buffer, bytes.byteOffset + offset + 1, 8);
      return { value: view.getFloat64(0), offset: offset + 9 };
    }
    case 0xd9:
      return readMsgStr(bytes, offset + 2, b(bytes, offset + 1));
    case 0xda:
      return readMsgStr(
        bytes,
        offset + 3,
        (b(bytes, offset + 1) << 8) | b(bytes, offset + 2)
      );
    case 0xdb:
      return readMsgStr(
        bytes,
        offset + 5,
        ((b(bytes, offset + 1) << 24) |
          (b(bytes, offset + 2) << 16) |
          (b(bytes, offset + 3) << 8) |
          b(bytes, offset + 4)) >>>
          0
      );
    case 0xdc:
      return readMsgArr(
        bytes,
        offset + 3,
        (b(bytes, offset + 1) << 8) | b(bytes, offset + 2)
      );
    case 0xdd:
      return readMsgArr(
        bytes,
        offset + 5,
        ((b(bytes, offset + 1) << 24) |
          (b(bytes, offset + 2) << 16) |
          (b(bytes, offset + 3) << 8) |
          b(bytes, offset + 4)) >>>
          0
      );
    case 0xde:
      return readMsgMap(
        bytes,
        offset + 3,
        (b(bytes, offset + 1) << 8) | b(bytes, offset + 2)
      );
    case 0xdf:
      return readMsgMap(
        bytes,
        offset + 5,
        ((b(bytes, offset + 1) << 24) |
          (b(bytes, offset + 2) << 16) |
          (b(bytes, offset + 3) << 8) |
          b(bytes, offset + 4)) >>>
          0
      );
    default:
      return { value: null, offset: offset + 1 };
  }
}

function readMsgStr(
  buf: Uint8Array,
  offset: number,
  len: number
): DecodeResult {
  const value = new TextDecoder().decode(buf.slice(offset, offset + len));
  return { value, offset: offset + len };
}

function readMsgArr(
  buf: Uint8Array,
  offset: number,
  len: number
): DecodeResult {
  const arr: unknown[] = [];
  let pos = offset;
  for (let i = 0; i < len; i++) {
    const r = msgpackDecode(buf, pos);
    arr.push(r.value);
    pos = r.offset;
  }
  return { value: arr, offset: pos };
}

function readMsgMap(
  buf: Uint8Array,
  offset: number,
  len: number
): DecodeResult {
  const map: Record<string, unknown> = {};
  let pos = offset;
  for (let i = 0; i < len; i++) {
    const kr = msgpackDecode(buf, pos);
    pos = kr.offset;
    const vr = msgpackDecode(buf, pos);
    pos = vr.offset;
    map[String(kr.value)] = vr.value;
  }
  return { value: map, offset: pos };
}

// ---- struct JS helpers ----

interface FmtSpec {
  type: string;
  size: number;
}

function parseStructFormat(fmt: string): {
  bigEndian: boolean;
  specs: FmtSpec[];
} {
  let bigEndian = true;
  const specs: FmtSpec[] = [];
  let i = 0;
  if (fmt[0] === '>') {
    bigEndian = true;
    i = 1;
  } else if (fmt[0] === '<') {
    bigEndian = false;
    i = 1;
  } else if (fmt[0] === '=') {
    bigEndian = false;
    i = 1;
  }

  while (i < fmt.length) {
    const ch = fmt[i];
    i++;
    switch (ch) {
      case 'b':
        specs.push({ type: 'int8', size: 1 });
        break;
      case 'B':
        specs.push({ type: 'uint8', size: 1 });
        break;
      case 'h':
        specs.push({ type: 'int16', size: 2 });
        break;
      case 'H':
        specs.push({ type: 'uint16', size: 2 });
        break;
      case 'i':
      case 'l':
        specs.push({ type: 'int32', size: 4 });
        break;
      case 'I':
      case 'L':
        specs.push({ type: 'uint32', size: 4 });
        break;
      case 'f':
        specs.push({ type: 'float32', size: 4 });
        break;
      case 'd':
        specs.push({ type: 'float64', size: 8 });
        break;
      case 's':
        specs.push({ type: 'string', size: 0 });
        break;
      case ' ':
        break;
      default:
        break;
    }
  }
  return { bigEndian, specs };
}

function structSize(fmt: string): number {
  const { specs } = parseStructFormat(fmt);
  let size = 0;
  for (const spec of specs) {
    if (spec.type === 'string') {
      // Zero-terminated string has variable size, not computable statically.
      // Redis struct library throws for variable-length formats in size().
      // Return 0 as placeholder — struct.size with 's' is rarely used.
      return 0;
    }
    size += spec.size;
  }
  return size;
}

function structPackHex(fmt: string, ...values: unknown[]): string {
  const { bigEndian, specs } = parseStructFormat(fmt);
  const parts: number[] = [];
  let vi = 0;

  for (const spec of specs) {
    const val = values[vi++];
    if (spec.type === 'string') {
      const str = String(val ?? '');
      const encoded = new TextEncoder().encode(str);
      for (const b of encoded) parts.push(b);
      parts.push(0); // null terminator
      continue;
    }
    const n = Number(val ?? 0);
    const buf = new ArrayBuffer(spec.size);
    const view = new DataView(buf);
    switch (spec.type) {
      case 'int8':
        view.setInt8(0, n);
        break;
      case 'uint8':
        view.setUint8(0, n);
        break;
      case 'int16':
        view.setInt16(0, n, !bigEndian);
        break;
      case 'uint16':
        view.setUint16(0, n, !bigEndian);
        break;
      case 'int32':
        view.setInt32(0, n, !bigEndian);
        break;
      case 'uint32':
        view.setUint32(0, n, !bigEndian);
        break;
      case 'float32':
        view.setFloat32(0, n, !bigEndian);
        break;
      case 'float64':
        view.setFloat64(0, n, !bigEndian);
        break;
    }
    for (let j = 0; j < spec.size; j++) {
      parts.push(view.getUint8(j));
    }
  }
  // Return as hex string to avoid null byte issues in wasmoon string transfer
  return parts.map((b) => b.toString(16).padStart(2, '0')).join('');
}

function structUnpackHex(
  fmt: string,
  hexData: string,
  startPos?: number
): string {
  // Convert hex string back to byte values
  const bytes: number[] = [];
  for (let i = 0; i < hexData.length; i += 2) {
    bytes.push(parseInt(hexData.substring(i, i + 2), 16));
  }

  const { bigEndian, specs } = parseStructFormat(fmt);
  const results: string[] = [];
  let offset = (startPos ?? 1) - 1;

  for (const spec of specs) {
    if (spec.type === 'string') {
      // Zero-terminated: scan for null byte
      let end = offset;
      while (end < bytes.length && (bytes[end] ?? 0) !== 0) {
        end++;
      }
      const len = end - offset;
      const strBytes = new Uint8Array(len);
      for (let i = 0; i < len; i++) {
        strBytes[i] = bytes[offset + i] ?? 0;
      }
      const str = new TextDecoder().decode(strBytes);
      // Escape for Lua string literal
      results.push(
        '"' +
          str
            .replace(/\\/g, '\\\\')
            .replace(/"/g, '\\"')
            .replace(/\n/g, '\\n')
            .replace(/\r/g, '\\r') +
          '"'
      );
      offset = end + 1; // skip past null terminator
      continue;
    }
    const buf = new ArrayBuffer(spec.size);
    const view = new DataView(buf);
    for (let j = 0; j < spec.size; j++) {
      view.setUint8(j, bytes[offset + j] ?? 0);
    }
    offset += spec.size;
    let val: number;
    switch (spec.type) {
      case 'int8':
        val = view.getInt8(0);
        break;
      case 'uint8':
        val = view.getUint8(0);
        break;
      case 'int16':
        val = view.getInt16(0, !bigEndian);
        break;
      case 'uint16':
        val = view.getUint16(0, !bigEndian);
        break;
      case 'int32':
        val = view.getInt32(0, !bigEndian);
        break;
      case 'uint32':
        val = view.getUint32(0, !bigEndian);
        break;
      case 'float32':
        val = view.getFloat32(0, !bigEndian);
        break;
      case 'float64':
        val = view.getFloat64(0, !bigEndian);
        break;
      default:
        val = 0;
    }
    results.push(String(val));
  }
  // Append final position (1-based for Lua)
  results.push(String(offset + 1));
  // Return as comma-separated Lua expression: "return val1,val2,...,pos"
  return 'return ' + results.join(',');
}

// ---- Apply sandbox to engine ----

/**
 * Apply Redis-compatible sandbox to a Lua engine.
 *
 * Must be called after engine creation and before any script execution.
 * Sets up: library shims (bit, cjson, cmsgpack, struct), PRNG override,
 * global restrictions, and sandbox (removal of dangerous globals).
 */
export async function applySandbox(engine: LuaEngine): Promise<void> {
  registerBridgeFunctions(engine);
  resetPrng();
  await engine.execute(LUA_SANDBOX_SETUP);
}

/**
 * Reset the PRNG state to Redis default. Called before each EVAL.
 */
export function resetPrngState(): void {
  resetPrng();
}

function registerBridgeFunctions(engine: LuaEngine): void {
  // PRNG — matches Redis redis_math_random (scripting.c)
  // r = (lrand48() % REDIS_LRAND48_MAX) / (double)REDIS_LRAND48_MAX
  engine.setGlobal('__rb_math_random', (...args: unknown[]) => {
    const r = (lrand48() % LRAND48_MAX) / LRAND48_MAX;
    if (args.length === 0) {
      return r;
    }
    if (args.length === 1) {
      const n = Number(args[0]);
      if (n < 1) throw new Error('invalid argument');
      return Math.floor(r * n) + 1;
    }
    const m = Number(args[0]);
    const n = Number(args[1]);
    if (n < m) throw new Error('invalid argument');
    return Math.floor(r * (n - m + 1)) + m;
  });

  engine.setGlobal('__rb_math_randomseed', (seed: unknown) => {
    srand48(Number(seed ?? 0));
  });

  // cjson.decode: parse JSON in JS, return tagged structure for Lua to decode
  engine.setGlobal('__rb_cjson_decode', (json: unknown) => {
    const parsed = JSON.parse(String(json));
    return jsToTagged(parsed);
  });

  // cmsgpack.pack: Lua sends a serialization descriptor, JS does binary encoding
  // We use a Lua-side function that walks the table and builds a descriptor string
  // Then JS encodes the descriptor to binary msgpack.
  // Actually simpler: Lua converts value to JSON-like string, JS parses and encodes.
  engine.setGlobal('__rb_msgpack_pack_json', (json: unknown) => {
    const parsed = JSON.parse(String(json));
    const bytes = msgpackEncode(parsed);
    return bytes.map((b) => String.fromCharCode(b)).join('');
  });

  // cmsgpack.unpack: JS decodes binary, returns tagged structure
  engine.setGlobal('__rb_msgpack_unpack', (data: unknown) => {
    const str = String(data);
    const bytes = new Uint8Array(str.length);
    for (let i = 0; i < str.length; i++) {
      bytes[i] = str.charCodeAt(i);
    }
    const result = msgpackDecode(bytes, 0);
    return jsToTagged(result.value);
  });

  // struct
  engine.setGlobal('__rb_struct_size', (fmt: unknown) => {
    return structSize(String(fmt));
  });

  engine.setGlobal('__rb_struct_pack', (...args: unknown[]) => {
    const fmt = String(args[0]);
    return structPackHex(fmt, ...args.slice(1));
  });

  engine.setGlobal('__rb_struct_unpack', (...args: unknown[]) => {
    const fmt = String(args[0]);
    const hexData = String(args[1]);
    const pos = args[2] != null ? Number(args[2]) : undefined;
    return structUnpackHex(fmt, hexData, pos);
  });
}

/**
 * Lua code that sets up the sandbox, library shims, and PRNG override.
 *
 * The approach:
 * - bit library: pure Lua implementation
 * - cjson.encode: pure Lua JSON encoder (avoids Lua→JS table transfer issues)
 * - cjson.decode: JS JSON.parse → tagged encoding → Lua decode
 * - cmsgpack.pack: Lua→JSON string→JS msgpack encode (avoids Lua→JS table issues)
 * - cmsgpack.unpack: JS msgpack decode → tagged encoding → Lua decode
 * - struct: individual values to/from JS (no table transfer needed for pack)
 */
const LUA_SANDBOX_SETUP = `
-- Capture and nil-out bridge functions
local rb_math_random = __rb_math_random
local rb_math_randomseed = __rb_math_randomseed
local rb_cjson_decode = __rb_cjson_decode
local rb_msgpack_pack_json = __rb_msgpack_pack_json
local rb_msgpack_unpack = __rb_msgpack_unpack
local rb_struct_size = __rb_struct_size
local rb_struct_pack = __rb_struct_pack
local rb_struct_unpack = __rb_struct_unpack

__rb_math_random = nil
__rb_math_randomseed = nil
__rb_cjson_decode = nil
__rb_msgpack_pack_json = nil
__rb_msgpack_unpack = nil
__rb_struct_size = nil
__rb_struct_pack = nil
__rb_struct_unpack = nil

-- ---- Tagged decoding (shared by cjson.decode, cmsgpack.unpack, struct.unpack) ----

local TAG_ARRAY = 1
local TAG_MAP = 2
local TAG_NULL = 3

local cjson_null_sentinel -- forward decl, set after cjson table creation

local function decode_tagged(raw)
  if type(raw) ~= "table" then return raw end
  local tag = raw.t
  if tag == TAG_NULL then return cjson_null_sentinel end
  if tag == TAG_ARRAY then
    local result = {}
    local n = raw.n
    for i = 0, n - 1 do
      result[i + 1] = decode_tagged(raw[tostring(i)])
    end
    return result
  end
  if tag == TAG_MAP then
    local result = {}
    local n = raw.n
    for i = 0, n - 1 do
      local k = raw["k" .. i]
      local v = decode_tagged(raw["v" .. i])
      result[k] = v
    end
    return result
  end
  return raw
end

-- ---- bit library (LuaBitOp compatible, pure Lua) ----

bit = {}

local function tobit(x)
  x = x % 4294967296
  if x >= 2147483648 then x = x - 4294967296 end
  return x
end
bit.tobit = tobit

bit.tohex = function(x, n)
  n = n or 8
  local upper = false
  if n < 0 then upper = true; n = -n end
  x = x % 4294967296
  local hex = string.format("%0" .. n .. "x", x)
  if #hex > n then hex = hex:sub(-n) end
  if upper then hex = hex:upper() end
  return hex
end

bit.bnot = function(x)
  return tobit(4294967295 - (x % 4294967296))
end

bit.band = function(x, y, ...)
  local r = x % 4294967296
  local b = y % 4294967296
  local result = 0
  local bv = 1
  for _ = 1, 32 do
    if r % 2 == 1 and b % 2 == 1 then result = result + bv end
    r = math.floor(r / 2)
    b = math.floor(b / 2)
    bv = bv * 2
  end
  result = tobit(result)
  if select('#', ...) > 0 then return bit.band(result, ...) end
  return result
end

bit.bor = function(x, y, ...)
  local r = x % 4294967296
  local b = y % 4294967296
  local result = 0
  local bv = 1
  for _ = 1, 32 do
    if r % 2 == 1 or b % 2 == 1 then result = result + bv end
    r = math.floor(r / 2)
    b = math.floor(b / 2)
    bv = bv * 2
  end
  result = tobit(result)
  if select('#', ...) > 0 then return bit.bor(result, ...) end
  return result
end

bit.bxor = function(x, y, ...)
  local r = x % 4294967296
  local b = y % 4294967296
  local result = 0
  local bv = 1
  for _ = 1, 32 do
    if (r % 2) ~= (b % 2) then result = result + bv end
    r = math.floor(r / 2)
    b = math.floor(b / 2)
    bv = bv * 2
  end
  result = tobit(result)
  if select('#', ...) > 0 then return bit.bxor(result, ...) end
  return result
end

bit.lshift = function(x, n)
  n = n % 32
  return tobit((x % 4294967296) * (2 ^ n))
end

bit.rshift = function(x, n)
  n = n % 32
  return tobit(math.floor((x % 4294967296) / (2 ^ n)))
end

bit.arshift = function(x, n)
  n = n % 32
  x = tobit(x)
  if x >= 0 then return tobit(math.floor(x / (2 ^ n))) end
  local shifted = math.floor((x % 4294967296) / (2 ^ n))
  local mask = 4294967296 - (2 ^ (32 - n))
  return tobit(shifted + mask)
end

bit.rol = function(x, n)
  n = n % 32
  x = x % 4294967296
  return tobit((x * (2 ^ n) + math.floor(x / (2 ^ (32 - n)))) % 4294967296)
end

bit.ror = function(x, n)
  n = n % 32
  x = x % 4294967296
  return tobit((math.floor(x / (2 ^ n)) + x * (2 ^ (32 - n))) % 4294967296)
end

bit.bswap = function(x)
  x = x % 4294967296
  local b0 = x % 256
  local b1 = math.floor(x / 256) % 256
  local b2 = math.floor(x / 65536) % 256
  local b3 = math.floor(x / 16777216) % 256
  return tobit(b0 * 16777216 + b1 * 65536 + b2 * 256 + b3)
end

-- ---- cjson library (encode: pure Lua, decode: JS bridge) ----

cjson = {}

local cjson_null = setmetatable({}, {
  __tostring = function() return "null" end
})
cjson.null = cjson_null
cjson_null_sentinel = cjson_null

-- Pure Lua JSON encoder
local encode_value -- forward decl

local function encode_string(s)
  s = s:gsub('\\\\', '\\\\\\\\')
  s = s:gsub('"', '\\\\"')
  s = s:gsub('%c', function(c)
    local b = string.byte(c)
    if b == 8 then return '\\\\b'
    elseif b == 9 then return '\\\\t'
    elseif b == 10 then return '\\\\n'
    elseif b == 12 then return '\\\\f'
    elseif b == 13 then return '\\\\r'
    else return string.format('\\\\u%04x', b)
    end
  end)
  return '"' .. s .. '"'
end

local function is_array(t)
  local max = 0
  local count = 0
  for k, _ in pairs(t) do
    if type(k) ~= "number" then return false end
    if k ~= math.floor(k) then return false end
    if k < 1 then return false end
    if k > max then max = k end
    count = count + 1
  end
  return count > 0 and count == max
end

local function encode_array(t)
  local parts = {}
  for i = 1, #t do
    parts[i] = encode_value(t[i])
  end
  return "[" .. table.concat(parts, ",") .. "]"
end

local function encode_object(t)
  local parts = {}
  for k, v in pairs(t) do
    parts[#parts + 1] = encode_string(tostring(k)) .. ":" .. encode_value(v)
  end
  return "{" .. table.concat(parts, ",") .. "}"
end

encode_value = function(v)
  if v == nil then return "null" end
  if v == cjson_null then return "null" end
  local tv = type(v)
  if tv == "boolean" then return v and "true" or "false" end
  if tv == "number" then
    if v ~= v then return "null" end -- NaN
    if v == math.huge or v == -math.huge then return "null" end
    if v == math.floor(v) and v >= -2147483648 and v <= 2147483647 then
      return string.format("%.0f", v)
    end
    return tostring(v)
  end
  if tv == "string" then return encode_string(v) end
  if tv == "table" then
    if is_array(v) then return encode_array(v) end
    return encode_object(v)
  end
  error("Cannot encode " .. tv .. " to JSON")
end

cjson.encode = function(value)
  return encode_value(value)
end

cjson.decode = function(json)
  local tagged = rb_cjson_decode(json)
  return decode_tagged(tagged)
end

-- ---- cmsgpack library (uses JSON bridge to avoid Lua→JS table issues) ----

cmsgpack = {}

-- For pack: encode value as JSON in Lua, then let JS convert JSON→msgpack binary
-- This avoids passing Lua tables through the wasmoon bridge.
cmsgpack.pack = function(value)
  -- nil needs special handling since encode_value(nil) returns "null"
  -- which JSON.parse turns into null, which msgpack encodes as 0xC0 (correct)
  if value == nil then
    return rb_msgpack_pack_json("null")
  end
  local json = encode_value(value)
  return rb_msgpack_pack_json(json)
end

cmsgpack.unpack = function(data)
  local tagged = rb_msgpack_unpack(data)
  local result = decode_tagged(tagged)
  -- In Redis cmsgpack, nil maps to false (not cjson.null)
  if result == cjson_null then return false end
  return result
end

-- ---- struct library ----

struct = {}

struct.size = function(fmt)
  return rb_struct_size(fmt)
end

-- Helper: convert binary Lua string to hex for JS bridge
local function to_hex(s)
  local hex = {}
  for i = 1, #s do
    hex[i] = string.format("%02x", string.byte(s, i))
  end
  return table.concat(hex)
end

-- Helper: convert hex string from JS bridge to binary Lua string
local function from_hex(h)
  local parts = {}
  for i = 1, #h, 2 do
    parts[#parts + 1] = string.char(tonumber(h:sub(i, i+1), 16))
  end
  return table.concat(parts)
end

struct.pack = function(fmt, ...)
  local hex = rb_struct_pack(fmt, ...)
  return from_hex(hex)
end

struct.unpack = function(fmt, data, pos)
  local hex = to_hex(data)
  local lua_expr = rb_struct_unpack(fmt, hex, pos)
  local f = loadstring(lua_expr)
  return f()
end

-- ---- PRNG override ----

math.random = function(...)
  return rb_math_random(...)
end

math.randomseed = function(seed)
  rb_math_randomseed(seed)
end

-- ---- Sandbox: remove dangerous globals ----

local writable_globals = {
  KEYS = true,
  ARGV = true,
}

loadfile = nil
dofile = nil
print = nil
io = nil
os = nil
package = nil
newproxy = nil
module = nil
setfenv = nil
getfenv = nil
debug = nil
require = nil

-- ---- Read-only _G ----

setmetatable(_G, {
  __newindex = function(t, name, value)
    if writable_globals[name] then
      rawset(t, name, value)
    else
      error("Script attempted to create global variable '" .. tostring(name) .. "'")
    end
  end,
})
`;
