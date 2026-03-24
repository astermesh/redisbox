/**
 * Lua sandbox setup — wires JS bridge functions and executes the Lua
 * sandbox/library-shim script that configures the engine for Redis-compatible
 * scripting (bit, cjson, cmsgpack, struct, PRNG, global restrictions).
 */

import type { LuaEngine } from '../lua-engine.ts';
import { lrand48, srand48, resetPrng, LRAND48_MAX } from './prng.ts';
import { jsToTagged } from './tagged-encoding.ts';
import { msgpackEncode, msgpackDecode } from './cmsgpack.ts';
import { structSize, structPackHex, structUnpackHex } from './struct.ts';

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
