import type { Database } from '../database.ts';
import type { Reply, CommandContext } from '../types.ts';
import {
  integerReply,
  arrayReply,
  bulkReply,
  errorReply,
  OK,
  ZERO,
  WRONGTYPE_ERR,
} from '../types.ts';
import type { CommandSpec } from '../command-table.ts';

// --- Constants ---

const HLL_P = 14; // register index bits
const HLL_Q = 50; // value bits (64 - P)
const HLL_REGISTERS = 1 << HLL_P; // 16384
const HLL_P_MASK = HLL_REGISTERS - 1; // 0x3FFF
const HLL_BITS = 6; // bits per register
const HLL_REGISTER_MAX = (1 << HLL_BITS) - 1; // 63
const HLL_HDR_SIZE = 16;
const HLL_DENSE_SIZE = HLL_HDR_SIZE + (HLL_REGISTERS * HLL_BITS) / 8; // 12304

const HLL_DENSE = 0;
const HLL_SPARSE = 1;

const HLL_MAGIC_0 = 0x48; // 'H'
const HLL_MAGIC_1 = 0x59; // 'Y'
const HLL_MAGIC_2 = 0x4c; // 'L'
const HLL_MAGIC_3 = 0x4c; // 'L'

// Sparse opcodes
const HLL_SPARSE_ZERO_MAX = 64;
const HLL_SPARSE_XZERO_MAX = 16384;
const HLL_SPARSE_VAL_MAX_VALUE = 32;
const HLL_SPARSE_VAL_MAX_LEN = 4;

// MurmurHash64A seed (matches Redis)
const MURMURHASH_SEED = 0xadc83b19n;

// Alpha constant for 16384 registers
const HLL_ALPHA = 0.7213 / (1 + 1.079 / HLL_REGISTERS);

// --- Error constants ---

const HLL_WRONGTYPE_ERR = errorReply(
  'WRONGTYPE',
  'Key is not a valid HyperLogLog string value.'
);

// --- Binary string helpers (Latin-1) ---

function stringToBytes(s: string): Uint8Array {
  const bytes = new Uint8Array(s.length);
  for (let i = 0; i < s.length; i++) {
    bytes[i] = s.charCodeAt(i) & 0xff;
  }
  return bytes;
}

function bytesToString(bytes: Uint8Array): string {
  const chunks: string[] = [];
  const chunkSize = 8192;
  for (let i = 0; i < bytes.length; i += chunkSize) {
    const end = Math.min(i + chunkSize, bytes.length);
    const slice = bytes.subarray(i, end);
    chunks.push(String.fromCharCode(...slice));
  }
  return chunks.join('');
}

/** Safe byte read — returns 0 for out-of-bounds. */
function b(arr: Uint8Array, idx: number): number {
  return idx < arr.length ? (arr[idx] ?? 0) : 0;
}

/** Safe BigInt byte read for hash computation. */
function bB(arr: Uint8Array, idx: number): bigint {
  return BigInt(b(arr, idx));
}

// --- MurmurHash64A (Redis-compatible) ---

function murmurHash64A(data: Uint8Array): bigint {
  const m = 0xc6a4a7935bd1e995n;
  const r = 47n;
  const len = data.length;
  let h = (MURMURHASH_SEED ^ (BigInt(len) * m)) & 0xffffffffffffffffn;

  const nblocks = Math.floor(len / 8);
  for (let i = 0; i < nblocks; i++) {
    const off = i * 8;
    let k =
      bB(data, off) |
      (bB(data, off + 1) << 8n) |
      (bB(data, off + 2) << 16n) |
      (bB(data, off + 3) << 24n) |
      (bB(data, off + 4) << 32n) |
      (bB(data, off + 5) << 40n) |
      (bB(data, off + 6) << 48n) |
      (bB(data, off + 7) << 56n);

    k = (k * m) & 0xffffffffffffffffn;
    k ^= k >> r;
    k = (k * m) & 0xffffffffffffffffn;

    h ^= k;
    h = (h * m) & 0xffffffffffffffffn;
  }

  const tail = nblocks * 8;
  const remaining = len & 7;
  /* eslint-disable no-fallthrough */
  switch (remaining) {
    case 7:
      h ^= bB(data, tail + 6) << 48n;
    case 6:
      h ^= bB(data, tail + 5) << 40n;
    case 5:
      h ^= bB(data, tail + 4) << 32n;
    case 4:
      h ^= bB(data, tail + 3) << 24n;
    case 3:
      h ^= bB(data, tail + 2) << 16n;
    case 2:
      h ^= bB(data, tail + 1) << 8n;
    case 1:
      h ^= bB(data, tail);
      h = (h * m) & 0xffffffffffffffffn;
  }
  /* eslint-enable no-fallthrough */

  h ^= h >> r;
  h = (h * m) & 0xffffffffffffffffn;
  h ^= h >> r;

  return h;
}

/** Hash an element and return [registerIndex, runLength]. */
function hllPatLen(element: string): [number, number] {
  const data = new TextEncoder().encode(element);
  const hash = murmurHash64A(data);

  const index = Number(hash & BigInt(HLL_P_MASK));
  let bits = hash >> BigInt(HLL_P);
  // Set sentinel bit at position HLL_Q to guarantee termination
  bits |= 1n << BigInt(HLL_Q);

  let count = 1;
  while ((bits & 1n) === 0n) {
    count++;
    bits >>= 1n;
  }
  return [index, count];
}

// --- HLL Header Operations ---

function isValidHll(bytes: Uint8Array): boolean {
  if (bytes.length < HLL_HDR_SIZE) return false;
  return (
    b(bytes, 0) === HLL_MAGIC_0 &&
    b(bytes, 1) === HLL_MAGIC_1 &&
    b(bytes, 2) === HLL_MAGIC_2 &&
    b(bytes, 3) === HLL_MAGIC_3 &&
    (b(bytes, 4) === HLL_DENSE || b(bytes, 4) === HLL_SPARSE)
  );
}

function hllEncoding(bytes: Uint8Array): number {
  return b(bytes, 4);
}

function createSparseHll(): Uint8Array {
  // Header + initial XZERO opcode covering all 16384 registers
  const bytes = new Uint8Array(HLL_HDR_SIZE + 2);
  bytes[0] = HLL_MAGIC_0;
  bytes[1] = HLL_MAGIC_1;
  bytes[2] = HLL_MAGIC_2;
  bytes[3] = HLL_MAGIC_3;
  bytes[4] = HLL_SPARSE;
  // Invalidate cache
  bytes[15] = 0x80;
  // XZERO opcode: 01xxxxxx yyyyyyyy where count = ((x<<8)|y) + 1 = 16384
  // count-1 = 16383 = 0x3FFF -> x = 0x3F, y = 0xFF
  bytes[HLL_HDR_SIZE] = 0x40 | 0x3f;
  bytes[HLL_HDR_SIZE + 1] = 0xff;
  return bytes;
}

function createDenseHll(): Uint8Array {
  const bytes = new Uint8Array(HLL_DENSE_SIZE);
  bytes[0] = HLL_MAGIC_0;
  bytes[1] = HLL_MAGIC_1;
  bytes[2] = HLL_MAGIC_2;
  bytes[3] = HLL_MAGIC_3;
  bytes[4] = HLL_DENSE;
  // Invalidate cache
  bytes[15] = 0x80;
  return bytes;
}

function invalidateCache(bytes: Uint8Array): void {
  bytes[15] = (b(bytes, 15) | 0x80) & 0xff;
}

function isCacheValid(bytes: Uint8Array): boolean {
  return (b(bytes, 15) & 0x80) === 0;
}

function getCachedCardinality(bytes: Uint8Array): number {
  // Read 8 bytes little-endian int64 from offset 8
  let val = 0;
  for (let i = 0; i < 7; i++) {
    val |= b(bytes, 8 + i) << (i * 8);
  }
  // Byte 15 has MSB as validity flag, so only use low 7 bits
  val |= (b(bytes, 15) & 0x7f) << 56;
  return val;
}

function setCachedCardinality(bytes: Uint8Array, card: number): void {
  for (let i = 0; i < 7; i++) {
    bytes[8 + i] = (card >> (i * 8)) & 0xff;
  }
  // Byte 15: low 7 bits of the top byte, MSB = 0 (valid)
  bytes[15] = (card >> 56) & 0x7f;
}

// --- Dense register operations ---

function denseGetRegister(bytes: Uint8Array, index: number): number {
  const bitOffset = index * HLL_BITS;
  const byteOffset = HLL_HDR_SIZE + (bitOffset >> 3);
  const bitPos = bitOffset & 7;

  // Read 2 bytes to handle register crossing byte boundary
  const b0 = b(bytes, byteOffset);
  const b1 = b(bytes, byteOffset + 1);
  const word = b0 | (b1 << 8);
  return (word >> bitPos) & HLL_REGISTER_MAX;
}

function denseSetRegister(bytes: Uint8Array, index: number, val: number): void {
  const bitOffset = index * HLL_BITS;
  const byteOffset = HLL_HDR_SIZE + (bitOffset >> 3);
  const bitPos = bitOffset & 7;

  const b0 = b(bytes, byteOffset);
  const b1 = b(bytes, byteOffset + 1);
  let word = b0 | (b1 << 8);
  word &= ~(HLL_REGISTER_MAX << bitPos);
  word |= (val & HLL_REGISTER_MAX) << bitPos;
  bytes[byteOffset] = word & 0xff;
  bytes[byteOffset + 1] = (word >> 8) & 0xff;
}

// --- Sparse representation operations ---

// Opcode type detection
function isSparseZero(byte: number): boolean {
  return (byte & 0xc0) === 0x00;
}

function isSparseXzero(byte: number): boolean {
  return (byte & 0xc0) === 0x40;
}

// Opcode field extraction
function sparseZeroLen(byte: number): number {
  return (byte & 0x3f) + 1;
}

function sparseXzeroLen(b0: number, b1: number): number {
  return (((b0 & 0x3f) << 8) | b1) + 1;
}

function sparseValValue(byte: number): number {
  return ((byte >> 2) & 0x1f) + 1;
}

function sparseValLen(byte: number): number {
  return (byte & 0x03) + 1;
}

// Opcode encoding
function encodeZero(len: number): number {
  return (len - 1) & 0x3f;
}

function encodeXzero(len: number): [number, number] {
  const val = len - 1;
  return [0x40 | ((val >> 8) & 0x3f), val & 0xff];
}

function encodeVal(value: number, len: number): number {
  return 0x80 | (((value - 1) & 0x1f) << 2) | ((len - 1) & 0x03);
}

/** Set a register in sparse representation. Returns updated bytes or null if need to promote to dense. */
function sparseSet(
  bytes: Uint8Array,
  index: number,
  value: number,
  sparseMaxBytes: number
): { bytes: Uint8Array; changed: boolean } | null {
  let pos = HLL_HDR_SIZE;
  let regIdx = 0;

  while (pos < bytes.length) {
    const opcode = b(bytes, pos);
    let span: number;
    let curVal: number;

    if (isSparseZero(opcode)) {
      span = sparseZeroLen(opcode);
      curVal = 0;
    } else if (isSparseXzero(opcode)) {
      span = sparseXzeroLen(opcode, b(bytes, pos + 1));
      curVal = 0;
    } else {
      span = sparseValLen(opcode);
      curVal = sparseValValue(opcode);
    }

    if (index >= regIdx && index < regIdx + span) {
      if (value <= curVal) {
        return { bytes, changed: false };
      }

      const before = index - regIdx;
      const after = span - before - 1;
      const newOpcodes: number[] = [];

      emitRun(newOpcodes, curVal, before);
      if (value > HLL_SPARSE_VAL_MAX_VALUE) {
        return null; // promote to dense
      }
      newOpcodes.push(encodeVal(value, 1));
      emitRun(newOpcodes, curVal, after);

      const opcodeStart = pos;
      const opcodeEnd = isSparseXzero(opcode) ? pos + 2 : pos + 1;

      const result = spliceOpcodes(bytes, opcodeStart, opcodeEnd, newOpcodes);

      if (result.length - HLL_HDR_SIZE > sparseMaxBytes) {
        return null;
      }

      const merged = mergeAdjacentOpcodes(result);
      if (merged.length - HLL_HDR_SIZE > sparseMaxBytes) {
        return null;
      }

      invalidateCache(merged);
      return { bytes: merged, changed: true };
    }

    regIdx += span;
    pos += isSparseXzero(opcode) ? 2 : 1;
  }

  return { bytes, changed: false };
}

function emitRun(out: number[], value: number, count: number): void {
  if (count === 0) return;

  if (value === 0) {
    let remaining = count;
    while (remaining > 0) {
      if (remaining <= HLL_SPARSE_ZERO_MAX) {
        out.push(encodeZero(remaining));
        remaining = 0;
      } else {
        const n = Math.min(remaining, HLL_SPARSE_XZERO_MAX);
        const [b0, b1] = encodeXzero(n);
        out.push(b0, b1);
        remaining -= n;
      }
    }
  } else {
    let remaining = count;
    while (remaining > 0) {
      const n = Math.min(remaining, HLL_SPARSE_VAL_MAX_LEN);
      out.push(encodeVal(value, n));
      remaining -= n;
    }
  }
}

function spliceOpcodes(
  bytes: Uint8Array,
  start: number,
  end: number,
  newOpcodes: number[]
): Uint8Array {
  const before = bytes.subarray(0, start);
  const after = bytes.subarray(end);
  const result = new Uint8Array(
    before.length + newOpcodes.length + after.length
  );
  result.set(before);
  for (let i = 0; i < newOpcodes.length; i++) {
    result[before.length + i] = newOpcodes[i] ?? 0;
  }
  result.set(after, before.length + newOpcodes.length);
  return result;
}

function mergeAdjacentOpcodes(bytes: Uint8Array): Uint8Array {
  const regs = sparseToRegisters(bytes);
  return registersToSparse(regs, bytes);
}

function sparseToRegisters(bytes: Uint8Array): Uint8Array {
  const regs = new Uint8Array(HLL_REGISTERS);
  let pos = HLL_HDR_SIZE;
  let regIdx = 0;

  while (pos < bytes.length && regIdx < HLL_REGISTERS) {
    const opcode = b(bytes, pos);

    if (isSparseZero(opcode)) {
      regIdx += sparseZeroLen(opcode);
      pos += 1;
    } else if (isSparseXzero(opcode)) {
      regIdx += sparseXzeroLen(opcode, b(bytes, pos + 1));
      pos += 2;
    } else {
      const val = sparseValValue(opcode);
      const span = sparseValLen(opcode);
      for (let i = 0; i < span && regIdx + i < HLL_REGISTERS; i++) {
        regs[regIdx + i] = val;
      }
      regIdx += span;
      pos += 1;
    }
  }

  return regs;
}

function registersToSparse(regs: Uint8Array, hdr: Uint8Array): Uint8Array {
  const opcodes: number[] = [];
  let i = 0;

  while (i < HLL_REGISTERS) {
    const val = regs[i] ?? 0;

    if (val === 0) {
      let count = 0;
      while (i + count < HLL_REGISTERS && regs[i + count] === 0) {
        count++;
      }
      let remaining = count;
      while (remaining > 0) {
        if (remaining <= HLL_SPARSE_ZERO_MAX) {
          opcodes.push(encodeZero(remaining));
          remaining = 0;
        } else {
          const n = Math.min(remaining, HLL_SPARSE_XZERO_MAX);
          const [b0, b1] = encodeXzero(n);
          opcodes.push(b0, b1);
          remaining -= n;
        }
      }
      i += count;
    } else {
      let count = 0;
      while (i + count < HLL_REGISTERS && regs[i + count] === val) {
        count++;
      }
      let remaining = count;
      while (remaining > 0) {
        const n = Math.min(remaining, HLL_SPARSE_VAL_MAX_LEN);
        opcodes.push(encodeVal(val, n));
        remaining -= n;
      }
      i += count;
    }
  }

  const result = new Uint8Array(HLL_HDR_SIZE + opcodes.length);
  result.set(hdr.subarray(0, HLL_HDR_SIZE));
  for (let j = 0; j < opcodes.length; j++) {
    result[HLL_HDR_SIZE + j] = opcodes[j] ?? 0;
  }
  return result;
}

function sparseToDense(bytes: Uint8Array): Uint8Array {
  const regs = sparseToRegisters(bytes);
  const dense = createDenseHll();
  for (let i = 0; i < HLL_REGISTERS; i++) {
    const val = regs[i] ?? 0;
    if (val > 0) {
      denseSetRegister(dense, i, val);
    }
  }
  invalidateCache(dense);
  return dense;
}

// --- Cardinality estimation ---

function hllCount(bytes: Uint8Array): number {
  let regs: Uint8Array;

  if (hllEncoding(bytes) === HLL_DENSE) {
    regs = new Uint8Array(HLL_REGISTERS);
    for (let i = 0; i < HLL_REGISTERS; i++) {
      regs[i] = denseGetRegister(bytes, i);
    }
  } else {
    regs = sparseToRegisters(bytes);
  }

  return estimateCardinality(regs);
}

function estimateCardinality(regs: Uint8Array): number {
  let sum = 0;
  let zeros = 0;

  for (let i = 0; i < HLL_REGISTERS; i++) {
    const val = regs[i] ?? 0;
    sum += Math.pow(2, -val);
    if (val === 0) zeros++;
  }

  const estimate = HLL_ALPHA * HLL_REGISTERS * HLL_REGISTERS * (1 / sum);

  if (estimate <= HLL_REGISTERS * 2.5 && zeros > 0) {
    return Math.round(HLL_REGISTERS * Math.log(HLL_REGISTERS / zeros));
  }

  return Math.round(estimate);
}

// --- HLL PFADD operation ---

function hllAdd(
  bytes: Uint8Array,
  element: string,
  sparseMaxBytes: number
): { bytes: Uint8Array; changed: boolean } | null {
  const [index, count] = hllPatLen(element);

  if (hllEncoding(bytes) === HLL_SPARSE) {
    const result = sparseSet(bytes, index, count, sparseMaxBytes);
    if (result === null) {
      const dense = sparseToDense(bytes);
      return hllAddDense(dense, index, count);
    }
    return result;
  }

  return hllAddDense(bytes, index, count);
}

function hllAddDense(
  bytes: Uint8Array,
  index: number,
  count: number
): { bytes: Uint8Array; changed: boolean } {
  const oldVal = denseGetRegister(bytes, index);
  if (count > oldVal) {
    denseSetRegister(bytes, index, count);
    invalidateCache(bytes);
    return { bytes, changed: true };
  }
  return { bytes, changed: false };
}

// --- HLL Merge ---

function getRegisters(bytes: Uint8Array): Uint8Array {
  if (hllEncoding(bytes) === HLL_DENSE) {
    const regs = new Uint8Array(HLL_REGISTERS);
    for (let i = 0; i < HLL_REGISTERS; i++) {
      regs[i] = denseGetRegister(bytes, i);
    }
    return regs;
  }
  return sparseToRegisters(bytes);
}

function hllMerge(target: Uint8Array, source: Uint8Array): Uint8Array {
  const targetRegs = getRegisters(target);
  const sourceRegs = getRegisters(source);

  for (let i = 0; i < HLL_REGISTERS; i++) {
    const sv = sourceRegs[i] ?? 0;
    const tv = targetRegs[i] ?? 0;
    if (sv > tv) {
      targetRegs[i] = sv;
    }
  }

  const result = createDenseHll();
  for (let i = 0; i < HLL_REGISTERS; i++) {
    const val = targetRegs[i] ?? 0;
    if (val > 0) {
      denseSetRegister(result, i, val);
    }
  }
  invalidateCache(result);
  return result;
}

// --- Database helpers ---

function getHll(
  db: Database,
  key: string
): { bytes: Uint8Array | null; error: Reply | null } {
  const entry = db.get(key);
  if (!entry) return { bytes: null, error: null };
  if (entry.type !== 'string') return { bytes: null, error: WRONGTYPE_ERR };
  const bytes = stringToBytes(entry.value as string);
  if (!isValidHll(bytes)) return { bytes: null, error: HLL_WRONGTYPE_ERR };
  return { bytes, error: null };
}

function saveHll(db: Database, key: string, bytes: Uint8Array): void {
  db.set(key, 'string', 'raw', bytesToString(bytes));
}

function getSparseMaxBytes(ctx: CommandContext): number {
  if (ctx.config) {
    const result = ctx.config.get('hll-sparse-max-bytes');
    if (result[1]) return parseInt(result[1], 10);
  }
  return 3000;
}

// --- Command implementations ---

export function pfadd(ctx: CommandContext, args: string[]): Reply {
  const key = args[0] ?? '';
  const elements = args.slice(1);
  const sparseMaxBytes = getSparseMaxBytes(ctx);

  const { bytes: existing, error } = getHll(ctx.db, key);
  if (error) return error;

  let hllBytes = existing ?? createSparseHll();
  let anyChanged = false;

  if (!existing) {
    if (elements.length === 0) {
      saveHll(ctx.db, key, hllBytes);
      return integerReply(1);
    }
    anyChanged = true;
  }

  for (const elem of elements) {
    const result = hllAdd(hllBytes, elem, sparseMaxBytes);
    if (result === null) {
      continue;
    }
    hllBytes = result.bytes;
    if (result.changed) anyChanged = true;
  }

  if (anyChanged) {
    invalidateCache(hllBytes);
    saveHll(ctx.db, key, hllBytes);
  }

  return integerReply(anyChanged ? 1 : 0);
}

export function pfcount(ctx: CommandContext, args: string[]): Reply {
  if (args.length === 1) {
    const key = args[0] ?? '';
    const { bytes, error } = getHll(ctx.db, key);
    if (error) return error;
    if (!bytes) return ZERO;

    if (isCacheValid(bytes)) {
      return integerReply(getCachedCardinality(bytes));
    }

    const card = hllCount(bytes);
    setCachedCardinality(bytes, card);
    saveHll(ctx.db, key, bytes);
    return integerReply(card);
  }

  // Multiple keys — temporary merge
  let merged: Uint8Array | null = null;

  for (const key of args) {
    const { bytes, error } = getHll(ctx.db, key);
    if (error) return error;
    if (!bytes) continue;

    if (!merged) {
      merged = new Uint8Array(bytes.length);
      merged.set(bytes);
      if (hllEncoding(merged) === HLL_SPARSE) {
        merged = sparseToDense(merged);
      }
    } else {
      merged = hllMerge(merged, bytes);
    }
  }

  if (!merged) return ZERO;
  return integerReply(hllCount(merged));
}

export function pfmerge(ctx: CommandContext, args: string[]): Reply {
  const destKey = args[0] ?? '';
  const sourceKeys = args.slice(1);

  const { bytes: destBytes, error: destError } = getHll(ctx.db, destKey);
  if (destError) return destError;

  let merged: Uint8Array | null = null;

  if (destBytes) {
    if (hllEncoding(destBytes) === HLL_SPARSE) {
      merged = sparseToDense(destBytes);
    } else {
      merged = new Uint8Array(destBytes.length);
      merged.set(destBytes);
    }
  }

  for (const key of sourceKeys) {
    if (key === destKey && merged) continue;
    const { bytes, error } = getHll(ctx.db, key);
    if (error) return error;
    if (!bytes) continue;

    if (!merged) {
      if (hllEncoding(bytes) === HLL_SPARSE) {
        merged = sparseToDense(bytes);
      } else {
        merged = new Uint8Array(bytes.length);
        merged.set(bytes);
      }
    } else {
      merged = hllMerge(merged, bytes);
    }
  }

  if (!merged) {
    merged = createSparseHll();
  }

  invalidateCache(merged);
  saveHll(ctx.db, destKey, merged);
  return OK;
}

export function pfdebug(ctx: CommandContext, args: string[]): Reply {
  const subcmd = (args[0] ?? '').toUpperCase();
  const key = args[1] ?? '';

  const { bytes, error } = getHll(ctx.db, key);
  if (error) return error;
  if (!bytes) {
    return errorReply('ERR', 'The specified key does not exist');
  }

  if (subcmd === 'GETREG') {
    const regs = getRegisters(bytes);
    const replies: Reply[] = new Array(HLL_REGISTERS);
    for (let i = 0; i < HLL_REGISTERS; i++) {
      replies[i] = integerReply(regs[i] ?? 0);
    }
    return arrayReply(replies);
  }

  if (subcmd === 'DECODE') {
    if (hllEncoding(bytes) === HLL_DENSE) {
      return bulkReply('dense');
    }
    const parts: string[] = [];
    let pos = HLL_HDR_SIZE;
    while (pos < bytes.length) {
      const opcode = b(bytes, pos);
      if (isSparseZero(opcode)) {
        parts.push(`z:${sparseZeroLen(opcode)}`);
        pos += 1;
      } else if (isSparseXzero(opcode)) {
        parts.push(`Z:${sparseXzeroLen(opcode, b(bytes, pos + 1))}`);
        pos += 2;
      } else {
        parts.push(`v:${sparseValValue(opcode)},${sparseValLen(opcode)}`);
        pos += 1;
      }
    }
    return bulkReply(parts.join(' '));
  }

  return errorReply('ERR', `Unknown PFDEBUG subcommand '${args[0] ?? ''}'`);
}

export function pfselftest(ctx: CommandContext): Reply {
  void ctx;
  const sparse = createSparseHll();
  if (!isValidHll(sparse)) {
    return errorReply('ERR', 'PFSELFTEST failed: invalid sparse HLL header');
  }

  const dense = createDenseHll();
  if (!isValidHll(dense)) {
    return errorReply('ERR', 'PFSELFTEST failed: invalid dense HLL header');
  }

  // Verify sparse-to-dense conversion preserves registers
  let testHll = createSparseHll();
  const testResult = sparseSet(testHll, 0, 5, 3000);
  if (testResult && testResult.changed) {
    testHll = testResult.bytes;
    const denseConverted = sparseToDense(testHll);
    if (denseGetRegister(denseConverted, 0) !== 5) {
      return errorReply(
        'ERR',
        'PFSELFTEST failed: sparse-to-dense register mismatch'
      );
    }
  }

  // Verify MurmurHash produces consistent results
  const h1 = murmurHash64A(new TextEncoder().encode('test'));
  const h2 = murmurHash64A(new TextEncoder().encode('test'));
  if (h1 !== h2) {
    return errorReply('ERR', 'PFSELFTEST failed: hash inconsistency');
  }

  return OK;
}

// --- Command specs ---

export const specs: CommandSpec[] = [
  {
    name: 'pfadd',
    handler: (ctx, args) => pfadd(ctx, args),
    arity: -2,
    flags: ['write', 'denyoom', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@hyperloglog', '@fast'],
  },
  {
    name: 'pfcount',
    handler: (ctx, args) => pfcount(ctx, args),
    arity: -2,
    flags: ['readonly'],
    firstKey: 1,
    lastKey: -1,
    keyStep: 1,
    categories: ['@read', '@hyperloglog'],
  },
  {
    name: 'pfmerge',
    handler: (ctx, args) => pfmerge(ctx, args),
    arity: -2,
    flags: ['write', 'denyoom'],
    firstKey: 1,
    lastKey: -1,
    keyStep: 1,
    categories: ['@write', '@hyperloglog'],
  },
  {
    name: 'pfdebug',
    handler: (ctx, args) => pfdebug(ctx, args),
    arity: 3,
    flags: ['admin'],
    firstKey: 2,
    lastKey: 2,
    keyStep: 1,
    categories: ['@admin', '@hyperloglog'],
  },
  {
    name: 'pfselftest',
    handler: (ctx) => pfselftest(ctx),
    arity: 1,
    flags: ['admin'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@admin', '@hyperloglog'],
  },
];
