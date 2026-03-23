// --- Constants ---

export const HLL_P = 14; // register index bits
export const HLL_Q = 50; // value bits (64 - P)
export const HLL_REGISTERS = 1 << HLL_P; // 16384
export const HLL_P_MASK = HLL_REGISTERS - 1; // 0x3FFF
export const HLL_BITS = 6; // bits per register
export const HLL_REGISTER_MAX = (1 << HLL_BITS) - 1; // 63
export const HLL_HDR_SIZE = 16;
export const HLL_DENSE_SIZE = HLL_HDR_SIZE + (HLL_REGISTERS * HLL_BITS) / 8; // 12304

export const HLL_DENSE = 0;
export const HLL_SPARSE = 1;

const HLL_MAGIC_0 = 0x48; // 'H'
const HLL_MAGIC_1 = 0x59; // 'Y'
const HLL_MAGIC_2 = 0x4c; // 'L'
const HLL_MAGIC_3 = 0x4c; // 'L'

// Sparse opcodes
const HLL_SPARSE_ZERO_MAX = 64;
const HLL_SPARSE_XZERO_MAX = 16384;
const HLL_SPARSE_VAL_MAX_VALUE = 32;
const HLL_SPARSE_VAL_MAX_LEN = 4;

// --- Binary string helpers (Latin-1) ---

export function stringToBytes(s: string): Uint8Array {
  const bytes = new Uint8Array(s.length);
  for (let i = 0; i < s.length; i++) {
    bytes[i] = s.charCodeAt(i) & 0xff;
  }
  return bytes;
}

export function bytesToString(bytes: Uint8Array): string {
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
export function b(arr: Uint8Array, idx: number): number {
  return idx < arr.length ? (arr[idx] ?? 0) : 0;
}

/** Safe BigInt byte read for hash computation. */
export function bB(arr: Uint8Array, idx: number): bigint {
  return BigInt(b(arr, idx));
}

// --- HLL Header Operations ---

export function isValidHll(bytes: Uint8Array): boolean {
  if (bytes.length < HLL_HDR_SIZE) return false;
  return (
    b(bytes, 0) === HLL_MAGIC_0 &&
    b(bytes, 1) === HLL_MAGIC_1 &&
    b(bytes, 2) === HLL_MAGIC_2 &&
    b(bytes, 3) === HLL_MAGIC_3 &&
    (b(bytes, 4) === HLL_DENSE || b(bytes, 4) === HLL_SPARSE)
  );
}

export function hllEncoding(bytes: Uint8Array): number {
  return b(bytes, 4);
}

export function createSparseHll(): Uint8Array {
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

export function createDenseHll(): Uint8Array {
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

export function invalidateCache(bytes: Uint8Array): void {
  bytes[15] = (b(bytes, 15) | 0x80) & 0xff;
}

export function isCacheValid(bytes: Uint8Array): boolean {
  return (b(bytes, 15) & 0x80) === 0;
}

export function getCachedCardinality(bytes: Uint8Array): number {
  // Read 8 bytes little-endian int64 from offset 8
  // Use multiplication instead of bitwise shifts to avoid 32-bit truncation
  let val = 0;
  for (let i = 0; i < 7; i++) {
    val += b(bytes, 8 + i) * 2 ** (i * 8);
  }
  // Byte 15 has MSB as validity flag, so only use low 7 bits
  val += (b(bytes, 15) & 0x7f) * 2 ** 56;
  return val;
}

export function setCachedCardinality(bytes: Uint8Array, card: number): void {
  // Use division instead of bitwise shifts to avoid 32-bit truncation
  let remaining = card;
  for (let i = 0; i < 7; i++) {
    bytes[8 + i] = remaining & 0xff;
    remaining = Math.floor(remaining / 256);
  }
  // Byte 15: low 7 bits of the top byte, MSB = 0 (valid)
  bytes[15] = remaining & 0x7f;
}

// --- Dense register operations ---

export function denseGetRegister(bytes: Uint8Array, index: number): number {
  const bitOffset = index * HLL_BITS;
  const byteOffset = HLL_HDR_SIZE + (bitOffset >> 3);
  const bitPos = bitOffset & 7;

  // Read 2 bytes to handle register crossing byte boundary
  const b0 = b(bytes, byteOffset);
  const b1 = b(bytes, byteOffset + 1);
  const word = b0 | (b1 << 8);
  return (word >> bitPos) & HLL_REGISTER_MAX;
}

export function denseSetRegister(
  bytes: Uint8Array,
  index: number,
  val: number
): void {
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

export function isSparseXzero(byte: number): boolean {
  return (byte & 0xc0) === 0x40;
}

// Opcode field extraction
function sparseZeroLen(byte: number): number {
  return (byte & 0x3f) + 1;
}

function sparseXzeroLen(b0: number, b1: number): number {
  return (((b0 & 0x3f) << 8) | b1) + 1;
}

export function sparseValValue(byte: number): number {
  return ((byte >> 2) & 0x1f) + 1;
}

export function sparseValLen(byte: number): number {
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
export function sparseSet(
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

export function emitRun(out: number[], value: number, count: number): void {
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

export function spliceOpcodes(
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

export function mergeAdjacentOpcodes(bytes: Uint8Array): Uint8Array {
  const regs = sparseToRegisters(bytes);
  return registersToSparse(regs, bytes);
}

export function sparseToRegisters(bytes: Uint8Array): Uint8Array {
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

export function registersToSparse(
  regs: Uint8Array,
  hdr: Uint8Array
): Uint8Array {
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

export function sparseToDense(bytes: Uint8Array): Uint8Array {
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

// --- Sparse DECODE (used by PFDEBUG DECODE) ---

export function decodeSparse(bytes: Uint8Array): string {
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
  return parts.join(' ');
}
