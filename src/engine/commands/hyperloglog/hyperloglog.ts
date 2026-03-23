import type { Database } from '../../database.ts';
import type { Reply, CommandContext } from '../../types.ts';
import {
  integerReply,
  arrayReply,
  bulkReply,
  statusReply,
  errorReply,
  OK,
  ZERO,
  WRONGTYPE_ERR,
} from '../../types.ts';
import type { CommandSpec } from '../../command-table.ts';
import { notify, EVENT_FLAGS } from '../../notify.ts';

import {
  HLL_Q,
  HLL_REGISTERS,
  HLL_SPARSE,
  HLL_DENSE,
  stringToBytes,
  bytesToString,
  createSparseHll,
  createDenseHll,
  isValidHll,
  hllEncoding,
  invalidateCache,
  isCacheValid,
  getCachedCardinality,
  setCachedCardinality,
  denseGetRegister,
  denseSetRegister,
  sparseSet,
  sparseToDense,
  decodeSparse,
} from './encoding.ts';
import { murmurHash64A, hllPatLen } from './hash.ts';
import { hllCount, hllMerge } from './cardinality.ts';

// --- Error constants ---

const HLL_INVALIDOBJ_ERR = errorReply(
  'INVALIDOBJ',
  'Corrupted HLL object detected'
);

// --- HLL PFADD operation ---

function hllAdd(
  bytes: Uint8Array,
  element: string,
  sparseMaxBytes: number
): { bytes: Uint8Array; changed: boolean } {
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

// --- Database helpers ---

function getHll(
  db: Database,
  key: string
): { bytes: Uint8Array | null; error: Reply | null } {
  const entry = db.get(key);
  if (!entry) return { bytes: null, error: null };
  if (entry.type !== 'string') return { bytes: null, error: WRONGTYPE_ERR };
  const bytes = stringToBytes(entry.value as string);
  if (!isValidHll(bytes)) return { bytes: null, error: HLL_INVALIDOBJ_ERR };
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
    // Redis converts sparse to dense in-place before reading registers
    let current = bytes;
    if (hllEncoding(current) === HLL_SPARSE) {
      current = sparseToDense(current);
      saveHll(ctx.db, key, current);
    }
    const replies: Reply[] = new Array(HLL_REGISTERS);
    for (let i = 0; i < HLL_REGISTERS; i++) {
      replies[i] = integerReply(denseGetRegister(current, i));
    }
    return arrayReply(replies);
  }

  if (subcmd === 'ENCODING') {
    return statusReply(hllEncoding(bytes) === HLL_DENSE ? 'dense' : 'sparse');
  }

  if (subcmd === 'TODENSE') {
    if (hllEncoding(bytes) === HLL_SPARSE) {
      const dense = sparseToDense(bytes);
      saveHll(ctx.db, key, dense);
      return integerReply(1);
    }
    return integerReply(0);
  }

  if (subcmd === 'DECODE') {
    if (hllEncoding(bytes) === HLL_DENSE) {
      return errorReply('ERR', 'HLL encoding is not sparse');
    }
    return bulkReply(decodeSparse(bytes));
  }

  return errorReply('ERR', `Unknown PFDEBUG subcommand '${args[0] ?? ''}'`);
}

export function pfselftest(ctx: CommandContext): Reply {
  void ctx;

  // Test 1: Validate HLL headers
  const sparse = createSparseHll();
  if (!isValidHll(sparse)) {
    return errorReply('ERR', 'PFSELFTEST failed: invalid sparse HLL header');
  }

  const dense = createDenseHll();
  if (!isValidHll(dense)) {
    return errorReply('ERR', 'PFSELFTEST failed: invalid dense HLL header');
  }

  // Test 2: Verify sparse-to-dense conversion preserves registers
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

  // Test 3: Verify MurmurHash against known test vectors (seed 0xadc83b19)
  const hashVectors: [string, bigint][] = [
    ['', 0xd8dfea6585bc9732n],
    ['test', 0xff211d0b0982e4e6n],
    ['hello', 0x0f656f01eecfe400n],
    ['Redis', 0x9b40c0c5a9e89bf0n],
  ];
  for (const [input, expected] of hashVectors) {
    const actual = murmurHash64A(stringToBytes(input));
    if (actual !== expected) {
      return errorReply(
        'ERR',
        `PFSELFTEST failed: hash mismatch for '${input}'`
      );
    }
  }

  // Test 4: Verify hash-to-register assignment consistency
  // Ensure hllPatLen returns valid register indices and run lengths
  const patTestInputs = ['a', 'b', 'c', '0', 'test', 'Redis', 'hello'];
  for (const input of patTestInputs) {
    const [index, runLen] = hllPatLen(input);
    if (index < 0 || index >= HLL_REGISTERS) {
      return errorReply(
        'ERR',
        `PFSELFTEST failed: register index ${index} out of range for '${input}'`
      );
    }
    if (runLen < 1 || runLen > HLL_Q + 1) {
      return errorReply(
        'ERR',
        `PFSELFTEST failed: run length ${runLen} out of range for '${input}'`
      );
    }
  }

  // Test 5: Verify cardinality estimation accuracy across ranges
  // Standard error of HLL is 1.04 / sqrt(m) ≈ 0.81% for m=16384
  const stdError = 1.04 / Math.sqrt(HLL_REGISTERS);
  const testRanges = [10, 100, 1000, 10000, 100000];

  for (const n of testRanges) {
    let hllBytes = createSparseHll();

    for (let j = 0; j < n; j++) {
      const elem = `selftest:${j}`;
      const result = hllAdd(hllBytes, elem, 3000);
      hllBytes = result.bytes;
    }

    const estimated = hllCount(hllBytes);
    // Allow 2x standard error, with a minimum of 5 for very small cardinalities
    const maxError = Math.max(n * 2 * stdError, 5);
    if (Math.abs(estimated - n) > maxError) {
      return errorReply(
        'ERR',
        `PFSELFTEST failed: cardinality ${n} estimated as ${estimated} (error ${Math.abs(estimated - n)}, max allowed ${Math.round(maxError)})`
      );
    }
  }

  return OK;
}

// --- Command specs ---

export const specs: CommandSpec[] = [
  {
    name: 'pfadd',
    handler: (ctx, args) => {
      const reply = pfadd(ctx, args);
      if (reply.kind === 'integer') {
        notify(ctx, EVENT_FLAGS.STRING, 'pfadd', args[0] ?? '');
      }
      return reply;
    },
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
    handler: (ctx, args) => {
      const reply = pfmerge(ctx, args);
      if (reply === OK) {
        notify(ctx, EVENT_FLAGS.STRING, 'pfmerge', args[0] ?? '');
      }
      return reply;
    },
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
    flags: ['write', 'denyoom', 'admin'],
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
