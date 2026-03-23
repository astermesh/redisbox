import { describe, it, expect } from 'vitest';
import {
  estimateCardinality,
  hllCount,
  getRegisters,
  hllMerge,
} from './cardinality.ts';
import {
  HLL_REGISTERS,
  createSparseHll,
  createDenseHll,
  denseSetRegister,
  sparseSet,
  invalidateCache,
} from './encoding.ts';

describe('estimateCardinality', () => {
  it('returns 0 for all-zero registers', () => {
    const regs = new Uint8Array(HLL_REGISTERS);
    expect(estimateCardinality(regs)).toBe(0);
  });

  it('returns a positive estimate when some registers are set', () => {
    const regs = new Uint8Array(HLL_REGISTERS);
    regs[0] = 1;
    const result = estimateCardinality(regs);
    expect(result).toBeGreaterThan(0);
  });

  it('estimate increases with more set registers', () => {
    const regs1 = new Uint8Array(HLL_REGISTERS);
    regs1[0] = 1;
    const est1 = estimateCardinality(regs1);

    const regs10 = new Uint8Array(HLL_REGISTERS);
    for (let i = 0; i < 10; i++) regs10[i] = 1;
    const est10 = estimateCardinality(regs10);

    const regs100 = new Uint8Array(HLL_REGISTERS);
    for (let i = 0; i < 100; i++) regs100[i] = 1;
    const est100 = estimateCardinality(regs100);

    expect(est10).toBeGreaterThan(est1);
    expect(est100).toBeGreaterThan(est10);
  });

  it('uses linear counting when estimate is small and zeros exist', () => {
    // With very few registers set (small estimate, many zeros), linear counting kicks in
    const regs = new Uint8Array(HLL_REGISTERS);
    regs[0] = 1;
    const result = estimateCardinality(regs);
    // Linear counting formula: HLL_REGISTERS * ln(HLL_REGISTERS / zeros)
    // With 1 register set, zeros = 16383
    const expected = Math.round(
      HLL_REGISTERS * Math.log(HLL_REGISTERS / (HLL_REGISTERS - 1))
    );
    expect(result).toBe(expected);
  });

  it('higher register values imply higher cardinality', () => {
    // Fill all registers to avoid linear counting (no zeros)
    const regsLow = new Uint8Array(HLL_REGISTERS);
    const regsHigh = new Uint8Array(HLL_REGISTERS);
    regsLow.fill(1);
    regsHigh.fill(10);
    const estLow = estimateCardinality(regsLow);
    const estHigh = estimateCardinality(regsHigh);
    expect(estHigh).toBeGreaterThan(estLow);
  });

  it('returns a finite number for all registers set to max', () => {
    const regs = new Uint8Array(HLL_REGISTERS);
    regs.fill(63); // HLL_REGISTER_MAX
    const result = estimateCardinality(regs);
    expect(Number.isFinite(result)).toBe(true);
    expect(result).toBeGreaterThan(0);
  });

  it('all registers set to 1 gives consistent HLL estimate', () => {
    // When all registers are 1, no zeros, so raw HLL estimate is used
    // HLL_ALPHA * m^2 / sum(2^-v) = HLL_ALPHA * m^2 / (m * 2^-1) = HLL_ALPHA * m * 2
    const regs = new Uint8Array(HLL_REGISTERS);
    regs.fill(1);
    const result = estimateCardinality(regs);
    // alpha * m^2 / (m * 0.5) = alpha * m * 2 ~ 0.7213 * 16384 * 2 ~ 23634
    expect(result).toBeGreaterThan(23000);
    expect(result).toBeLessThan(24000);
  });
});

describe('hllCount', () => {
  it('returns 0 for an empty sparse HLL', () => {
    const hll = createSparseHll();
    expect(hllCount(hll)).toBe(0);
  });

  it('returns 0 for an empty dense HLL', () => {
    const hll = createDenseHll();
    expect(hllCount(hll)).toBe(0);
  });

  it('returns non-zero for a dense HLL with registers set', () => {
    const hll = createDenseHll();
    denseSetRegister(hll, 0, 5);
    denseSetRegister(hll, 100, 3);
    denseSetRegister(hll, 1000, 7);
    invalidateCache(hll);
    const result = hllCount(hll);
    expect(result).toBeGreaterThan(0);
  });

  it('returns non-zero for a sparse HLL with a register set', () => {
    const hll = createSparseHll();
    const result = sparseSet(hll, 42, 5, 3000);
    expect(result).not.toBeNull();
    if (result) {
      const count = hllCount(result.bytes);
      expect(count).toBeGreaterThan(0);
    }
  });

  it('dense and sparse give same count for equivalent data', () => {
    // Build sparse with a few values
    let sparse = createSparseHll();
    const entries: [number, number][] = [
      [10, 3],
      [500, 7],
      [1000, 2],
      [8000, 5],
    ];
    for (const [idx, val] of entries) {
      const r = sparseSet(sparse, idx, val, 10000);
      expect(r).not.toBeNull();
      if (r) sparse = r.bytes;
    }

    // Build equivalent dense
    const dense = createDenseHll();
    for (const [idx, val] of entries) {
      denseSetRegister(dense, idx, val);
    }
    invalidateCache(dense);

    expect(hllCount(sparse)).toBe(hllCount(dense));
  });
});

describe('getRegisters', () => {
  it('returns all zeros for empty sparse HLL', () => {
    const hll = createSparseHll();
    const regs = getRegisters(hll);
    expect(regs.length).toBe(HLL_REGISTERS);
    for (let i = 0; i < HLL_REGISTERS; i++) {
      expect(regs[i]).toBe(0);
    }
  });

  it('returns all zeros for empty dense HLL', () => {
    const hll = createDenseHll();
    const regs = getRegisters(hll);
    expect(regs.length).toBe(HLL_REGISTERS);
    for (let i = 0; i < HLL_REGISTERS; i++) {
      expect(regs[i]).toBe(0);
    }
  });

  it('extracts correct register values from dense HLL', () => {
    const hll = createDenseHll();
    denseSetRegister(hll, 0, 10);
    denseSetRegister(hll, 100, 20);
    denseSetRegister(hll, 16383, 63);
    const regs = getRegisters(hll);
    expect(regs[0]).toBe(10);
    expect(regs[100]).toBe(20);
    expect(regs[16383]).toBe(63);
    expect(regs[1]).toBe(0);
  });

  it('extracts correct register values from sparse HLL', () => {
    let hll = createSparseHll();
    const r = sparseSet(hll, 42, 5, 10000);
    expect(r).not.toBeNull();
    if (r) hll = r.bytes;
    const regs = getRegisters(hll);
    expect(regs[42]).toBe(5);
    expect(regs[0]).toBe(0);
    expect(regs[43]).toBe(0);
  });
});

describe('hllMerge', () => {
  it('merging two empty HLLs produces an empty result', () => {
    const a = createDenseHll();
    const b = createSparseHll();
    const result = hllMerge(a, b);
    const regs = getRegisters(result);
    for (let i = 0; i < HLL_REGISTERS; i++) {
      expect(regs[i]).toBe(0);
    }
  });

  it('merging takes max of each register', () => {
    const a = createDenseHll();
    const b = createDenseHll();

    denseSetRegister(a, 0, 5);
    denseSetRegister(a, 1, 10);
    denseSetRegister(a, 2, 3);
    invalidateCache(a);

    denseSetRegister(b, 0, 8);
    denseSetRegister(b, 1, 7);
    denseSetRegister(b, 2, 15);
    invalidateCache(b);

    const result = hllMerge(a, b);
    const regs = getRegisters(result);
    expect(regs[0]).toBe(8); // max(5, 8)
    expect(regs[1]).toBe(10); // max(10, 7)
    expect(regs[2]).toBe(15); // max(3, 15)
  });

  it('merge result is always dense', () => {
    const a = createSparseHll();
    const b = createSparseHll();
    const result = hllMerge(a, b);
    // Dense HLL has a specific size
    expect(result.length).toBe(16 + (HLL_REGISTERS * 6) / 8); // HLL_DENSE_SIZE
  });

  it('merge is commutative', () => {
    const a = createDenseHll();
    const b = createDenseHll();
    denseSetRegister(a, 100, 5);
    denseSetRegister(a, 200, 10);
    denseSetRegister(b, 100, 8);
    denseSetRegister(b, 300, 12);
    invalidateCache(a);
    invalidateCache(b);

    const ab = hllMerge(a, b);
    const ba = hllMerge(b, a);
    const regsAB = getRegisters(ab);
    const regsBA = getRegisters(ba);
    for (let i = 0; i < HLL_REGISTERS; i++) {
      expect(regsAB[i]).toBe(regsBA[i]);
    }
  });

  it('merging with self is idempotent', () => {
    const hll = createDenseHll();
    denseSetRegister(hll, 50, 7);
    denseSetRegister(hll, 150, 12);
    invalidateCache(hll);

    const result = hllMerge(hll, hll);
    const regsOrig = getRegisters(hll);
    const regsMerged = getRegisters(result);
    for (let i = 0; i < HLL_REGISTERS; i++) {
      expect(regsMerged[i]).toBe(regsOrig[i]);
    }
  });

  it('merge preserves registers only in target when source is empty', () => {
    const a = createDenseHll();
    denseSetRegister(a, 500, 9);
    invalidateCache(a);
    const b = createDenseHll();

    const result = hllMerge(a, b);
    const regs = getRegisters(result);
    expect(regs[500]).toBe(9);
  });

  it('merge preserves registers only in source when target is empty', () => {
    const a = createDenseHll();
    const b = createDenseHll();
    denseSetRegister(b, 500, 9);
    invalidateCache(b);

    const result = hllMerge(a, b);
    const regs = getRegisters(result);
    expect(regs[500]).toBe(9);
  });

  it('merge can combine sparse and dense HLLs', () => {
    let sparse = createSparseHll();
    const r = sparseSet(sparse, 42, 5, 10000);
    expect(r).not.toBeNull();
    if (r) sparse = r.bytes;

    const dense = createDenseHll();
    denseSetRegister(dense, 42, 3);
    denseSetRegister(dense, 100, 8);
    invalidateCache(dense);

    const result = hllMerge(sparse, dense);
    const regs = getRegisters(result);
    expect(regs[42]).toBe(5); // max(5, 3)
    expect(regs[100]).toBe(8);
  });

  it('merged cardinality is at least as large as either input', () => {
    const a = createDenseHll();
    const b = createDenseHll();
    // Set different registers
    for (let i = 0; i < 50; i++) {
      denseSetRegister(a, i, 3);
    }
    for (let i = 50; i < 100; i++) {
      denseSetRegister(b, i, 3);
    }
    invalidateCache(a);
    invalidateCache(b);

    const countA = hllCount(a);
    const countB = hllCount(b);
    const merged = hllMerge(a, b);
    const countMerged = hllCount(merged);
    expect(countMerged).toBeGreaterThanOrEqual(countA);
    expect(countMerged).toBeGreaterThanOrEqual(countB);
  });
});
