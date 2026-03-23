import {
  HLL_REGISTERS,
  HLL_DENSE,
  hllEncoding,
  denseGetRegister,
  denseSetRegister,
  sparseToRegisters,
  createDenseHll,
  invalidateCache,
} from './encoding.ts';

// Alpha constant for 16384 registers
const HLL_ALPHA = 0.7213 / (1 + 1.079 / HLL_REGISTERS);

// --- Cardinality estimation ---

export function hllCount(bytes: Uint8Array): number {
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

export function estimateCardinality(regs: Uint8Array): number {
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

// --- HLL Merge ---

export function getRegisters(bytes: Uint8Array): Uint8Array {
  if (hllEncoding(bytes) === HLL_DENSE) {
    const regs = new Uint8Array(HLL_REGISTERS);
    for (let i = 0; i < HLL_REGISTERS; i++) {
      regs[i] = denseGetRegister(bytes, i);
    }
    return regs;
  }
  return sparseToRegisters(bytes);
}

export function hllMerge(target: Uint8Array, source: Uint8Array): Uint8Array {
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
