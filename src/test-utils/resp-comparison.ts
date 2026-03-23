/**
 * RESP response comparison utilities for parity testing.
 *
 * Pure functions with no Node.js dependencies — safe for browser use.
 */

import type { RespValue } from '../resp/types.ts';

/**
 * Normalize a RespValue to a plain JS structure for deep comparison.
 */
export function normalizeResp(value: RespValue): unknown {
  switch (value.type) {
    case 'simple':
      return { type: 'simple', value: value.value };
    case 'error':
      return { type: 'error', value: value.value };
    case 'integer':
      return { type: 'integer', value: Number(value.value) };
    case 'bulk':
      return {
        type: 'bulk',
        value: value.value === null ? null : value.value.toString('utf8'),
      };
    case 'array':
      if (value.value === null) {
        return { type: 'array', value: null };
      }
      return {
        type: 'array',
        value: value.value.map(normalizeResp),
      };
  }
}

/**
 * Assert two RESP values are identical.
 */
export function assertRespEqual(
  actual: RespValue,
  expected: RespValue,
  context = ''
): void {
  const a = normalizeResp(actual);
  const e = normalizeResp(expected);
  const prefix = context ? `[${context}] ` : '';

  if (JSON.stringify(a) !== JSON.stringify(e)) {
    throw new Error(
      `${prefix}RESP mismatch:\n` +
        `  RedisBox: ${JSON.stringify(a)}\n` +
        `  Redis:    ${JSON.stringify(e)}`
    );
  }
}

/**
 * Assert two RESP values are identical when treated as unordered
 * collections. Both must be arrays of the same length with the same
 * elements (in any order).
 */
export function assertRespUnordered(
  actual: RespValue,
  expected: RespValue,
  context = ''
): void {
  const prefix = context ? `[${context}] ` : '';

  // Both must be the same type
  if (actual.type !== expected.type) {
    throw new Error(
      `${prefix}type mismatch: ${actual.type} vs ${expected.type}`
    );
  }

  // Non-array types: exact match
  if (actual.type !== 'array' || expected.type !== 'array') {
    assertRespEqual(actual, expected, context);
    return;
  }

  // Null arrays
  if (actual.value === null && expected.value === null) return;
  if (actual.value === null || expected.value === null) {
    throw new Error(`${prefix}one array is null, other is not`);
  }

  // Length check
  if (actual.value.length !== expected.value.length) {
    throw new Error(
      `${prefix}array length mismatch: ${actual.value.length} vs ${expected.value.length}`
    );
  }

  // Sort both and compare
  const sortedActual = actual.value
    .map(normalizeResp)
    .map((v) => JSON.stringify(v))
    .sort();
  const sortedExpected = expected.value
    .map(normalizeResp)
    .map((v) => JSON.stringify(v))
    .sort();

  for (let i = 0; i < sortedActual.length; i++) {
    if (sortedActual[i] !== sortedExpected[i]) {
      throw new Error(
        `${prefix}unordered mismatch at sorted index ${i}:\n` +
          `  RedisBox: ${sortedActual[i]}\n` +
          `  Redis:    ${sortedExpected[i]}`
      );
    }
  }
}

/**
 * Assert two RESP values have the same type and structure, without
 * comparing actual values. For arrays, checks that both are arrays
 * of the same length with elements of matching types.
 */
export function assertRespStructure(
  actual: RespValue,
  expected: RespValue,
  context = ''
): void {
  const prefix = context ? `[${context}] ` : '';

  if (actual.type !== expected.type) {
    throw new Error(
      `${prefix}type mismatch: ${actual.type} vs ${expected.type}`
    );
  }

  if (actual.type === 'array' && expected.type === 'array') {
    if (actual.value === null && expected.value === null) return;
    if (actual.value === null || expected.value === null) {
      throw new Error(`${prefix}one array is null, other is not`);
    }

    if (actual.value.length !== expected.value.length) {
      throw new Error(
        `${prefix}array length mismatch: ${actual.value.length} vs ${expected.value.length}`
      );
    }

    for (let i = 0; i < actual.value.length; i++) {
      const a = actual.value[i];
      const e = expected.value[i];
      if (a && e) {
        assertRespStructure(a, e, `${context}[${i}]`);
      }
    }
  }
}

/**
 * Format a RespValue for human-readable display.
 */
export function formatResp(value: RespValue): string {
  switch (value.type) {
    case 'simple':
      return `+${value.value}`;
    case 'error':
      return `-${value.value}`;
    case 'integer':
      return `:${value.value}`;
    case 'bulk':
      return value.value === null
        ? '(nil)'
        : `"${value.value.toString('utf8')}"`;
    case 'array':
      if (value.value === null) return '(nil array)';
      if (value.value.length === 0) return '(empty array)';
      return `[${value.value.map(formatResp).join(', ')}]`;
  }
}
