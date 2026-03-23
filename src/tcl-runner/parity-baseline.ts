/**
 * Parity baseline enforcement.
 *
 * Compares the actual TCL test pass rate against an established
 * minimum baseline, failing CI when the parity rate regresses.
 */

import type { TclTestSummary } from './tcl-output-parser.ts';
import { passRate } from './report.ts';

export interface ParityBaseline {
  /** Minimum acceptable pass rate (0–100) */
  minPassRate: number;
}

export interface BaselineResult {
  /** Whether the actual rate meets the baseline */
  passed: boolean;
  /** Actual pass rate percentage */
  actualRate: number;
  /** Required minimum pass rate */
  requiredRate: number;
  /** Total passed test count */
  totalPassed: number;
  /** Total failed test count */
  totalFailed: number;
}

/**
 * Check whether a test summary meets the parity baseline.
 */
export function checkBaseline(
  summary: TclTestSummary,
  baseline: ParityBaseline
): BaselineResult {
  const actualRate = passRate(summary);
  return {
    passed: actualRate >= baseline.minPassRate,
    actualRate,
    requiredRate: baseline.minPassRate,
    totalPassed: summary.totalPassed,
    totalFailed: summary.totalFailed,
  };
}

/**
 * Format a baseline check result as a human-readable string for CI output.
 */
export function formatBaselineResult(result: BaselineResult): string {
  const status = result.passed ? 'PASS' : 'FAIL';
  const lines = [
    `Parity baseline check: ${status}`,
    `  Actual pass rate:   ${result.actualRate.toFixed(1)}% (${result.totalPassed} passed, ${result.totalFailed} failed)`,
    `  Required minimum:   ${result.requiredRate.toFixed(1)}%`,
  ];
  if (!result.passed) {
    const drop = result.requiredRate - result.actualRate;
    lines.push(
      `  Regression:         ${drop.toFixed(1)} percentage points below baseline`
    );
  }
  return lines.join('\n');
}
