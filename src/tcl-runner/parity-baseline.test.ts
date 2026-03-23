import { describe, it, expect } from 'vitest';
import {
  checkBaseline,
  formatBaselineResult,
  type ParityBaseline,
} from './parity-baseline.ts';
import type { TclTestSummary } from './tcl-output-parser.ts';

function makeSummary(
  passed: number,
  failed: number,
  skipped = 0
): TclTestSummary {
  return {
    results: [
      {
        name: 'unit/test',
        passed,
        failed,
        skipped,
        errors: [],
        duration: 1.0,
      },
    ],
    totalPassed: passed,
    totalFailed: failed,
    totalSkipped: skipped,
    allPassed: failed === 0,
  };
}

describe('checkBaseline', () => {
  it('passes when rate meets minimum', () => {
    const baseline: ParityBaseline = { minPassRate: 80 };
    const summary = makeSummary(9, 1);
    const result = checkBaseline(summary, baseline);
    expect(result.passed).toBe(true);
    expect(result.actualRate).toBeCloseTo(90);
    expect(result.requiredRate).toBe(80);
  });

  it('passes when rate equals minimum exactly', () => {
    const baseline: ParityBaseline = { minPassRate: 80 };
    const summary = makeSummary(8, 2);
    const result = checkBaseline(summary, baseline);
    expect(result.passed).toBe(true);
    expect(result.actualRate).toBeCloseTo(80);
  });

  it('fails when rate is below minimum', () => {
    const baseline: ParityBaseline = { minPassRate: 80 };
    const summary = makeSummary(7, 3);
    const result = checkBaseline(summary, baseline);
    expect(result.passed).toBe(false);
    expect(result.actualRate).toBeCloseTo(70);
  });

  it('passes with 100% pass rate', () => {
    const baseline: ParityBaseline = { minPassRate: 50 };
    const summary = makeSummary(10, 0);
    const result = checkBaseline(summary, baseline);
    expect(result.passed).toBe(true);
    expect(result.actualRate).toBe(100);
  });

  it('passes with zero tests (vacuously true)', () => {
    const baseline: ParityBaseline = { minPassRate: 80 };
    const summary = makeSummary(0, 0);
    const result = checkBaseline(summary, baseline);
    expect(result.passed).toBe(true);
    expect(result.actualRate).toBe(100);
  });

  it('handles 0% minimum pass rate', () => {
    const baseline: ParityBaseline = { minPassRate: 0 };
    const summary = makeSummary(0, 10);
    const result = checkBaseline(summary, baseline);
    expect(result.passed).toBe(true);
    expect(result.actualRate).toBe(0);
  });

  it('includes totalPassed and totalFailed in result', () => {
    const baseline: ParityBaseline = { minPassRate: 50 };
    const summary = makeSummary(7, 3);
    const result = checkBaseline(summary, baseline);
    expect(result.totalPassed).toBe(7);
    expect(result.totalFailed).toBe(3);
  });
});

describe('formatBaselineResult', () => {
  it('formats passing result', () => {
    const output = formatBaselineResult({
      passed: true,
      actualRate: 90,
      requiredRate: 80,
      totalPassed: 9,
      totalFailed: 1,
    });
    expect(output).toContain('PASS');
    expect(output).toContain('90.0%');
    expect(output).toContain('80.0%');
    expect(output).toContain('9');
  });

  it('formats failing result', () => {
    const output = formatBaselineResult({
      passed: false,
      actualRate: 70,
      requiredRate: 80,
      totalPassed: 7,
      totalFailed: 3,
    });
    expect(output).toContain('FAIL');
    expect(output).toContain('70.0%');
    expect(output).toContain('80.0%');
  });

  it('formats result with zero tests', () => {
    const output = formatBaselineResult({
      passed: true,
      actualRate: 100,
      requiredRate: 80,
      totalPassed: 0,
      totalFailed: 0,
    });
    expect(output).toContain('PASS');
    expect(output).toContain('100.0%');
  });
});
