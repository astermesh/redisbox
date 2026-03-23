import { describe, it, expect } from 'vitest';
import {
  categorizeFailure,
  passRate,
  formatReport,
  type FailureCategory,
} from './report.ts';
import type { TclTestResult, TclTestSummary } from './tcl-output-parser.ts';

function makeResult(overrides: Partial<TclTestResult> = {}): TclTestResult {
  return {
    name: 'unit/test',
    passed: 5,
    skipped: 0,
    failed: 0,
    errors: [],
    duration: 1.0,
    ...overrides,
  };
}

function makeSummary(results: TclTestResult[]): TclTestSummary {
  return {
    results,
    totalPassed: results.reduce((s, r) => s + r.passed, 0),
    totalFailed: results.reduce((s, r) => s + r.failed, 0),
    totalSkipped: results.reduce((s, r) => s + r.skipped, 0),
    allPassed: results.every((r) => r.failed === 0),
  };
}

describe('categorizeFailure', () => {
  it('categorizes unknown command errors as missing-command', () => {
    const result = makeResult({
      failed: 1,
      errors: ['ERR unknown command `CLUSTER`'],
    });
    const cf = categorizeFailure(result);
    expect(cf.category).toBe<FailureCategory>('missing-command');
  });

  it('categorizes unknown subcommand errors as missing-command', () => {
    const result = makeResult({
      failed: 1,
      errors: ['ERR unknown subcommand `RESETSTAT`'],
    });
    const cf = categorizeFailure(result);
    expect(cf.category).toBe<FailureCategory>('missing-command');
  });

  it('categorizes connection errors as test-infra', () => {
    const result = makeResult({
      failed: 1,
      errors: ['Connection refused'],
    });
    const cf = categorizeFailure(result);
    expect(cf.category).toBe<FailureCategory>('test-infra');
  });

  it('categorizes timeout errors as test-infra', () => {
    const result = makeResult({
      failed: 1,
      errors: ['Timeout waiting for response'],
    });
    const cf = categorizeFailure(result);
    expect(cf.category).toBe<FailureCategory>('test-infra');
  });

  it('categorizes other errors as behavioral-difference', () => {
    const result = makeResult({
      failed: 1,
      errors: ['Expected OK but got ERR wrong type'],
    });
    const cf = categorizeFailure(result);
    expect(cf.category).toBe<FailureCategory>('behavioral-difference');
  });

  it('preserves test name and errors', () => {
    const result = makeResult({
      name: 'unit/auth',
      failed: 1,
      errors: ['AUTH failed'],
    });
    const cf = categorizeFailure(result);
    expect(cf.testName).toBe('unit/auth');
    expect(cf.errors).toEqual(['AUTH failed']);
  });
});

describe('passRate', () => {
  it('returns 100 when all tests pass', () => {
    const summary = makeSummary([makeResult({ passed: 10 })]);
    expect(passRate(summary)).toBe(100);
  });

  it('returns 0 when all tests fail', () => {
    const summary = makeSummary([makeResult({ passed: 0, failed: 5 })]);
    expect(passRate(summary)).toBe(0);
  });

  it('computes percentage correctly', () => {
    const summary = makeSummary([makeResult({ passed: 8, failed: 2 })]);
    expect(passRate(summary)).toBe(80);
  });

  it('returns 100 for empty results', () => {
    const summary = makeSummary([]);
    expect(passRate(summary)).toBe(100);
  });

  it('ignores skipped tests in rate calculation', () => {
    const summary = makeSummary([
      makeResult({ passed: 5, skipped: 3, failed: 0 }),
    ]);
    expect(passRate(summary)).toBe(100);
  });
});

describe('formatReport', () => {
  it('includes pass rate in output', () => {
    const summary = makeSummary([makeResult({ passed: 8, failed: 2 })]);
    const report = formatReport(summary);
    expect(report).toContain('80.0%');
    expect(report).toContain('8/10');
  });

  it('lists passed tests', () => {
    const summary = makeSummary([makeResult({ name: 'unit/auth', passed: 5 })]);
    const report = formatReport(summary);
    expect(report).toContain('unit/auth');
    expect(report).toContain('5 tests');
  });

  it('lists failed tests with errors', () => {
    const summary = makeSummary([
      makeResult({
        name: 'unit/basic',
        passed: 3,
        failed: 1,
        errors: ['PING returns wrong value'],
      }),
    ]);
    const report = formatReport(summary);
    expect(report).toContain('unit/basic');
    expect(report).toContain('PING returns wrong value');
    expect(report).toContain('Failed Tests');
  });

  it('categorizes failures in the report', () => {
    const summary = makeSummary([
      makeResult({
        name: 'unit/cluster',
        failed: 1,
        errors: ['ERR unknown command `CLUSTER`'],
      }),
      makeResult({
        name: 'unit/auth',
        failed: 1,
        errors: ['Expected OK but got error'],
      }),
    ]);
    const report = formatReport(summary);
    expect(report).toContain('Missing Command');
    expect(report).toContain('Behavioral Difference');
  });

  it('shows skipped count for tests with skips', () => {
    const summary = makeSummary([
      makeResult({ name: 'unit/wait', passed: 2, skipped: 3 }),
    ]);
    const report = formatReport(summary);
    expect(report).toContain('3 skipped');
  });

  it('handles empty summary', () => {
    const summary = makeSummary([]);
    const report = formatReport(summary);
    expect(report).toContain('100.0%');
    expect(report).toContain('0/0');
  });
});
