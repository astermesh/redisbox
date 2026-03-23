import { describe, it, expect } from 'vitest';
import { parseTclTestOutput, type TclTestResult } from './tcl-output-parser.ts';

describe('parseTclTestOutput', () => {
  it('parses a fully passing run', () => {
    const output = [
      'Cleanup: may take some time... OK',
      '[tls:no,tcp:yes] Testing unit/auth',
      'Passed 5 tests in 1.234 seconds',
      '[tls:no,tcp:yes] Testing unit/protocol',
      'Passed 12 tests in 0.567 seconds',
      '',
      '\\o/ All tests passed without errors!',
    ].join('\n');

    const summary = parseTclTestOutput(output);

    expect(summary.results).toHaveLength(2);
    expect(summary.results[0]).toEqual<TclTestResult>({
      name: 'unit/auth',
      passed: 5,
      skipped: 0,
      failed: 0,
      errors: [],
      duration: 1.234,
    });
    expect(summary.results[1]).toEqual<TclTestResult>({
      name: 'unit/protocol',
      passed: 12,
      skipped: 0,
      failed: 0,
      errors: [],
      duration: 0.567,
    });
    expect(summary.allPassed).toBe(true);
    expect(summary.totalPassed).toBe(17);
    expect(summary.totalFailed).toBe(0);
    expect(summary.totalSkipped).toBe(0);
  });

  it('parses output with failures', () => {
    const output = [
      '[tls:no,tcp:yes] Testing unit/auth',
      '[err]: AUTH fails without password set',
      'Expected OK but got ERR Client sent AUTH, but no password is set',
      'Passed 3 tests in 0.100 seconds (1 skipped, 1 failed)',
      '',
      '!!! WARNING The following tests failed:',
      '  unit/auth',
    ].join('\n');

    const summary = parseTclTestOutput(output);

    expect(summary.results).toHaveLength(1);
    const result = summary.results[0] as TclTestResult;
    expect(result.name).toBe('unit/auth');
    expect(result.passed).toBe(3);
    expect(result.skipped).toBe(1);
    expect(result.failed).toBe(1);
    expect(result.errors).toEqual(['AUTH fails without password set']);
    expect(summary.allPassed).toBe(false);
    expect(summary.totalFailed).toBe(1);
  });

  it('parses output with multiple errors in one test file', () => {
    const output = [
      '[tls:no,tcp:yes] Testing unit/basic',
      '[err]: PING returns PONG',
      'Expected PONG but got empty',
      '[err]: SET and GET basic',
      'Expected OK but got ERR',
      'Passed 8 tests in 0.200 seconds (0 skipped, 2 failed)',
    ].join('\n');

    const summary = parseTclTestOutput(output);

    expect(summary.results).toHaveLength(1);
    const result = summary.results[0] as TclTestResult;
    expect(result.failed).toBe(2);
    expect(result.errors).toEqual(['PING returns PONG', 'SET and GET basic']);
  });

  it('parses skipped tests', () => {
    const output = [
      '[tls:no,tcp:yes] Testing unit/wait',
      'Passed 0 tests in 0.010 seconds (3 skipped)',
    ].join('\n');

    const summary = parseTclTestOutput(output);

    expect(summary.results).toHaveLength(1);
    const result = summary.results[0] as TclTestResult;
    expect(result.passed).toBe(0);
    expect(result.skipped).toBe(3);
    expect(result.failed).toBe(0);
  });

  it('handles empty output', () => {
    const summary = parseTclTestOutput('');

    expect(summary.results).toHaveLength(0);
    expect(summary.allPassed).toBe(true);
    expect(summary.totalPassed).toBe(0);
    expect(summary.totalFailed).toBe(0);
    expect(summary.totalSkipped).toBe(0);
  });

  it('handles output with only noise (no test results)', () => {
    const output = [
      'Cleanup: may take some time... OK',
      'Starting test server at port 11111',
      'Some random log line',
    ].join('\n');

    const summary = parseTclTestOutput(output);

    expect(summary.results).toHaveLength(0);
    expect(summary.allPassed).toBe(true);
  });

  it('computes summary totals correctly', () => {
    const output = [
      '[tls:no,tcp:yes] Testing unit/auth',
      'Passed 5 tests in 1.000 seconds (2 skipped, 1 failed)',
      '[tls:no,tcp:yes] Testing unit/basic',
      'Passed 10 tests in 2.000 seconds',
      '[tls:no,tcp:yes] Testing unit/expire',
      '[err]: PERSIST removes TTL',
      'Passed 7 tests in 1.500 seconds (0 skipped, 1 failed)',
    ].join('\n');

    const summary = parseTclTestOutput(output);

    expect(summary.totalPassed).toBe(22);
    expect(summary.totalFailed).toBe(2);
    expect(summary.totalSkipped).toBe(2);
    expect(summary.allPassed).toBe(false);
    expect(summary.results).toHaveLength(3);
  });

  it('handles "Passed N tests" without parenthesized details', () => {
    const output = [
      '[tls:no,tcp:yes] Testing unit/scan',
      'Passed 15 tests in 3.456 seconds',
    ].join('\n');

    const summary = parseTclTestOutput(output);
    const result = summary.results[0] as TclTestResult;
    expect(result.passed).toBe(15);
    expect(result.skipped).toBe(0);
    expect(result.failed).toBe(0);
  });

  it('categorizes results by status', () => {
    const output = [
      '[tls:no,tcp:yes] Testing unit/auth',
      'Passed 5 tests in 1.000 seconds',
      '[tls:no,tcp:yes] Testing unit/basic',
      '[err]: Some failure',
      'Passed 3 tests in 0.500 seconds (0 skipped, 1 failed)',
      '[tls:no,tcp:yes] Testing unit/wait',
      'Passed 0 tests in 0.010 seconds (5 skipped)',
    ].join('\n');

    const summary = parseTclTestOutput(output);

    const passed = summary.results.filter(
      (r) => r.failed === 0 && r.skipped < r.passed
    );
    const failed = summary.results.filter((r) => r.failed > 0);
    expect(passed).toHaveLength(1);
    expect(failed).toHaveLength(1);
  });
});

describe('TclTestSummary', () => {
  it('passRate computes correctly', () => {
    const output = [
      '[tls:no,tcp:yes] Testing unit/auth',
      'Passed 8 tests in 1.000 seconds (0 skipped, 2 failed)',
    ].join('\n');

    const summary = parseTclTestOutput(output);
    const total = summary.totalPassed + summary.totalFailed;
    const passRate = total > 0 ? summary.totalPassed / total : 1;
    expect(passRate).toBe(0.8);
  });

  it('passRate is 1 when no tests', () => {
    const summary = parseTclTestOutput('');
    const total = summary.totalPassed + summary.totalFailed;
    const passRate = total > 0 ? summary.totalPassed / total : 1;
    expect(passRate).toBe(1);
  });
});
