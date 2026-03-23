/**
 * TCL test result report generator.
 *
 * Formats structured test results into human-readable reports
 * and tracks pass/fail counts as the parity metric.
 */

import type { TclTestSummary, TclTestResult } from './tcl-output-parser.ts';

export type FailureCategory =
  | 'missing-command'
  | 'behavioral-difference'
  | 'test-infra';

export interface CategorizedFailure {
  testName: string;
  errors: string[];
  category: FailureCategory;
}

/**
 * Categorize a test failure based on error patterns.
 *
 * - "ERR unknown command" → missing-command
 * - Connection/timeout errors → test-infra
 * - Everything else → behavioral-difference
 */
export function categorizeFailure(result: TclTestResult): CategorizedFailure {
  const allErrors = result.errors.join('\n').toLowerCase();

  let category: FailureCategory;
  if (
    allErrors.includes('unknown command') ||
    allErrors.includes('unknown subcommand')
  ) {
    category = 'missing-command';
  } else if (
    allErrors.includes('connection') ||
    allErrors.includes('timeout') ||
    allErrors.includes('broken pipe') ||
    allErrors.includes('refused')
  ) {
    category = 'test-infra';
  } else {
    category = 'behavioral-difference';
  }

  return {
    testName: result.name,
    errors: result.errors,
    category,
  };
}

/**
 * Compute the parity pass rate as a percentage.
 */
export function passRate(summary: TclTestSummary): number {
  const total = summary.totalPassed + summary.totalFailed;
  if (total === 0) return 100;
  return (summary.totalPassed / total) * 100;
}

/**
 * Format a test summary as a human-readable text report.
 */
export function formatReport(summary: TclTestSummary): string {
  const lines: string[] = [];

  lines.push('═══════════════════════════════════════════════════');
  lines.push('  Redis TCL Test Suite — Parity Report');
  lines.push('═══════════════════════════════════════════════════');
  lines.push('');

  // Overall stats
  const total = summary.totalPassed + summary.totalFailed;
  const rate = passRate(summary);
  lines.push(
    `  Pass rate: ${rate.toFixed(1)}% (${summary.totalPassed}/${total})`
  );
  lines.push(`  Passed:  ${summary.totalPassed}`);
  lines.push(`  Failed:  ${summary.totalFailed}`);
  lines.push(`  Skipped: ${summary.totalSkipped}`);
  lines.push('');

  // Per-test results
  const passed = summary.results.filter((r) => r.failed === 0);
  const failed = summary.results.filter((r) => r.failed > 0);

  if (failed.length > 0) {
    lines.push('── Failed Tests ──────────────────────────────────');
    lines.push('');

    const categorized = failed.map(categorizeFailure);
    const byCategory = new Map<FailureCategory, CategorizedFailure[]>();
    for (const cf of categorized) {
      const list = byCategory.get(cf.category) ?? [];
      list.push(cf);
      byCategory.set(cf.category, list);
    }

    const categoryLabels: Record<FailureCategory, string> = {
      'missing-command': 'Missing Command',
      'behavioral-difference': 'Behavioral Difference',
      'test-infra': 'Test Infrastructure',
    };

    for (const [cat, items] of byCategory) {
      lines.push(`  [${categoryLabels[cat]}]`);
      for (const item of items) {
        lines.push(`    - ${item.testName}`);
        for (const err of item.errors) {
          lines.push(`      ${err}`);
        }
      }
      lines.push('');
    }
  }

  if (passed.length > 0) {
    lines.push('── Passed Tests ──────────────────────────────────');
    lines.push('');
    for (const r of passed) {
      const skipNote = r.skipped > 0 ? ` (${r.skipped} skipped)` : '';
      lines.push(`  ✓ ${r.name} — ${r.passed} tests${skipNote}`);
    }
    lines.push('');
  }

  lines.push('═══════════════════════════════════════════════════');

  return lines.join('\n');
}
