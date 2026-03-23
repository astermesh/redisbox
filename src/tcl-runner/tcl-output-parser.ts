/**
 * Parser for Redis TCL test suite output.
 *
 * Extracts structured test results from the text output produced by
 * the Redis `runtest` script when running in external server mode.
 */

export interface TclTestResult {
  /** Test unit name (e.g. "unit/auth", "unit/protocol") */
  name: string;
  /** Number of passed tests */
  passed: number;
  /** Number of skipped tests */
  skipped: number;
  /** Number of failed tests */
  failed: number;
  /** Error descriptions from [err]: lines */
  errors: string[];
  /** Duration in seconds */
  duration: number;
}

export interface TclTestSummary {
  results: TclTestResult[];
  totalPassed: number;
  totalFailed: number;
  totalSkipped: number;
  allPassed: boolean;
}

// Matches: [tls:no,tcp:yes] Testing unit/auth
const TESTING_RE = /Testing\s+(\S+)/;

// Matches: Passed 5 tests in 1.234 seconds
// Optionally followed by: (2 skipped, 1 failed) or (3 skipped) or (1 failed)
const PASSED_RE =
  /Passed\s+(\d+)\s+tests?\s+in\s+([\d.]+)\s+seconds?(?:\s*\(([^)]*)\))?/;

// Matches: [err]: test description
const ERR_RE = /\[err\]:\s*(.+)/;

// Parses "(2 skipped, 1 failed)" or "(3 skipped)" or "(1 failed)"
const SKIPPED_RE = /(\d+)\s+skipped/;
const FAILED_RE = /(\d+)\s+failed/;

export function parseTclTestOutput(output: string): TclTestSummary {
  const lines = output.split('\n');
  const results: TclTestResult[] = [];

  let currentName: string | null = null;
  let currentErrors: string[] = [];

  for (const line of lines) {
    const testingMatch = TESTING_RE.exec(line);
    if (testingMatch?.[1]) {
      currentName = testingMatch[1];
      currentErrors = [];
      continue;
    }

    const errMatch = ERR_RE.exec(line);
    if (errMatch?.[1]) {
      currentErrors.push(errMatch[1]);
      continue;
    }

    const passedMatch = PASSED_RE.exec(line);
    if (passedMatch) {
      const passed = parseInt(passedMatch[1] ?? '0', 10);
      const duration = parseFloat(passedMatch[2] ?? '0');
      const details = passedMatch[3] ?? '';

      const skippedMatch = SKIPPED_RE.exec(details);
      const failedMatch = FAILED_RE.exec(details);

      const skipped = skippedMatch ? parseInt(skippedMatch[1] ?? '0', 10) : 0;
      const failed = failedMatch ? parseInt(failedMatch[1] ?? '0', 10) : 0;

      results.push({
        name: currentName ?? 'unknown',
        passed,
        skipped,
        failed,
        errors: [...currentErrors],
        duration,
      });

      currentName = null;
      currentErrors = [];
    }
  }

  const totalPassed = results.reduce((sum, r) => sum + r.passed, 0);
  const totalFailed = results.reduce((sum, r) => sum + r.failed, 0);
  const totalSkipped = results.reduce((sum, r) => sum + r.skipped, 0);

  return {
    results,
    totalPassed,
    totalFailed,
    totalSkipped,
    allPassed: totalFailed === 0,
  };
}
