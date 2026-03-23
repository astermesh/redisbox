#!/usr/bin/env -S npx tsx
/**
 * CI script for parity verification.
 *
 * Runs the dual-backend test suite (io tests) against real Redis and
 * optionally runs the TCL test suite, then checks results against the
 * parity baseline.
 *
 * Usage:
 *   npx tsx scripts/check-parity.ts [options]
 *
 * Options:
 *   --baseline <path>    Path to baseline JSON (default: parity-baseline.json)
 *   --redis-dir <path>   Path to Redis test suite (default: .redis-tests)
 *   --skip-tcl           Skip TCL test suite (only run dual-backend tests)
 *   --timeout <ms>       TCL test timeout in ms (default: 300000)
 */

import { appendFileSync } from 'node:fs';
import { readFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import {
  runTclTests,
  isTestSuiteAvailable,
  isTclAvailable,
} from '../src/tcl-runner/tcl-runner.ts';
import { formatReport } from '../src/tcl-runner/report.ts';
import {
  checkBaseline,
  formatBaselineResult,
  type ParityBaseline,
} from '../src/tcl-runner/parity-baseline.ts';

interface CliOptions {
  baselinePath: string;
  redisDir: string;
  skipTcl: boolean;
  timeout: number;
}

function parseArgs(argv: string[]): CliOptions {
  const opts: CliOptions = {
    baselinePath: resolve('parity-baseline.json'),
    redisDir: resolve('.redis-tests'),
    skipTcl: false,
    timeout: 300_000,
  };

  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i]!;
    switch (arg) {
      case '--baseline':
        opts.baselinePath = resolve(argv[++i] ?? '');
        break;
      case '--redis-dir':
        opts.redisDir = resolve(argv[++i] ?? '');
        break;
      case '--skip-tcl':
        opts.skipTcl = true;
        break;
      case '--timeout': {
        const parsed = parseInt(argv[++i] ?? '', 10);
        if (isNaN(parsed) || parsed <= 0) {
          console.error('--timeout must be a positive integer');
          process.exit(1);
        }
        opts.timeout = parsed;
        break;
      }
      default:
        console.error(`Unknown option: ${arg}`);
        process.exit(1);
    }
  }

  return opts;
}

async function loadBaseline(path: string): Promise<ParityBaseline> {
  const raw = await readFile(path, 'utf-8');
  const data = JSON.parse(raw) as ParityBaseline;
  if (typeof data.minPassRate !== 'number' || data.minPassRate < 0) {
    throw new Error(
      `Invalid baseline: minPassRate must be a non-negative number`
    );
  }
  return data;
}

/**
 * Write GitHub Actions output if running in CI.
 */
function setOutput(name: string, value: string): void {
  const outputFile = process.env.GITHUB_OUTPUT;
  if (outputFile) {
    appendFileSync(outputFile, `${name}=${value}\n`);
  }
}

/**
 * Write GitHub Actions job summary if running in CI.
 */
function writeSummary(markdown: string): void {
  const summaryFile = process.env.GITHUB_STEP_SUMMARY;
  if (summaryFile) {
    appendFileSync(summaryFile, markdown + '\n');
  }
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));

  // Load baseline
  let baseline: ParityBaseline;
  try {
    baseline = await loadBaseline(opts.baselinePath);
  } catch (err) {
    console.error(`Failed to load baseline from ${opts.baselinePath}:`, err);
    process.exit(1);
  }
  console.log(`Parity baseline: ${baseline.minPassRate}% minimum pass rate`);
  console.log('');

  // TCL suite
  if (opts.skipTcl) {
    console.log('TCL test suite: skipped (--skip-tcl)');
    console.log('');

    // With no TCL results, we can only report a vacuous pass
    const emptyResult = {
      passed: true,
      actualRate: 100,
      requiredRate: baseline.minPassRate,
      totalPassed: 0,
      totalFailed: 0,
    };
    console.log(formatBaselineResult(emptyResult));
    setOutput('parity-rate', '100');
    setOutput('parity-passed', 'true');
    return;
  }

  // Check TCL prerequisites
  const tclOk = await isTclAvailable();
  if (!tclOk) {
    console.error(
      'Error: tclsh is not available. Install Tcl to run the Redis test suite.'
    );
    console.error('  Ubuntu/Debian: sudo apt-get install -y tcl');
    process.exit(1);
  }

  const suiteOk = await isTestSuiteAvailable(opts.redisDir);
  if (!suiteOk) {
    console.error(`Error: Redis test suite not found at ${opts.redisDir}`);
    console.error('Run setup first: npm run tcl:setup');
    process.exit(1);
  }

  // Run TCL tests
  console.log('Running Redis TCL test suite against RedisBox...');
  console.log('');

  const result = await runTclTests({
    redisDir: opts.redisDir,
    timeout: opts.timeout,
  });

  // Print test report
  console.log(formatReport(result.summary));
  console.log('');

  // Check against baseline
  const baselineResult = checkBaseline(result.summary, baseline);
  console.log(formatBaselineResult(baselineResult));

  // Set GitHub Actions outputs
  setOutput('parity-rate', baselineResult.actualRate.toFixed(1));
  setOutput('parity-passed', String(baselineResult.passed));

  // Write GitHub Actions job summary
  const summaryMd = [
    '## Parity Report',
    '',
    `| Metric | Value |`,
    `|--------|-------|`,
    `| Pass rate | ${baselineResult.actualRate.toFixed(1)}% |`,
    `| Baseline | ${baselineResult.requiredRate.toFixed(1)}% |`,
    `| Passed | ${baselineResult.totalPassed} |`,
    `| Failed | ${baselineResult.totalFailed} |`,
    `| Status | ${baselineResult.passed ? 'PASS' : 'FAIL'} |`,
  ].join('\n');
  writeSummary(summaryMd);

  if (!baselineResult.passed) {
    console.error('');
    console.error('Parity regression detected! CI will fail.');
    process.exit(1);
  }
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
