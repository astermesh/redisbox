#!/usr/bin/env -S npx tsx
/**
 * CLI entry point for running Redis TCL test suite against RedisBox.
 *
 * Usage:
 *   npx tsx scripts/run-tcl-tests.ts [options]
 *
 * Options:
 *   --redis-dir <path>   Path to Redis source (default: .redis-tests)
 *   --test <name>        Run specific test (can be repeated)
 *   --port <port>        RedisBox port (default: random)
 *   --verbose            Stream test output in real time
 *   --list               List available test files and exit
 *   --timeout <ms>       Test run timeout in ms (default: 300000)
 */

import { resolve } from 'node:path';
import {
  runTclTests,
  discoverTests,
  isTestSuiteAvailable,
  isTclAvailable,
} from '../src/tcl-runner/tcl-runner.ts';
import { formatReport } from '../src/tcl-runner/report.ts';

interface CliOptions {
  redisDir: string;
  tests: string[];
  port: number;
  verbose: boolean;
  list: boolean;
  timeout: number;
}

function parseArgs(argv: string[]): CliOptions {
  const opts: CliOptions = {
    redisDir: resolve('.redis-tests'),
    tests: [],
    port: 0,
    verbose: false,
    list: false,
    timeout: 300_000,
  };

  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i]!;
    switch (arg) {
      case '--redis-dir':
        opts.redisDir = resolve(argv[++i] ?? '');
        break;
      case '--test':
        opts.tests.push(argv[++i] ?? '');
        break;
      case '--port':
        opts.port = parseInt(argv[++i] ?? '0', 10);
        break;
      case '--verbose':
        opts.verbose = true;
        break;
      case '--list':
        opts.list = true;
        break;
      case '--timeout':
        opts.timeout = parseInt(argv[++i] ?? '300000', 10);
        break;
      default:
        console.error(`Unknown option: ${arg}`);
        process.exit(1);
    }
  }

  return opts;
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));

  // Check prerequisites
  const tclOk = await isTclAvailable();
  if (!tclOk) {
    console.error(
      'Error: tclsh is not available. Install Tcl to run the Redis test suite.'
    );
    console.error('  Ubuntu/Debian: sudo apt install tcl');
    console.error('  macOS: brew install tcl-tk');
    process.exit(1);
  }

  const suiteOk = await isTestSuiteAvailable(opts.redisDir);
  if (!suiteOk) {
    console.error(`Error: Redis test suite not found at ${opts.redisDir}`);
    console.error('Run setup first: npm run tcl:setup');
    process.exit(1);
  }

  // List mode
  if (opts.list) {
    const tests = await discoverTests(opts.redisDir);
    console.log(`Found ${tests.length} test files:\n`);
    for (const t of tests) {
      console.log(`  ${t}`);
    }
    process.exit(0);
  }

  // Run tests
  console.log('Starting RedisBox TCL test run...');
  console.log(`Redis test suite: ${opts.redisDir}`);
  if (opts.tests.length > 0) {
    console.log(`Tests: ${opts.tests.join(', ')}`);
  } else {
    console.log('Tests: all available');
  }
  console.log('');

  const result = await runTclTests({
    redisDir: opts.redisDir,
    tests: opts.tests.length > 0 ? opts.tests : undefined,
    port: opts.port,
    verbose: opts.verbose,
    timeout: opts.timeout,
  });

  // Print report
  console.log('');
  console.log(formatReport(result.summary));

  // Exit with appropriate code
  process.exit(result.summary.allPassed ? 0 : 1);
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
