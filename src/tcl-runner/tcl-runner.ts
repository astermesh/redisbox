/**
 * Redis TCL test suite runner.
 *
 * Orchestrates the full cycle: start RedisBox → run applicable TCL
 * tests → collect and report results.
 */

import { spawn } from 'node:child_process';
import { access, readdir } from 'node:fs/promises';
import { join, relative } from 'node:path';
import { startRedisBoxServer, type RedisBoxServer } from './redisbox-server.ts';
import {
  parseTclTestOutput,
  type TclTestSummary,
} from './tcl-output-parser.ts';

export interface TclRunnerOptions {
  /** Path to the cloned Redis repository */
  redisDir: string;
  /** Specific test files to run (e.g. ["unit/auth", "unit/protocol"]) */
  tests?: string[];
  /** Port for the RedisBox server (0 = random) */
  port?: number;
  /** Host for the RedisBox server */
  host?: string;
  /** Timeout for the entire test run in ms (default: 300000 = 5 min) */
  timeout?: number;
  /** Stream output to stdout in real time */
  verbose?: boolean;
}

export interface TclRunResult {
  summary: TclTestSummary;
  /** Raw stdout from the test runner */
  stdout: string;
  /** Raw stderr from the test runner */
  stderr: string;
  /** Exit code from the test runner process */
  exitCode: number | null;
}

const DEFAULT_TIMEOUT = 300_000;

/**
 * Discover test files that can run in external server mode.
 *
 * Scans the Redis tests/ directory for .tcl test files.
 * Returns relative paths like "unit/auth", "unit/protocol".
 */
export async function discoverTests(redisDir: string): Promise<string[]> {
  const testsDir = join(redisDir, 'tests');
  const results: string[] = [];

  async function scanDir(dirPath: string): Promise<void> {
    let entries;
    try {
      entries = await readdir(dirPath, { withFileTypes: true });
    } catch {
      return;
    }

    for (const entry of entries) {
      const fullPath = join(dirPath, entry.name);
      if (entry.isDirectory()) {
        await scanDir(fullPath);
      } else if (entry.name.endsWith('.tcl')) {
        const rel = relative(testsDir, fullPath).replace('.tcl', '');
        results.push(rel);
      }
    }
  }

  for (const subdir of ['unit', 'integration']) {
    await scanDir(join(testsDir, subdir));
  }

  return results.sort();
}

/**
 * Check if the Redis test suite is available at the given path.
 */
export async function isTestSuiteAvailable(redisDir: string): Promise<boolean> {
  try {
    await access(join(redisDir, 'tests', 'test_helper.tcl'));
    return true;
  } catch {
    return false;
  }
}

/**
 * Check if tclsh is available on the system.
 */
export async function isTclAvailable(): Promise<boolean> {
  return new Promise((resolve) => {
    const proc = spawn('tclsh', { stdio: ['pipe', 'ignore', 'ignore'] });
    proc.on('error', () => resolve(false));
    proc.stdin.write('puts ok\nexit 0\n');
    proc.stdin.end();
    proc.on('close', (code) => resolve(code === 0));
  });
}

/**
 * Run the Redis TCL test suite against a RedisBox server.
 *
 * Starts a RedisBox TCP server, invokes the Redis `runtest` script
 * in external server mode, parses the output, and returns structured
 * results.
 */
export async function runTclTests(
  options: TclRunnerOptions
): Promise<TclRunResult> {
  const timeout = options.timeout ?? DEFAULT_TIMEOUT;
  const host = options.host ?? '127.0.0.1';

  // Verify test suite exists
  const available = await isTestSuiteAvailable(options.redisDir);
  if (!available) {
    throw new Error(
      `Redis test suite not found at ${options.redisDir}. ` +
        'Run the setup script first: npm run tcl:setup'
    );
  }

  // Start RedisBox server
  let server: RedisBoxServer | null = null;
  try {
    server = await startRedisBoxServer({
      port: options.port ?? 0,
      host,
    });

    // Build runtest command
    const args = buildRunTestArgs(server.port, host, options.tests);
    const result = await executeRunTest(
      options.redisDir,
      args,
      timeout,
      options.verbose ?? false
    );

    const summary = parseTclTestOutput(result.stdout);

    return {
      summary,
      stdout: result.stdout,
      stderr: result.stderr,
      exitCode: result.exitCode,
    };
  } finally {
    if (server) {
      await server.stop();
    }
  }
}

function buildRunTestArgs(
  port: number,
  host: string,
  tests?: string[]
): string[] {
  const args = ['--host', host, '--port', String(port), '--single-server'];

  if (tests && tests.length > 0) {
    for (const test of tests) {
      args.push('--single', test);
    }
  }

  return args;
}

function executeRunTest(
  redisDir: string,
  args: string[],
  timeout: number,
  verbose: boolean
): Promise<{ stdout: string; stderr: string; exitCode: number | null }> {
  return new Promise((resolve, reject) => {
    const runtest = join(redisDir, 'runtest');

    const proc = spawn(runtest, args, {
      cwd: redisDir,
      stdio: ['ignore', 'pipe', 'pipe'],
    });

    let stdout = '';
    let stderr = '';

    proc.stdout.on('data', (data: Buffer) => {
      const chunk = data.toString();
      stdout += chunk;
      if (verbose) {
        process.stdout.write(chunk);
      }
    });

    proc.stderr.on('data', (data: Buffer) => {
      const chunk = data.toString();
      stderr += chunk;
      if (verbose) {
        process.stderr.write(chunk);
      }
    });

    const timer = setTimeout(() => {
      proc.kill('SIGTERM');
      reject(new Error(`TCL test run timed out after ${timeout}ms`));
    }, timeout);

    proc.on('error', (err) => {
      clearTimeout(timer);
      reject(err);
    });

    proc.on('close', (code) => {
      clearTimeout(timer);
      resolve({ stdout, stderr, exitCode: code });
    });
  });
}
