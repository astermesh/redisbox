import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { mkdtemp, mkdir, writeFile, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import {
  discoverTests,
  isTestSuiteAvailable,
  isTclAvailable,
} from './tcl-runner.ts';

describe('isTestSuiteAvailable', () => {
  let tempDir: string;

  beforeEach(async () => {
    tempDir = await mkdtemp(join(tmpdir(), 'redisbox-tcl-'));
  });

  afterEach(async () => {
    await rm(tempDir, { recursive: true, force: true });
  });

  it('returns true when test_helper.tcl exists', async () => {
    await mkdir(join(tempDir, 'tests'), { recursive: true });
    await writeFile(join(tempDir, 'tests', 'test_helper.tcl'), '# test helper');

    expect(await isTestSuiteAvailable(tempDir)).toBe(true);
  });

  it('returns false when test_helper.tcl is missing', async () => {
    expect(await isTestSuiteAvailable(tempDir)).toBe(false);
  });

  it('returns false when directory does not exist', async () => {
    expect(await isTestSuiteAvailable('/nonexistent/path')).toBe(false);
  });
});

describe('discoverTests', () => {
  let tempDir: string;

  beforeEach(async () => {
    tempDir = await mkdtemp(join(tmpdir(), 'redisbox-tcl-'));
  });

  afterEach(async () => {
    await rm(tempDir, { recursive: true, force: true });
  });

  it('discovers unit test files', async () => {
    const unitDir = join(tempDir, 'tests', 'unit');
    await mkdir(unitDir, { recursive: true });
    await writeFile(join(unitDir, 'auth.tcl'), '');
    await writeFile(join(unitDir, 'protocol.tcl'), '');
    await writeFile(join(unitDir, 'basic.tcl'), '');

    const tests = await discoverTests(tempDir);

    expect(tests).toEqual(['unit/auth', 'unit/basic', 'unit/protocol']);
  });

  it('discovers integration test files', async () => {
    const intDir = join(tempDir, 'tests', 'integration');
    await mkdir(intDir, { recursive: true });
    await writeFile(join(intDir, 'replication.tcl'), '');

    const tests = await discoverTests(tempDir);

    expect(tests).toEqual(['integration/replication']);
  });

  it('discovers both unit and integration tests', async () => {
    const unitDir = join(tempDir, 'tests', 'unit');
    const intDir = join(tempDir, 'tests', 'integration');
    await mkdir(unitDir, { recursive: true });
    await mkdir(intDir, { recursive: true });
    await writeFile(join(unitDir, 'auth.tcl'), '');
    await writeFile(join(intDir, 'repl.tcl'), '');

    const tests = await discoverTests(tempDir);

    expect(tests).toEqual(['integration/repl', 'unit/auth']);
  });

  it('ignores non-tcl files', async () => {
    const unitDir = join(tempDir, 'tests', 'unit');
    await mkdir(unitDir, { recursive: true });
    await writeFile(join(unitDir, 'auth.tcl'), '');
    await writeFile(join(unitDir, 'README.md'), '');
    await writeFile(join(unitDir, 'helpers.rb'), '');

    const tests = await discoverTests(tempDir);

    expect(tests).toEqual(['unit/auth']);
  });

  it('returns empty array when no tests directory', async () => {
    const tests = await discoverTests(tempDir);
    expect(tests).toEqual([]);
  });

  it('returns empty array when tests directory is empty', async () => {
    await mkdir(join(tempDir, 'tests'), { recursive: true });
    const tests = await discoverTests(tempDir);
    expect(tests).toEqual([]);
  });
});

describe('isTclAvailable', () => {
  it('returns a boolean', async () => {
    const result = await isTclAvailable();
    expect(typeof result).toBe('boolean');
  });
});
