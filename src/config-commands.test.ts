import { describe, it, expect, beforeEach } from 'vitest';
import { ConfigStore } from './config-store.ts';
import { executeConfig } from './config-commands.ts';
import type { ConfigResponse } from './config-commands.ts';

describe('CONFIG commands', () => {
  let store: ConfigStore;

  beforeEach(() => {
    store = new ConfigStore();
  });

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  function exec(...args: string[]): ConfigResponse {
    return executeConfig(store, args);
  }

  function expectOk(resp: ConfigResponse): void {
    expect(resp).toEqual({ kind: 'ok' });
  }

  function expectArray(resp: ConfigResponse): string[] {
    expect(resp.kind).toBe('array');
    if (resp.kind !== 'array') throw new Error('not array');
    return resp.data;
  }

  function expectError(resp: ConfigResponse, substring?: string): void {
    expect(resp.kind).toBe('error');
    if (resp.kind === 'error' && substring) {
      expect(resp.message).toContain(substring);
    }
  }

  // -------------------------------------------------------------------------
  // No subcommand
  // -------------------------------------------------------------------------

  describe('no subcommand', () => {
    it('returns error with no arguments', () => {
      expectError(executeConfig(store, []), 'wrong number of arguments');
    });
  });

  // -------------------------------------------------------------------------
  // Unknown subcommand
  // -------------------------------------------------------------------------

  describe('unknown subcommand', () => {
    it('returns error for unknown subcommand', () => {
      expectError(exec('UNKNOWN'), 'unknown subcommand');
    });

    it('includes subcommand name in error', () => {
      const resp = exec('BADCMD');
      expect(resp.kind).toBe('error');
      if (resp.kind === 'error') {
        expect(resp.message).toContain('badcmd');
      }
    });
  });

  // -------------------------------------------------------------------------
  // CONFIG GET
  // -------------------------------------------------------------------------

  describe('CONFIG GET', () => {
    it('returns value for exact key', () => {
      const data = expectArray(exec('GET', 'maxmemory'));
      expect(data).toEqual(['maxmemory', '0']);
    });

    it('returns empty array for non-matching pattern', () => {
      const data = expectArray(exec('GET', 'zzz-nonexistent'));
      expect(data).toEqual([]);
    });

    it('returns multiple matches for glob', () => {
      const data = expectArray(exec('GET', 'maxmemory*'));
      expect(data.length).toBeGreaterThanOrEqual(4);
      expect(data).toContain('maxmemory');
      expect(data).toContain('maxmemory-policy');
    });

    it('returns all params for *', () => {
      const data = expectArray(exec('GET', '*'));
      expect(data.length).toBeGreaterThan(100);
    });

    it('returns error with no pattern', () => {
      expectError(exec('GET'), 'wrong number of arguments');
    });

    it('supports multiple patterns', () => {
      const data = expectArray(exec('GET', 'hz', 'maxmemory'));
      expect(data).toContain('hz');
      expect(data).toContain('maxmemory');
    });

    it('is case-insensitive for subcommand', () => {
      const data = expectArray(exec('get', 'hz'));
      expect(data).toEqual(['hz', '10']);
    });

    it('reflects changes made by SET', () => {
      expectOk(exec('SET', 'hz', '50'));
      const data = expectArray(exec('GET', 'hz'));
      expect(data).toEqual(['hz', '50']);
    });
  });

  // -------------------------------------------------------------------------
  // CONFIG SET
  // -------------------------------------------------------------------------

  describe('CONFIG SET', () => {
    it('sets a valid parameter', () => {
      expectOk(exec('SET', 'hz', '100'));
      const data = expectArray(exec('GET', 'hz'));
      expect(data).toEqual(['hz', '100']);
    });

    it('returns error for unknown parameter', () => {
      expectError(
        exec('SET', 'bogus', 'value'),
        'Unsupported CONFIG parameter'
      );
    });

    it('returns error for invalid value', () => {
      expectError(exec('SET', 'hz', 'notanumber'), 'Invalid argument');
    });

    it('returns error with no arguments', () => {
      expectError(exec('SET'), 'wrong number of arguments');
    });

    it('returns error with odd number of arguments', () => {
      expectError(exec('SET', 'hz'), 'wrong number of arguments');
    });

    it('supports multiple key-value pairs', () => {
      expectOk(exec('SET', 'hz', '20', 'maxmemory', '1024'));
      expect(expectArray(exec('GET', 'hz'))).toEqual(['hz', '20']);
      expect(expectArray(exec('GET', 'maxmemory'))).toEqual([
        'maxmemory',
        '1024',
      ]);
    });

    it('is atomic — fails all on error', () => {
      exec('SET', 'hz', '10');
      const resp = exec('SET', 'hz', '20', 'bogus', 'val');
      expectError(resp, 'Unsupported CONFIG parameter');
      // hz should remain unchanged
      expect(expectArray(exec('GET', 'hz'))).toEqual(['hz', '10']);
    });

    it('validates maxmemory-policy', () => {
      expectOk(exec('SET', 'maxmemory-policy', 'allkeys-lru'));
      expectError(
        exec('SET', 'maxmemory-policy', 'invalid'),
        'Invalid argument'
      );
    });

    it('validates yes/no fields', () => {
      expectOk(exec('SET', 'appendonly', 'yes'));
      expectError(exec('SET', 'appendonly', 'true'), 'Invalid argument');
    });

    it('is case-insensitive for subcommand', () => {
      expectOk(exec('set', 'hz', '50'));
      expect(expectArray(exec('GET', 'hz'))).toEqual(['hz', '50']);
    });
  });

  // -------------------------------------------------------------------------
  // CONFIG RESETSTAT
  // -------------------------------------------------------------------------

  describe('CONFIG RESETSTAT', () => {
    it('returns OK', () => {
      expectOk(exec('RESETSTAT'));
    });

    it('returns error with extra arguments', () => {
      expectError(exec('RESETSTAT', 'extra'), 'wrong number of arguments');
    });

    it('is case-insensitive', () => {
      expectOk(exec('resetstat'));
    });
  });

  // -------------------------------------------------------------------------
  // CONFIG REWRITE
  // -------------------------------------------------------------------------

  describe('CONFIG REWRITE', () => {
    it('returns OK (no-op)', () => {
      expectOk(exec('REWRITE'));
    });

    it('returns error with extra arguments', () => {
      expectError(exec('REWRITE', 'extra'), 'wrong number of arguments');
    });

    it('is case-insensitive', () => {
      expectOk(exec('rewrite'));
    });
  });
});
