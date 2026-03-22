import { describe, it, expect } from 'vitest';
import { RedisEngine } from '../engine.ts';
import type { CommandContext } from '../types.ts';
import * as persistence from './persistence.ts';

function createCtx(clockMs = 1_700_000_000_000): CommandContext {
  const engine = new RedisEngine({ clock: () => clockMs });
  return {
    db: engine.db(0),
    engine,
  };
}

describe('BGSAVE', () => {
  it('returns "Background saving started"', () => {
    const ctx = createCtx();
    const reply = persistence.bgsave(ctx, []);
    expect(reply).toEqual({
      kind: 'status',
      value: 'Background saving started',
    });
  });

  it('returns "Background saving scheduled" with SCHEDULE option', () => {
    const ctx = createCtx();
    const reply = persistence.bgsave(ctx, ['SCHEDULE']);
    expect(reply).toEqual({
      kind: 'status',
      value: 'Background saving scheduled',
    });
  });

  it('returns "Background saving scheduled" with schedule lowercase', () => {
    const ctx = createCtx();
    const reply = persistence.bgsave(ctx, ['schedule']);
    expect(reply).toEqual({
      kind: 'status',
      value: 'Background saving scheduled',
    });
  });

  it('returns syntax error for unknown subcommand', () => {
    const ctx = createCtx();
    const reply = persistence.bgsave(ctx, ['INVALID']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'syntax error',
    });
  });
});

describe('BGREWRITEAOF', () => {
  it('returns "Background append only file rewriting started"', () => {
    const ctx = createCtx();
    const reply = persistence.bgrewriteaof(ctx);
    expect(reply).toEqual({
      kind: 'status',
      value: 'Background append only file rewriting started',
    });
  });
});

describe('SAVE', () => {
  it('returns OK', () => {
    const ctx = createCtx();
    const reply = persistence.save(ctx);
    expect(reply).toEqual({ kind: 'status', value: 'OK' });
  });
});

describe('LASTSAVE', () => {
  it('returns unix timestamp in seconds', () => {
    const ctx = createCtx(1_700_000_000_000);
    const reply = persistence.lastsave(ctx);
    expect(reply).toEqual({ kind: 'integer', value: 1_700_000_000 });
  });

  it('floors the timestamp', () => {
    const ctx = createCtx(1_700_000_500_999);
    const reply = persistence.lastsave(ctx);
    expect(reply).toEqual({ kind: 'integer', value: 1_700_000_500 });
  });
});

describe('SHUTDOWN', () => {
  it('returns OK with no arguments', () => {
    const reply = persistence.shutdown([]);
    expect(reply).toEqual({ kind: 'status', value: 'OK' });
  });

  it('accepts NOSAVE', () => {
    const reply = persistence.shutdown(['NOSAVE']);
    expect(reply).toEqual({ kind: 'status', value: 'OK' });
  });

  it('accepts SAVE', () => {
    const reply = persistence.shutdown(['SAVE']);
    expect(reply).toEqual({ kind: 'status', value: 'OK' });
  });

  it('accepts NOW', () => {
    const reply = persistence.shutdown(['NOW']);
    expect(reply).toEqual({ kind: 'status', value: 'OK' });
  });

  it('accepts FORCE', () => {
    const reply = persistence.shutdown(['FORCE']);
    expect(reply).toEqual({ kind: 'status', value: 'OK' });
  });

  it('accepts combined options NOSAVE NOW FORCE', () => {
    const reply = persistence.shutdown(['NOSAVE', 'NOW', 'FORCE']);
    expect(reply).toEqual({ kind: 'status', value: 'OK' });
  });

  it('is case insensitive', () => {
    const reply = persistence.shutdown(['nosave', 'now']);
    expect(reply).toEqual({ kind: 'status', value: 'OK' });
  });

  it('returns error for unknown subcommand', () => {
    const reply = persistence.shutdown(['INVALID']);
    expect(reply.kind).toBe('error');
  });

  it('rejects NOSAVE and SAVE together with syntax error', () => {
    const reply = persistence.shutdown(['NOSAVE', 'SAVE']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'syntax error',
    });
  });

  it('ABORT returns error when no shutdown in progress', () => {
    const reply = persistence.shutdown(['ABORT']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'No shutdown in progress.',
    });
  });

  it('rejects ABORT combined with other options', () => {
    const reply = persistence.shutdown(['ABORT', 'NOSAVE']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'syntax error',
    });
  });
});

describe('specs', () => {
  it('exports all persistence command specs', () => {
    const names = persistence.specs.map((s) => s.name);
    expect(names).toContain('bgsave');
    expect(names).toContain('bgrewriteaof');
    expect(names).toContain('save');
    expect(names).toContain('lastsave');
    expect(names).toContain('shutdown');
  });

  it('BGSAVE has variable arity', () => {
    const spec = persistence.specs.find((s) => s.name === 'bgsave');
    expect(spec?.arity).toBe(-1);
  });

  it('BGREWRITEAOF has arity 1', () => {
    const spec = persistence.specs.find((s) => s.name === 'bgrewriteaof');
    expect(spec?.arity).toBe(1);
  });

  it('SAVE has arity 1', () => {
    const spec = persistence.specs.find((s) => s.name === 'save');
    expect(spec?.arity).toBe(1);
  });

  it('LASTSAVE has arity 1', () => {
    const spec = persistence.specs.find((s) => s.name === 'lastsave');
    expect(spec?.arity).toBe(1);
  });

  it('SHUTDOWN has variable arity', () => {
    const spec = persistence.specs.find((s) => s.name === 'shutdown');
    expect(spec?.arity).toBe(-1);
  });
});
