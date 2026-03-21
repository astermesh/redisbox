import { describe, it, expect } from 'vitest';
import { SlowlogManager } from './slowlog.ts';

describe('SlowlogManager', () => {
  it('records entry when duration exceeds threshold', () => {
    const mgr = new SlowlogManager();
    mgr.record(
      15000,
      10000,
      128,
      1000,
      ['SET', 'key', 'val'],
      '127.0.0.1:1234',
      ''
    );
    expect(mgr.len()).toBe(1);

    const entries = mgr.get();
    expect(entries).toHaveLength(1);
    const entry = entries[0];
    expect(entry).toBeDefined();
    if (!entry) return;
    expect(entry.id).toBe(0);
    expect(entry.timestamp).toBe(1000);
    expect(entry.duration).toBe(15000);
    expect(entry.args).toEqual(['SET', 'key', 'val']);
    expect(entry.clientAddr).toBe('127.0.0.1:1234');
    expect(entry.clientName).toBe('');
  });

  it('does not record when duration is below threshold', () => {
    const mgr = new SlowlogManager();
    mgr.record(5000, 10000, 128, 1000, ['GET', 'key'], '', '');
    expect(mgr.len()).toBe(0);
  });

  it('records when duration equals threshold', () => {
    const mgr = new SlowlogManager();
    mgr.record(10000, 10000, 128, 1000, ['GET', 'key'], '', '');
    expect(mgr.len()).toBe(1);
  });

  it('does not record when threshold is negative (disabled)', () => {
    const mgr = new SlowlogManager();
    mgr.record(999999, -1, 128, 1000, ['GET', 'key'], '', '');
    expect(mgr.len()).toBe(0);
  });

  it('records everything when threshold is 0', () => {
    const mgr = new SlowlogManager();
    mgr.record(0, 0, 128, 1000, ['GET', 'key'], '', '');
    expect(mgr.len()).toBe(1);
  });

  it('trims entries to max length', () => {
    const mgr = new SlowlogManager();
    for (let i = 0; i < 5; i++) {
      mgr.record(100, 0, 3, 1000 + i, ['CMD', String(i)], '', '');
    }
    expect(mgr.len()).toBe(3);
    const entries = mgr.get(-1);
    expect(entries[0]?.args).toEqual(['CMD', '4']);
    expect(entries[2]?.args).toEqual(['CMD', '2']);
  });

  it('returns newest entries first', () => {
    const mgr = new SlowlogManager();
    mgr.record(100, 0, 128, 1000, ['FIRST'], '', '');
    mgr.record(200, 0, 128, 2000, ['SECOND'], '', '');
    const entries = mgr.get(-1);
    expect(entries[0]?.args).toEqual(['SECOND']);
    expect(entries[1]?.args).toEqual(['FIRST']);
  });

  it('get without count returns default 10 entries', () => {
    const mgr = new SlowlogManager();
    for (let i = 0; i < 20; i++) {
      mgr.record(100, 0, 128, 1000, ['CMD', String(i)], '', '');
    }
    expect(mgr.len()).toBe(20);
    expect(mgr.get()).toHaveLength(10);
  });

  it('get with count returns limited entries', () => {
    const mgr = new SlowlogManager();
    for (let i = 0; i < 10; i++) {
      mgr.record(100, 0, 128, 1000, ['CMD', String(i)], '', '');
    }
    expect(mgr.get(3)).toHaveLength(3);
    expect(mgr.get(0)).toHaveLength(0);
  });

  it('get with negative count returns all entries', () => {
    const mgr = new SlowlogManager();
    for (let i = 0; i < 15; i++) {
      mgr.record(100, 0, 128, 1000, ['CMD'], '', '');
    }
    expect(mgr.get(-1)).toHaveLength(15);
  });

  it('auto-increments IDs', () => {
    const mgr = new SlowlogManager();
    mgr.record(100, 0, 128, 1000, ['A'], '', '');
    mgr.record(100, 0, 128, 1000, ['B'], '', '');
    const entries = mgr.get(-1);
    expect(entries[0]?.id).toBe(1);
    expect(entries[1]?.id).toBe(0);
  });

  it('reset clears all entries', () => {
    const mgr = new SlowlogManager();
    mgr.record(100, 0, 128, 1000, ['CMD'], '', '');
    mgr.record(100, 0, 128, 1000, ['CMD'], '', '');
    mgr.reset();
    expect(mgr.len()).toBe(0);
    expect(mgr.get(-1)).toEqual([]);
  });

  it('IDs continue incrementing after reset', () => {
    const mgr = new SlowlogManager();
    mgr.record(100, 0, 128, 1000, ['A'], '', '');
    mgr.record(100, 0, 128, 1000, ['B'], '', '');
    mgr.reset();
    mgr.record(100, 0, 128, 1000, ['C'], '', '');
    expect(mgr.get()[0]?.id).toBe(2);
  });

  it('stores client name', () => {
    const mgr = new SlowlogManager();
    mgr.record(100, 0, 128, 1000, ['CMD'], '10.0.0.1:5000', 'my-client');
    const entries = mgr.get();
    expect(entries[0]?.clientAddr).toBe('10.0.0.1:5000');
    expect(entries[0]?.clientName).toBe('my-client');
  });

  it('truncates arguments exceeding 32 entries', () => {
    const mgr = new SlowlogManager();
    const manyArgs = Array.from({ length: 50 }, (_, i) => `arg${i}`);
    mgr.record(100, 0, 128, 1000, manyArgs, '', '');
    const entry = mgr.get()[0];
    expect(entry).toBeDefined();
    if (!entry) return;
    expect(entry.args).toHaveLength(32);
    expect(entry.args[31]).toBe('... (19 more arguments)');
  });

  it('truncates individual arguments exceeding 128 bytes', () => {
    const mgr = new SlowlogManager();
    const longArg = 'x'.repeat(200);
    mgr.record(100, 0, 128, 1000, ['SET', 'key', longArg], '', '');
    const entry = mgr.get()[0];
    expect(entry).toBeDefined();
    if (!entry) return;
    expect(entry.args[2]).toBe('x'.repeat(128) + '... (72 more bytes)');
  });

  it('truncates both arg count and arg length simultaneously', () => {
    const mgr = new SlowlogManager();
    const manyLongArgs = Array.from({ length: 40 }, () => 'y'.repeat(200));
    mgr.record(100, 0, 128, 1000, manyLongArgs, '', '');
    const entry = mgr.get()[0];
    expect(entry).toBeDefined();
    if (!entry) return;
    expect(entry.args).toHaveLength(32);
    expect(entry.args[30]).toBe('y'.repeat(128) + '... (72 more bytes)');
    expect(entry.args[31]).toBe('... (9 more arguments)');
  });
});
