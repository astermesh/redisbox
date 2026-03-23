import { describe, it, expect } from 'vitest';
import { keySlot } from './keyslot.ts';

describe('keySlot', () => {
  it('is exported for use by other modules', () => {
    expect(typeof keySlot).toBe('function');
  });

  it('returns consistent results', () => {
    expect(keySlot('test')).toBe(keySlot('test'));
  });

  it('handles binary-safe key names', () => {
    const slot = keySlot('key with spaces');
    expect(slot).toBeGreaterThanOrEqual(0);
    expect(slot).toBeLessThanOrEqual(16383);
  });

  it('returns slot in range 0-16383', () => {
    const slot = keySlot('test');
    expect(slot).toBeGreaterThanOrEqual(0);
    expect(slot).toBeLessThanOrEqual(16383);
  });

  it('computes correct slot for "foo"', () => {
    expect(keySlot('foo')).toBe(12182);
  });

  it('computes correct slot for "bar"', () => {
    expect(keySlot('bar')).toBe(5061);
  });

  it('computes correct slot for "hello"', () => {
    expect(keySlot('hello')).toBe(866);
  });

  it('computes correct slot for empty string', () => {
    expect(keySlot('')).toBe(0);
  });

  it('computes correct slot for "123456789"', () => {
    expect(keySlot('123456789')).toBe(12739);
  });

  it('handles hash tags - {user}.info', () => {
    expect(keySlot('{user}.info')).toBe(keySlot('user'));
  });

  it('handles hash tags - {user}.session', () => {
    expect(keySlot('{user}.session')).toBe(keySlot('user'));
  });

  it('ignores empty hash tag {}', () => {
    expect(keySlot('{}key')).not.toBe(keySlot(''));
  });

  it('uses first valid hash tag only', () => {
    expect(keySlot('{a}{b}')).toBe(keySlot('a'));
  });
});
