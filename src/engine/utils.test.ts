import { describe, it, expect } from 'vitest';
import { ConfigStore } from '../config-store.ts';
import { configInt } from './utils.ts';

describe('configInt', () => {
  it('reads integer from config', () => {
    const config = new ConfigStore();
    config.set('hash-max-listpack-entries', '42');
    expect(configInt(config, 'hash-max-listpack-entries', 128)).toBe(42);
  });

  it('returns fallback when config is undefined', () => {
    expect(configInt(undefined, 'hash-max-listpack-entries', 128)).toBe(128);
  });

  it('returns fallback for unknown key', () => {
    const config = new ConfigStore();
    expect(configInt(config, 'nonexistent-key', 99)).toBe(99);
  });
});
