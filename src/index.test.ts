import { describe, it, expect } from 'vitest';
import * as indexExports from './index.ts';
import { createRedisBox } from './index.ts';
import { RedisBox } from './redisbox.ts';

describe('index exports', () => {
  it('exports createRedisBox function', () => {
    expect(indexExports.createRedisBox).toBeDefined();
    expect(typeof indexExports.createRedisBox).toBe('function');
  });

  it('createRedisBox from index creates a valid instance', () => {
    const box = createRedisBox();
    expect(box).toBeInstanceOf(RedisBox);
  });

  it('does not export RedisBox class directly', () => {
    expect('RedisBox' in indexExports).toBe(false);
  });

  it('has exactly the expected exports', () => {
    const exportedKeys = Object.keys(indexExports);
    expect(exportedKeys).toEqual(['createRedisBox']);
  });
});
