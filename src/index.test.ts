import { describe, it, expect } from 'vitest';
import * as indexExports from './index.ts';
import { createRedisBox, RedisSim, VirtualClock } from './index.ts';
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

  it('exports RedisSim class', () => {
    expect(indexExports.RedisSim).toBeDefined();
    expect(typeof indexExports.RedisSim).toBe('function');
    const sim = new RedisSim();
    expect(sim).toBeInstanceOf(RedisSim);
  });

  it('exports VirtualClock class', () => {
    expect(indexExports.VirtualClock).toBeDefined();
    expect(typeof indexExports.VirtualClock).toBe('function');
    const clock = new VirtualClock();
    expect(clock).toBeInstanceOf(VirtualClock);
  });

  it('has exactly the expected exports', () => {
    const exportedKeys = Object.keys(indexExports).sort();
    expect(exportedKeys).toEqual(
      ['RedisSim', 'VirtualClock', 'createRedisBox'].sort()
    );
  });
});
