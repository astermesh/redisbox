import { describe, it, expect } from 'vitest';
import { createRedisBox, RedisBox } from './redisbox.ts';

describe('RedisBox', () => {
  it('creates instance with default options', () => {
    const box = createRedisBox();
    expect(box).toBeInstanceOf(RedisBox);
    expect(box.options.mode).toBe('auto');
    expect(box.options.port).toBe(0);
    expect(box.options.host).toBe('127.0.0.1');
  });

  it('creates instance with custom options', () => {
    const box = createRedisBox({ mode: 'engine', port: 6380 });
    expect(box.options.mode).toBe('engine');
    expect(box.options.port).toBe(6380);
    expect(box.options.host).toBe('127.0.0.1');
  });
});
