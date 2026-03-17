import { describe, it, expect } from 'vitest';
import { createRedisBox, RedisBox } from './redisbox.ts';
import type { RedisBoxOptions } from './types.ts';

describe('RedisBox', () => {
  describe('createRedisBox factory', () => {
    it('returns a RedisBox instance', () => {
      const box = createRedisBox();
      expect(box).toBeInstanceOf(RedisBox);
    });

    it('passes options through to constructor', () => {
      const options: RedisBoxOptions = { mode: 'engine', port: 9999 };
      const box = createRedisBox(options);
      expect(box.options.mode).toBe('engine');
      expect(box.options.port).toBe(9999);
    });
  });

  describe('default options', () => {
    it('defaults mode to auto', () => {
      const box = createRedisBox();
      expect(box.options.mode).toBe('auto');
    });

    it('defaults port to 0', () => {
      const box = createRedisBox();
      expect(box.options.port).toBe(0);
    });

    it('defaults host to 127.0.0.1', () => {
      const box = createRedisBox();
      expect(box.options.host).toBe('127.0.0.1');
    });

    it('applies all defaults when no options provided', () => {
      const box = createRedisBox();
      expect(box.options).toEqual({
        mode: 'auto',
        port: 0,
        host: '127.0.0.1',
      });
    });

    it('applies all defaults when empty object provided', () => {
      const box = createRedisBox({});
      expect(box.options).toEqual({
        mode: 'auto',
        port: 0,
        host: '127.0.0.1',
      });
    });

    it('applies all defaults when undefined provided', () => {
      const box = createRedisBox(undefined);
      expect(box.options).toEqual({
        mode: 'auto',
        port: 0,
        host: '127.0.0.1',
      });
    });
  });

  describe('custom options', () => {
    it('overrides mode only', () => {
      const box = createRedisBox({ mode: 'proxy' });
      expect(box.options.mode).toBe('proxy');
      expect(box.options.port).toBe(0);
      expect(box.options.host).toBe('127.0.0.1');
    });

    it('overrides port only', () => {
      const box = createRedisBox({ port: 6380 });
      expect(box.options.mode).toBe('auto');
      expect(box.options.port).toBe(6380);
      expect(box.options.host).toBe('127.0.0.1');
    });

    it('overrides host only', () => {
      const box = createRedisBox({ host: '0.0.0.0' });
      expect(box.options.mode).toBe('auto');
      expect(box.options.port).toBe(0);
      expect(box.options.host).toBe('0.0.0.0');
    });

    it('overrides all options', () => {
      const box = createRedisBox({
        mode: 'engine',
        port: 7777,
        host: '192.168.1.1',
      });
      expect(box.options).toEqual({
        mode: 'engine',
        port: 7777,
        host: '192.168.1.1',
      });
    });

    it.each(['proxy', 'engine', 'auto'] as const)('accepts mode=%s', (mode) => {
      const box = createRedisBox({ mode });
      expect(box.options.mode).toBe(mode);
    });

    it('accepts port 0 (random)', () => {
      const box = createRedisBox({ port: 0 });
      expect(box.options.port).toBe(0);
    });

    it('accepts standard redis port', () => {
      const box = createRedisBox({ port: 6379 });
      expect(box.options.port).toBe(6379);
    });

    it('accepts high port numbers', () => {
      const box = createRedisBox({ port: 65535 });
      expect(box.options.port).toBe(65535);
    });

    it('accepts localhost as host', () => {
      const box = createRedisBox({ host: 'localhost' });
      expect(box.options.host).toBe('localhost');
    });

    it('accepts IPv6 loopback', () => {
      const box = createRedisBox({ host: '::1' });
      expect(box.options.host).toBe('::1');
    });
  });

  describe('options immutability', () => {
    it('does not share options between instances', () => {
      const box1 = createRedisBox({ port: 1111 });
      const box2 = createRedisBox({ port: 2222 });
      expect(box1.options.port).toBe(1111);
      expect(box2.options.port).toBe(2222);
    });

    it('does not mutate the input options object', () => {
      const options: RedisBoxOptions = { mode: 'engine' };
      createRedisBox(options);
      expect(options).toEqual({ mode: 'engine' });
    });

    it('options object is not the same reference as input', () => {
      const options: RedisBoxOptions = { mode: 'engine' };
      const box = createRedisBox(options);
      expect(box.options).not.toBe(options);
    });
  });

  describe('constructor', () => {
    it('can be called with new directly', () => {
      const box = new RedisBox();
      expect(box).toBeInstanceOf(RedisBox);
      expect(box.options.mode).toBe('auto');
    });

    it('accepts options via constructor', () => {
      const box = new RedisBox({ mode: 'proxy', port: 3333 });
      expect(box.options.mode).toBe('proxy');
      expect(box.options.port).toBe(3333);
    });
  });
});
