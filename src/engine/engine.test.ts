import { describe, it, expect } from 'vitest';
import { RedisEngine } from './engine.ts';

describe('RedisEngine', () => {
  it('creates 16 databases', () => {
    const engine = new RedisEngine();
    expect(engine.databases).toHaveLength(16);
  });

  it('accepts custom clock via constructor', () => {
    let now = 1000;
    const engine = new RedisEngine({ clock: () => now });
    expect(engine.clock()).toBe(1000);
    now = 2000;
    expect(engine.clock()).toBe(2000);
  });

  it('accepts custom rng via constructor', () => {
    const engine = new RedisEngine({ rng: () => 0.42 });
    expect(engine.rng()).toBe(0.42);
  });

  it('defaults clock to Date.now', () => {
    const engine = new RedisEngine();
    const before = Date.now();
    const clock = engine.clock();
    const after = Date.now();
    expect(clock).toBeGreaterThanOrEqual(before);
    expect(clock).toBeLessThanOrEqual(after);
  });

  it('defaults rng to Math.random', () => {
    const engine = new RedisEngine();
    const val = engine.rng();
    expect(val).toBeGreaterThanOrEqual(0);
    expect(val).toBeLessThan(1);
  });

  it('db() returns correct database by index', () => {
    const engine = new RedisEngine();
    const db0 = engine.db(0);
    const db15 = engine.db(15);
    expect(db0).toBe(engine.databases[0]);
    expect(db15).toBe(engine.databases[15]);
  });

  it('db() throws for out-of-range index', () => {
    const engine = new RedisEngine();
    expect(() => engine.db(16)).toThrow('Database index out of range: 16');
    expect(() => engine.db(-1)).toThrow('Database index out of range: -1');
  });

  it('all databases use injected clock', () => {
    let now = 5000;
    const engine = new RedisEngine({ clock: () => now });
    const db = engine.db(3);
    db.set('k', 'string', 'raw', 'v');
    db.setExpiry('k', 6000);
    now = 6000;
    expect(db.get('k')).toBeNull();
  });

  it('databases are independent', () => {
    const engine = new RedisEngine();
    engine.db(0).set('k', 'string', 'raw', 'v0');
    engine.db(1).set('k', 'string', 'raw', 'v1');
    expect(engine.db(0).get('k')?.value).toBe('v0');
    expect(engine.db(1).get('k')?.value).toBe('v1');
  });
});
