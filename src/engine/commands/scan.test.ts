import { describe, it, expect } from 'vitest';
import { RedisEngine } from '../engine.ts';
import type { Reply } from '../types.ts';
import * as scanCmd from './scan.ts';

function createDb() {
  const engine = new RedisEngine({ clock: () => 1000, rng: () => 0.5 });
  return engine.db(0);
}

function extractBulkValues(reply: Reply): (string | null)[] {
  if (reply.kind !== 'array') return [];
  return reply.value
    .filter((r): r is Reply & { kind: 'bulk' } => r.kind === 'bulk')
    .map((r) => r.value);
}

describe('KEYS', () => {
  it('returns matching keys', () => {
    const db = createDb();
    db.set('user:1', 'string', 'raw', 'v');
    db.set('user:2', 'string', 'raw', 'v');
    db.set('admin:1', 'string', 'raw', 'v');

    const reply = scanCmd.keys(db, ['user:*']);
    const values = extractBulkValues(reply).filter(
      (v): v is string => v !== null
    );
    expect(values.sort()).toEqual(['user:1', 'user:2']);
  });

  it('returns all keys with *', () => {
    const db = createDb();
    db.set('a', 'string', 'raw', 'v');
    db.set('b', 'string', 'raw', 'v');
    const reply = scanCmd.keys(db, ['*']);
    const values = extractBulkValues(reply).filter(
      (v): v is string => v !== null
    );
    expect(values.sort()).toEqual(['a', 'b']);
  });

  it('returns empty array for no matches', () => {
    const db = createDb();
    db.set('a', 'string', 'raw', 'v');
    const reply = scanCmd.keys(db, ['z*']);
    expect(reply).toEqual({ kind: 'array', value: [] });
  });

  it('uses glob patterns', () => {
    const db = createDb();
    db.set('key1', 'string', 'raw', 'v');
    db.set('key2', 'string', 'raw', 'v');
    db.set('kex3', 'string', 'raw', 'v');
    const reply = scanCmd.keys(db, ['key?']);
    const values = extractBulkValues(reply).filter(
      (v): v is string => v !== null
    );
    expect(values.sort()).toEqual(['key1', 'key2']);
  });
});

describe('SCAN', () => {
  it('iterates all keys with cursor 0', () => {
    const db = createDb();
    db.set('a', 'string', 'raw', 'v');
    db.set('b', 'string', 'raw', 'v');
    db.set('c', 'string', 'raw', 'v');

    const reply = scanCmd.scan(db, ['0', 'COUNT', '100']);
    expect(reply.kind).toBe('array');
    if (reply.kind !== 'array') return;

    const cursorReply = reply.value[0];
    const keysReply = reply.value[1];
    expect(cursorReply).toEqual({ kind: 'bulk', value: '0' });
    expect(keysReply?.kind).toBe('array');
    if (keysReply?.kind !== 'array') return;
    expect(keysReply.value).toHaveLength(3);
  });

  it('respects MATCH filter', () => {
    const db = createDb();
    db.set('user:1', 'string', 'raw', 'v');
    db.set('user:2', 'string', 'raw', 'v');
    db.set('admin:1', 'string', 'raw', 'v');

    const reply = scanCmd.scan(db, ['0', 'MATCH', 'user:*', 'COUNT', '100']);
    if (reply.kind !== 'array') return;
    const keysReply = reply.value[1];
    if (keysReply?.kind !== 'array') return;
    const keys = keysReply.value
      .filter((r): r is Reply & { kind: 'bulk' } => r.kind === 'bulk')
      .map((r) => r.value)
      .filter((v): v is string => v !== null);
    expect(keys.sort()).toEqual(['user:1', 'user:2']);
  });

  it('respects TYPE filter', () => {
    const db = createDb();
    db.set('str', 'string', 'raw', 'v');
    db.set('lst', 'list', 'quicklist', []);

    const reply = scanCmd.scan(db, ['0', 'TYPE', 'string', 'COUNT', '100']);
    if (reply.kind !== 'array') return;
    const keysReply = reply.value[1];
    if (keysReply?.kind !== 'array') return;
    expect(keysReply.value).toHaveLength(1);
  });

  it('returns cursor 0 when iteration complete', () => {
    const db = createDb();
    db.set('a', 'string', 'raw', 'v');

    const reply = scanCmd.scan(db, ['0', 'COUNT', '100']);
    if (reply.kind !== 'array') return;
    expect(reply.value[0]).toEqual({ kind: 'bulk', value: '0' });
  });

  it('paginates with cursor', () => {
    const db = createDb();
    for (let i = 0; i < 20; i++) {
      db.set(`key${i}`, 'string', 'raw', 'v');
    }

    const allKeys: string[] = [];
    let cursor = '0';
    let iterations = 0;

    do {
      const reply = scanCmd.scan(db, [cursor, 'COUNT', '5']);
      if (reply.kind !== 'array') break;
      const cursorReply = reply.value[0];
      if (cursorReply?.kind !== 'bulk' || cursorReply.value === null) break;
      cursor = cursorReply.value;
      const keysReply = reply.value[1];
      if (keysReply?.kind === 'array') {
        for (const kr of keysReply.value) {
          if (kr.kind === 'bulk' && kr.value !== null) allKeys.push(kr.value);
        }
      }
      iterations++;
    } while (cursor !== '0' && iterations < 100);

    expect(allKeys).toHaveLength(20);
    expect(new Set(allKeys).size).toBe(20);
  });

  it('returns empty for empty db', () => {
    const db = createDb();
    const reply = scanCmd.scan(db, ['0']);
    if (reply.kind !== 'array') return;
    expect(reply.value[0]).toEqual({ kind: 'bulk', value: '0' });
    expect(reply.value[1]).toEqual({ kind: 'array', value: [] });
  });
});
