import { describe, it, expect } from 'vitest';
import {
  jemallocSize,
  sdsAllocSize,
  estimateKeyMemory,
  estimateKeyMemoryWithSamples,
  parseMemorySize,
} from './memory.ts';
import type { RedisEntry } from './types.ts';

function entry(
  type: RedisEntry['type'],
  encoding: RedisEntry['encoding'],
  value: unknown
): RedisEntry {
  return { type, encoding, value, lruClock: 0, lruFreq: 0, lfuLastDecrTime: 0 };
}

describe('jemallocSize', () => {
  it('rounds up to nearest bin', () => {
    expect(jemallocSize(1)).toBe(8);
    expect(jemallocSize(8)).toBe(8);
    expect(jemallocSize(9)).toBe(16);
    expect(jemallocSize(17)).toBe(24);
    expect(jemallocSize(25)).toBe(32);
    expect(jemallocSize(64)).toBe(64);
    expect(jemallocSize(65)).toBe(80);
    expect(jemallocSize(128)).toBe(128);
    expect(jemallocSize(129)).toBe(160);
  });

  it('rounds large sizes to page boundaries', () => {
    expect(jemallocSize(4097)).toBe(8192);
    expect(jemallocSize(8192)).toBe(8192);
    expect(jemallocSize(10000)).toBe(12288);
  });
});

describe('sdsAllocSize', () => {
  it('includes sds header and null terminator', () => {
    // sdshdr8 (3 bytes header) + 3 bytes data + 1 null = 7 → jemalloc bin 8
    expect(sdsAllocSize(3)).toBe(8);
    // sdshdr8 + 10 + 1 = 14 → 16
    expect(sdsAllocSize(10)).toBe(16);
    // sdshdr16 for strings >= 256 bytes
    expect(sdsAllocSize(256)).toBeGreaterThanOrEqual(256);
  });
});

describe('estimateKeyMemory', () => {
  describe('string values', () => {
    it('estimates int-encoded string', () => {
      const e = entry('string', 'int', '42');
      const mem = estimateKeyMemory('k', e, false);
      // dictEntry(56) + key SDS(8) + robj(16) + value(0) = 80
      expect(mem).toBe(80);
    });

    it('estimates embstr-encoded string', () => {
      const e = entry('string', 'embstr', 'hello');
      const mem = estimateKeyMemory('k', e, false);
      // Base: 56 + 8 + 16 = 80
      // embstr value: jemallocSize(16 + 3 + 5 + 1) - 16 = jemallocSize(25) - 16 = 32 - 16 = 16
      expect(mem).toBe(96);
    });

    it('estimates raw-encoded string', () => {
      const e = entry('string', 'raw', 'a'.repeat(100));
      const mem = estimateKeyMemory('k', e, false);
      // Base: 56 + 8 + 16 = 80
      // raw value: sdsAllocSize(100) = jemallocSize(3 + 100 + 1) = jemallocSize(104) = 112
      expect(mem).toBe(80 + 112);
    });

    it('adds expiry overhead when expiry is set', () => {
      const e = entry('string', 'int', '1');
      const withExpiry = estimateKeyMemory('k', e, true);
      const withoutExpiry = estimateKeyMemory('k', e, false);
      expect(withExpiry).toBeGreaterThan(withoutExpiry);
      // expiry adds DICT_ENTRY_SIZE(56) + 8 = 64
      expect(withExpiry - withoutExpiry).toBe(64);
    });
  });

  describe('hash values', () => {
    it('estimates listpack-encoded hash', () => {
      const hash = new Map([['f1', 'v1']]);
      const e = entry('hash', 'listpack', hash);
      const mem = estimateKeyMemory('k', e, false);
      expect(mem).toBeGreaterThan(80);
    });

    it('estimates hashtable-encoded hash', () => {
      const hash = new Map<string, string>();
      for (let i = 0; i < 200; i++) {
        hash.set(`field${i}`, `value${i}`);
      }
      const e = entry('hash', 'hashtable', hash);
      const mem = estimateKeyMemory('k', e, false);
      // Should be significantly larger than listpack
      expect(mem).toBeGreaterThan(1000);
    });
  });

  describe('list values', () => {
    it('estimates listpack-encoded list', () => {
      const e = entry('list', 'listpack', ['a', 'b', 'c']);
      const mem = estimateKeyMemory('k', e, false);
      expect(mem).toBeGreaterThan(80);
    });

    it('estimates quicklist-encoded list', () => {
      const items: string[] = [];
      for (let i = 0; i < 200; i++) items.push(`item${i}`);
      const e = entry('list', 'quicklist', items);
      const mem = estimateKeyMemory('k', e, false);
      expect(mem).toBeGreaterThan(500);
    });
  });

  describe('set values', () => {
    it('estimates intset-encoded set', () => {
      const s = new Set(['1', '2', '3']);
      const e = entry('set', 'intset', s);
      const mem = estimateKeyMemory('k', e, false);
      expect(mem).toBeGreaterThan(80);
    });

    it('estimates hashtable-encoded set', () => {
      const s = new Set<string>();
      for (let i = 0; i < 200; i++) s.add(`member${i}`);
      const e = entry('set', 'hashtable', s);
      const mem = estimateKeyMemory('k', e, false);
      expect(mem).toBeGreaterThan(1000);
    });
  });

  describe('sorted set values', () => {
    it('estimates listpack-encoded zset', () => {
      const z = { dict: new Map([['a', 1]]) };
      const e = entry('zset', 'listpack', z);
      const mem = estimateKeyMemory('k', e, false);
      expect(mem).toBeGreaterThan(80);
    });

    it('estimates skiplist-encoded zset', () => {
      const dict = new Map<string, number>();
      for (let i = 0; i < 200; i++) dict.set(`member${i}`, i);
      const e = entry('zset', 'skiplist', { dict });
      const mem = estimateKeyMemory('k', e, false);
      expect(mem).toBeGreaterThan(1000);
    });
  });

  it('returns positive value for all types', () => {
    const cases: [RedisEntry['type'], RedisEntry['encoding'], unknown][] = [
      ['string', 'int', '42'],
      ['string', 'embstr', 'hi'],
      ['string', 'raw', 'a'.repeat(50)],
      ['hash', 'listpack', new Map([['f', 'v']])],
      ['hash', 'hashtable', new Map([['f', 'v']])],
      ['list', 'listpack', ['a']],
      ['list', 'quicklist', ['a']],
      ['set', 'intset', new Set(['1'])],
      ['set', 'hashtable', new Set(['a'])],
      ['zset', 'listpack', { dict: new Map([['a', 1]]) }],
      ['zset', 'skiplist', { dict: new Map([['a', 1]]) }],
      ['stream', 'stream', null],
    ];
    for (const [type, encoding, value] of cases) {
      const e = entry(type, encoding, value);
      const mem = estimateKeyMemory('key', e, false);
      expect(mem).toBeGreaterThan(0);
    }
  });
});

describe('estimateKeyMemoryWithSamples', () => {
  it('with samples=0, returns full estimate', () => {
    const hash = new Map<string, string>();
    for (let i = 0; i < 100; i++) hash.set(`f${i}`, `v${i}`);
    const e = entry('hash', 'hashtable', hash);
    const full = estimateKeyMemory('k', e, false);
    const sampled = estimateKeyMemoryWithSamples('k', e, false, 0);
    expect(sampled).toBe(full);
  });

  it('with samples>0, returns reasonable estimate', () => {
    const hash = new Map<string, string>();
    for (let i = 0; i < 100; i++) hash.set(`field${i}`, `value${i}`);
    const e = entry('hash', 'hashtable', hash);
    const full = estimateKeyMemory('k', e, false);
    const sampled = estimateKeyMemoryWithSamples('k', e, false, 5);
    // Sampled estimate should be in the same ballpark
    expect(sampled).toBeGreaterThan(full * 0.5);
    expect(sampled).toBeLessThan(full * 2);
  });

  it('handles string types without sampling', () => {
    const e = entry('string', 'raw', 'hello');
    const full = estimateKeyMemory('k', e, false);
    const sampled = estimateKeyMemoryWithSamples('k', e, false, 5);
    expect(sampled).toBe(full);
  });
});

describe('parseMemorySize', () => {
  it('parses plain bytes', () => {
    expect(parseMemorySize('0')).toBe(0);
    expect(parseMemorySize('1024')).toBe(1024);
    expect(parseMemorySize('1048576')).toBe(1048576);
  });

  it('parses kilobytes', () => {
    expect(parseMemorySize('1kb')).toBe(1024);
    expect(parseMemorySize('10k')).toBe(10240);
    expect(parseMemorySize('100KB')).toBe(102400);
  });

  it('parses megabytes', () => {
    expect(parseMemorySize('1mb')).toBe(1048576);
    expect(parseMemorySize('10m')).toBe(10485760);
    expect(parseMemorySize('256MB')).toBe(268435456);
  });

  it('parses gigabytes', () => {
    expect(parseMemorySize('1gb')).toBe(1073741824);
    expect(parseMemorySize('2g')).toBe(2147483648);
    expect(parseMemorySize('4GB')).toBe(4294967296);
  });

  it('returns -1 for invalid input', () => {
    expect(parseMemorySize('')).toBe(0);
    expect(parseMemorySize('abc')).toBe(-1);
    expect(parseMemorySize('-1')).toBe(-1);
    expect(parseMemorySize('1tb')).toBe(-1);
  });

  it('handles whitespace', () => {
    expect(parseMemorySize('  1024  ')).toBe(1024);
    expect(parseMemorySize(' 1mb ')).toBe(1048576);
  });
});
