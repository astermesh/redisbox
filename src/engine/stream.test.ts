import { describe, it, expect } from 'vitest';
import {
  RedisStream,
  parseStreamId,
  streamIdToString,
  compareStreamIds,
} from './stream.ts';

describe('parseStreamId', () => {
  it('parses ms-seq format', () => {
    expect(parseStreamId('1526985054069-0')).toEqual({
      ms: 1526985054069,
      seq: 0,
    });
  });

  it('parses ms-only format (defaults seq to 0)', () => {
    expect(parseStreamId('1000')).toEqual({ ms: 1000, seq: 0 });
  });

  it('returns null for non-numeric ms', () => {
    expect(parseStreamId('abc-0')).toBeNull();
  });

  it('returns null for non-numeric seq', () => {
    expect(parseStreamId('100-abc')).toBeNull();
  });

  it('returns null for negative ms', () => {
    expect(parseStreamId('-1-0')).toBeNull();
  });

  it('returns null for empty string', () => {
    expect(parseStreamId('')).toBeNull();
  });

  it('parses 0-0', () => {
    expect(parseStreamId('0-0')).toEqual({ ms: 0, seq: 0 });
  });
});

describe('streamIdToString', () => {
  it('formats ms-seq', () => {
    expect(streamIdToString({ ms: 1000, seq: 5 })).toBe('1000-5');
  });

  it('formats 0-0', () => {
    expect(streamIdToString({ ms: 0, seq: 0 })).toBe('0-0');
  });
});

describe('compareStreamIds', () => {
  it('returns 0 for equal IDs', () => {
    expect(compareStreamIds({ ms: 1, seq: 2 }, { ms: 1, seq: 2 })).toBe(0);
  });

  it('returns -1 when a < b by ms', () => {
    expect(compareStreamIds({ ms: 1, seq: 0 }, { ms: 2, seq: 0 })).toBe(-1);
  });

  it('returns 1 when a > b by ms', () => {
    expect(compareStreamIds({ ms: 2, seq: 0 }, { ms: 1, seq: 0 })).toBe(1);
  });

  it('returns -1 when a < b by seq', () => {
    expect(compareStreamIds({ ms: 1, seq: 0 }, { ms: 1, seq: 1 })).toBe(-1);
  });

  it('returns 1 when a > b by seq', () => {
    expect(compareStreamIds({ ms: 1, seq: 1 }, { ms: 1, seq: 0 })).toBe(1);
  });
});

describe('RedisStream', () => {
  describe('auto-generate ID', () => {
    it('generates ms-0 for first entry', () => {
      const s = new RedisStream();
      const id = s.resolveNextId('*', 1000);
      expect(id).toEqual({ ms: 1000, seq: 0 });
    });

    it('increments seq for same ms', () => {
      const s = new RedisStream();
      const id1 = s.resolveNextId('*', 1000);
      expect('error' in id1).toBe(false);
      s.addEntry(id1 as { ms: number; seq: number }, [['a', '1']]);

      const id2 = s.resolveNextId('*', 1000);
      expect(id2).toEqual({ ms: 1000, seq: 1 });
    });

    it('resets seq for higher ms', () => {
      const s = new RedisStream();
      const id1 = s.resolveNextId('*', 1000);
      s.addEntry(id1 as { ms: number; seq: number }, [['a', '1']]);

      const id2 = s.resolveNextId('*', 2000);
      expect(id2).toEqual({ ms: 2000, seq: 0 });
    });

    it('keeps lastId ms when clock goes backward', () => {
      const s = new RedisStream();
      const id1 = s.resolveNextId('*', 5000);
      s.addEntry(id1 as { ms: number; seq: number }, [['a', '1']]);

      const id2 = s.resolveNextId('*', 3000);
      expect(id2).toEqual({ ms: 5000, seq: 1 });
    });
  });

  describe('partial auto ID', () => {
    it('generates ms-0 for new ms', () => {
      const s = new RedisStream();
      const id = s.resolveNextId('500-*', 1000);
      expect(id).toEqual({ ms: 500, seq: 0 });
    });

    it('increments seq for same ms as lastId', () => {
      const s = new RedisStream();
      s.addEntry({ ms: 500, seq: 3 }, [['a', '1']]);
      const id = s.resolveNextId('500-*', 1000);
      expect(id).toEqual({ ms: 500, seq: 4 });
    });

    it('errors when ms < lastId ms', () => {
      const s = new RedisStream();
      s.addEntry({ ms: 500, seq: 0 }, [['a', '1']]);
      const id = s.resolveNextId('400-*', 1000);
      expect(id).toHaveProperty('error');
    });
  });

  describe('explicit ID', () => {
    it('accepts valid explicit ID', () => {
      const s = new RedisStream();
      const id = s.resolveNextId('100-5', 1000);
      expect(id).toEqual({ ms: 100, seq: 5 });
    });

    it('rejects 0-0', () => {
      const s = new RedisStream();
      const id = s.resolveNextId('0-0', 1000);
      expect(id).toHaveProperty('error');
    });

    it('rejects ID <= lastId', () => {
      const s = new RedisStream();
      s.addEntry({ ms: 100, seq: 5 }, [['a', '1']]);
      const id = s.resolveNextId('100-5', 1000);
      expect(id).toHaveProperty('error');
    });

    it('rejects ID < lastId', () => {
      const s = new RedisStream();
      s.addEntry({ ms: 100, seq: 5 }, [['a', '1']]);
      const id = s.resolveNextId('100-3', 1000);
      expect(id).toHaveProperty('error');
    });
  });

  describe('trimByMaxlen', () => {
    it('trims oldest entries to maxlen', () => {
      const s = new RedisStream();
      s.addEntry({ ms: 1, seq: 0 }, [['a', '1']]);
      s.addEntry({ ms: 2, seq: 0 }, [['b', '2']]);
      s.addEntry({ ms: 3, seq: 0 }, [['c', '3']]);

      const removed = s.trimByMaxlen(2, false);
      expect(removed).toBe(1);
      expect(s.length).toBe(2);
      expect(s.firstEntry()?.id).toBe('2-0');
    });

    it('does nothing when length <= maxlen', () => {
      const s = new RedisStream();
      s.addEntry({ ms: 1, seq: 0 }, [['a', '1']]);
      const removed = s.trimByMaxlen(5, false);
      expect(removed).toBe(0);
      expect(s.length).toBe(1);
    });

    it('maxlen 0 removes all entries', () => {
      const s = new RedisStream();
      s.addEntry({ ms: 1, seq: 0 }, [['a', '1']]);
      s.addEntry({ ms: 2, seq: 0 }, [['b', '2']]);
      const removed = s.trimByMaxlen(0, false);
      expect(removed).toBe(2);
      expect(s.length).toBe(0);
    });
  });

  describe('trimByMinid', () => {
    it('removes entries with ID < minId', () => {
      const s = new RedisStream();
      s.addEntry({ ms: 1, seq: 0 }, [['a', '1']]);
      s.addEntry({ ms: 2, seq: 0 }, [['b', '2']]);
      s.addEntry({ ms: 3, seq: 0 }, [['c', '3']]);

      const removed = s.trimByMinid({ ms: 2, seq: 0 }, false);
      expect(removed).toBe(1);
      expect(s.length).toBe(2);
      expect(s.firstEntry()?.id).toBe('2-0');
    });

    it('does nothing when all entries >= minId', () => {
      const s = new RedisStream();
      s.addEntry({ ms: 5, seq: 0 }, [['a', '1']]);
      const removed = s.trimByMinid({ ms: 3, seq: 0 }, false);
      expect(removed).toBe(0);
      expect(s.length).toBe(1);
    });
  });

  describe('entry access', () => {
    it('firstEntry returns first entry', () => {
      const s = new RedisStream();
      s.addEntry({ ms: 1, seq: 0 }, [['a', '1']]);
      s.addEntry({ ms: 2, seq: 0 }, [['b', '2']]);
      expect(s.firstEntry()?.id).toBe('1-0');
    });

    it('lastEntry returns last entry', () => {
      const s = new RedisStream();
      s.addEntry({ ms: 1, seq: 0 }, [['a', '1']]);
      s.addEntry({ ms: 2, seq: 0 }, [['b', '2']]);
      expect(s.lastEntry()?.id).toBe('2-0');
    });

    it('firstEntry returns null for empty stream', () => {
      const s = new RedisStream();
      expect(s.firstEntry()).toBeNull();
    });

    it('lastEntry returns null for empty stream', () => {
      const s = new RedisStream();
      expect(s.lastEntry()).toBeNull();
    });
  });
});
