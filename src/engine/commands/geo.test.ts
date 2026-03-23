import { describe, it, expect } from 'vitest';
import { RedisEngine } from '../engine.ts';
import type { Database } from '../database.ts';
import type { Reply } from '../types.ts';
import * as geo from './geo.ts';
import * as zset from './sorted-set.ts';

let rngValue = 0.5;
function createDb(): { db: Database; engine: RedisEngine; rng: () => number } {
  rngValue = 0.5;
  const rng = () => rngValue;
  const engine = new RedisEngine({ clock: () => 1000, rng });
  return { db: engine.db(0), engine, rng };
}

function bulk(value: string | null): Reply {
  return { kind: 'bulk', value };
}

function integer(value: number | bigint): Reply {
  return { kind: 'integer', value };
}

function err(prefix: string, message: string): Reply {
  return { kind: 'error', prefix, message };
}

const ZERO = integer(0);
const ONE = integer(1);
const EMPTY_ARRAY: Reply = { kind: 'array', value: [] };
const WRONGTYPE: Reply = {
  kind: 'error',
  prefix: 'WRONGTYPE',
  message: 'Operation against a key holding the wrong kind of value',
};

// Helper to extract numeric value from bulk reply (for distance/coord checks)
function bulkNum(reply: Reply): number {
  if (reply.kind === 'bulk' && reply.value !== null) {
    return parseFloat(reply.value);
  }
  return NaN;
}

// Helper to extract array items
function arrayItems(reply: Reply): Reply[] {
  if (reply.kind === 'array') return reply.value as Reply[];
  return [];
}

// Helper to add standard Sicily cities
function addSicily(db: Database, rng: () => number, key = 'k'): void {
  geo.geoadd(
    db,
    [
      key,
      '13.361389',
      '38.115556',
      'Palermo',
      '15.087269',
      '37.502669',
      'Catania',
    ],
    rng
  );
}

// --- GEOADD ---

describe('GEOADD', () => {
  it('adds a single member', () => {
    const { db, rng } = createDb();
    expect(
      geo.geoadd(db, ['k', '13.361389', '38.115556', 'Palermo'], rng)
    ).toEqual(ONE);
  });

  it('adds multiple members', () => {
    const { db, rng } = createDb();
    expect(
      geo.geoadd(
        db,
        [
          'k',
          '13.361389',
          '38.115556',
          'Palermo',
          '15.087269',
          '37.502669',
          'Catania',
        ],
        rng
      )
    ).toEqual(integer(2));
  });

  it('updates existing member (returns 0)', () => {
    const { db, rng } = createDb();
    geo.geoadd(db, ['k', '13.361389', '38.115556', 'Palermo'], rng);
    expect(geo.geoadd(db, ['k', '14.0', '39.0', 'Palermo'], rng)).toEqual(ZERO);
  });

  it('NX flag prevents update', () => {
    const { db, rng } = createDb();
    geo.geoadd(db, ['k', '13.361389', '38.115556', 'Palermo'], rng);
    expect(geo.geoadd(db, ['k', 'NX', '14.0', '39.0', 'Palermo'], rng)).toEqual(
      ZERO
    );
    // Position should not change
    const pos = geo.geopos(db, ['k', 'Palermo']);
    const items = arrayItems(pos);
    expect(items.length).toBe(1);
    const coords = arrayItems(items[0] as Reply);
    expect(Math.abs(bulkNum(coords[0] as Reply) - 13.361389)).toBeLessThan(
      0.001
    );
  });

  it('NX adds new member but skips existing', () => {
    const { db, rng } = createDb();
    geo.geoadd(db, ['k', '13.361389', '38.115556', 'Palermo'], rng);
    expect(
      geo.geoadd(
        db,
        [
          'k',
          'NX',
          '15.087269',
          '37.502669',
          'Catania',
          '14.0',
          '39.0',
          'Palermo',
        ],
        rng
      )
    ).toEqual(ONE); // only Catania added
  });

  it('XX flag only updates existing', () => {
    const { db, rng } = createDb();
    geo.geoadd(db, ['k', '13.361389', '38.115556', 'Palermo'], rng);
    // XX: update Palermo, skip NewCity
    expect(
      geo.geoadd(
        db,
        ['k', 'XX', '14.0', '39.0', 'Palermo', '15.0', '37.0', 'NewCity'],
        rng
      )
    ).toEqual(ZERO);
    // NewCity should not exist
    const pos = geo.geopos(db, ['k', 'NewCity']);
    const items = arrayItems(pos);
    expect(items[0]).toEqual(bulk(null));
  });

  it('CH flag counts changes', () => {
    const { db, rng } = createDb();
    geo.geoadd(db, ['k', '13.361389', '38.115556', 'Palermo'], rng);
    // CH: count Palermo as changed + add Catania
    const result = geo.geoadd(
      db,
      [
        'k',
        'CH',
        '14.0',
        '39.0',
        'Palermo',
        '15.087269',
        '37.502669',
        'Catania',
      ],
      rng
    );
    expect(result).toEqual(integer(2));
  });

  it('NX and XX together returns error', () => {
    const { db, rng } = createDb();
    expect(geo.geoadd(db, ['k', 'NX', 'XX', '13.0', '38.0', 'a'], rng)).toEqual(
      err('ERR', 'XX and NX options at the same time are not compatible')
    );
  });

  it('rejects invalid longitude', () => {
    const { db, rng } = createDb();
    const result = geo.geoadd(db, ['k', '181.0', '38.0', 'bad'], rng);
    expect(result.kind).toBe('error');
    expect((result as { prefix: string }).prefix).toBe('ERR');
  });

  it('rejects invalid latitude', () => {
    const { db, rng } = createDb();
    const result = geo.geoadd(db, ['k', '13.0', '86.0', 'bad'], rng);
    expect(result.kind).toBe('error');
  });

  it('rejects non-numeric coordinates', () => {
    const { db, rng } = createDb();
    const result = geo.geoadd(db, ['k', 'abc', '38.0', 'bad'], rng);
    expect(result).toEqual(err('ERR', 'value is not a valid float'));
  });

  it('returns WRONGTYPE for non-zset key', () => {
    const { db, rng } = createDb();
    db.set('k', 'string', 'raw', 'hello');
    expect(geo.geoadd(db, ['k', '13.0', '38.0', 'a'], rng)).toEqual(WRONGTYPE);
  });

  it('wrong number of arguments', () => {
    const { db, rng } = createDb();
    expect(geo.geoadd(db, ['k'], rng).kind).toBe('error');
    expect(geo.geoadd(db, ['k', '13.0', '38.0'], rng).kind).toBe('error');
  });

  it('stores data as zset (TYPE returns zset)', () => {
    const { db, rng } = createDb();
    geo.geoadd(db, ['k', '13.361389', '38.115556', 'Palermo'], rng);
    const entry = db.get('k');
    expect(entry).toBeTruthy();
    if (entry) {
      expect(entry.type).toBe('zset');
    }
  });
});

// --- GEOPOS ---

describe('GEOPOS', () => {
  it('returns position with high precision', () => {
    const { db, rng } = createDb();
    geo.geoadd(db, ['k', '13.361389', '38.115556', 'Palermo'], rng);
    const result = geo.geopos(db, ['k', 'Palermo']);
    const items = arrayItems(result);
    expect(items.length).toBe(1);
    const coords = arrayItems(items[0] as Reply);
    expect(coords.length).toBe(2);
    // At least 6 decimal places of precision (acceptance criteria)
    const lon = bulkNum(coords[0] as Reply);
    const lat = bulkNum(coords[1] as Reply);
    expect(Math.abs(lon - 13.361389)).toBeLessThan(0.0001);
    expect(Math.abs(lat - 38.115556)).toBeLessThan(0.0001);
  });

  it('returns nil for non-existing member', () => {
    const { db, rng } = createDb();
    geo.geoadd(db, ['k', '13.361389', '38.115556', 'Palermo'], rng);
    const result = geo.geopos(db, ['k', 'NonExistent']);
    const items = arrayItems(result);
    expect(items.length).toBe(1);
    expect(items[0]).toEqual(bulk(null));
  });

  it('returns nil for non-existing key', () => {
    const { db } = createDb();
    const result = geo.geopos(db, ['k', 'a']);
    const items = arrayItems(result);
    expect(items.length).toBe(1);
    expect(items[0]).toEqual(bulk(null));
  });

  it('returns empty array when no members specified', () => {
    const { db, rng } = createDb();
    geo.geoadd(db, ['k', '13.361389', '38.115556', 'Palermo'], rng);
    const result = geo.geopos(db, ['k']);
    expect(result).toEqual({ kind: 'array', value: [] });
  });

  it('returns mixed results for existing and non-existing members', () => {
    const { db, rng } = createDb();
    geo.geoadd(db, ['k', '13.361389', '38.115556', 'Palermo'], rng);
    const result = geo.geopos(db, ['k', 'Palermo', 'NonExistent']);
    const items = arrayItems(result);
    expect(items.length).toBe(2);
    expect(arrayItems(items[0] as Reply).length).toBe(2);
    expect(items[1]).toEqual(bulk(null));
  });

  it('returns WRONGTYPE for non-zset key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'hello');
    expect(geo.geopos(db, ['k', 'a'])).toEqual(WRONGTYPE);
  });
});

// --- GEODIST ---

describe('GEODIST', () => {
  it('returns distance in meters matching Redis (166274.1516)', () => {
    const { db, rng } = createDb();
    addSicily(db, rng);
    const result = geo.geodist(db, ['k', 'Palermo', 'Catania']);
    expect(result.kind).toBe('bulk');
    if (result.kind === 'bulk' && result.value !== null) {
      // Redis returns exactly "166274.1516" for Palermo-Catania in meters
      expect(result.value).toBe('166274.1516');
    }
  });

  it('returns distance in km matching Redis (166.2742)', () => {
    const { db, rng } = createDb();
    addSicily(db, rng);
    const result = geo.geodist(db, ['k', 'Palermo', 'Catania', 'km']);
    expect(result.kind).toBe('bulk');
    if (result.kind === 'bulk' && result.value !== null) {
      expect(result.value).toBe('166.2742');
    }
  });

  it('returns distance in miles matching Redis (103.3182)', () => {
    const { db, rng } = createDb();
    addSicily(db, rng);
    const result = geo.geodist(db, ['k', 'Palermo', 'Catania', 'mi']);
    expect(result.kind).toBe('bulk');
    if (result.kind === 'bulk' && result.value !== null) {
      expect(result.value).toBe('103.3182');
    }
  });

  it('returns distance in feet matching Redis (545518.8700)', () => {
    const { db, rng } = createDb();
    addSicily(db, rng);
    const result = geo.geodist(db, ['k', 'Palermo', 'Catania', 'ft']);
    expect(result.kind).toBe('bulk');
    if (result.kind === 'bulk' && result.value !== null) {
      expect(result.value).toBe('545518.8700');
    }
  });

  it('returns distance with 4 decimal places', () => {
    const { db, rng } = createDb();
    addSicily(db, rng);
    const result = geo.geodist(db, ['k', 'Palermo', 'Catania']);
    expect(result.kind).toBe('bulk');
    if (result.kind === 'bulk' && result.value !== null) {
      expect(result.value).toMatch(/^\d+\.\d{4}$/);
    }
  });

  it('unit is case-insensitive', () => {
    const { db, rng } = createDb();
    addSicily(db, rng);
    const r1 = geo.geodist(db, ['k', 'Palermo', 'Catania', 'KM']);
    const r2 = geo.geodist(db, ['k', 'Palermo', 'Catania', 'km']);
    expect(r1).toEqual(r2);
  });

  it('returns nil when member does not exist', () => {
    const { db, rng } = createDb();
    geo.geoadd(db, ['k', '13.361389', '38.115556', 'Palermo'], rng);
    expect(geo.geodist(db, ['k', 'Palermo', 'NonExistent'])).toEqual(
      bulk(null)
    );
  });

  it('returns nil when both members do not exist', () => {
    const { db, rng } = createDb();
    geo.geoadd(db, ['k', '13.361389', '38.115556', 'Palermo'], rng);
    expect(geo.geodist(db, ['k', 'A', 'B'])).toEqual(bulk(null));
  });

  it('returns nil for non-existing key', () => {
    const { db } = createDb();
    expect(geo.geodist(db, ['k', 'a', 'b'])).toEqual(bulk(null));
  });

  it('rejects unsupported unit with correct error', () => {
    const { db, rng } = createDb();
    geo.geoadd(db, ['k', '13.0', '38.0', 'a', '14.0', '39.0', 'b'], rng);
    expect(geo.geodist(db, ['k', 'a', 'b', 'lightyears'])).toEqual(
      err('ERR', 'unsupported unit provided. please use M, KM, FT, MI')
    );
  });

  it('returns WRONGTYPE for non-zset key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'hello');
    expect(geo.geodist(db, ['k', 'a', 'b'])).toEqual(WRONGTYPE);
  });
});

// --- GEOHASH ---

describe('GEOHASH', () => {
  it('returns 11-char geohash matching Redis for Palermo', () => {
    const { db, rng } = createDb();
    geo.geoadd(db, ['k', '13.361389', '38.115556', 'Palermo'], rng);
    const result = geo.geohash(db, ['k', 'Palermo']);
    const items = arrayItems(result);
    expect(items.length).toBe(1);
    const hash = items[0] as Reply;
    expect(hash.kind).toBe('bulk');
    if (hash.kind === 'bulk' && hash.value !== null) {
      expect(hash.value.length).toBe(11);
      // Redis returns "sqc8b49rny0" for Palermo
      expect(hash.value.startsWith('sqc8b49rny')).toBe(true);
    }
  });

  it('returns correct geohash for Catania', () => {
    const { db, rng } = createDb();
    geo.geoadd(db, ['k', '15.087269', '37.502669', 'Catania'], rng);
    const result = geo.geohash(db, ['k', 'Catania']);
    const items = arrayItems(result);
    const hash = items[0] as Reply;
    if (hash.kind === 'bulk' && hash.value !== null) {
      expect(hash.value.length).toBe(11);
      // Redis returns "sqdtr74hyu0" for Catania
      expect(hash.value.startsWith('sqdtr74hyu')).toBe(true);
    }
  });

  it('returns multiple geohashes', () => {
    const { db, rng } = createDb();
    addSicily(db, rng);
    const result = geo.geohash(db, ['k', 'Palermo', 'Catania']);
    const items = arrayItems(result);
    expect(items.length).toBe(2);
    expect((items[0] as { kind: string; value: string }).value.length).toBe(11);
    expect((items[1] as { kind: string; value: string }).value.length).toBe(11);
  });

  it('returns nil for non-existing member', () => {
    const { db, rng } = createDb();
    geo.geoadd(db, ['k', '13.361389', '38.115556', 'Palermo'], rng);
    const result = geo.geohash(db, ['k', 'NonExistent']);
    const items = arrayItems(result);
    expect(items[0]).toEqual(bulk(null));
  });

  it('returns empty array when no members specified', () => {
    const { db, rng } = createDb();
    geo.geoadd(db, ['k', '13.361389', '38.115556', 'Palermo'], rng);
    const result = geo.geohash(db, ['k']);
    expect(result).toEqual({ kind: 'array', value: [] });
  });

  it('returns WRONGTYPE for non-zset key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'hello');
    expect(geo.geohash(db, ['k', 'a'])).toEqual(WRONGTYPE);
  });

  it('returns nil for non-existing key members', () => {
    const { db } = createDb();
    const result = geo.geohash(db, ['nonexistent', 'a']);
    const items = arrayItems(result);
    expect(items[0]).toEqual(bulk(null));
  });
});

// --- GEOSEARCH ---

describe('GEOSEARCH', () => {
  function addSicilyCities(db: Database, rng: () => number): void {
    geo.geoadd(
      db,
      [
        'k',
        '13.361389',
        '38.115556',
        'Palermo',
        '15.087269',
        '37.502669',
        'Catania',
        '2.349014',
        '48.864716',
        'Paris',
      ],
      rng
    );
  }

  it('FROMMEMBER BYRADIUS returns members within radius', () => {
    const { db, rng } = createDb();
    addSicilyCities(db, rng);
    const result = geo.geosearch(db, [
      'k',
      'FROMMEMBER',
      'Palermo',
      'BYRADIUS',
      '200',
      'km',
      'ASC',
    ]);
    const items = arrayItems(result);
    // Palermo and Catania within 200km, Paris is not
    expect(items.length).toBe(2);
    // ASC order: Palermo first (distance 0), then Catania
    expect(items[0]).toEqual(bulk('Palermo'));
    expect(items[1]).toEqual(bulk('Catania'));
  });

  it('FROMLONLAT BYRADIUS returns members within radius', () => {
    const { db, rng } = createDb();
    addSicilyCities(db, rng);
    const result = geo.geosearch(db, [
      'k',
      'FROMLONLAT',
      '13.361389',
      '38.115556',
      'BYRADIUS',
      '200',
      'km',
      'ASC',
    ]);
    const items = arrayItems(result);
    expect(items.length).toBe(2);
  });

  it('BYBOX returns members within box', () => {
    const { db, rng } = createDb();
    addSicilyCities(db, rng);
    const result = geo.geosearch(db, [
      'k',
      'FROMLONLAT',
      '14.0',
      '38.0',
      'BYBOX',
      '400',
      '200',
      'km',
      'ASC',
    ]);
    const items = arrayItems(result);
    // Both Palermo and Catania should be within the box
    expect(items.length).toBe(2);
  });

  it('DESC order reverses results', () => {
    const { db, rng } = createDb();
    addSicilyCities(db, rng);
    const result = geo.geosearch(db, [
      'k',
      'FROMMEMBER',
      'Palermo',
      'BYRADIUS',
      '200',
      'km',
      'DESC',
    ]);
    const items = arrayItems(result);
    expect(items.length).toBe(2);
    // DESC: Catania first (farther), then Palermo
    expect(items[0]).toEqual(bulk('Catania'));
    expect(items[1]).toEqual(bulk('Palermo'));
  });

  it('COUNT limits results', () => {
    const { db, rng } = createDb();
    addSicilyCities(db, rng);
    const result = geo.geosearch(db, [
      'k',
      'FROMMEMBER',
      'Palermo',
      'BYRADIUS',
      '200',
      'km',
      'ASC',
      'COUNT',
      '1',
    ]);
    const items = arrayItems(result);
    expect(items.length).toBe(1);
    expect(items[0]).toEqual(bulk('Palermo'));
  });

  it('WITHCOORD returns coordinates', () => {
    const { db, rng } = createDb();
    addSicilyCities(db, rng);
    const result = geo.geosearch(db, [
      'k',
      'FROMMEMBER',
      'Palermo',
      'BYRADIUS',
      '1',
      'km',
      'WITHCOORD',
    ]);
    const items = arrayItems(result);
    expect(items.length).toBe(1);
    // Each item is [name, [lon, lat]]
    const item = arrayItems(items[0] as Reply);
    expect(item[0]).toEqual(bulk('Palermo'));
    const coords = arrayItems(item[1] as Reply);
    expect(Math.abs(bulkNum(coords[0] as Reply) - 13.361389)).toBeLessThan(
      0.001
    );
  });

  it('WITHDIST returns distances', () => {
    const { db, rng } = createDb();
    addSicilyCities(db, rng);
    const result = geo.geosearch(db, [
      'k',
      'FROMMEMBER',
      'Palermo',
      'BYRADIUS',
      '200',
      'km',
      'ASC',
      'WITHDIST',
    ]);
    const items = arrayItems(result);
    expect(items.length).toBe(2);
    // First item is Palermo with dist 0
    const item0 = arrayItems(items[0] as Reply);
    expect(item0[0]).toEqual(bulk('Palermo'));
    expect(bulkNum(item0[1] as Reply)).toBeCloseTo(0, 0);
  });

  it('WITHHASH returns geohash score', () => {
    const { db, rng } = createDb();
    addSicilyCities(db, rng);
    const result = geo.geosearch(db, [
      'k',
      'FROMMEMBER',
      'Palermo',
      'BYRADIUS',
      '1',
      'km',
      'WITHHASH',
    ]);
    const items = arrayItems(result);
    expect(items.length).toBe(1);
    const item = arrayItems(items[0] as Reply);
    expect(item[0]).toEqual(bulk('Palermo'));
    // Hash should be an integer
    expect(item[1]?.kind).toBe('integer');
  });

  it('WITHCOORD WITHDIST WITHHASH combined', () => {
    const { db, rng } = createDb();
    addSicilyCities(db, rng);
    const result = geo.geosearch(db, [
      'k',
      'FROMMEMBER',
      'Palermo',
      'BYRADIUS',
      '1',
      'km',
      'WITHCOORD',
      'WITHDIST',
      'WITHHASH',
    ]);
    const items = arrayItems(result);
    expect(items.length).toBe(1);
    // [name, dist, hash, [lon, lat]]
    const item = arrayItems(items[0] as Reply);
    expect(item[0]).toEqual(bulk('Palermo'));
    expect(item[1]?.kind).toBe('bulk'); // dist
    expect(item[2]?.kind).toBe('integer'); // hash
    expect(item[3]?.kind).toBe('array'); // coords
  });

  it('returns empty for non-existing key', () => {
    const { db } = createDb();
    expect(
      geo.geosearch(db, [
        'k',
        'FROMLONLAT',
        '0',
        '0',
        'BYRADIUS',
        '1',
        'km',
        'ASC',
      ])
    ).toEqual(EMPTY_ARRAY);
  });

  it('errors on missing FROMMEMBER/FROMLONLAT', () => {
    const { db, rng } = createDb();
    geo.geoadd(db, ['k', '13.0', '38.0', 'a'], rng);
    expect(geo.geosearch(db, ['k', 'BYRADIUS', '100', 'km', 'ASC']).kind).toBe(
      'error'
    );
  });

  it('errors on missing BYRADIUS/BYBOX', () => {
    const { db, rng } = createDb();
    geo.geoadd(db, ['k', '13.0', '38.0', 'a'], rng);
    expect(geo.geosearch(db, ['k', 'FROMMEMBER', 'a', 'ASC']).kind).toBe(
      'error'
    );
  });

  it('COUNT ANY returns approximate count', () => {
    const { db, rng } = createDb();
    addSicilyCities(db, rng);
    const result = geo.geosearch(db, [
      'k',
      'FROMLONLAT',
      '14.0',
      '38.0',
      'BYRADIUS',
      '200',
      'km',
      'ASC',
      'COUNT',
      '1',
      'ANY',
    ]);
    const items = arrayItems(result);
    // With ANY, we stop as soon as we have COUNT items (may not be closest)
    expect(items.length).toBe(1);
  });

  it('errors on negative BYBOX dimensions', () => {
    const { db, rng } = createDb();
    geo.geoadd(db, ['k', '13.0', '38.0', 'a'], rng);
    const result = geo.geosearch(db, [
      'k',
      'FROMLONLAT',
      '13.0',
      '38.0',
      'BYBOX',
      '-100',
      '200',
      'km',
      'ASC',
    ]);
    expect(result).toEqual(err('ERR', 'height or width cannot be negative'));
  });

  it('errors on negative BYRADIUS radius', () => {
    const { db, rng } = createDb();
    geo.geoadd(db, ['k', '13.0', '38.0', 'a'], rng);
    const result = geo.geosearch(db, [
      'k',
      'FROMLONLAT',
      '13.0',
      '38.0',
      'BYRADIUS',
      '-10',
      'km',
      'ASC',
    ]);
    expect(result).toEqual(err('ERR', 'radius cannot be negative'));
  });

  it('FROMMEMBER errors for non-existing member', () => {
    const { db, rng } = createDb();
    geo.geoadd(db, ['k', '13.0', '38.0', 'a'], rng);
    const result = geo.geosearch(db, [
      'k',
      'FROMMEMBER',
      'nonexistent',
      'BYRADIUS',
      '100',
      'km',
      'ASC',
    ]);
    expect(result).toEqual(
      err('ERR', 'could not decode requested zset member')
    );
  });
});

// --- GEOSEARCHSTORE ---

describe('GEOSEARCHSTORE', () => {
  it('stores results in destination key', () => {
    const { db, rng } = createDb();
    addSicily(db, rng);
    const result = geo.geosearchstore(
      db,
      [
        'dst',
        'k',
        'FROMLONLAT',
        '14.0',
        '38.0',
        'BYRADIUS',
        '200',
        'km',
        'ASC',
      ],
      rng
    );
    expect(result).toEqual(integer(2));
    // Verify destination is a zset with the members
    const pos = geo.geopos(db, ['dst', 'Palermo']);
    const items = arrayItems(pos);
    expect(items[0]?.kind).toBe('array');
  });

  it('STOREDIST stores distances as scores', () => {
    const { db, rng } = createDb();
    addSicily(db, rng);
    const result = geo.geosearchstore(
      db,
      [
        'dst',
        'k',
        'FROMLONLAT',
        '14.0',
        '38.0',
        'BYRADIUS',
        '200',
        'km',
        'ASC',
        'STOREDIST',
      ],
      rng
    );
    expect(result).toEqual(integer(2));
  });

  it('returns 0 for no matches', () => {
    const { db, rng } = createDb();
    geo.geoadd(db, ['k', '13.361389', '38.115556', 'Palermo'], rng);
    const result = geo.geosearchstore(
      db,
      ['dst', 'k', 'FROMLONLAT', '100.0', '0.0', 'BYRADIUS', '1', 'km', 'ASC'],
      rng
    );
    expect(result).toEqual(ZERO);
  });

  it('deletes destination when source key does not exist', () => {
    const { db, rng } = createDb();
    // Pre-create dst
    geo.geoadd(db, ['dst', '0', '0', 'old'], rng);
    const result = geo.geosearchstore(
      db,
      [
        'dst',
        'nonexistent',
        'FROMLONLAT',
        '0',
        '0',
        'BYRADIUS',
        '100',
        'km',
        'ASC',
      ],
      rng
    );
    expect(result).toEqual(ZERO);
    expect(db.get('dst')).toBeNull();
  });
});

// --- GEORADIUS (deprecated but supported) ---

describe('GEORADIUS', () => {
  it('returns members within radius', () => {
    const { db, rng } = createDb();
    addSicily(db, rng);
    const result = geo.georadius(
      db,
      ['k', '15', '37', '200', 'km', 'ASC'],
      rng
    );
    const items = arrayItems(result);
    expect(items.length).toBe(2);
  });

  it('works without optional flags', () => {
    const { db, rng } = createDb();
    addSicily(db, rng);
    const result = geo.georadius(db, ['k', '15', '37', '200', 'km'], rng);
    const items = arrayItems(result);
    expect(items.length).toBe(2);
  });

  it('WITHCOORD WITHDIST options', () => {
    const { db, rng } = createDb();
    geo.geoadd(db, ['k', '13.361389', '38.115556', 'Palermo'], rng);
    const result = geo.georadius(
      db,
      ['k', '13.361389', '38.115556', '1', 'km', 'WITHCOORD', 'WITHDIST'],
      rng
    );
    const items = arrayItems(result);
    expect(items.length).toBe(1);
    const item = arrayItems(items[0] as Reply);
    expect(item[0]).toEqual(bulk('Palermo'));
  });

  it('STORE option stores to destination', () => {
    const { db, rng } = createDb();
    addSicily(db, rng);
    const result = geo.georadius(
      db,
      ['k', '14', '38', '200', 'km', 'ASC', 'STORE', 'dst'],
      rng
    );
    expect(result).toEqual(integer(2));
  });

  it('STOREDIST option stores distances', () => {
    const { db, rng } = createDb();
    geo.geoadd(db, ['k', '13.361389', '38.115556', 'Palermo'], rng);
    const result = geo.georadius(
      db,
      ['k', '13.361389', '38.115556', '1', 'km', 'STOREDIST', 'dst'],
      rng
    );
    expect(result).toEqual(integer(1));
  });

  it('STORE incompatible with WITH* flags', () => {
    const { db, rng } = createDb();
    addSicily(db, rng);
    const result = geo.georadius(
      db,
      ['k', '14', '38', '200', 'km', 'STORE', 'dst', 'WITHCOORD'],
      rng
    );
    expect(result.kind).toBe('error');
  });

  it('returns empty array for non-existing key', () => {
    const { db, rng } = createDb();
    const result = geo.georadius(db, ['k', '14', '38', '200', 'km'], rng);
    expect(result).toEqual(EMPTY_ARRAY);
  });

  it('rejects negative radius', () => {
    const { db, rng } = createDb();
    addSicily(db, rng);
    const result = geo.georadius(db, ['k', '14', '38', '-1', 'km'], rng);
    expect(result).toEqual(err('ERR', 'radius cannot be negative'));
  });
});

// --- GEORADIUSBYMEMBER ---

describe('GEORADIUSBYMEMBER', () => {
  it('returns members within radius from a member', () => {
    const { db, rng } = createDb();
    addSicily(db, rng);
    const result = geo.georadiusbymember(
      db,
      ['k', 'Palermo', '200', 'km', 'ASC'],
      rng
    );
    const items = arrayItems(result);
    expect(items.length).toBe(2);
    expect(items[0]).toEqual(bulk('Palermo'));
    expect(items[1]).toEqual(bulk('Catania'));
  });

  it('returns error for non-existing member', () => {
    const { db, rng } = createDb();
    geo.geoadd(db, ['k', '13.0', '38.0', 'a'], rng);
    const result = geo.georadiusbymember(
      db,
      ['k', 'NonExistent', '100', 'km', 'ASC'],
      rng
    );
    expect(result.kind).toBe('error');
  });
});

// --- GEORADIUS_RO / GEORADIUSBYMEMBER_RO ---

describe('GEORADIUS_RO', () => {
  it('works like GEORADIUS but readonly (no STORE)', () => {
    const { db, rng } = createDb();
    addSicily(db, rng);
    const result = geo.georadius_ro(db, ['k', '15', '37', '200', 'km', 'ASC']);
    const items = arrayItems(result);
    expect(items.length).toBe(2);
  });

  it('rejects STORE option', () => {
    const { db, rng } = createDb();
    addSicily(db, rng);
    const result = geo.georadius_ro(db, [
      'k',
      '15',
      '37',
      '200',
      'km',
      'STORE',
      'dst',
    ]);
    expect(result.kind).toBe('error');
  });

  it('rejects STOREDIST option', () => {
    const { db, rng } = createDb();
    addSicily(db, rng);
    const result = geo.georadius_ro(db, [
      'k',
      '15',
      '37',
      '200',
      'km',
      'STOREDIST',
      'dst',
    ]);
    expect(result.kind).toBe('error');
  });
});

describe('GEORADIUSBYMEMBER_RO', () => {
  it('works like GEORADIUSBYMEMBER but readonly', () => {
    const { db, rng } = createDb();
    addSicily(db, rng);
    const result = geo.georadiusbymember_ro(db, [
      'k',
      'Palermo',
      '200',
      'km',
      'ASC',
    ]);
    const items = arrayItems(result);
    expect(items.length).toBe(2);
  });

  it('rejects STORE option', () => {
    const { db, rng } = createDb();
    addSicily(db, rng);
    const result = geo.georadiusbymember_ro(db, [
      'k',
      'Palermo',
      '200',
      'km',
      'STORE',
      'dst',
    ]);
    expect(result.kind).toBe('error');
  });
});

// --- Sort behavior ---

describe('GEO sort behavior', () => {
  it('GEOSEARCH without ASC/DESC returns unsorted (natural hash order)', () => {
    const { db, rng } = createDb();
    geo.geoadd(
      db,
      [
        'k',
        '13.361389',
        '38.115556',
        'Palermo',
        '15.087269',
        '37.502669',
        'Catania',
        '2.349014',
        '48.864716',
        'Paris',
      ],
      rng
    );
    // Without ASC/DESC, results are in natural skip list order (by geohash score)
    const result = geo.geosearch(db, [
      'k',
      'FROMLONLAT',
      '14.0',
      '38.0',
      'BYRADIUS',
      '200',
      'km',
    ]);
    const items = arrayItems(result);
    expect(items.length).toBe(2);
    // Just verify we get results, order is natural (not necessarily by distance)
  });

  it('GEOSEARCH COUNT without ANY forces ASC sort', () => {
    const { db, rng } = createDb();
    geo.geoadd(
      db,
      [
        'k',
        '13.361389',
        '38.115556',
        'Palermo',
        '15.087269',
        '37.502669',
        'Catania',
      ],
      rng
    );
    // COUNT without ANY and no ASC/DESC → Redis forces ASC
    const result = geo.geosearch(db, [
      'k',
      'FROMLONLAT',
      '14.0',
      '38.0',
      'BYRADIUS',
      '200',
      'km',
      'COUNT',
      '2',
    ]);
    const items = arrayItems(result);
    expect(items.length).toBe(2);
    // Should be ASC order: closer first — matches explicit ASC
    const ascResult = geo.geosearch(db, [
      'k',
      'FROMLONLAT',
      '14.0',
      '38.0',
      'BYRADIUS',
      '200',
      'km',
      'ASC',
      'COUNT',
      '2',
    ]);
    expect(result).toEqual(ascResult);
  });

  it('GEORADIUS without ASC/DESC returns unsorted', () => {
    const { db, rng } = createDb();
    geo.geoadd(
      db,
      [
        'k',
        '13.361389',
        '38.115556',
        'Palermo',
        '15.087269',
        '37.502669',
        'Catania',
      ],
      rng
    );
    const result = geo.georadius(db, ['k', '15', '37', '200', 'km'], rng);
    const items = arrayItems(result);
    expect(items.length).toBe(2);
    // Just verify we get results, no order guarantee
  });

  it('GEORADIUS COUNT without ANY forces ASC sort', () => {
    const { db, rng } = createDb();
    geo.geoadd(
      db,
      [
        'k',
        '13.361389',
        '38.115556',
        'Palermo',
        '15.087269',
        '37.502669',
        'Catania',
      ],
      rng
    );
    const result = geo.georadius(
      db,
      ['k', '14', '38', '200', 'km', 'COUNT', '2'],
      rng
    );
    const ascResult = geo.georadius(
      db,
      ['k', '14', '38', '200', 'km', 'ASC', 'COUNT', '2'],
      rng
    );
    // COUNT without ANY should produce same order as explicit ASC
    expect(result).toEqual(ascResult);
  });
});

// --- Edge cases ---

describe('GEO edge cases', () => {
  it('boundary coordinates are accepted', () => {
    const { db, rng } = createDb();
    // Exact boundary values
    expect(geo.geoadd(db, ['k', '-180', '-85.05112878', 'sw'], rng)).toEqual(
      ONE
    );
    expect(geo.geoadd(db, ['k', '180', '85.05112878', 'ne'], rng)).toEqual(ONE);
  });

  it('zero distance between same point', () => {
    const { db, rng } = createDb();
    geo.geoadd(db, ['k', '0', '0', 'origin'], rng);
    const result = geo.geodist(db, ['k', 'origin', 'origin']);
    expect(result.kind).toBe('bulk');
    if (result.kind === 'bulk' && result.value !== null) {
      expect(result.value).toBe('0.0000');
    }
  });

  it('GEOSEARCH with COUNT 0 returns error', () => {
    const { db, rng } = createDb();
    geo.geoadd(db, ['k', '0', '0', 'origin'], rng);
    const result = geo.geosearch(db, [
      'k',
      'FROMLONLAT',
      '0',
      '0',
      'BYRADIUS',
      '100',
      'km',
      'ASC',
      'COUNT',
      '0',
    ]);
    expect(result.kind).toBe('error');
  });

  it('distance between points on same longitude', () => {
    const { db, rng } = createDb();
    geo.geoadd(db, ['k', '0', '0', 'a', '0', '10', 'b'], rng);
    const result = geo.geodist(db, ['k', 'a', 'b']);
    const dist = bulkNum(result);
    // ~1111km for 10 degrees of latitude
    expect(dist).toBeGreaterThan(1100000);
    expect(dist).toBeLessThan(1120000);
  });

  it('distance between points on same latitude', () => {
    const { db, rng } = createDb();
    geo.geoadd(db, ['k', '0', '0', 'a', '10', '0', 'b'], rng);
    const result = geo.geodist(db, ['k', 'a', 'b']);
    const dist = bulkNum(result);
    // ~1111km for 10 degrees of longitude at equator
    expect(dist).toBeGreaterThan(1100000);
    expect(dist).toBeLessThan(1120000);
  });

  it('geohash encoding round-trips with acceptable precision', () => {
    const { db, rng } = createDb();
    // Add at origin, check round-trip
    geo.geoadd(db, ['k', '0', '0', 'origin'], rng);
    const pos = geo.geopos(db, ['k', 'origin']);
    const items = arrayItems(pos);
    const coords = arrayItems(items[0] as Reply);
    const lon = bulkNum(coords[0] as Reply);
    const lat = bulkNum(coords[1] as Reply);
    // 52-bit geohash gives sub-meter precision
    expect(Math.abs(lon)).toBeLessThan(0.0001);
    expect(Math.abs(lat)).toBeLessThan(0.0001);
  });
});

// --- STOREDIST score verification ---

describe('GEOSEARCHSTORE STOREDIST scores', () => {
  it('stores distances in the requested unit as scores', () => {
    const { db, rng } = createDb();
    addSicily(db, rng);
    geo.geosearchstore(
      db,
      [
        'dst',
        'k',
        'FROMLONLAT',
        '13.361389',
        '38.115556',
        'BYRADIUS',
        '200',
        'km',
        'ASC',
        'STOREDIST',
      ],
      rng
    );
    // Palermo is at the center, so distance should be ~0
    const palScore = zset.zscore(db, ['dst', 'Palermo']);
    expect(palScore.kind).toBe('bulk');
    if (palScore.kind === 'bulk' && palScore.value !== null) {
      expect(parseFloat(palScore.value)).toBeCloseTo(0, 0);
    }
    // Catania is ~166 km away
    const catScore = zset.zscore(db, ['dst', 'Catania']);
    expect(catScore.kind).toBe('bulk');
    if (catScore.kind === 'bulk' && catScore.value !== null) {
      const dist = parseFloat(catScore.value);
      expect(dist).toBeGreaterThan(160);
      expect(dist).toBeLessThan(170);
    }
  });

  it('GEORADIUS STOREDIST stores distances in requested unit', () => {
    const { db, rng } = createDb();
    addSicily(db, rng);
    geo.georadius(
      db,
      ['k', '13.361389', '38.115556', '200', 'km', 'STOREDIST', 'dst'],
      rng
    );
    const palScore = zset.zscore(db, ['dst', 'Palermo']);
    expect(palScore.kind).toBe('bulk');
    if (palScore.kind === 'bulk' && palScore.value !== null) {
      expect(parseFloat(palScore.value)).toBeCloseTo(0, 0);
    }
  });
});

// --- WRONGTYPE errors for search commands ---

describe('GEO search WRONGTYPE errors', () => {
  function setupStringKey(db: Database): void {
    db.set('str', 'string', 'raw', Buffer.from('hello'));
  }

  it('GEOSEARCH returns WRONGTYPE for non-zset key', () => {
    const { db } = createDb();
    setupStringKey(db);
    expect(
      geo.geosearch(db, [
        'str',
        'FROMLONLAT',
        '0',
        '0',
        'BYRADIUS',
        '100',
        'km',
        'ASC',
      ])
    ).toEqual(WRONGTYPE);
  });

  it('GEOSEARCHSTORE returns WRONGTYPE for non-zset source', () => {
    const { db, rng } = createDb();
    setupStringKey(db);
    expect(
      geo.geosearchstore(
        db,
        ['dst', 'str', 'FROMLONLAT', '0', '0', 'BYRADIUS', '100', 'km', 'ASC'],
        rng
      )
    ).toEqual(WRONGTYPE);
  });

  it('GEORADIUS returns WRONGTYPE for non-zset key', () => {
    const { db, rng } = createDb();
    setupStringKey(db);
    expect(geo.georadius(db, ['str', '0', '0', '100', 'km'], rng)).toEqual(
      WRONGTYPE
    );
  });

  it('GEORADIUSBYMEMBER returns WRONGTYPE for non-zset key', () => {
    const { db, rng } = createDb();
    setupStringKey(db);
    expect(geo.georadiusbymember(db, ['str', 'a', '100', 'km'], rng)).toEqual(
      WRONGTYPE
    );
  });

  it('GEORADIUS_RO returns WRONGTYPE for non-zset key', () => {
    const { db } = createDb();
    setupStringKey(db);
    expect(geo.georadius_ro(db, ['str', '0', '0', '100', 'km'])).toEqual(
      WRONGTYPE
    );
  });

  it('GEORADIUSBYMEMBER_RO returns WRONGTYPE for non-zset key', () => {
    const { db } = createDb();
    setupStringKey(db);
    expect(geo.georadiusbymember_ro(db, ['str', 'a', '100', 'km'])).toEqual(
      WRONGTYPE
    );
  });
});

// --- GEOSEARCHSTORE destination overwrite ---

describe('GEOSEARCHSTORE destination overwrite', () => {
  it('overwrites existing destination key', () => {
    const { db, rng } = createDb();
    addSicily(db, rng);
    // Create destination with some old data
    geo.geoadd(db, ['dst', '0', '0', 'old_member'], rng);

    geo.geosearchstore(
      db,
      [
        'dst',
        'k',
        'FROMLONLAT',
        '14.0',
        '38.0',
        'BYRADIUS',
        '200',
        'km',
        'ASC',
      ],
      rng
    );

    // old_member should not exist in destination
    const pos = geo.geopos(db, ['dst', 'old_member']);
    const items = arrayItems(pos);
    expect(items[0]).toEqual(bulk(null));

    // New members should be there
    const pos2 = geo.geopos(db, ['dst', 'Palermo']);
    const items2 = arrayItems(pos2);
    expect(items2[0]?.kind).toBe('array');
  });

  it('deletes destination when no results match', () => {
    const { db, rng } = createDb();
    addSicily(db, rng);
    // Create destination
    geo.geoadd(db, ['dst', '0', '0', 'old'], rng);

    geo.geosearchstore(
      db,
      ['dst', 'k', 'FROMLONLAT', '100.0', '0.0', 'BYRADIUS', '1', 'km', 'ASC'],
      rng
    );

    // Destination should be deleted
    expect(db.get('dst')).toBeNull();
  });
});

// --- GEOSEARCHSTORE rejects WITH* flags ---

describe('GEOSEARCHSTORE rejects WITH* flags', () => {
  it('rejects WITHCOORD', () => {
    const { db, rng } = createDb();
    addSicily(db, rng);
    const result = geo.geosearchstore(
      db,
      [
        'dst',
        'k',
        'FROMLONLAT',
        '14.0',
        '38.0',
        'BYRADIUS',
        '200',
        'km',
        'ASC',
        'WITHCOORD',
      ],
      rng
    );
    expect(result).toEqual(err('ERR', 'syntax error'));
  });

  it('rejects WITHDIST', () => {
    const { db, rng } = createDb();
    addSicily(db, rng);
    const result = geo.geosearchstore(
      db,
      [
        'dst',
        'k',
        'FROMLONLAT',
        '14.0',
        '38.0',
        'BYRADIUS',
        '200',
        'km',
        'ASC',
        'WITHDIST',
      ],
      rng
    );
    expect(result).toEqual(err('ERR', 'syntax error'));
  });

  it('rejects WITHHASH', () => {
    const { db, rng } = createDb();
    addSicily(db, rng);
    const result = geo.geosearchstore(
      db,
      [
        'dst',
        'k',
        'FROMLONLAT',
        '14.0',
        '38.0',
        'BYRADIUS',
        '200',
        'km',
        'ASC',
        'WITHHASH',
      ],
      rng
    );
    expect(result).toEqual(err('ERR', 'syntax error'));
  });
});

// --- Duplicate FROM/BY options ---

describe('GEOSEARCH duplicate option validation', () => {
  it('errors on duplicate FROMMEMBER', () => {
    const { db, rng } = createDb();
    addSicily(db, rng);
    const result = geo.geosearch(db, [
      'k',
      'FROMMEMBER',
      'Palermo',
      'FROMMEMBER',
      'Catania',
      'BYRADIUS',
      '200',
      'km',
    ]);
    expect(result).toEqual(
      err(
        'ERR',
        'exactly one of FROMMEMBER or FROMLONLAT can be specified for GEOSEARCH/GEOSEARCHSTORE'
      )
    );
  });

  it('errors on FROMMEMBER + FROMLONLAT', () => {
    const { db, rng } = createDb();
    addSicily(db, rng);
    const result = geo.geosearch(db, [
      'k',
      'FROMMEMBER',
      'Palermo',
      'FROMLONLAT',
      '14.0',
      '38.0',
      'BYRADIUS',
      '200',
      'km',
    ]);
    expect(result).toEqual(
      err(
        'ERR',
        'exactly one of FROMMEMBER or FROMLONLAT can be specified for GEOSEARCH/GEOSEARCHSTORE'
      )
    );
  });

  it('errors on duplicate BYRADIUS', () => {
    const { db, rng } = createDb();
    addSicily(db, rng);
    const result = geo.geosearch(db, [
      'k',
      'FROMLONLAT',
      '14.0',
      '38.0',
      'BYRADIUS',
      '200',
      'km',
      'BYRADIUS',
      '100',
      'km',
    ]);
    expect(result).toEqual(
      err(
        'ERR',
        'exactly one of BYRADIUS and BYBOX can be specified for GEOSEARCH/GEOSEARCHSTORE'
      )
    );
  });

  it('errors on BYRADIUS + BYBOX', () => {
    const { db, rng } = createDb();
    addSicily(db, rng);
    const result = geo.geosearch(db, [
      'k',
      'FROMLONLAT',
      '14.0',
      '38.0',
      'BYRADIUS',
      '200',
      'km',
      'BYBOX',
      '400',
      '200',
      'km',
    ]);
    expect(result).toEqual(
      err(
        'ERR',
        'exactly one of BYRADIUS and BYBOX can be specified for GEOSEARCH/GEOSEARCHSTORE'
      )
    );
  });
});

// --- GEORADIUS / GEOSEARCH equivalence ---

describe('GEORADIUS / GEOSEARCH equivalence', () => {
  it('GEORADIUS produces same results as equivalent GEOSEARCH', () => {
    const { db, rng } = createDb();
    addSicily(db, rng);
    // Add more cities for richer test
    geo.geoadd(
      db,
      ['k', '2.349014', '48.864716', 'Paris', '12.496366', '41.902782', 'Rome'],
      rng
    );

    const radiusResult = geo.georadius(
      db,
      ['k', '14.0', '38.0', '500', 'km', 'ASC', 'WITHDIST'],
      rng
    );
    const searchResult = geo.geosearch(db, [
      'k',
      'FROMLONLAT',
      '14.0',
      '38.0',
      'BYRADIUS',
      '500',
      'km',
      'ASC',
      'WITHDIST',
    ]);
    expect(radiusResult).toEqual(searchResult);
  });

  it('GEORADIUSBYMEMBER produces same results as equivalent GEOSEARCH FROMMEMBER', () => {
    const { db, rng } = createDb();
    addSicily(db, rng);

    const byMemberResult = geo.georadiusbymember(
      db,
      ['k', 'Palermo', '200', 'km', 'ASC', 'WITHDIST', 'WITHCOORD'],
      rng
    );
    const searchResult = geo.geosearch(db, [
      'k',
      'FROMMEMBER',
      'Palermo',
      'BYRADIUS',
      '200',
      'km',
      'ASC',
      'WITHDIST',
      'WITHCOORD',
    ]);
    expect(byMemberResult).toEqual(searchResult);
  });
});

// --- GEOSEARCH BYBOX additional tests ---

describe('GEOSEARCH BYBOX additional', () => {
  it('BYBOX excludes members outside the box', () => {
    const { db, rng } = createDb();
    geo.geoadd(
      db,
      [
        'k',
        '13.361389',
        '38.115556',
        'Palermo',
        '15.087269',
        '37.502669',
        'Catania',
        '2.349014',
        '48.864716',
        'Paris',
      ],
      rng
    );
    // Small box around Palermo, should not include Catania or Paris
    const result = geo.geosearch(db, [
      'k',
      'FROMLONLAT',
      '13.361389',
      '38.115556',
      'BYBOX',
      '50',
      '50',
      'km',
      'ASC',
    ]);
    const items = arrayItems(result);
    expect(items.length).toBe(1);
    expect(items[0]).toEqual(bulk('Palermo'));
  });

  it('BYBOX with WITHDIST returns correct distances', () => {
    const { db, rng } = createDb();
    addSicily(db, rng);
    const result = geo.geosearch(db, [
      'k',
      'FROMLONLAT',
      '14.0',
      '38.0',
      'BYBOX',
      '400',
      '200',
      'km',
      'ASC',
      'WITHDIST',
    ]);
    const items = arrayItems(result);
    expect(items.length).toBe(2);
    // Each item is [name, dist]
    const item0 = arrayItems(items[0] as Reply);
    const dist0 = bulkNum(item0[1] as Reply);
    expect(dist0).toBeGreaterThan(0);
  });

  it('GEOSEARCHSTORE with BYBOX stores results', () => {
    const { db, rng } = createDb();
    addSicily(db, rng);
    const result = geo.geosearchstore(
      db,
      [
        'dst',
        'k',
        'FROMLONLAT',
        '14.0',
        '38.0',
        'BYBOX',
        '400',
        '200',
        'km',
        'ASC',
      ],
      rng
    );
    expect(result).toEqual(integer(2));
    // Verify stored members can be retrieved
    const pos = geo.geopos(db, ['dst', 'Palermo', 'Catania']);
    const items = arrayItems(pos);
    expect(items[0]?.kind).toBe('array');
    expect(items[1]?.kind).toBe('array');
  });
});

// --- GEORADIUS_RO / GEORADIUSBYMEMBER_RO additional ---

describe('GEORADIUS_RO additional', () => {
  it('supports WITHCOORD WITHDIST WITHHASH', () => {
    const { db, rng } = createDb();
    addSicily(db, rng);
    const result = geo.georadius_ro(db, [
      'k',
      '15',
      '37',
      '200',
      'km',
      'ASC',
      'WITHCOORD',
      'WITHDIST',
      'WITHHASH',
    ]);
    const items = arrayItems(result);
    expect(items.length).toBe(2);
    // Each item: [name, dist, hash, [lon, lat]]
    const item = arrayItems(items[0] as Reply);
    expect(item.length).toBe(4);
    expect(item[0]?.kind).toBe('bulk');
    expect(item[1]?.kind).toBe('bulk');
    expect(item[2]?.kind).toBe('integer');
    expect(item[3]?.kind).toBe('array');
  });

  it('returns empty array for non-existing key', () => {
    const { db } = createDb();
    expect(
      geo.georadius_ro(db, ['nonexistent', '0', '0', '100', 'km'])
    ).toEqual(EMPTY_ARRAY);
  });
});

describe('GEORADIUSBYMEMBER_RO additional', () => {
  it('supports WITHCOORD WITHDIST', () => {
    const { db, rng } = createDb();
    addSicily(db, rng);
    const result = geo.georadiusbymember_ro(db, [
      'k',
      'Palermo',
      '200',
      'km',
      'ASC',
      'WITHCOORD',
      'WITHDIST',
    ]);
    const items = arrayItems(result);
    expect(items.length).toBe(2);
    const item = arrayItems(items[0] as Reply);
    expect(item[0]).toEqual(bulk('Palermo'));
    expect(item[1]?.kind).toBe('bulk'); // distance
    expect(item[2]?.kind).toBe('array'); // coords
  });

  it('rejects STOREDIST option', () => {
    const { db, rng } = createDb();
    addSicily(db, rng);
    const result = geo.georadiusbymember_ro(db, [
      'k',
      'Palermo',
      '200',
      'km',
      'STOREDIST',
      'dst',
    ]);
    expect(result.kind).toBe('error');
  });

  it('returns empty array for non-existing key', () => {
    const { db } = createDb();
    expect(
      geo.georadiusbymember_ro(db, ['nonexistent', 'a', '100', 'km'])
    ).toEqual(EMPTY_ARRAY);
  });
});

// --- COUNT with ANY and DESC ---

describe('COUNT ANY with sorting', () => {
  it('COUNT ANY with DESC returns count items without sorting', () => {
    const { db, rng } = createDb();
    geo.geoadd(
      db,
      [
        'k',
        '13.361389',
        '38.115556',
        'Palermo',
        '15.087269',
        '37.502669',
        'Catania',
        '2.349014',
        '48.864716',
        'Paris',
      ],
      rng
    );
    const result = geo.geosearch(db, [
      'k',
      'FROMLONLAT',
      '14.0',
      '38.0',
      'BYRADIUS',
      '2000',
      'km',
      'DESC',
      'COUNT',
      '2',
      'ANY',
    ]);
    const items = arrayItems(result);
    // ANY returns first 2 found, then sorts DESC
    expect(items.length).toBe(2);
  });

  it('ANY without COUNT is an error', () => {
    const { db, rng } = createDb();
    geo.geoadd(db, ['k', '0', '0', 'a'], rng);
    const result = geo.geosearch(db, [
      'k',
      'FROMLONLAT',
      '0',
      '0',
      'BYRADIUS',
      '100',
      'km',
      'ANY',
    ]);
    expect(result.kind).toBe('error');
  });
});
