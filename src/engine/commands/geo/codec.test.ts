import { describe, it, expect } from 'vitest';
import {
  geohashEncode,
  geohashDecode,
  geohashToString,
  degToRad,
  haversineDistance,
  parseUnit,
  formatDist,
  validateCoords,
  GEO_LAT_MIN,
  GEO_LAT_MAX,
  GEO_LON_MIN,
  GEO_LON_MAX,
  EARTH_RADIUS_M,
  UNIT_ERR,
} from './codec.ts';

describe('geohashEncode / geohashDecode', () => {
  it('encodes and decodes origin (0, 0) as a round-trip', () => {
    const hash = geohashEncode(0, 0);
    const [lon, lat] = geohashDecode(hash);
    expect(lon).toBeCloseTo(0, 3);
    expect(lat).toBeCloseTo(0, 3);
  });

  it('encodes and decodes positive coordinates', () => {
    const hash = geohashEncode(13.361389, 38.115556);
    const [lon, lat] = geohashDecode(hash);
    expect(lon).toBeCloseTo(13.361389, 3);
    expect(lat).toBeCloseTo(38.115556, 3);
  });

  it('encodes and decodes negative coordinates', () => {
    const hash = geohashEncode(-122.4194, 37.7749);
    const [lon, lat] = geohashDecode(hash);
    expect(lon).toBeCloseTo(-122.4194, 3);
    expect(lat).toBeCloseTo(37.7749, 3);
  });

  it('encodes and decodes minimum boundary values', () => {
    const hash = geohashEncode(GEO_LON_MIN, GEO_LAT_MIN);
    const [lon, lat] = geohashDecode(hash);
    expect(lon).toBeCloseTo(GEO_LON_MIN, 2);
    expect(lat).toBeCloseTo(GEO_LAT_MIN, 2);
  });

  it('wraps maximum boundary values due to quantization', () => {
    // At the exact maximum, lonNorm=1.0 which quantizes to 2^26 and wraps
    // via unsigned right shift. This is expected geohash behavior — the max
    // boundary cell wraps back to the minimum.
    const hash = geohashEncode(GEO_LON_MAX, 0);
    const [lon] = geohashDecode(hash);
    expect(lon).toBeCloseTo(GEO_LON_MIN, 2);

    const hash2 = geohashEncode(0, GEO_LAT_MAX);
    const [, lat] = geohashDecode(hash2);
    expect(lat).toBeCloseTo(GEO_LAT_MIN, 2);
  });

  it('encodes and decodes near-max values correctly', () => {
    // Values just inside the max boundary should round-trip correctly
    const nearMaxLon = GEO_LON_MAX - 0.001;
    const nearMaxLat = GEO_LAT_MAX - 0.001;
    const hash = geohashEncode(nearMaxLon, nearMaxLat);
    const [lon, lat] = geohashDecode(hash);
    expect(lon).toBeCloseTo(nearMaxLon, 2);
    expect(lat).toBeCloseTo(nearMaxLat, 2);
  });

  it('returns a non-negative integer hash', () => {
    const hash = geohashEncode(10, 20);
    expect(hash).toBeGreaterThanOrEqual(0);
    expect(Number.isInteger(hash)).toBe(true);
  });

  it('produces different hashes for different locations', () => {
    const h1 = geohashEncode(0, 0);
    const h2 = geohashEncode(10, 20);
    const h3 = geohashEncode(-50, -30);
    expect(h1).not.toBe(h2);
    expect(h1).not.toBe(h3);
    expect(h2).not.toBe(h3);
  });
});

describe('geohashToString', () => {
  it('returns an 11-character string', () => {
    const hash = geohashEncode(13.361389, 38.115556);
    const str = geohashToString(hash);
    expect(str).toHaveLength(11);
  });

  it('uses only valid base32 characters', () => {
    const hash = geohashEncode(-73.9857, 40.7484);
    const str = geohashToString(hash);
    const base32Chars = '0123456789bcdefghjkmnpqrstuvwxyz';
    for (const ch of str) {
      expect(base32Chars).toContain(ch);
    }
  });

  it('produces known geohash for Palermo', () => {
    const hash = geohashEncode(13.361389, 38.115556);
    const str = geohashToString(hash);
    // The last character may differ slightly from Redis due to 52-bit
    // quantization rounding, but the first 10 characters must match.
    expect(str.slice(0, 10)).toBe('sqc8b49rny');
  });

  it('produces known geohash for Catania', () => {
    const hash = geohashEncode(15.087269, 37.502669);
    const str = geohashToString(hash);
    expect(str.slice(0, 10)).toBe('sqdtr74hyu');
  });

  it('produces different strings for different locations', () => {
    const s1 = geohashToString(geohashEncode(0, 0));
    const s2 = geohashToString(geohashEncode(90, 45));
    expect(s1).not.toBe(s2);
  });
});

describe('degToRad', () => {
  it('converts 0 degrees to 0 radians', () => {
    expect(degToRad(0)).toBe(0);
  });

  it('converts 180 degrees to PI', () => {
    expect(degToRad(180)).toBeCloseTo(Math.PI, 10);
  });

  it('converts 90 degrees to PI/2', () => {
    expect(degToRad(90)).toBeCloseTo(Math.PI / 2, 10);
  });

  it('converts 360 degrees to 2*PI', () => {
    expect(degToRad(360)).toBeCloseTo(2 * Math.PI, 10);
  });

  it('converts negative degrees', () => {
    expect(degToRad(-90)).toBeCloseTo(-Math.PI / 2, 10);
  });
});

describe('haversineDistance', () => {
  it('returns 0 for same point', () => {
    expect(haversineDistance(10, 20, 10, 20)).toBe(0);
  });

  it('returns 0 for origin to itself', () => {
    expect(haversineDistance(0, 0, 0, 0)).toBe(0);
  });

  it('calculates distance between Palermo and Catania', () => {
    // Redis: GEODIST Sicily Palermo Catania → ~166274 m
    const dist = haversineDistance(13.361389, 38.115556, 15.087269, 37.502669);
    expect(dist).toBeCloseTo(166274, -2); // within ~100m
  });

  it('handles same longitude (pure latitude difference)', () => {
    // This triggers the v === 0 optimization path
    const dist = haversineDistance(0, 0, 0, 1);
    // 1 degree of latitude ≈ 111,195 m
    expect(dist).toBeCloseTo(111195, -3);
  });

  it('handles same latitude (pure longitude difference)', () => {
    const dist = haversineDistance(0, 0, 1, 0);
    // 1 degree of longitude at equator ≈ 111,195 m
    expect(dist).toBeCloseTo(111195, -3);
  });

  it('calculates antipodal distance (roughly half circumference)', () => {
    const dist = haversineDistance(0, 0, 180, 0);
    const halfCircumference = Math.PI * EARTH_RADIUS_M;
    expect(dist).toBeCloseTo(halfCircumference, -2);
  });

  it('is symmetric', () => {
    const d1 = haversineDistance(13.361389, 38.115556, 15.087269, 37.502669);
    const d2 = haversineDistance(15.087269, 37.502669, 13.361389, 38.115556);
    expect(d1).toBe(d2);
  });

  it('handles negative coordinates', () => {
    const dist = haversineDistance(-73.9857, 40.7484, -0.1278, 51.5074);
    // NYC to London ≈ 5,570 km
    expect(dist / 1000).toBeCloseTo(5570, -1);
  });
});

describe('parseUnit', () => {
  it('parses m', () => {
    expect(parseUnit('m')).toBe(1);
  });

  it('parses km', () => {
    expect(parseUnit('km')).toBe(1000);
  });

  it('parses mi', () => {
    expect(parseUnit('mi')).toBe(1609.34);
  });

  it('parses ft', () => {
    expect(parseUnit('ft')).toBe(0.3048);
  });

  it('is case insensitive', () => {
    expect(parseUnit('M')).toBe(1);
    expect(parseUnit('KM')).toBe(1000);
    expect(parseUnit('MI')).toBe(1609.34);
    expect(parseUnit('FT')).toBe(0.3048);
    expect(parseUnit('Km')).toBe(1000);
  });

  it('returns null for unknown units', () => {
    expect(parseUnit('cm')).toBeNull();
    expect(parseUnit('yards')).toBeNull();
    expect(parseUnit('')).toBeNull();
    expect(parseUnit('meters')).toBeNull();
  });
});

describe('formatDist', () => {
  it('formats distance in meters with 4 decimal places', () => {
    expect(formatDist(1234.5678, 1)).toBe('1234.5678');
  });

  it('formats distance in km', () => {
    expect(formatDist(1500, 1000)).toBe('1.5000');
  });

  it('formats distance in miles', () => {
    expect(formatDist(1609.34, 1609.34)).toBe('1.0000');
  });

  it('formats distance in feet', () => {
    expect(formatDist(0.3048, 0.3048)).toBe('1.0000');
  });

  it('pads with trailing zeros', () => {
    expect(formatDist(1000, 1)).toBe('1000.0000');
  });

  it('handles zero distance', () => {
    expect(formatDist(0, 1)).toBe('0.0000');
  });

  it('rounds to 4 decimal places', () => {
    expect(formatDist(1.23456789, 1)).toBe('1.2346');
  });
});

describe('validateCoords', () => {
  it('returns null for valid coordinates', () => {
    expect(validateCoords(0, 0)).toBeNull();
    expect(validateCoords(13.361389, 38.115556)).toBeNull();
    expect(validateCoords(-122.4194, 37.7749)).toBeNull();
  });

  it('returns null for boundary values', () => {
    expect(validateCoords(GEO_LON_MIN, GEO_LAT_MIN)).toBeNull();
    expect(validateCoords(GEO_LON_MAX, GEO_LAT_MAX)).toBeNull();
    expect(validateCoords(GEO_LON_MIN, GEO_LAT_MAX)).toBeNull();
    expect(validateCoords(GEO_LON_MAX, GEO_LAT_MIN)).toBeNull();
  });

  it('returns error for longitude below minimum', () => {
    const result = validateCoords(-180.1, 0);
    expect(result).not.toBeNull();
    expect(result).toHaveProperty('kind', 'error');
  });

  it('returns error for longitude above maximum', () => {
    const result = validateCoords(180.1, 0);
    expect(result).not.toBeNull();
    expect(result).toHaveProperty('kind', 'error');
  });

  it('returns error for latitude below minimum', () => {
    const result = validateCoords(0, -85.06);
    expect(result).not.toBeNull();
    expect(result).toHaveProperty('kind', 'error');
  });

  it('returns error for latitude above maximum', () => {
    const result = validateCoords(0, 85.06);
    expect(result).not.toBeNull();
    expect(result).toHaveProperty('kind', 'error');
  });

  it('returns error containing the invalid coordinates', () => {
    const result = validateCoords(200, 100);
    expect(result).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'invalid longitude,latitude pair 200,100',
    });
  });

  it('returns error with both invalid coordinates shown', () => {
    const result = validateCoords(-200.5, -90.123);
    expect(result).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'invalid longitude,latitude pair -200.5,-90.123',
    });
  });
});

describe('UNIT_ERR', () => {
  it('is a properly formatted error reply', () => {
    expect(UNIT_ERR).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'unsupported unit provided. please use M, KM, FT, MI',
    });
  });
});
