import type { Reply } from '../../types.ts';
import { errorReply } from '../../types.ts';
import { formatFloat } from '../incr.ts';

// --- Constants ---

export const GEO_LAT_MIN = -85.05112878;
export const GEO_LAT_MAX = 85.05112878;
export const GEO_LON_MIN = -180;
export const GEO_LON_MAX = 180;
const GEO_STEP = 26; // 52-bit geohash (26 bits lon + 26 bits lat)
export const EARTH_RADIUS_M = 6372797.560856;

// Unit conversion factors (from meters)
const UNIT_M = 1;
const UNIT_KM = 1000;
const UNIT_MI = 1609.34;
const UNIT_FT = 0.3048;

// Base32 alphabet for geohash string encoding
const BASE32 = '0123456789bcdefghjkmnpqrstuvwxyz';

// --- Error constants ---

export const UNIT_ERR = errorReply(
  'ERR',
  'unsupported unit provided. please use M, KM, FT, MI'
);

// --- Geohash encoding/decoding ---

/**
 * Encode longitude/latitude to a 52-bit geohash integer.
 * This matches Redis's internal geohashEncodeWGS84 with GEO_STEP=26.
 */
export function geohashEncode(lon: number, lat: number): number {
  // Normalize to [0, 1] range
  const lonNorm = (lon - GEO_LON_MIN) / (GEO_LON_MAX - GEO_LON_MIN);
  const latNorm = (lat - GEO_LAT_MIN) / (GEO_LAT_MAX - GEO_LAT_MIN);

  // Quantize to 26-bit integers
  const lonBits = Math.floor(lonNorm * (1 << GEO_STEP)) >>> 0;
  const latBits = Math.floor(latNorm * (1 << GEO_STEP)) >>> 0;

  // Interleave bits: lon in even positions, lat in odd positions
  return interleave(lonBits, latBits);
}

/**
 * Decode a 52-bit geohash integer back to longitude/latitude.
 * Returns the center of the cell (matching Redis behavior).
 */
export function geohashDecode(hash: number): [number, number] {
  const [lonBits, latBits] = deinterleave(hash);
  const scale = 1 << GEO_STEP;

  // Center of the cell: (2*bits + 1) / (2 * scale)
  const lon =
    GEO_LON_MIN +
    ((2 * lonBits + 1) / (2 * scale)) * (GEO_LON_MAX - GEO_LON_MIN);
  const lat =
    GEO_LAT_MIN +
    ((2 * latBits + 1) / (2 * scale)) * (GEO_LAT_MAX - GEO_LAT_MIN);

  return [lon, lat];
}

/**
 * Interleave two 26-bit integers into a 52-bit number.
 * Bit layout: lon[25] lat[25] lon[24] lat[24] ... lon[0] lat[0]
 */
function interleave(lon: number, lat: number): number {
  let result = 0;
  for (let i = GEO_STEP - 1; i >= 0; i--) {
    result = result * 4 + (((lon >>> i) & 1) * 2 + ((lat >>> i) & 1));
  }
  return result;
}

/**
 * Deinterleave a 52-bit hash into two 26-bit integers.
 */
function deinterleave(hash: number): [number, number] {
  let lon = 0;
  let lat = 0;
  for (let i = 0; i < GEO_STEP; i++) {
    const bitPos = i * 2;
    lat |= ((Math.floor(hash / Math.pow(2, bitPos)) & 1) << i) >>> 0;
    lon |= ((Math.floor(hash / Math.pow(2, bitPos + 1)) & 1) << i) >>> 0;
  }
  return [lon, lat];
}

/**
 * Convert a 52-bit internal geohash to an 11-char standard geohash string.
 * Redis decodes the internal hash to coordinates, then re-encodes using the
 * standard geohash algorithm with lat range [-90, 90].
 */
export function geohashToString(hash: number): string {
  const [lon, lat] = geohashDecode(hash);

  // Standard geohash bisection: lon in even bit positions, lat in odd
  let minLon = -180;
  let maxLon = 180;
  let minLat = -90;
  let maxLat = 90;
  let bits = 0;
  let ch = 0;
  let result = '';
  let isLon = true;
  const totalBits = 55; // 11 chars × 5 bits
  let bitCount = 0;

  while (bitCount < totalBits) {
    if (isLon) {
      const mid = (minLon + maxLon) / 2;
      if (lon >= mid) {
        ch = ch * 2 + 1;
        minLon = mid;
      } else {
        ch = ch * 2;
        maxLon = mid;
      }
    } else {
      const mid = (minLat + maxLat) / 2;
      if (lat >= mid) {
        ch = ch * 2 + 1;
        minLat = mid;
      } else {
        ch = ch * 2;
        maxLat = mid;
      }
    }
    isLon = !isLon;
    bits++;
    bitCount++;
    if (bits === 5) {
      result += BASE32[ch] as string;
      bits = 0;
      ch = 0;
    }
  }
  return result;
}

// --- Haversine distance ---

export function degToRad(deg: number): number {
  return (deg * Math.PI) / 180;
}

/**
 * Calculate distance between two points using the Haversine formula.
 * Matches Redis geohashGetDistance exactly: 2R·asin(√a).
 */
export function haversineDistance(
  lon1: number,
  lat1: number,
  lon2: number,
  lat2: number
): number {
  const lon1r = degToRad(lon1);
  const lon2r = degToRad(lon2);
  const v = Math.sin((lon2r - lon1r) / 2);

  // Redis optimization: if longitude difference is zero, only compute lat distance
  if (v === 0.0) {
    const lat1r = degToRad(lat1);
    const lat2r = degToRad(lat2);
    const u = Math.sin((lat2r - lat1r) / 2);
    return 2.0 * EARTH_RADIUS_M * Math.asin(Math.abs(u));
  }

  const lat1r = degToRad(lat1);
  const lat2r = degToRad(lat2);
  const u = Math.sin((lat2r - lat1r) / 2);
  const a = u * u + Math.cos(lat1r) * Math.cos(lat2r) * v * v;
  return 2.0 * EARTH_RADIUS_M * Math.asin(Math.sqrt(a));
}

// --- Unit parsing ---

export function parseUnit(unit: string): number | null {
  switch (unit.toLowerCase()) {
    case 'm':
      return UNIT_M;
    case 'km':
      return UNIT_KM;
    case 'mi':
      return UNIT_MI;
    case 'ft':
      return UNIT_FT;
    default:
      return null;
  }
}

export function formatDist(meters: number, unitFactor: number): string {
  // Redis uses %.4f formatting for distances (4 fixed decimal places)
  return (meters / unitFactor).toFixed(4);
}

// --- Coordinate validation ---

export function validateCoords(lon: number, lat: number): Reply | null {
  if (
    lon < GEO_LON_MIN ||
    lon > GEO_LON_MAX ||
    lat < GEO_LAT_MIN ||
    lat > GEO_LAT_MAX
  ) {
    return errorReply(
      'ERR',
      `invalid longitude,latitude pair ${formatFloat(lon)},${formatFloat(lat)}`
    );
  }
  return null;
}
