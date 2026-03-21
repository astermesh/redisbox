import type { Database } from '../database.ts';
import type { Reply } from '../types.ts';
import {
  bulkReply,
  integerReply,
  arrayReply,
  errorReply,
  NIL,
  ZERO,
  EMPTY_ARRAY,
  WRONGTYPE_ERR,
} from '../types.ts';
import { SkipList } from '../skip-list.ts';
import { parseFloat64, formatFloat } from './incr.ts';
import type { CommandSpec } from '../command-table.ts';
import type { SortedSetData } from './sorted-set.ts';

// --- Constants ---

const GEO_LAT_MIN = -85.05112878;
const GEO_LAT_MAX = 85.05112878;
const GEO_LON_MIN = -180;
const GEO_LON_MAX = 180;
const GEO_STEP = 26; // 52-bit geohash (26 bits lon + 26 bits lat)
const EARTH_RADIUS_M = 6372797.560856;
// Unit conversion factors (from meters)
const UNIT_M = 1;
const UNIT_KM = 1000;
const UNIT_MI = 1609.34;
const UNIT_FT = 0.3048;

// Base32 alphabet for geohash string encoding
const BASE32 = '0123456789bcdefghjkmnpqrstuvwxyz';

// --- Error constants ---

const UNIT_ERR = errorReply(
  'ERR',
  'unsupported unit provided. please use M, KM, FT, MI'
);

// --- Geohash encoding/decoding ---

/**
 * Encode longitude/latitude to a 52-bit geohash integer.
 * This matches Redis's internal geohashEncodeWGS84 with GEO_STEP=26.
 */
function geohashEncode(lon: number, lat: number): number {
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
function geohashDecode(hash: number): [number, number] {
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
function geohashToString(hash: number): string {
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

function degToRad(deg: number): number {
  return (deg * Math.PI) / 180;
}

/**
 * Calculate distance between two points using the Haversine formula.
 * Returns distance in meters.
 */
function haversineDistance(
  lon1: number,
  lat1: number,
  lon2: number,
  lat2: number
): number {
  const dLat = degToRad(lat2 - lat1);
  const dLon = degToRad(lon2 - lon1);
  const rLat1 = degToRad(lat1);
  const rLat2 = degToRad(lat2);

  const a =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos(rLat1) * Math.cos(rLat2) * Math.sin(dLon / 2) * Math.sin(dLon / 2);
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return EARTH_RADIUS_M * c;
}

// --- Unit parsing ---

function parseUnit(unit: string): number | null {
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

function formatDist(meters: number, unitFactor: number): string {
  return formatFloat(meters / unitFactor);
}

// --- Sorted set helpers (reuse pattern from sorted-set.ts) ---

function getOrCreateZset(
  db: Database,
  key: string,
  rng: () => number
): { zset: SortedSetData; error: null } | { zset: null; error: Reply } {
  const entry = db.get(key);
  if (entry) {
    if (entry.type !== 'zset') return { zset: null, error: WRONGTYPE_ERR };
    return { zset: entry.value as SortedSetData, error: null };
  }
  const zset: SortedSetData = {
    sl: new SkipList(rng),
    dict: new Map(),
  };
  db.set(key, 'zset', 'skiplist', zset);
  return { zset, error: null };
}

function getExistingZset(
  db: Database,
  key: string
): { zset: SortedSetData | null; error: Reply | null } {
  const entry = db.get(key);
  if (!entry) return { zset: null, error: null };
  if (entry.type !== 'zset') return { zset: null, error: WRONGTYPE_ERR };
  return { zset: entry.value as SortedSetData, error: null };
}

function removeIfEmpty(db: Database, key: string, zset: SortedSetData): void {
  if (zset.dict.size === 0) {
    db.delete(key);
  }
}

// --- Coordinate validation ---

function validateCoords(lon: number, lat: number): Reply | null {
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

// --- Bounding box for radius search ---

interface BoundingBox {
  minLon: number;
  maxLon: number;
  minLat: number;
  maxLat: number;
}

function boundingBoxByRadius(
  lon: number,
  lat: number,
  radiusM: number
): BoundingBox {
  const latDelta = (radiusM / EARTH_RADIUS_M) * (180 / Math.PI);
  const lonDelta =
    (radiusM / (EARTH_RADIUS_M * Math.cos(degToRad(lat)))) * (180 / Math.PI);

  return {
    minLon: lon - lonDelta,
    maxLon: lon + lonDelta,
    minLat: Math.max(lat - latDelta, GEO_LAT_MIN),
    maxLat: Math.min(lat + latDelta, GEO_LAT_MAX),
  };
}

function boundingBoxByBox(
  lon: number,
  lat: number,
  widthM: number,
  heightM: number
): BoundingBox {
  const halfHeight = heightM / 2;
  const halfWidth = widthM / 2;
  const latDelta = (halfHeight / EARTH_RADIUS_M) * (180 / Math.PI);
  const lonDelta =
    (halfWidth / (EARTH_RADIUS_M * Math.cos(degToRad(lat)))) * (180 / Math.PI);

  return {
    minLon: lon - lonDelta,
    maxLon: lon + lonDelta,
    minLat: Math.max(lat - latDelta, GEO_LAT_MIN),
    maxLat: Math.min(lat + latDelta, GEO_LAT_MAX),
  };
}

// --- Member position helper ---

function getMemberPos(
  zset: SortedSetData,
  member: string
): [number, number] | null {
  const score = zset.dict.get(member);
  if (score === undefined) return null;
  return geohashDecode(score);
}

// --- Search result type ---

interface GeoResult {
  member: string;
  dist: number;
  hash: number;
  lon: number;
  lat: number;
}

/**
 * Core search function: iterates all members, filters by shape, returns sorted results.
 */
function geoSearch(
  zset: SortedSetData,
  centerLon: number,
  centerLat: number,
  shape:
    | { type: 'radius'; radiusM: number }
    | { type: 'box'; widthM: number; heightM: number },
  unitFactor: number,
  asc: boolean,
  count: number,
  any: boolean
): GeoResult[] {
  // Compute bounding box for quick filtering
  let bbox: BoundingBox;
  if (shape.type === 'radius') {
    bbox = boundingBoxByRadius(centerLon, centerLat, shape.radiusM);
  } else {
    bbox = boundingBoxByBox(centerLon, centerLat, shape.widthM, shape.heightM);
  }

  const results: GeoResult[] = [];

  // Iterate all members and check against shape
  let node = zset.sl.head.lvl(0).forward;
  while (node) {
    const hash = node.score;
    const [lon, lat] = geohashDecode(hash);

    // Quick bounding box check
    if (
      lon >= bbox.minLon &&
      lon <= bbox.maxLon &&
      lat >= bbox.minLat &&
      lat <= bbox.maxLat
    ) {
      const dist = haversineDistance(centerLon, centerLat, lon, lat);

      let inShape = false;
      if (shape.type === 'radius') {
        inShape = dist <= shape.radiusM;
      } else {
        // Box check: use the bounding box itself (already computed)
        inShape = true; // If within bbox, it's in the box
      }

      if (inShape) {
        results.push({
          member: node.element,
          dist,
          hash,
          lon,
          lat,
        });

        // With ANY + COUNT, stop early
        if (any && count > 0 && results.length >= count) {
          break;
        }
      }
    }

    node = node.lvl(0).forward;
  }

  // Sort by distance
  if (asc) {
    results.sort((a, b) => a.dist - b.dist || a.member.localeCompare(b.member));
  } else {
    results.sort((a, b) => b.dist - a.dist || a.member.localeCompare(b.member));
  }

  // Apply COUNT limit (if not ANY, we sort first then trim)
  if (count > 0 && results.length > count) {
    results.length = count;
  }

  return results;
}

/**
 * Format search results based on WITH* flags.
 */
function formatResults(
  results: GeoResult[],
  withDist: boolean,
  withHash: boolean,
  withCoord: boolean,
  unitFactor: number
): Reply {
  const items: Reply[] = [];

  for (const r of results) {
    if (!withDist && !withHash && !withCoord) {
      items.push(bulkReply(r.member));
    } else {
      const entry: Reply[] = [bulkReply(r.member)];
      if (withDist) {
        entry.push(bulkReply(formatDist(r.dist, unitFactor)));
      }
      if (withHash) {
        entry.push(integerReply(Math.floor(r.hash)));
      }
      if (withCoord) {
        entry.push(
          arrayReply([
            bulkReply(formatFloat(r.lon)),
            bulkReply(formatFloat(r.lat)),
          ])
        );
      }
      items.push(arrayReply(entry));
    }
  }

  return arrayReply(items);
}

// --- GEOADD ---

export function geoadd(db: Database, args: string[], rng: () => number): Reply {
  if (args.length < 4) {
    return errorReply('ERR', "wrong number of arguments for 'geoadd' command");
  }

  const key = args[0] as string;
  let i = 1;

  // Parse flags
  let nx = false;
  let xx = false;
  let ch = false;

  while (i < args.length) {
    const flag = (args[i] as string).toUpperCase();
    if (flag === 'NX') {
      nx = true;
      i++;
    } else if (flag === 'XX') {
      xx = true;
      i++;
    } else if (flag === 'CH') {
      ch = true;
      i++;
    } else {
      break;
    }
  }

  if (nx && xx) {
    return errorReply(
      'ERR',
      'XX and NX options at the same time are not compatible'
    );
  }

  // Remaining args must be lon lat member triples
  const remaining = args.length - i;
  if (remaining < 3 || remaining % 3 !== 0) {
    return errorReply('ERR', "wrong number of arguments for 'geoadd' command");
  }

  // Parse and validate all triples first
  const triples: { lon: number; lat: number; member: string; hash: number }[] =
    [];
  for (; i < args.length; i += 3) {
    const lonParsed = parseFloat64(args[i] as string);
    const latParsed = parseFloat64(args[i + 1] as string);
    if (!lonParsed || !latParsed) {
      return errorReply('ERR', 'value is not a valid float');
    }
    const lon = lonParsed.value;
    const lat = latParsed.value;
    const member = args[i + 2] as string;

    const err = validateCoords(lon, lat);
    if (err) return err;

    const hash = geohashEncode(lon, lat);
    triples.push({ lon, lat, member, hash });
  }

  const { zset, error } = getOrCreateZset(db, key, rng);
  if (error) return error;

  let added = 0;
  let updated = 0;

  for (const { member, hash } of triples) {
    const existing = zset.dict.get(member);

    if (existing !== undefined) {
      if (nx) continue;
      if (hash !== existing) {
        zset.sl.delete(existing, member);
        zset.sl.insert(hash, member);
        zset.dict.set(member, hash);
        updated++;
      }
    } else {
      if (xx) continue;
      zset.sl.insert(hash, member);
      zset.dict.set(member, hash);
      added++;
    }
  }

  removeIfEmpty(db, key, zset);

  return integerReply(ch ? added + updated : added);
}

// --- GEOPOS ---

export function geopos(db: Database, args: string[]): Reply {
  if (args.length < 2) {
    return errorReply('ERR', "wrong number of arguments for 'geopos' command");
  }

  const key = args[0] as string;
  const { zset, error } = getExistingZset(db, key);
  if (error) return error;

  const results: Reply[] = [];
  for (let i = 1; i < args.length; i++) {
    const member = args[i] as string;
    if (!zset) {
      results.push(NIL);
      continue;
    }
    const pos = getMemberPos(zset, member);
    if (!pos) {
      results.push(NIL);
    } else {
      results.push(
        arrayReply([
          bulkReply(formatFloat(pos[0])),
          bulkReply(formatFloat(pos[1])),
        ])
      );
    }
  }

  return arrayReply(results);
}

// --- GEODIST ---

export function geodist(db: Database, args: string[]): Reply {
  if (args.length < 3 || args.length > 4) {
    return errorReply('ERR', "wrong number of arguments for 'geodist' command");
  }

  const key = args[0] as string;
  const member1 = args[1] as string;
  const member2 = args[2] as string;
  const unitStr = args.length === 4 ? (args[3] as string) : 'm';

  const unitFactor = parseUnit(unitStr);
  if (unitFactor === null) return UNIT_ERR;

  const { zset, error } = getExistingZset(db, key);
  if (error) return error;
  if (!zset) return NIL;

  const pos1 = getMemberPos(zset, member1);
  const pos2 = getMemberPos(zset, member2);
  if (!pos1 || !pos2) return NIL;

  const dist = haversineDistance(pos1[0], pos1[1], pos2[0], pos2[1]);
  return bulkReply(formatDist(dist, unitFactor));
}

// --- GEOHASH ---

export function geohash(db: Database, args: string[]): Reply {
  if (args.length < 2) {
    return errorReply('ERR', "wrong number of arguments for 'geohash' command");
  }

  const key = args[0] as string;
  const { zset, error } = getExistingZset(db, key);
  if (error) return error;

  const results: Reply[] = [];
  for (let i = 1; i < args.length; i++) {
    const member = args[i] as string;
    if (!zset) {
      results.push(NIL);
      continue;
    }
    const score = zset.dict.get(member);
    if (score === undefined) {
      results.push(NIL);
    } else {
      results.push(bulkReply(geohashToString(score)));
    }
  }

  return arrayReply(results);
}

// --- GEOSEARCH ---

interface GeoSearchParams {
  fromLon: number;
  fromLat: number;
  shape:
    | { type: 'radius'; radiusM: number }
    | { type: 'box'; widthM: number; heightM: number };
  unitFactor: number;
  asc: boolean;
  count: number;
  any: boolean;
  withDist: boolean;
  withHash: boolean;
  withCoord: boolean;
}

function parseGeoSearchArgs(
  args: string[],
  startIdx: number,
  zset: SortedSetData | null
): GeoSearchParams | Reply {
  let fromLon: number | null = null;
  let fromLat: number | null = null;
  let shape: GeoSearchParams['shape'] | null = null;
  let unitFactor = UNIT_M;
  let asc = true;
  let count = -1;
  let any = false;
  let withDist = false;
  let withHash = false;
  let withCoord = false;

  let i = startIdx;
  while (i < args.length) {
    const opt = (args[i] as string).toUpperCase();

    if (opt === 'FROMMEMBER') {
      i++;
      if (i >= args.length) return errorReply('ERR', 'syntax error');
      const member = args[i] as string;
      if (!zset) {
        // Key doesn't exist - we still parse but center will be null
        fromLon = 0;
        fromLat = 0;
      } else {
        const pos = getMemberPos(zset, member);
        if (!pos) {
          return errorReply('ERR', 'could not decode requested zset member');
        }
        fromLon = pos[0];
        fromLat = pos[1];
      }
      i++;
    } else if (opt === 'FROMLONLAT') {
      i++;
      if (i + 1 >= args.length) return errorReply('ERR', 'syntax error');
      const lonP = parseFloat64(args[i] as string);
      const latP = parseFloat64(args[i + 1] as string);
      if (!lonP || !latP)
        return errorReply('ERR', 'value is not a valid float');
      fromLon = lonP.value;
      fromLat = latP.value;
      const coordErr = validateCoords(fromLon, fromLat);
      if (coordErr) return coordErr;
      i += 2;
    } else if (opt === 'BYRADIUS') {
      i++;
      if (i + 1 >= args.length) return errorReply('ERR', 'syntax error');
      const radiusP = parseFloat64(args[i] as string);
      if (!radiusP || radiusP.value < 0)
        return errorReply('ERR', 'radius cannot be negative');
      const unit = parseUnit(args[i + 1] as string);
      if (unit === null) return UNIT_ERR;
      shape = { type: 'radius', radiusM: radiusP.value * unit };
      unitFactor = unit;
      i += 2;
    } else if (opt === 'BYBOX') {
      i++;
      if (i + 2 >= args.length) return errorReply('ERR', 'syntax error');
      const widthP = parseFloat64(args[i] as string);
      const heightP = parseFloat64(args[i + 1] as string);
      if (!widthP || !heightP)
        return errorReply('ERR', 'value is not a valid float');
      const unit = parseUnit(args[i + 2] as string);
      if (unit === null) return UNIT_ERR;
      shape = {
        type: 'box',
        widthM: widthP.value * unit,
        heightM: heightP.value * unit,
      };
      unitFactor = unit;
      i += 3;
    } else if (opt === 'ASC') {
      asc = true;
      i++;
    } else if (opt === 'DESC') {
      asc = false;
      i++;
    } else if (opt === 'COUNT') {
      i++;
      if (i >= args.length) return errorReply('ERR', 'syntax error');
      const countVal = Number(args[i]);
      if (!Number.isInteger(countVal) || countVal < 0) {
        return errorReply('ERR', 'value is not an integer or out of range');
      }
      count = countVal;
      i++;
      // Check for ANY
      if (i < args.length && (args[i] as string).toUpperCase() === 'ANY') {
        any = true;
        i++;
      }
    } else if (opt === 'WITHCOORD') {
      withCoord = true;
      i++;
    } else if (opt === 'WITHDIST') {
      withDist = true;
      i++;
    } else if (opt === 'WITHHASH') {
      withHash = true;
      i++;
    } else {
      return errorReply('ERR', 'syntax error');
    }
  }

  if (fromLon === null || fromLat === null) {
    return errorReply(
      'ERR',
      'exactly one of FROMMEMBER or FROMLONLAT can be provided for GEOSEARCH'
    );
  }
  if (!shape) {
    return errorReply(
      'ERR',
      'exactly one of BYRADIUS and BYBOX can be provided for GEOSEARCH'
    );
  }

  // ANY without COUNT is an error
  if (any && count < 0) {
    return errorReply('ERR', 'syntax error');
  }

  return {
    fromLon,
    fromLat,
    shape,
    unitFactor,
    asc,
    count,
    any,
    withDist,
    withHash,
    withCoord,
  };
}

export function geosearch(db: Database, args: string[]): Reply {
  if (args.length < 2) {
    return errorReply(
      'ERR',
      "wrong number of arguments for 'geosearch' command"
    );
  }

  const key = args[0] as string;
  const { zset, error } = getExistingZset(db, key);
  if (error) return error;

  const params = parseGeoSearchArgs(args, 1, zset);
  if ('kind' in params) return params;

  if (!zset) return EMPTY_ARRAY;

  if (params.count === 0) return EMPTY_ARRAY;

  const results = geoSearch(
    zset,
    params.fromLon,
    params.fromLat,
    params.shape,
    params.unitFactor,
    params.asc,
    params.count,
    params.any
  );

  return formatResults(
    results,
    params.withDist,
    params.withHash,
    params.withCoord,
    params.unitFactor
  );
}

// --- GEOSEARCHSTORE ---

export function geosearchstore(
  db: Database,
  args: string[],
  rng: () => number
): Reply {
  if (args.length < 2) {
    return errorReply(
      'ERR',
      "wrong number of arguments for 'geosearchstore' command"
    );
  }

  const dst = args[0] as string;
  const src = args[1] as string;

  const { zset: srcZset, error: srcError } = getExistingZset(db, src);
  if (srcError) return srcError;

  // Check for STOREDIST option (can be at end)
  let storeDist = false;
  const searchArgs: string[] = [];
  for (let i = 2; i < args.length; i++) {
    if ((args[i] as string).toUpperCase() === 'STOREDIST') {
      storeDist = true;
    } else {
      searchArgs.push(args[i] as string);
    }
  }

  const params = parseGeoSearchArgs(searchArgs, 0, srcZset);
  if ('kind' in params) return params;

  if (!srcZset) {
    db.delete(dst);
    return ZERO;
  }

  if (params.count === 0) {
    db.delete(dst);
    return ZERO;
  }

  const results = geoSearch(
    srcZset,
    params.fromLon,
    params.fromLat,
    params.shape,
    params.unitFactor,
    params.asc,
    params.count,
    params.any
  );

  if (results.length === 0) {
    db.delete(dst);
    return ZERO;
  }

  // Create destination sorted set
  db.delete(dst);
  const dstZset: SortedSetData = {
    sl: new SkipList(rng),
    dict: new Map(),
  };

  for (const r of results) {
    const score = storeDist ? r.dist / params.unitFactor : r.hash;
    dstZset.sl.insert(score, r.member);
    dstZset.dict.set(r.member, score);
  }

  db.set(dst, 'zset', 'skiplist', dstZset);
  return integerReply(results.length);
}

// --- GEORADIUS (deprecated) ---

export function georadius(
  db: Database,
  args: string[],
  rng: () => number
): Reply {
  if (args.length < 6) {
    return errorReply(
      'ERR',
      "wrong number of arguments for 'georadius' command"
    );
  }

  const key = args[0] as string;
  const lonP = parseFloat64(args[1] as string);
  const latP = parseFloat64(args[2] as string);
  if (!lonP || !latP) return errorReply('ERR', 'value is not a valid float');
  const centerLon = lonP.value;
  const centerLat = latP.value;

  const coordErr = validateCoords(centerLon, centerLat);
  if (coordErr) return coordErr;

  const radiusP = parseFloat64(args[3] as string);
  if (!radiusP) return errorReply('ERR', 'value is not a valid float');

  const unitFactor = parseUnit(args[4] as string);
  if (unitFactor === null) return UNIT_ERR;

  const radiusM = radiusP.value * unitFactor;

  // Parse optional flags
  let withDist = false;
  let withHash = false;
  let withCoord = false;
  let asc = true;
  let count = -1;
  let any = false;
  let storeKey: string | null = null;
  let storeDistKey: string | null = null;

  let i = 5;
  while (i < args.length) {
    const opt = (args[i] as string).toUpperCase();
    if (opt === 'WITHCOORD') {
      withCoord = true;
      i++;
    } else if (opt === 'WITHDIST') {
      withDist = true;
      i++;
    } else if (opt === 'WITHHASH') {
      withHash = true;
      i++;
    } else if (opt === 'ASC') {
      asc = true;
      i++;
    } else if (opt === 'DESC') {
      asc = false;
      i++;
    } else if (opt === 'COUNT') {
      i++;
      if (i >= args.length) return errorReply('ERR', 'syntax error');
      const countVal = Number(args[i]);
      if (!Number.isInteger(countVal) || countVal < 0)
        return errorReply('ERR', 'value is not an integer or out of range');
      count = countVal;
      i++;
      if (i < args.length && (args[i] as string).toUpperCase() === 'ANY') {
        any = true;
        i++;
      }
    } else if (opt === 'STORE') {
      i++;
      if (i >= args.length) return errorReply('ERR', 'syntax error');
      storeKey = args[i] as string;
      i++;
    } else if (opt === 'STOREDIST') {
      i++;
      if (i >= args.length) return errorReply('ERR', 'syntax error');
      storeDistKey = args[i] as string;
      i++;
    } else {
      return errorReply('ERR', 'syntax error');
    }
  }

  // STORE/STOREDIST incompatible with WITH*
  if ((storeKey || storeDistKey) && (withDist || withHash || withCoord)) {
    return errorReply(
      'ERR',
      'STORE option in GEORADIUS is not compatible with WITHDIST, WITHHASH and WITHCOORD options'
    );
  }

  const { zset, error } = getExistingZset(db, key);
  if (error) return error;
  if (!zset) {
    if (storeKey || storeDistKey) return ZERO;
    return EMPTY_ARRAY;
  }

  if (count === 0) {
    if (storeKey || storeDistKey) return ZERO;
    return EMPTY_ARRAY;
  }

  const results = geoSearch(
    zset,
    centerLon,
    centerLat,
    { type: 'radius', radiusM },
    unitFactor,
    asc,
    count,
    any
  );

  if (storeKey || storeDistKey) {
    const dstKey = (storeKey || storeDistKey) as string;
    const storeDist = !!storeDistKey;

    if (results.length === 0) {
      db.delete(dstKey);
      return ZERO;
    }

    db.delete(dstKey);
    const dstZset: SortedSetData = {
      sl: new SkipList(rng),
      dict: new Map(),
    };

    for (const r of results) {
      const score = storeDist ? r.dist / unitFactor : r.hash;
      dstZset.sl.insert(score, r.member);
      dstZset.dict.set(r.member, score);
    }

    db.set(dstKey, 'zset', 'skiplist', dstZset);
    return integerReply(results.length);
  }

  return formatResults(results, withDist, withHash, withCoord, unitFactor);
}

// --- GEORADIUSBYMEMBER ---

export function georadiusbymember(
  db: Database,
  args: string[],
  rng: () => number
): Reply {
  if (args.length < 5) {
    return errorReply(
      'ERR',
      "wrong number of arguments for 'georadiusbymember' command"
    );
  }

  const key = args[0] as string;
  const member = args[1] as string;

  const { zset, error } = getExistingZset(db, key);
  if (error) return error;
  if (!zset) return EMPTY_ARRAY;

  const pos = getMemberPos(zset, member);
  if (!pos) {
    return errorReply('ERR', 'could not decode requested zset member');
  }

  // Rewrite as georadius call: key lon lat radius unit ...rest
  const newArgs = [key, String(pos[0]), String(pos[1]), ...args.slice(2)];
  return georadius(db, newArgs, rng);
}

// --- GEORADIUS_RO ---

export function georadius_ro(db: Database, args: string[]): Reply {
  if (args.length < 6) {
    return errorReply(
      'ERR',
      "wrong number of arguments for 'georadius_ro' command"
    );
  }

  // Same as georadius but without STORE/STOREDIST
  // Check for forbidden options
  for (let i = 5; i < args.length; i++) {
    const opt = (args[i] as string).toUpperCase();
    if (opt === 'STORE' || opt === 'STOREDIST') {
      return errorReply('ERR', 'syntax error');
    }
  }

  // Use a dummy rng since STORE is forbidden
  return georadius(db, args, () => 0.5);
}

// --- GEORADIUSBYMEMBER_RO ---

export function georadiusbymember_ro(db: Database, args: string[]): Reply {
  if (args.length < 5) {
    return errorReply(
      'ERR',
      "wrong number of arguments for 'georadiusbymember_ro' command"
    );
  }

  // Check for forbidden options
  for (let i = 4; i < args.length; i++) {
    const opt = (args[i] as string).toUpperCase();
    if (opt === 'STORE' || opt === 'STOREDIST') {
      return errorReply('ERR', 'syntax error');
    }
  }

  return georadiusbymember(db, args, () => 0.5);
}

// --- Command specs ---

export const specs: CommandSpec[] = [
  {
    name: 'geoadd',
    handler: (ctx, args) => geoadd(ctx.db, args, ctx.engine.rng),
    arity: -5,
    flags: ['write', 'denyoom'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@geo'],
  },
  {
    name: 'geopos',
    handler: (ctx, args) => geopos(ctx.db, args),
    arity: -2,
    flags: ['readonly'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@geo'],
  },
  {
    name: 'geodist',
    handler: (ctx, args) => geodist(ctx.db, args),
    arity: -4,
    flags: ['readonly'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@geo'],
  },
  {
    name: 'geohash',
    handler: (ctx, args) => geohash(ctx.db, args),
    arity: -2,
    flags: ['readonly'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@geo'],
  },
  {
    name: 'geosearch',
    handler: (ctx, args) => geosearch(ctx.db, args),
    arity: -7,
    flags: ['readonly'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@geo'],
  },
  {
    name: 'geosearchstore',
    handler: (ctx, args) => geosearchstore(ctx.db, args, ctx.engine.rng),
    arity: -8,
    flags: ['write', 'denyoom'],
    firstKey: 1,
    lastKey: 2,
    keyStep: 1,
    categories: ['@write', '@geo'],
  },
  {
    name: 'georadius',
    handler: (ctx, args) => georadius(ctx.db, args, ctx.engine.rng),
    arity: -6,
    flags: ['write', 'movablekeys'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@geo'],
  },
  {
    name: 'georadiusbymember',
    handler: (ctx, args) => georadiusbymember(ctx.db, args, ctx.engine.rng),
    arity: -5,
    flags: ['write', 'movablekeys'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@geo'],
  },
  {
    name: 'georadius_ro',
    handler: (ctx, args) => georadius_ro(ctx.db, args),
    arity: -6,
    flags: ['readonly'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@geo'],
  },
  {
    name: 'georadiusbymember_ro',
    handler: (ctx, args) => georadiusbymember_ro(ctx.db, args),
    arity: -5,
    flags: ['readonly'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@geo'],
  },
];
