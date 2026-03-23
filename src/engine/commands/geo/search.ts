import type { Database } from '../../database.ts';
import type { Reply } from '../../types.ts';
import {
  bulkReply,
  integerReply,
  arrayReply,
  errorReply,
  ZERO,
  EMPTY_ARRAY,
} from '../../types.ts';
import { SkipList } from '../../skip-list.ts';
import { parseFloat64, formatFloat } from '../incr.ts';
import type { ConfigStore } from '../../../config-store.ts';
import {
  type SortedSetData,
  chooseEncoding,
  getExistingZset,
} from '../sorted-set/index.ts';
import {
  GEO_LAT_MIN,
  GEO_LAT_MAX,
  EARTH_RADIUS_M,
  UNIT_ERR,
  geohashDecode,
  degToRad,
  haversineDistance,
  parseUnit,
  formatDist,
  validateCoords,
} from './codec.ts';

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

export function getMemberPos(
  zset: SortedSetData,
  member: string
): [number, number] | null {
  const score = zset.dict.get(member);
  if (score === undefined) return null;
  return geohashDecode(score);
}

// --- Search result type ---

export interface GeoResult {
  member: string;
  dist: number;
  hash: number;
  lon: number;
  lat: number;
}

/**
 * Core search function: iterates all members, filters by shape, returns sorted results.
 */
export function geoSearch(
  zset: SortedSetData,
  centerLon: number,
  centerLat: number,
  shape:
    | { type: 'radius'; radiusM: number }
    | { type: 'box'; widthM: number; heightM: number },
  unitFactor: number,
  sort: 'asc' | 'desc' | 'none',
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

  // Sort by distance (Redis: SORT_NONE when no ASC/DESC specified,
  // but COUNT without ANY forces ASC)
  const effectiveSort = sort === 'none' && count > 0 && !any ? 'asc' : sort;
  if (effectiveSort === 'asc') {
    results.sort((a, b) => a.dist - b.dist || a.member.localeCompare(b.member));
  } else if (effectiveSort === 'desc') {
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
export function formatResults(
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

// --- GEOSEARCH argument parsing ---

export interface GeoSearchParams {
  fromLon: number;
  fromLat: number;
  shape:
    | { type: 'radius'; radiusM: number }
    | { type: 'box'; widthM: number; heightM: number };
  unitFactor: number;
  sort: 'asc' | 'desc' | 'none';
  count: number;
  any: boolean;
  withDist: boolean;
  withHash: boolean;
  withCoord: boolean;
}

export function parseGeoSearchArgs(
  args: string[],
  startIdx: number,
  zset: SortedSetData | null
): GeoSearchParams | Reply {
  let fromLon: number | null = null;
  let fromLat: number | null = null;
  let hasFrom = false;
  let shape: GeoSearchParams['shape'] | null = null;
  let hasShape = false;
  let unitFactor = 1; // UNIT_M
  let sort: 'asc' | 'desc' | 'none' = 'none';
  let count = -1;
  let any = false;
  let withDist = false;
  let withHash = false;
  let withCoord = false;

  let i = startIdx;
  while (i < args.length) {
    const opt = (args[i] as string).toUpperCase();

    if (opt === 'FROMMEMBER') {
      if (hasFrom) {
        return errorReply(
          'ERR',
          'exactly one of FROMMEMBER or FROMLONLAT can be specified for GEOSEARCH/GEOSEARCHSTORE'
        );
      }
      hasFrom = true;
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
      if (hasFrom) {
        return errorReply(
          'ERR',
          'exactly one of FROMMEMBER or FROMLONLAT can be specified for GEOSEARCH/GEOSEARCHSTORE'
        );
      }
      hasFrom = true;
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
      if (hasShape) {
        return errorReply(
          'ERR',
          'exactly one of BYRADIUS and BYBOX can be specified for GEOSEARCH/GEOSEARCHSTORE'
        );
      }
      hasShape = true;
      i++;
      if (i + 1 >= args.length) return errorReply('ERR', 'syntax error');
      const radiusP = parseFloat64(args[i] as string);
      if (!radiusP) return errorReply('ERR', 'need numeric radius');
      if (radiusP.value < 0)
        return errorReply('ERR', 'radius cannot be negative');
      const unit = parseUnit(args[i + 1] as string);
      if (unit === null) return UNIT_ERR;
      shape = { type: 'radius', radiusM: radiusP.value * unit };
      unitFactor = unit;
      i += 2;
    } else if (opt === 'ASC') {
      sort = 'asc';
      i++;
    } else if (opt === 'DESC') {
      sort = 'desc';
      i++;
    } else if (opt === 'BYBOX') {
      if (hasShape) {
        return errorReply(
          'ERR',
          'exactly one of BYRADIUS and BYBOX can be specified for GEOSEARCH/GEOSEARCHSTORE'
        );
      }
      hasShape = true;
      i++;
      if (i + 2 >= args.length) return errorReply('ERR', 'syntax error');
      const widthP = parseFloat64(args[i] as string);
      if (!widthP) return errorReply('ERR', 'need numeric width');
      const heightP = parseFloat64(args[i + 1] as string);
      if (!heightP) return errorReply('ERR', 'need numeric height');
      if (widthP.value < 0 || heightP.value < 0)
        return errorReply('ERR', 'height or width cannot be negative');
      const unit = parseUnit(args[i + 2] as string);
      if (unit === null) return UNIT_ERR;
      shape = {
        type: 'box',
        widthM: widthP.value * unit,
        heightM: heightP.value * unit,
      };
      unitFactor = unit;
      i += 3;
    } else if (opt === 'COUNT') {
      i++;
      if (i >= args.length) return errorReply('ERR', 'syntax error');
      const countVal = Number(args[i]);
      if (!Number.isInteger(countVal) || countVal < 0) {
        return errorReply('ERR', 'value is not an integer or out of range');
      }
      if (countVal === 0) {
        return errorReply('ERR', 'COUNT must be > 0');
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
      'exactly one of FROMMEMBER or FROMLONLAT can be specified for GEOSEARCH'
    );
  }
  if (!shape) {
    return errorReply(
      'ERR',
      'exactly one of BYRADIUS and BYBOX can be specified for GEOSEARCH'
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
    sort,
    count,
    any,
    withDist,
    withHash,
    withCoord,
  };
}

// --- GEOSEARCH command handler ---

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

  const results = geoSearch(
    zset,
    params.fromLon,
    params.fromLat,
    params.shape,
    params.unitFactor,
    params.sort,
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

// --- GEOSEARCHSTORE command handler ---

export function geosearchstore(
  db: Database,
  args: string[],
  rng: () => number,
  config?: ConfigStore
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

  // Check for STOREDIST option and reject WITH* options
  let storeDist = false;
  const searchArgs: string[] = [];
  for (let i = 2; i < args.length; i++) {
    const upper = (args[i] as string).toUpperCase();
    if (upper === 'STOREDIST') {
      storeDist = true;
    } else if (
      upper === 'WITHCOORD' ||
      upper === 'WITHDIST' ||
      upper === 'WITHHASH'
    ) {
      return errorReply('ERR', 'syntax error');
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

  const results = geoSearch(
    srcZset,
    params.fromLon,
    params.fromLat,
    params.shape,
    params.unitFactor,
    params.sort,
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

  db.set(dst, 'zset', chooseEncoding(dstZset.dict, config), dstZset);
  return integerReply(results.length);
}

// --- GEORADIUS (deprecated) ---

export function georadius(
  db: Database,
  args: string[],
  rng: () => number,
  config?: ConfigStore
): Reply {
  if (args.length < 5) {
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
  if (!radiusP) return errorReply('ERR', 'need numeric radius');
  if (radiusP.value < 0) return errorReply('ERR', 'radius cannot be negative');

  const unitFactor = parseUnit(args[4] as string);
  if (unitFactor === null) return UNIT_ERR;

  const radiusM = radiusP.value * unitFactor;

  // Parse optional flags
  let withDist = false;
  let withHash = false;
  let withCoord = false;
  let sort: 'asc' | 'desc' | 'none' = 'none';
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
      sort = 'asc';
      i++;
    } else if (opt === 'DESC') {
      sort = 'desc';
      i++;
    } else if (opt === 'COUNT') {
      i++;
      if (i >= args.length) return errorReply('ERR', 'syntax error');
      const countVal = Number(args[i]);
      if (!Number.isInteger(countVal) || countVal < 0)
        return errorReply('ERR', 'value is not an integer or out of range');
      if (countVal === 0) return errorReply('ERR', 'COUNT must be > 0');
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

  const results = geoSearch(
    zset,
    centerLon,
    centerLat,
    { type: 'radius', radiusM },
    unitFactor,
    sort,
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

    db.set(dstKey, 'zset', chooseEncoding(dstZset.dict, config), dstZset);
    return integerReply(results.length);
  }

  return formatResults(results, withDist, withHash, withCoord, unitFactor);
}

// --- GEORADIUSBYMEMBER ---

export function georadiusbymember(
  db: Database,
  args: string[],
  rng: () => number,
  config?: ConfigStore
): Reply {
  if (args.length < 4) {
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
  return georadius(db, newArgs, rng, config);
}

// --- GEORADIUS_RO ---

export function georadius_ro(db: Database, args: string[]): Reply {
  if (args.length < 5) {
    return errorReply(
      'ERR',
      "wrong number of arguments for 'georadius_ro' command"
    );
  }

  for (let i = 5; i < args.length; i++) {
    const opt = (args[i] as string).toUpperCase();
    if (opt === 'STORE' || opt === 'STOREDIST') {
      return errorReply('ERR', 'syntax error');
    }
  }

  return georadius(db, args, () => 0.5);
}

// --- GEORADIUSBYMEMBER_RO ---

export function georadiusbymember_ro(db: Database, args: string[]): Reply {
  if (args.length < 4) {
    return errorReply(
      'ERR',
      "wrong number of arguments for 'georadiusbymember_ro' command"
    );
  }

  for (let i = 4; i < args.length; i++) {
    const opt = (args[i] as string).toUpperCase();
    if (opt === 'STORE' || opt === 'STOREDIST') {
      return errorReply('ERR', 'syntax error');
    }
  }

  return georadiusbymember(db, args, () => 0.5);
}
