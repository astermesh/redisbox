import type { Database } from '../../database.ts';
import type { Reply } from '../../types.ts';
import {
  bulkReply,
  integerReply,
  arrayReply,
  ZERO,
  WRONGTYPE_ERR,
  SYNTAX_ERR,
  STRING_EXCEEDS_512MB_ERR,
  OFFSET_OUT_OF_RANGE_ERR,
} from '../../types.ts';
import { strByteLength } from '../../utils.ts';
import {
  strToBytes,
  bytesToStr,
  parseIntArg,
  determineStringEncoding,
} from './encoding.ts';

// --- APPEND ---

export function append(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';
  const appendValue = args[1] ?? '';

  const entry = db.get(key);
  if (entry && entry.type !== 'string') return WRONGTYPE_ERR;

  if (!entry) {
    // Key doesn't exist: create with determined encoding
    const encoding = determineStringEncoding(appendValue);
    db.set(key, 'string', encoding, appendValue);
    return integerReply(strByteLength(appendValue));
  }

  // Key exists: append and always use raw encoding
  const existingValue = entry.value as string;
  const newValue = existingValue + appendValue;
  db.set(key, 'string', 'raw', newValue);

  return integerReply(strByteLength(newValue));
}

// --- STRLEN ---

export function strlen(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';
  const entry = db.get(key);
  if (!entry) return ZERO;
  if (entry.type !== 'string') return WRONGTYPE_ERR;
  return integerReply(strByteLength(entry.value as string));
}

// --- SETRANGE ---

export function setrange(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';
  const { value: offset, error } = parseIntArg(args[1] ?? '');
  if (error) return error;
  if (offset < 0) return OFFSET_OUT_OF_RANGE_ERR;

  const newValueBytes = strToBytes(args[2] ?? '');

  // Empty value special case
  if (newValueBytes.length === 0) {
    const entry = db.get(key);
    if (!entry) return ZERO;
    if (entry.type !== 'string') return WRONGTYPE_ERR;
    return integerReply(strByteLength(entry.value as string));
  }

  const entry = db.get(key);
  if (entry && entry.type !== 'string') return WRONGTYPE_ERR;

  const existingBytes = entry
    ? strToBytes(entry.value as string)
    : new Uint8Array(0);
  const requiredLen = Math.max(
    existingBytes.length,
    offset + newValueBytes.length
  );

  if (requiredLen > 512 * 1024 * 1024) {
    return STRING_EXCEEDS_512MB_ERR;
  }

  const result = new Uint8Array(requiredLen);
  result.set(existingBytes);
  result.set(newValueBytes, offset);

  db.set(key, 'string', 'raw', bytesToStr(result));

  return integerReply(requiredLen);
}

// --- GETRANGE / SUBSTR ---

export function getrange(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';
  const { value: start, error: startErr } = parseIntArg(args[1] ?? '');
  if (startErr) return startErr;
  const { value: end, error: endErr } = parseIntArg(args[2] ?? '');
  if (endErr) return endErr;

  const entry = db.get(key);
  if (!entry) return bulkReply('');
  if (entry.type !== 'string') return WRONGTYPE_ERR;

  const bytes = strToBytes(entry.value as string);
  const len = bytes.length;
  if (len === 0) return bulkReply('');

  const s = start < 0 ? Math.max(len + start, 0) : start;
  let e = end < 0 ? Math.max(len + end, 0) : end;
  if (e >= len) e = len - 1;
  if (s > e) return bulkReply('');

  return bulkReply(bytesToStr(bytes.slice(s, e + 1)));
}

// --- LCS ---

export function lcs(db: Database, args: string[]): Reply {
  const key1 = args[0] ?? '';
  const key2 = args[1] ?? '';

  const entry1 = db.get(key1);
  const entry2 = db.get(key2);

  if (entry1 && entry1.type !== 'string') return WRONGTYPE_ERR;
  if (entry2 && entry2.type !== 'string') return WRONGTYPE_ERR;

  const s1 = entry1 ? (entry1.value as string) : '';
  const s2 = entry2 ? (entry2.value as string) : '';

  // Parse options
  let wantLen = false;
  let wantIdx = false;
  let minMatchLen = 0;
  let withMatchLen = false;

  let i = 2;
  while (i < args.length) {
    const opt = (args[i] ?? '').toUpperCase();
    switch (opt) {
      case 'LEN':
        wantLen = true;
        break;
      case 'IDX':
        wantIdx = true;
        break;
      case 'MINMATCHLEN': {
        i++;
        if (i >= args.length) return SYNTAX_ERR;
        const { value: val, error: parseErr } = parseIntArg(args[i] ?? '');
        if (parseErr) return parseErr;
        minMatchLen = Math.max(val, 0);
        break;
      }
      case 'WITHMATCHLEN':
        withMatchLen = true;
        break;
      default:
        return SYNTAX_ERR;
    }
    i++;
  }

  // Compute LCS using DP — use flat array for efficient access
  const m = s1.length;
  const n = s2.length;
  const w = n + 1;

  const dp = new Int32Array((m + 1) * w);
  const at = (r: number, c: number): number => dp[r * w + c] ?? 0;

  for (let r = 1; r <= m; r++) {
    for (let c = 1; c <= n; c++) {
      if (s1[r - 1] === s2[c - 1]) {
        dp[r * w + c] = at(r - 1, c - 1) + 1;
      } else {
        dp[r * w + c] = Math.max(at(r - 1, c), at(r, c - 1));
      }
    }
  }

  const lcsLen = at(m, n);

  // If only length requested (and not IDX)
  if (wantLen && !wantIdx) {
    return integerReply(lcsLen);
  }

  // Backtrack to find the LCS positions
  const positions: [number, number][] = [];
  {
    let r = m,
      c = n;
    while (r > 0 && c > 0) {
      if (s1[r - 1] === s2[c - 1]) {
        positions.push([r - 1, c - 1]);
        r--;
        c--;
      } else if (at(r - 1, c) >= at(r, c - 1)) {
        r--;
      } else {
        c--;
      }
    }
    positions.reverse();
  }

  if (!wantIdx) {
    // Build LCS string
    const lcsStr = positions.map(([a]) => s1[a]).join('');
    return bulkReply(lcsStr);
  }

  // Build IDX output — group consecutive matching positions into ranges
  interface Match {
    aStart: number;
    aEnd: number;
    bStart: number;
    bEnd: number;
  }

  const rawMatches: Match[] = [];
  for (const [a, b] of positions) {
    const last = rawMatches[rawMatches.length - 1];
    if (last && a === last.aEnd + 1 && b === last.bEnd + 1) {
      last.aEnd = a;
      last.bEnd = b;
    } else {
      rawMatches.push({ aStart: a, aEnd: a, bStart: b, bEnd: b });
    }
  }

  // Filter by MINMATCHLEN
  const filteredMatches = rawMatches.filter(
    (match) => match.aEnd - match.aStart + 1 >= minMatchLen
  );

  // Build array reply — matches are in reverse order in Redis
  const matchReplies: Reply[] = filteredMatches.reverse().map((match) => {
    const parts: Reply[] = [
      arrayReply([integerReply(match.aStart), integerReply(match.aEnd)]),
      arrayReply([integerReply(match.bStart), integerReply(match.bEnd)]),
    ];
    if (withMatchLen) {
      parts.push(integerReply(match.aEnd - match.aStart + 1));
    }
    return arrayReply(parts);
  });

  return arrayReply([
    bulkReply('matches'),
    arrayReply(matchReplies),
    bulkReply('len'),
    integerReply(lcsLen),
  ]);
}
