import type { RedisEncoding, Reply } from '../../types.ts';
import { NOT_INTEGER_ERR } from '../../types.ts';
import { INT64_MAX, INT64_MIN, strByteLength } from '../../utils.ts';

const INT_PATTERN = /^-?[1-9]\d*$|^0$/;

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

export function strToBytes(s: string): Uint8Array {
  return textEncoder.encode(s);
}

export function bytesToStr(b: Uint8Array): string {
  return textDecoder.decode(b);
}

export function parseIntArg(s: string): { value: number; error: Reply | null } {
  const val = parseInt(s, 10);
  if (isNaN(val) || String(val) !== s) {
    return { value: 0, error: NOT_INTEGER_ERR };
  }
  return { value: val, error: null };
}

export function determineStringEncoding(value: string): RedisEncoding {
  if (INT_PATTERN.test(value)) {
    try {
      const n = BigInt(value);
      if (n >= INT64_MIN && n <= INT64_MAX) {
        return 'int';
      }
    } catch {
      // not a valid bigint — fall through
    }
  }

  return strByteLength(value) <= 44 ? 'embstr' : 'raw';
}
