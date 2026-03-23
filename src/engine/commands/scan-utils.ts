import type { Reply } from '../types.ts';
import { errorReply, SYNTAX_ERR, NOT_INTEGER_ERR } from '../types.ts';

export const INVALID_CURSOR_ERR = errorReply('ERR', 'invalid cursor');

/**
 * Parse and validate a SCAN cursor string.
 * Returns the numeric cursor or an error reply.
 */
export function parseScanCursor(
  s: string
): { cursor: number; error: null } | { cursor: null; error: Reply } {
  const cursor = parseInt(s, 10);
  if (isNaN(cursor) || cursor < 0) {
    return { cursor: null, error: INVALID_CURSOR_ERR };
  }
  return { cursor, error: null };
}

export interface ScanOptions {
  matchPattern: string | null;
  count: number;
}

/**
 * Parse MATCH and COUNT options from a SCAN-family command.
 * Handles only the common MATCH/COUNT flags — command-specific flags
 * (TYPE for SCAN, NOVALUES for HSCAN) must be handled by the caller
 * via the `extraFlags` callback.
 *
 * @param args - full argument array
 * @param startIndex - index where option flags begin
 * @param extraFlags - optional callback for command-specific flags;
 *   receives (flag, args, currentIndex) and returns the new index after
 *   consuming the flag's arguments, or null to signal SYNTAX_ERR
 */
export function parseScanOptions(
  args: string[],
  startIndex: number,
  extraFlags?: (flag: string, args: string[], i: number) => number | null
): { options: ScanOptions; error: null } | { options: null; error: Reply } {
  let matchPattern: string | null = null;
  let count = 10;

  let i = startIndex;
  while (i < args.length) {
    const flag = (args[i] ?? '').toUpperCase();
    if (flag === 'MATCH') {
      i++;
      matchPattern = args[i] ?? '*';
    } else if (flag === 'COUNT') {
      i++;
      count = parseInt(args[i] ?? '10', 10);
      if (isNaN(count)) {
        return { options: null, error: NOT_INTEGER_ERR };
      }
      if (count < 1) {
        return { options: null, error: SYNTAX_ERR };
      }
    } else if (extraFlags) {
      const next = extraFlags(flag, args, i);
      if (next === null) {
        return { options: null, error: SYNTAX_ERR };
      }
      i = next;
    } else {
      return { options: null, error: SYNTAX_ERR };
    }
    i++;
  }

  return { options: { matchPattern, count }, error: null };
}
