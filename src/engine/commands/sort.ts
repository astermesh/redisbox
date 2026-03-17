import type { Database } from '../database.ts';
import type { Reply } from '../types.ts';
import {
  bulkReply,
  arrayReply,
  integerReply,
  errorReply,
  EMPTY_ARRAY,
  wrongTypeError,
  NIL,
} from '../types.ts';

interface SortOptions {
  by: string | null;
  limit: { offset: number; count: number } | null;
  getPatterns: string[];
  desc: boolean;
  alpha: boolean;
  store: string | null;
}

function parseSortArgs(args: string[]): SortOptions | Reply {
  const options: SortOptions = {
    by: null,
    limit: null,
    getPatterns: [],
    desc: false,
    alpha: false,
    store: null,
  };

  let i = 1; // skip key
  while (i < args.length) {
    const flag = (args[i] ?? '').toUpperCase();
    switch (flag) {
      case 'BY':
        i++;
        if (i >= args.length) {
          return errorReply('ERR', 'syntax error');
        }
        options.by = args[i] ?? '';
        break;
      case 'LIMIT':
        i++;
        if (i + 1 >= args.length) {
          return errorReply('ERR', 'syntax error');
        }
        {
          const offset = parseInt(args[i] ?? '', 10);
          i++;
          const count = parseInt(args[i] ?? '', 10);
          if (isNaN(offset) || isNaN(count)) {
            return errorReply('ERR', 'value is not an integer or out of range');
          }
          options.limit = { offset, count };
        }
        break;
      case 'GET':
        i++;
        if (i >= args.length) {
          return errorReply('ERR', 'syntax error');
        }
        options.getPatterns.push(args[i] ?? '');
        break;
      case 'ASC':
        options.desc = false;
        break;
      case 'DESC':
        options.desc = true;
        break;
      case 'ALPHA':
        options.alpha = true;
        break;
      case 'STORE':
        i++;
        if (i >= args.length) {
          return errorReply('ERR', 'syntax error');
        }
        options.store = args[i] ?? '';
        break;
      default:
        return errorReply('ERR', 'syntax error');
    }
    i++;
  }
  return options;
}

function resolvePattern(
  db: Database,
  pattern: string,
  element: string
): string | null {
  const resolved = pattern.replace('*', element);

  if (resolved.includes('->')) {
    const arrowIdx = resolved.indexOf('->');
    const hashKey = resolved.substring(0, arrowIdx);
    const field = resolved.substring(arrowIdx + 2);
    const entry = db.get(hashKey);
    if (!entry || entry.type !== 'hash') return null;
    const hash = entry.value as Map<string, string>;
    return hash.get(field) ?? null;
  }

  const entry = db.get(resolved);
  if (!entry || entry.type !== 'string') return null;
  return entry.value as string;
}

function getElements(db: Database, key: string): string[] | Reply {
  const entry = db.get(key);
  if (!entry) return [];

  switch (entry.type) {
    case 'list':
      return (entry.value as string[]).slice();
    case 'set':
      return Array.from(entry.value as Set<string>);
    case 'zset': {
      const zset = entry.value as Map<string, number>;
      return Array.from(zset.keys());
    }
    default:
      return wrongTypeError();
  }
}

export function sort(db: Database, args: string[]): Reply {
  if (args.length === 0) {
    return errorReply('ERR', "wrong number of arguments for 'sort' command");
  }

  const key = args[0] ?? '';
  const parsed = parseSortArgs(args);
  if ('kind' in parsed) return parsed;
  const options = parsed;

  return executeSortCommand(db, key, options);
}

export function sortRo(db: Database, args: string[]): Reply {
  if (args.length === 0) {
    return errorReply('ERR', "wrong number of arguments for 'sort_ro' command");
  }

  const key = args[0] ?? '';
  const parsed = parseSortArgs(args);
  if ('kind' in parsed) return parsed;
  const options = parsed;

  if (options.store !== null) {
    return errorReply('ERR', 'syntax error');
  }

  return executeSortCommand(db, key, options);
}

function executeSortCommand(
  db: Database,
  key: string,
  options: SortOptions
): Reply {
  const elementsResult = getElements(db, key);
  if (!Array.isArray(elementsResult)) return elementsResult;
  const elements = elementsResult;

  if (elements.length === 0) {
    if (options.store !== null) {
      db.delete(options.store);
      return integerReply(0);
    }
    return EMPTY_ARRAY;
  }

  // Sort unless BY nosort
  const isNosort = options.by !== null && options.by.toLowerCase() === 'nosort';

  if (!isNosort) {
    // Check for non-numeric values when not using ALPHA and no BY
    if (!options.alpha && options.by === null) {
      for (const el of elements) {
        if (isNaN(parseFloat(el))) {
          return errorReply(
            'ERR',
            "One or more scores can't be converted into double"
          );
        }
      }
    }

    const sortKeys = new Map<string, string | null>();

    if (options.by !== null) {
      for (const el of elements) {
        sortKeys.set(el, resolvePattern(db, options.by, el));
      }
    }

    elements.sort((a, b) => {
      let va: string | null = a;
      let vb: string | null = b;

      if (options.by !== null) {
        va = sortKeys.get(a) ?? null;
        vb = sortKeys.get(b) ?? null;
      }

      let cmp: number;
      if (options.alpha) {
        const sa = va ?? '';
        const sb = vb ?? '';
        cmp = sa < sb ? -1 : sa > sb ? 1 : 0;
      } else {
        const na = va !== null ? parseFloat(va) : 0;
        const nb = vb !== null ? parseFloat(vb) : 0;
        cmp = na - nb;
      }

      return options.desc ? -cmp : cmp;
    });
  }

  // Apply LIMIT
  let result = elements;
  if (options.limit !== null) {
    const { offset, count } = options.limit;
    result = elements.slice(offset, offset + count);
  }

  // Apply GET patterns
  if (options.getPatterns.length > 0) {
    const getResults: Reply[] = [];
    for (const el of result) {
      for (const pattern of options.getPatterns) {
        if (pattern === '#') {
          getResults.push(bulkReply(el));
        } else {
          const val = resolvePattern(db, pattern, el);
          getResults.push(val !== null ? bulkReply(val) : NIL);
        }
      }
    }

    if (options.store !== null) {
      return storeResults(db, options.store, getResults);
    }
    return arrayReply(getResults);
  }

  // No GET patterns — return elements directly
  const replyItems = result.map((el) => bulkReply(el));

  if (options.store !== null) {
    return storeResults(db, options.store, replyItems);
  }
  return arrayReply(replyItems);
}

function storeResults(db: Database, destKey: string, items: Reply[]): Reply {
  if (items.length === 0) {
    db.delete(destKey);
    return integerReply(0);
  }

  const values = items.map((item) =>
    item.kind === 'bulk' ? (item.value ?? '') : ''
  );
  db.set(destKey, 'list', 'quicklist', values);
  return integerReply(items.length);
}
