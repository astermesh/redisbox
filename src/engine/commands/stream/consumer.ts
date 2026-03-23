import type { Database } from '../../database.ts';
import type { Reply } from '../../types.ts';
import {
  arrayReply,
  bulkReply,
  errorReply,
  integerReply,
  NIL_ARRAY,
  ZERO,
  SYNTAX_ERR,
} from '../../types.ts';
import {
  parseStreamId,
  compareStreamIds,
  streamIdToString,
} from '../../stream.ts';
import type {
  StreamEntry,
  StreamId,
  PendingEntry,
  ConsumerGroup,
  StreamConsumer,
} from '../../stream.ts';
import {
  getStream,
  safeParseId,
  entryToReply,
  parseCount,
  parseRangeId,
  INVALID_STREAM_ID_ERR,
} from './utils.ts';

function ensureConsumer(
  group: ConsumerGroup,
  name: string,
  clockMs: number
): StreamConsumer {
  let consumer = group.consumers.get(name);
  if (!consumer) {
    consumer = { name, seenTime: clockMs, activeTime: 0, pending: new Map() };
    group.consumers.set(name, consumer);
  }
  consumer.seenTime = clockMs;
  return consumer;
}

/**
 * XREADGROUP GROUP group consumer [COUNT count] [BLOCK milliseconds] [NOACK] STREAMS key [key ...] id [id ...]
 */
export function xreadgroup(
  db: Database,
  clockMs: number,
  args: string[]
): Reply {
  let i = 0;

  // Must start with GROUP keyword
  if (args.length < 4 || (args[i] as string).toUpperCase() !== 'GROUP') {
    return errorReply(
      'ERR',
      "wrong number of arguments for 'xreadgroup' command"
    );
  }
  i++;

  const groupName = args[i++] as string;
  const consumerName = args[i++] as string;

  let count: number | undefined;
  let noack = false;
  let streamsIdx = -1;

  while (i < args.length) {
    const upper = (args[i] as string).toUpperCase();

    if (upper === 'COUNT') {
      const result = parseCount(args, i);
      if ('error' in result) return result.error;
      count = result.count;
      i = result.nextIdx;
      continue;
    }

    if (upper === 'BLOCK') {
      // Accept BLOCK syntax but don't actually block
      i++;
      const blockMs = args[i];
      if (blockMs === undefined) return SYNTAX_ERR;
      const n = Number(blockMs);
      if (!Number.isInteger(n) || n < 0) {
        return errorReply('ERR', 'value is not an integer or out of range');
      }
      i++;
      continue;
    }

    if (upper === 'NOACK') {
      noack = true;
      i++;
      continue;
    }

    if (upper === 'STREAMS') {
      streamsIdx = i + 1;
      break;
    }

    return SYNTAX_ERR;
  }

  if (streamsIdx === -1) {
    return errorReply(
      'ERR',
      "Unbalanced 'xreadgroup' list of streams: for each stream key an ID or '>' must be specified."
    );
  }

  const remaining = args.slice(streamsIdx);
  if (remaining.length === 0 || remaining.length % 2 !== 0) {
    return errorReply(
      'ERR',
      "Unbalanced 'xreadgroup' list of streams: for each stream key an ID or '>' must be specified."
    );
  }

  const numStreams = remaining.length / 2;
  const keys = remaining.slice(0, numStreams);
  const ids = remaining.slice(numStreams);

  const resultStreams: Reply[] = [];

  for (let j = 0; j < numStreams; j++) {
    const key = keys[j] as string;
    const idArg = ids[j] as string;

    const lookup = getStream(db, key);
    if (lookup.error) return lookup.error;
    if (!lookup.stream) {
      return errorReply(
        'NOGROUP',
        "No such key '" +
          key +
          "' or consumer group '" +
          groupName +
          "' in XREADGROUP with GROUP option"
      );
    }

    const stream = lookup.stream;
    const group = stream.getGroup(groupName);
    if (!group) {
      return errorReply(
        'NOGROUP',
        "No such key '" +
          key +
          "' or consumer group '" +
          groupName +
          "' in XREADGROUP with GROUP option"
      );
    }

    // Ensure consumer exists
    const consumer = ensureConsumer(group, consumerName, clockMs);

    if (idArg === '$') {
      return errorReply(
        'ERR',
        'The $ ID is meaningless in the context of XREADGROUP: you want to read the history of this consumer by specifying a proper ID, or use the > ID to get new messages. The $ ID would just return an empty result set.'
      );
    }

    if (idArg === '>') {
      // Read new (undelivered) messages
      const entries = stream.entriesAfter(group.lastDeliveredId, count);
      if (entries.length === 0) continue;

      // Update lastDeliveredId to the last entry we're delivering
      const lastEntry = entries[entries.length - 1] as StreamEntry;
      group.lastDeliveredId = { ...safeParseId(lastEntry.id) };
      group.entriesRead += entries.length;

      if (!noack) {
        // Add to PEL
        for (const entry of entries) {
          const pe: PendingEntry = {
            entryId: entry.id,
            consumer: consumerName,
            deliveryTime: clockMs,
            deliveryCount: 1,
          };
          group.pel.set(entry.id, pe);
          consumer.pending.set(entry.id, pe);
        }
      }

      // Entries were actually delivered — update activeTime
      consumer.activeTime = clockMs;

      resultStreams.push(
        arrayReply([bulkReply(key), arrayReply(entries.map(entryToReply))])
      );
    } else {
      // Read pending entries for this consumer (entries in consumer's PEL with ID > idArg)
      const afterId = parseStreamId(idArg);
      if (!afterId) return INVALID_STREAM_ID_ERR;

      // Collect pending replies with ID > afterId
      const pendingReplies: Reply[] = [];
      // We need to iterate in order, so sort by entry ID
      const sortedPending = [...consumer.pending.keys()].sort((a, b) =>
        compareStreamIds(safeParseId(a), safeParseId(b))
      );

      for (const entryId of sortedPending) {
        const eid = safeParseId(entryId);
        if (compareStreamIds(eid, afterId) <= 0) continue;

        // Update delivery time and count (matches real Redis behavior)
        const pe = consumer.pending.get(entryId);
        if (pe) {
          pe.deliveryTime = clockMs;
          pe.deliveryCount++;
        }

        // Find the actual entry in the stream
        const entryData = stream.range(eid, eid, 1);
        if (entryData.length > 0) {
          pendingReplies.push(entryToReply(entryData[0] as StreamEntry));
        } else {
          // Entry was deleted (XDEL/XTRIM) — return [id, null]
          pendingReplies.push(
            arrayReply([bulkReply(entryId), bulkReply(null)])
          );
        }
        if (count !== undefined && pendingReplies.length >= count) break;
      }

      resultStreams.push(
        arrayReply([bulkReply(key), arrayReply(pendingReplies)])
      );
    }
  }

  if (resultStreams.length === 0) return NIL_ARRAY;
  return arrayReply(resultStreams);
}

/**
 * XACK key group id [id ...]
 */
export function xack(db: Database, args: string[]): Reply {
  if (args.length < 3) {
    return errorReply('ERR', "wrong number of arguments for 'xack' command");
  }

  const key = args[0] as string;
  const groupName = args[1] as string;

  const lookup = getStream(db, key);
  if (lookup.error) return lookup.error;
  if (!lookup.stream) return ZERO;

  const group = lookup.stream.getGroup(groupName);
  if (!group) return ZERO;

  let acked = 0;
  for (let i = 2; i < args.length; i++) {
    const idStr = args[i] as string;
    const parsed = parseStreamId(idStr);
    if (!parsed) return INVALID_STREAM_ID_ERR;

    const entryId = `${parsed.ms}-${parsed.seq}`;
    const pe = group.pel.get(entryId);
    if (pe) {
      // Remove from group PEL
      group.pel.delete(entryId);
      // Remove from consumer's pending
      const consumer = group.consumers.get(pe.consumer);
      if (consumer) {
        consumer.pending.delete(entryId);
      }
      acked++;
    }
  }

  return integerReply(acked);
}

/**
 * XPENDING key group [[IDLE min-idle-time] start end count [consumer]]
 */
export function xpending(db: Database, clockMs: number, args: string[]): Reply {
  if (args.length < 2) {
    return errorReply(
      'ERR',
      "wrong number of arguments for 'xpending' command"
    );
  }

  const key = args[0] as string;
  const groupName = args[1] as string;

  const lookup = getStream(db, key);
  if (lookup.error) return lookup.error;
  if (!lookup.stream) {
    return errorReply(
      'NOGROUP',
      "No such key '" + key + "' or consumer group '" + groupName + "'"
    );
  }

  const group = lookup.stream.getGroup(groupName);
  if (!group) {
    return errorReply(
      'NOGROUP',
      "No such key '" + key + "' or consumer group '" + groupName + "'"
    );
  }

  // Summary form: XPENDING key group
  if (args.length === 2) {
    return xpendingSummary(group);
  }

  // Detail form: XPENDING key group [[IDLE min-idle-time] start end count [consumer]]
  let i = 2;
  let minIdle = 0;

  if ((args[i] as string).toUpperCase() === 'IDLE') {
    i++;
    const idleStr = args[i];
    if (idleStr === undefined) return SYNTAX_ERR;
    const n = Number(idleStr);
    if (!Number.isInteger(n) || n < 0) {
      return errorReply('ERR', 'value is not an integer or out of range');
    }
    minIdle = n;
    i++;
  }

  const startArg = args[i++];
  const endArg = args[i++];
  const countArg = args[i++];

  if (
    startArg === undefined ||
    endArg === undefined ||
    countArg === undefined
  ) {
    return SYNTAX_ERR;
  }

  const start = parseRangeId(startArg, 'start');
  if (!start) return INVALID_STREAM_ID_ERR;
  if ('error' in start) return start.error;

  const end = parseRangeId(endArg, 'end');
  if (!end) return INVALID_STREAM_ID_ERR;
  if ('error' in end) return end.error;

  const countN = Number(countArg);
  if (!Number.isInteger(countN) || countN < 0) {
    return errorReply('ERR', 'value is not an integer or out of range');
  }

  const consumerFilter = args[i] as string | undefined;

  return xpendingDetail(
    group,
    start,
    end,
    countN,
    consumerFilter,
    minIdle,
    clockMs
  );
}

function xpendingSummary(group: ConsumerGroup): Reply {
  if (group.pel.size === 0) {
    return arrayReply([
      integerReply(0),
      bulkReply(null),
      bulkReply(null),
      NIL_ARRAY,
    ]);
  }

  // Find min and max IDs, and count per consumer
  let minId: StreamId | null = null;
  let maxId: StreamId | null = null;
  const consumerCounts = new Map<string, number>();

  for (const [entryId, pe] of group.pel) {
    const eid = safeParseId(entryId);
    if (minId === null || compareStreamIds(eid, minId) < 0) minId = eid;
    if (maxId === null || compareStreamIds(eid, maxId) > 0) maxId = eid;
    consumerCounts.set(pe.consumer, (consumerCounts.get(pe.consumer) ?? 0) + 1);
  }

  // Build consumer list sorted by name
  const consumerList: Reply[] = [...consumerCounts.entries()]
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([name, cnt]) =>
      arrayReply([bulkReply(name), bulkReply(String(cnt))])
    );

  return arrayReply([
    integerReply(group.pel.size),
    bulkReply(minId ? streamIdToString(minId) : null),
    bulkReply(maxId ? streamIdToString(maxId) : null),
    arrayReply(consumerList),
  ]);
}

function xpendingDetail(
  group: ConsumerGroup,
  start: StreamId,
  end: StreamId,
  count: number,
  consumerFilter: string | undefined,
  minIdle: number,
  clockMs: number
): Reply {
  // Collect and sort all PEL entries
  const entries: PendingEntry[] = [];

  for (const [entryId, pe] of group.pel) {
    if (consumerFilter && pe.consumer !== consumerFilter) continue;

    const eid = safeParseId(entryId);
    if (compareStreamIds(eid, start) < 0) continue;
    if (compareStreamIds(eid, end) > 0) continue;

    if (minIdle > 0) {
      const idle = clockMs - pe.deliveryTime;
      if (idle < minIdle) continue;
    }

    entries.push(pe);
  }

  // Sort by entry ID
  entries.sort((a, b) =>
    compareStreamIds(safeParseId(a.entryId), safeParseId(b.entryId))
  );

  // Apply count limit
  const limited = entries.slice(0, count);

  // Format: [id, consumer, idle-time-ms, delivery-count]
  const result: Reply[] = limited.map((pe) => {
    const idle = clockMs - pe.deliveryTime;
    return arrayReply([
      bulkReply(pe.entryId),
      bulkReply(pe.consumer),
      integerReply(idle),
      integerReply(pe.deliveryCount),
    ]);
  });

  return arrayReply(result);
}

/**
 * XCLAIM key group consumer min-idle-time id [id ...] [IDLE ms] [TIME ms] [RETRYCOUNT count] [FORCE] [JUSTID] [LASTID id]
 */
export function xclaim(db: Database, clockMs: number, args: string[]): Reply {
  if (args.length < 5) {
    return errorReply('ERR', "wrong number of arguments for 'xclaim' command");
  }

  const key = args[0] as string;
  const groupName = args[1] as string;
  const consumerName = args[2] as string;
  const minIdleStr = args[3] as string;

  const minIdleTime = Number(minIdleStr);
  if (!Number.isInteger(minIdleTime) || minIdleTime < 0) {
    return errorReply('ERR', 'value is not an integer or out of range');
  }

  const lookup = getStream(db, key);
  if (lookup.error) return lookup.error;
  if (!lookup.stream) {
    return errorReply(
      'ERR',
      "No such key '" +
        key +
        "' or consumer group '" +
        groupName +
        "' in XCLAIM"
    );
  }

  const stream = lookup.stream;
  const group = stream.getGroup(groupName);
  if (!group) {
    return errorReply(
      'NOGROUP',
      "No such key '" +
        key +
        "' or consumer group '" +
        groupName +
        "' in XCLAIM"
    );
  }

  // Parse IDs and options
  const claimIds: StreamId[] = [];
  let idle: number | null = null;
  let timeMs: number | null = null;
  let retrycount: number | null = null;
  let force = false;
  let justid = false;
  let _lastid: StreamId | null = null;

  let i = 4;
  // First parse IDs until we hit an option keyword
  while (i < args.length) {
    const upper = (args[i] as string).toUpperCase();
    if (
      upper === 'IDLE' ||
      upper === 'TIME' ||
      upper === 'RETRYCOUNT' ||
      upper === 'FORCE' ||
      upper === 'JUSTID' ||
      upper === 'LASTID'
    ) {
      break;
    }
    const parsed = parseStreamId(args[i] as string);
    if (!parsed) return INVALID_STREAM_ID_ERR;
    claimIds.push(parsed);
    i++;
  }

  if (claimIds.length === 0) {
    return errorReply('ERR', "wrong number of arguments for 'xclaim' command");
  }

  // Parse options
  while (i < args.length) {
    const upper = (args[i] as string).toUpperCase();
    if (upper === 'IDLE') {
      i++;
      if (i >= args.length) return SYNTAX_ERR;
      const n = Number(args[i]);
      if (!Number.isInteger(n) || n < 0) {
        return errorReply('ERR', 'value is not an integer or out of range');
      }
      idle = n;
      i++;
    } else if (upper === 'TIME') {
      i++;
      if (i >= args.length) return SYNTAX_ERR;
      const n = Number(args[i]);
      if (!Number.isInteger(n) || n < 0) {
        return errorReply('ERR', 'value is not an integer or out of range');
      }
      timeMs = n;
      i++;
    } else if (upper === 'RETRYCOUNT') {
      i++;
      if (i >= args.length) return SYNTAX_ERR;
      const n = Number(args[i]);
      if (!Number.isInteger(n) || n < 0) {
        return errorReply('ERR', 'value is not an integer or out of range');
      }
      retrycount = n;
      i++;
    } else if (upper === 'FORCE') {
      force = true;
      i++;
    } else if (upper === 'JUSTID') {
      justid = true;
      i++;
    } else if (upper === 'LASTID') {
      i++;
      if (i >= args.length) return SYNTAX_ERR;
      const parsed = parseStreamId(args[i] as string);
      if (!parsed) return INVALID_STREAM_ID_ERR;
      _lastid = parsed;
      i++;
    } else {
      return SYNTAX_ERR;
    }
  }

  // Determine delivery time for claimed entries
  let deliveryTime: number;
  if (timeMs !== null) {
    deliveryTime = timeMs;
  } else if (idle !== null) {
    deliveryTime = clockMs - idle;
  } else {
    deliveryTime = clockMs;
  }

  // Ensure consumer exists
  const consumer = ensureConsumer(group, consumerName, clockMs);

  // Update LASTID if provided
  if (_lastid !== null) {
    if (compareStreamIds(_lastid, group.lastDeliveredId) > 0) {
      group.lastDeliveredId = { ..._lastid };
    }
  }

  const result: Reply[] = [];

  for (const claimId of claimIds) {
    const entryIdStr = streamIdToString(claimId);
    const pe = group.pel.get(entryIdStr);

    if (!pe) {
      // Not in PEL — only claim if FORCE and entry exists in stream
      // (min-idle-time does not apply to FORCE-created entries)
      if (force && stream.hasEntry(entryIdStr)) {
        const newPe: PendingEntry = {
          entryId: entryIdStr,
          consumer: consumerName,
          deliveryTime,
          deliveryCount: retrycount !== null ? retrycount : 1,
        };
        group.pel.set(entryIdStr, newPe);
        consumer.pending.set(entryIdStr, newPe);

        if (justid) {
          result.push(bulkReply(entryIdStr));
        } else {
          const entries = stream.range(claimId, claimId, 1);
          if (entries.length > 0) {
            result.push(entryToReply(entries[0] as StreamEntry));
          }
        }
      }
      // If not in PEL and not FORCE, skip silently
      continue;
    }

    // Check min-idle-time: skip entries that haven't been idle long enough
    const idleTime = clockMs - pe.deliveryTime;
    if (idleTime < minIdleTime) continue;

    // Check if entry was deleted from stream — remove from PEL (Redis 7.0+)
    if (!stream.hasEntry(entryIdStr)) {
      const oldConsumer = group.consumers.get(pe.consumer);
      if (oldConsumer) {
        oldConsumer.pending.delete(entryIdStr);
      }
      group.pel.delete(entryIdStr);
      continue;
    }

    // Transfer ownership: remove from old consumer
    const oldConsumer = group.consumers.get(pe.consumer);
    if (oldConsumer) {
      oldConsumer.pending.delete(entryIdStr);
    }

    // Update PEL entry
    pe.consumer = consumerName;
    pe.deliveryTime = deliveryTime;
    pe.deliveryCount = retrycount !== null ? retrycount : pe.deliveryCount + 1;

    // Add to new consumer's pending
    consumer.pending.set(entryIdStr, pe);
    consumer.activeTime = clockMs;

    if (justid) {
      result.push(bulkReply(entryIdStr));
    } else {
      const entries = stream.range(claimId, claimId, 1);
      if (entries.length > 0) {
        result.push(entryToReply(entries[0] as StreamEntry));
      }
    }
  }

  return arrayReply(result);
}

/**
 * XAUTOCLAIM key group consumer min-idle-time start [COUNT count] [JUSTID]
 */
export function xautoclaim(
  db: Database,
  clockMs: number,
  args: string[]
): Reply {
  if (args.length < 5) {
    return errorReply(
      'ERR',
      "wrong number of arguments for 'xautoclaim' command"
    );
  }

  const key = args[0] as string;
  const groupName = args[1] as string;
  const consumerName = args[2] as string;
  const minIdleStr = args[3] as string;
  const startArg = args[4] as string;

  const minIdleTime = Number(minIdleStr);
  if (!Number.isInteger(minIdleTime) || minIdleTime < 0) {
    return errorReply('ERR', 'value is not an integer or out of range');
  }

  // Parse start ID — "0-0" means start from beginning
  const startId = parseStreamId(startArg);
  if (!startId) return INVALID_STREAM_ID_ERR;

  let count = 100; // default
  let justid = false;

  let i = 5;
  while (i < args.length) {
    const upper = (args[i] as string).toUpperCase();
    if (upper === 'COUNT') {
      i++;
      if (i >= args.length) return SYNTAX_ERR;
      const n = Number(args[i]);
      if (!Number.isInteger(n) || n < 0) {
        return errorReply('ERR', 'value is not an integer or out of range');
      }
      count = n;
      i++;
    } else if (upper === 'JUSTID') {
      justid = true;
      i++;
    } else {
      return SYNTAX_ERR;
    }
  }

  const lookup = getStream(db, key);
  if (lookup.error) return lookup.error;
  if (!lookup.stream) {
    return errorReply(
      'NOGROUP',
      "No such key '" +
        key +
        "' or consumer group '" +
        groupName +
        "' in XAUTOCLAIM"
    );
  }

  const stream = lookup.stream;
  const group = stream.getGroup(groupName);
  if (!group) {
    return errorReply(
      'NOGROUP',
      "No such key '" +
        key +
        "' or consumer group '" +
        groupName +
        "' in XAUTOCLAIM"
    );
  }

  // Ensure consumer exists
  const consumer = ensureConsumer(group, consumerName, clockMs);

  // Collect all PEL entries with ID >= startId, sorted by ID
  const candidates: PendingEntry[] = [];
  for (const [entryId, pe] of group.pel) {
    const eid = safeParseId(entryId);
    if (compareStreamIds(eid, startId) < 0) continue;
    candidates.push(pe);
  }
  candidates.sort((a, b) =>
    compareStreamIds(safeParseId(a.entryId), safeParseId(b.entryId))
  );

  // Iterate PEL entries matching Redis behavior:
  // - Deleted entries decrement count
  // - Entries with insufficient idle time do NOT decrement count
  // - Successfully claimed entries decrement count
  // - attempts cap: count * 10
  const claimedEntries: Reply[] = [];
  const deletedIds: Reply[] = [];
  let remaining = count;
  let attempts = count * 10;
  let lastScannedIdx = -1;

  for (
    let ci = 0;
    ci < candidates.length && remaining > 0 && attempts > 0;
    ci++
  ) {
    attempts--;
    const pe = candidates[ci] as PendingEntry;
    const entryIdStr = pe.entryId;
    lastScannedIdx = ci;

    // Check if entry was deleted from stream
    if (!stream.hasEntry(entryIdStr)) {
      deletedIds.push(bulkReply(entryIdStr));
      const oldConsumer = group.consumers.get(pe.consumer);
      if (oldConsumer) {
        oldConsumer.pending.delete(entryIdStr);
      }
      group.pel.delete(entryIdStr);
      remaining--;
      continue;
    }

    // Check idle time — skip without consuming count
    const idleTime = clockMs - pe.deliveryTime;
    if (idleTime < minIdleTime) continue;

    // Transfer ownership
    const oldConsumer = group.consumers.get(pe.consumer);
    if (oldConsumer) {
      oldConsumer.pending.delete(entryIdStr);
    }

    pe.consumer = consumerName;
    pe.deliveryTime = clockMs;
    pe.deliveryCount++;

    consumer.pending.set(entryIdStr, pe);
    consumer.activeTime = clockMs;

    if (justid) {
      claimedEntries.push(bulkReply(entryIdStr));
    } else {
      const eid = safeParseId(entryIdStr);
      const entries = stream.range(eid, eid, 1);
      if (entries.length > 0) {
        claimedEntries.push(entryToReply(entries[0] as StreamEntry));
      }
    }
    remaining--;
  }

  // Compute next cursor: points to the entry after the last scanned one
  let nextCursor = '0-0';
  if (lastScannedIdx >= 0 && lastScannedIdx + 1 < candidates.length) {
    nextCursor = (candidates[lastScannedIdx + 1] as PendingEntry).entryId;
  }

  return arrayReply([
    bulkReply(nextCursor),
    arrayReply(claimedEntries),
    arrayReply(deletedIds),
  ]);
}
