/**
 * Redis Stream data structure.
 *
 * A stream is an append-only log of entries, each keyed by a unique ID
 * in the format "<ms>-<seq>". IDs are strictly increasing.
 */

export interface StreamEntry {
  id: string;
  fields: [string, string][];
}

export interface StreamId {
  ms: number;
  seq: number;
}

export function parseStreamId(id: string): StreamId | null {
  const dashIdx = id.indexOf('-');
  if (dashIdx === -1) {
    const ms = parseNonNegativeInt(id);
    if (ms === null) return null;
    return { ms, seq: 0 };
  }
  const msPart = id.substring(0, dashIdx);
  const seqPart = id.substring(dashIdx + 1);
  const ms = parseNonNegativeInt(msPart);
  const seq = parseNonNegativeInt(seqPart);
  if (ms === null || seq === null) return null;
  return { ms, seq };
}

/**
 * Parse an entry ID that is known to be valid (was validated on insert).
 * Falls back to 0-0 if somehow invalid (should never happen).
 */
function parseEntryId(id: string): StreamId {
  return parseStreamId(id) ?? { ms: 0, seq: 0 };
}

function parseNonNegativeInt(s: string): number | null {
  if (s === '' || s.length > 15) return null;
  const n = Number(s);
  if (!Number.isInteger(n) || n < 0) return null;
  return n;
}

export function streamIdToString(id: StreamId): string {
  return `${id.ms}-${id.seq}`;
}

export function compareStreamIds(a: StreamId, b: StreamId): number {
  if (a.ms !== b.ms) return a.ms < b.ms ? -1 : 1;
  if (a.seq !== b.seq) return a.seq < b.seq ? -1 : 1;
  return 0;
}

export interface PendingEntry {
  entryId: string;
  consumer: string;
  deliveryTime: number;
  deliveryCount: number;
}

export interface StreamConsumer {
  name: string;
  seenTime: number;
  pending: Map<string, PendingEntry>;
}

export interface ConsumerGroup {
  name: string;
  lastDeliveredId: StreamId;
  consumers: Map<string, StreamConsumer>;
  pel: Map<string, PendingEntry>;
  entriesRead: number;
}

export class RedisStream {
  private entries: StreamEntry[] = [];
  private _lastId: StreamId = { ms: 0, seq: 0 };
  private _length = 0;
  private _groups = new Map<string, ConsumerGroup>();

  get length(): number {
    return this._length;
  }

  get lastId(): StreamId {
    return { ...this._lastId };
  }

  get lastIdString(): string {
    return streamIdToString(this._lastId);
  }

  /**
   * Generate the next ID for XADD.
   *
   * @param requestedId - The user-specified ID:
   *   - "*" for full auto-generation
   *   - "<ms>-*" for partial auto-generation (sequence auto)
   *   - "<ms>-<seq>" for explicit ID
   * @param clockMs - Current time in milliseconds
   * @returns The resolved StreamId or an error string
   */
  resolveNextId(
    requestedId: string,
    clockMs: number
  ): StreamId | { error: string } {
    if (requestedId === '*') {
      return this.autoGenerateId(clockMs);
    }

    const dashIdx = requestedId.indexOf('-');
    if (dashIdx !== -1) {
      const seqPart = requestedId.substring(dashIdx + 1);
      if (seqPart === '*') {
        // Partial auto: <ms>-*
        const msPart = requestedId.substring(0, dashIdx);
        const ms = parseNonNegativeInt(msPart);
        if (ms === null) {
          return {
            error: 'ERR Invalid stream ID specified as stream command argument',
          };
        }
        return this.partialAutoGenerateId(ms);
      }
    }

    // Explicit ID
    const parsed = parseStreamId(requestedId);
    if (!parsed) {
      return {
        error: 'ERR Invalid stream ID specified as stream command argument',
      };
    }

    // ID 0-0 is not allowed
    if (parsed.ms === 0 && parsed.seq === 0) {
      return {
        error: 'ERR The ID specified in XADD must be greater than 0-0',
      };
    }

    // Must be strictly greater than last ID
    if (compareStreamIds(parsed, this._lastId) <= 0) {
      return {
        error:
          'ERR The ID specified in XADD is equal or smaller than the target stream top item',
      };
    }

    return parsed;
  }

  private autoGenerateId(clockMs: number): StreamId {
    if (clockMs > this._lastId.ms) {
      return { ms: clockMs, seq: 0 };
    }
    // Same or earlier ms — increment sequence
    return { ms: this._lastId.ms, seq: this._lastId.seq + 1 };
  }

  private partialAutoGenerateId(ms: number): StreamId | { error: string } {
    if (ms > this._lastId.ms) {
      return { ms, seq: 0 };
    }
    if (ms === this._lastId.ms) {
      return { ms, seq: this._lastId.seq + 1 };
    }
    // ms < lastId.ms — the minimum seq for this ms is 0, but we need
    // the resulting ID to be > lastId. Since ms < lastId.ms, this is impossible.
    return {
      error:
        'ERR The ID specified in XADD is equal or smaller than the target stream top item',
    };
  }

  /**
   * Add an entry to the stream. The id must already be resolved and validated.
   */
  addEntry(id: StreamId, fields: [string, string][]): string {
    const idStr = streamIdToString(id);
    this.entries.push({ id: idStr, fields });
    this._lastId = { ...id };
    this._length++;
    return idStr;
  }

  /**
   * Trim by MAXLEN. Removes oldest entries until length <= maxlen.
   * Returns number of entries removed.
   */
  trimByMaxlen(maxlen: number, approximate: boolean): number {
    if (maxlen < 0) maxlen = 0;
    if (this.entries.length <= maxlen) return 0;

    const toRemove = this.entries.length - maxlen;
    if (approximate) {
      // With ~, Redis trims in blocks and may remove fewer entries.
      // In practice, for small streams Redis still trims to the target.
      // We match Redis behavior: trim whole radix tree nodes, which for
      // our implementation means we trim to the exact target.
      // This is acceptable since Redis documentation says ~ means "at least"
      // this many entries will remain.
    }
    this.entries.splice(0, toRemove);
    this._length = this.entries.length;
    return toRemove;
  }

  /**
   * Trim by MINID. Removes entries with ID less than minId.
   * Returns number of entries removed.
   */
  trimByMinid(minId: StreamId, _approximate: boolean): number {
    let removeCount = 0;
    for (const entry of this.entries) {
      const entryId = parseStreamId(entry.id);
      if (entryId && compareStreamIds(entryId, minId) < 0) {
        removeCount++;
      } else {
        break;
      }
    }
    if (removeCount === 0) return 0;
    this.entries.splice(0, removeCount);
    this._length = this.entries.length;
    return removeCount;
  }

  /**
   * Return entries with IDs in [start, end] inclusive, in ascending order.
   * Optionally limited by count.
   */
  range(start: StreamId, end: StreamId, count?: number): StreamEntry[] {
    const result: StreamEntry[] = [];
    if (count === 0) return result;
    for (const entry of this.entries) {
      const eid = parseEntryId(entry.id);
      if (compareStreamIds(eid, end) > 0) break;
      if (compareStreamIds(eid, start) >= 0) {
        result.push(entry);
        if (count !== undefined && result.length >= count) break;
      }
    }
    return result;
  }

  /**
   * Return entries with IDs in [start, end] inclusive, in descending order.
   * Note: start >= end (start is the higher ID).
   * Optionally limited by count.
   */
  revrange(start: StreamId, end: StreamId, count?: number): StreamEntry[] {
    const result: StreamEntry[] = [];
    for (let i = this.entries.length - 1; i >= 0; i--) {
      const entry = this.entries[i] as StreamEntry;
      const eid = parseEntryId(entry.id);
      if (compareStreamIds(eid, end) < 0) break;
      if (compareStreamIds(eid, start) <= 0) {
        result.push(entry);
        if (count !== undefined && result.length >= count) break;
      }
    }
    return result;
  }

  /**
   * Return entries with IDs strictly greater than the given ID.
   * Used by XREAD.
   */
  entriesAfter(id: StreamId, count?: number): StreamEntry[] {
    const result: StreamEntry[] = [];
    for (const entry of this.entries) {
      const eid = parseEntryId(entry.id);
      if (compareStreamIds(eid, id) > 0) {
        result.push(entry);
        if (count !== undefined && result.length >= count) break;
      }
    }
    return result;
  }

  /**
   * Get all entries (for testing and XRANGE).
   */
  getEntries(): StreamEntry[] {
    return this.entries;
  }

  /**
   * Get the first entry, or null if empty.
   */
  firstEntry(): StreamEntry | null {
    return this.entries[0] ?? null;
  }

  /**
   * Get the last entry, or null if empty.
   */
  lastEntry(): StreamEntry | null {
    return this.entries[this.entries.length - 1] ?? null;
  }

  // ─── Consumer Group Operations ──────────────────────────────────────

  getGroup(name: string): ConsumerGroup | undefined {
    return this._groups.get(name);
  }

  get groups(): Map<string, ConsumerGroup> {
    return this._groups;
  }

  createGroup(
    name: string,
    lastDeliveredId: StreamId,
    entriesRead: number
  ): boolean {
    if (this._groups.has(name)) return false;
    this._groups.set(name, {
      name,
      lastDeliveredId: { ...lastDeliveredId },
      consumers: new Map(),
      pel: new Map(),
      entriesRead,
    });
    return true;
  }

  destroyGroup(name: string): boolean {
    return this._groups.delete(name);
  }

  setGroupId(name: string, id: StreamId, entriesRead: number): boolean {
    const group = this._groups.get(name);
    if (!group) return false;
    group.lastDeliveredId = { ...id };
    group.entriesRead = entriesRead;
    return true;
  }

  createConsumer(groupName: string, consumerName: string): 0 | 1 | null {
    const group = this._groups.get(groupName);
    if (!group) return null;
    if (group.consumers.has(consumerName)) return 0;
    group.consumers.set(consumerName, {
      name: consumerName,
      seenTime: 0,
      pending: new Map(),
    });
    return 1;
  }

  deleteConsumer(groupName: string, consumerName: string): number | null {
    const group = this._groups.get(groupName);
    if (!group) return null;
    const consumer = group.consumers.get(consumerName);
    if (!consumer) return 0;
    const pendingCount = consumer.pending.size;
    // Remove consumer's entries from group PEL
    for (const entryId of consumer.pending.keys()) {
      group.pel.delete(entryId);
    }
    group.consumers.delete(consumerName);
    return pendingCount;
  }
}
