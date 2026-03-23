/**
 * Keyspace event notification dispatch.
 *
 * Implements Redis keyspace notifications — pub/sub messages emitted
 * when keys are mutated. Controlled by the `notify-keyspace-events`
 * configuration parameter.
 *
 * Channel formats:
 *   __keyspace@{db}__:{key}  → event name as message  (K flag)
 *   __keyevent@{db}__:{event} → key name as message   (E flag)
 */

import type { ConfigStore } from '../config-store.ts';
import type { PubSubManager } from './pubsub-manager.ts';

// ---------------------------------------------------------------------------
// Event type flags — bitmask constants matching Redis server.h
// ---------------------------------------------------------------------------

export const EVENT_FLAGS = {
  KEYSPACE: 1 << 0, // K
  KEYEVENT: 1 << 1, // E
  GENERIC: 1 << 2, // g
  STRING: 1 << 3, // $
  LIST: 1 << 4, // l
  SET: 1 << 5, // s
  HASH: 1 << 6, // h
  SORTEDSET: 1 << 7, // z
  EXPIRED: 1 << 8, // x
  EVICTED: 1 << 9, // e
  STREAM: 1 << 10, // t
  KEY_MISS: 1 << 11, // m
  MODULE: 1 << 12, // d
  NEW: 1 << 13, // n
} as const;

/** All type flags combined — the A alias (g$lshzxet). Does NOT include K, E, m, d, or n. */
const ALL_TYPE_FLAGS =
  EVENT_FLAGS.GENERIC |
  EVENT_FLAGS.STRING |
  EVENT_FLAGS.LIST |
  EVENT_FLAGS.SET |
  EVENT_FLAGS.HASH |
  EVENT_FLAGS.SORTEDSET |
  EVENT_FLAGS.EXPIRED |
  EVENT_FLAGS.EVICTED |
  EVENT_FLAGS.STREAM;

// Character → flag mapping
const CHAR_TO_FLAG: Record<string, number> = {
  K: EVENT_FLAGS.KEYSPACE,
  E: EVENT_FLAGS.KEYEVENT,
  g: EVENT_FLAGS.GENERIC,
  $: EVENT_FLAGS.STRING,
  l: EVENT_FLAGS.LIST,
  s: EVENT_FLAGS.SET,
  h: EVENT_FLAGS.HASH,
  z: EVENT_FLAGS.SORTEDSET,
  x: EVENT_FLAGS.EXPIRED,
  e: EVENT_FLAGS.EVICTED,
  t: EVENT_FLAGS.STREAM,
  m: EVENT_FLAGS.KEY_MISS,
  d: EVENT_FLAGS.MODULE,
  n: EVENT_FLAGS.NEW,
};

/**
 * Type flags emitted in the else-branch (when not using A).
 * Includes d (module) and n (new) — these are dropped when A is used,
 * matching Redis's keyspaceEventsFlagsToString() in notify.c.
 */
const TYPE_FLAG_CHARS: [number, string][] = [
  [EVENT_FLAGS.GENERIC, 'g'],
  [EVENT_FLAGS.STRING, '$'],
  [EVENT_FLAGS.LIST, 'l'],
  [EVENT_FLAGS.SET, 's'],
  [EVENT_FLAGS.HASH, 'h'],
  [EVENT_FLAGS.SORTEDSET, 'z'],
  [EVENT_FLAGS.EXPIRED, 'x'],
  [EVENT_FLAGS.EVICTED, 'e'],
  [EVENT_FLAGS.STREAM, 't'],
  [EVENT_FLAGS.MODULE, 'd'],
  [EVENT_FLAGS.NEW, 'n'],
];

/**
 * Channel/delivery flags — always emitted regardless of A.
 */
const CHANNEL_FLAG_CHARS: [number, string][] = [
  [EVENT_FLAGS.KEYSPACE, 'K'],
  [EVENT_FLAGS.KEYEVENT, 'E'],
  [EVENT_FLAGS.KEY_MISS, 'm'],
];

// ---------------------------------------------------------------------------
// Flag parsing
// ---------------------------------------------------------------------------

/**
 * Parse a `notify-keyspace-events` config string into a bitmask.
 * Mirrors `keyspaceEventsStringToFlags()` from Redis `config.c`.
 * Returns -1 if the string contains invalid characters.
 */
export function parseKeyspaceEventFlags(config: string): number {
  let flags = 0;
  for (const ch of config) {
    if (ch === 'A') {
      flags |= ALL_TYPE_FLAGS;
    } else {
      const flag = CHAR_TO_FLAG[ch];
      if (flag === undefined) {
        return -1;
      }
      flags |= flag;
    }
  }
  return flags;
}

/**
 * Convert a bitmask back to a canonical config string.
 * Mirrors `keyspaceEventsFlagsToString()` from Redis `notify.c`.
 *
 * Structure matches Redis exactly:
 * - If all A-covered type flags are set → emit 'A' (d and n are dropped)
 * - Else → emit individual type flags including d and n
 * - Then always → emit K, E, m
 */
export function keyspaceEventsFlagsToString(flags: number): string {
  if (flags === 0) return '';

  let result = '';

  if ((flags & ALL_TYPE_FLAGS) === ALL_TYPE_FLAGS) {
    // Collapse to 'A' — d (module) and n (new) are inside the else branch
    // in Redis, so they get dropped when A is used. This matches Redis behavior.
    result += 'A';
  } else {
    for (const [flag, ch] of TYPE_FLAG_CHARS) {
      if (flags & flag) result += ch;
    }
  }

  // K, E, m are always emitted (outside the if/else in Redis)
  for (const [flag, ch] of CHANNEL_FLAG_CHARS) {
    if (flags & flag) result += ch;
  }

  return result;
}

/**
 * Validate and normalize a `notify-keyspace-events` config value.
 * Returns the normalized string, or null if invalid characters are present.
 * Used by ConfigStore as a normalizer for this parameter.
 */
export function normalizeKeyspaceEventConfig(value: string): string | null {
  if (value === '') return '';
  const flags = parseKeyspaceEventFlags(value);
  if (flags === -1) return null;
  return keyspaceEventsFlagsToString(flags);
}

// ---------------------------------------------------------------------------
// Notification dispatch
// ---------------------------------------------------------------------------

/**
 * Emit keyspace event notifications.
 *
 * Called after every key mutation. Checks the `notify-keyspace-events`
 * configuration to decide whether to publish, and to which channels.
 *
 * @param config  - ConfigStore to read notification settings
 * @param pubsub  - PubSubManager for message delivery
 * @param type    - Event type bitmask (one of EVENT_FLAGS.GENERIC, STRING, etc.)
 * @param event   - Event name (e.g. "set", "del", "expire", "lpush")
 * @param key     - The key that was mutated
 * @param dbid    - Database index (0-15)
 */
export function notifyKeyspaceEvent(
  config: ConfigStore,
  pubsub: PubSubManager,
  type: number,
  event: string,
  key: string,
  dbid: number
): void {
  // Read current config — returns [key, value] flat array
  const configPair = config.get('notify-keyspace-events');
  const configValue = configPair.length >= 2 ? configPair[1] : '';

  if (!configValue) {
    return;
  }

  const flags = parseKeyspaceEventFlags(configValue);
  if (flags <= 0) return;

  // Must have at least one of K or E, plus the event type must be enabled
  if (!(flags & (EVENT_FLAGS.KEYSPACE | EVENT_FLAGS.KEYEVENT))) {
    return;
  }
  if (!(flags & type)) {
    return;
  }

  // Publish to __keyspace@{db}__:{key} channel with event as message
  if (flags & EVENT_FLAGS.KEYSPACE) {
    const channel = `__keyspace@${dbid}__:${key}`;
    pubsub.publish(channel, event);
  }

  // Publish to __keyevent@{db}__:{event} channel with key as message
  if (flags & EVENT_FLAGS.KEYEVENT) {
    const channel = `__keyevent@${dbid}__:${event}`;
    pubsub.publish(channel, key);
  }
}
