import { describe, it, expect, beforeEach } from 'vitest';
import { PubSubManager } from './pubsub-manager.ts';
import { ConfigStore } from '../../config-store.ts';
import {
  notifyKeyspaceEvent,
  parseKeyspaceEventFlags,
  keyspaceEventsFlagsToString,
  normalizeKeyspaceEventConfig,
  EVENT_FLAGS,
} from './keyspace-events.ts';
import type { Reply } from '../types.ts';

describe('parseKeyspaceEventFlags', () => {
  it('returns 0 for empty string', () => {
    expect(parseKeyspaceEventFlags('')).toBe(0);
  });

  it('parses K flag', () => {
    const flags = parseKeyspaceEventFlags('K');
    expect(flags & EVENT_FLAGS.KEYSPACE).not.toBe(0);
  });

  it('parses E flag', () => {
    const flags = parseKeyspaceEventFlags('E');
    expect(flags & EVENT_FLAGS.KEYEVENT).not.toBe(0);
  });

  it('parses A flag as alias for g$lshzxet', () => {
    const flags = parseKeyspaceEventFlags('A');
    expect(flags & EVENT_FLAGS.GENERIC).not.toBe(0);
    expect(flags & EVENT_FLAGS.STRING).not.toBe(0);
    expect(flags & EVENT_FLAGS.LIST).not.toBe(0);
    expect(flags & EVENT_FLAGS.SET).not.toBe(0);
    expect(flags & EVENT_FLAGS.HASH).not.toBe(0);
    expect(flags & EVENT_FLAGS.SORTEDSET).not.toBe(0);
    expect(flags & EVENT_FLAGS.EXPIRED).not.toBe(0);
    expect(flags & EVENT_FLAGS.EVICTED).not.toBe(0);
    expect(flags & EVENT_FLAGS.STREAM).not.toBe(0);
    // A does NOT include K, E, m, d, or n
    expect(flags & EVENT_FLAGS.MODULE).toBe(0);
    expect(flags & EVENT_FLAGS.KEYSPACE).toBe(0);
    expect(flags & EVENT_FLAGS.KEYEVENT).toBe(0);
    expect(flags & EVENT_FLAGS.KEY_MISS).toBe(0);
    expect(flags & EVENT_FLAGS.NEW).toBe(0);
  });

  it('parses combined flags', () => {
    const flags = parseKeyspaceEventFlags('KEg');
    expect(flags & EVENT_FLAGS.KEYSPACE).not.toBe(0);
    expect(flags & EVENT_FLAGS.KEYEVENT).not.toBe(0);
    expect(flags & EVENT_FLAGS.GENERIC).not.toBe(0);
    expect(flags & EVENT_FLAGS.STRING).toBe(0);
  });

  it('parses all individual type flags', () => {
    expect(parseKeyspaceEventFlags('g') & EVENT_FLAGS.GENERIC).not.toBe(0);
    expect(parseKeyspaceEventFlags('$') & EVENT_FLAGS.STRING).not.toBe(0);
    expect(parseKeyspaceEventFlags('l') & EVENT_FLAGS.LIST).not.toBe(0);
    expect(parseKeyspaceEventFlags('s') & EVENT_FLAGS.SET).not.toBe(0);
    expect(parseKeyspaceEventFlags('h') & EVENT_FLAGS.HASH).not.toBe(0);
    expect(parseKeyspaceEventFlags('z') & EVENT_FLAGS.SORTEDSET).not.toBe(0);
    expect(parseKeyspaceEventFlags('x') & EVENT_FLAGS.EXPIRED).not.toBe(0);
    expect(parseKeyspaceEventFlags('e') & EVENT_FLAGS.EVICTED).not.toBe(0);
    expect(parseKeyspaceEventFlags('t') & EVENT_FLAGS.STREAM).not.toBe(0);
    expect(parseKeyspaceEventFlags('m') & EVENT_FLAGS.KEY_MISS).not.toBe(0);
    expect(parseKeyspaceEventFlags('d') & EVENT_FLAGS.MODULE).not.toBe(0);
    expect(parseKeyspaceEventFlags('n') & EVENT_FLAGS.NEW).not.toBe(0);
  });

  it('returns -1 for unknown characters', () => {
    expect(parseKeyspaceEventFlags('KQZ')).toBe(-1);
    expect(parseKeyspaceEventFlags('Q')).toBe(-1);
    expect(parseKeyspaceEventFlags('Kg!')).toBe(-1);
  });

  it('parses A combined with K and E', () => {
    const flags = parseKeyspaceEventFlags('AKE');
    expect(flags & EVENT_FLAGS.KEYSPACE).not.toBe(0);
    expect(flags & EVENT_FLAGS.KEYEVENT).not.toBe(0);
    expect(flags & EVENT_FLAGS.GENERIC).not.toBe(0);
    expect(flags & EVENT_FLAGS.STRING).not.toBe(0);
  });

  it('parses A combined with extra flags d and n', () => {
    const flags = parseKeyspaceEventFlags('AKdn');
    expect(flags & EVENT_FLAGS.MODULE).not.toBe(0);
    expect(flags & EVENT_FLAGS.NEW).not.toBe(0);
    expect(flags & EVENT_FLAGS.KEYSPACE).not.toBe(0);
  });

  it('returns -1 for lowercase k (only uppercase K is valid)', () => {
    expect(parseKeyspaceEventFlags('k')).toBe(-1);
  });

  it('handles duplicate characters idempotently', () => {
    const single = parseKeyspaceEventFlags('Kg');
    const duped = parseKeyspaceEventFlags('KKKggg');
    expect(single).toBe(duped);
  });
});

describe('keyspaceEventsFlagsToString', () => {
  it('returns empty string for 0', () => {
    expect(keyspaceEventsFlagsToString(0)).toBe('');
  });

  it('round-trips individual flags', () => {
    expect(keyspaceEventsFlagsToString(EVENT_FLAGS.KEYSPACE)).toBe('K');
    expect(keyspaceEventsFlagsToString(EVENT_FLAGS.KEYEVENT)).toBe('E');
    expect(keyspaceEventsFlagsToString(EVENT_FLAGS.GENERIC)).toBe('g');
    expect(keyspaceEventsFlagsToString(EVENT_FLAGS.STRING)).toBe('$');
    expect(keyspaceEventsFlagsToString(EVENT_FLAGS.KEY_MISS)).toBe('m');
    expect(keyspaceEventsFlagsToString(EVENT_FLAGS.NEW)).toBe('n');
  });

  it('collapses all type flags to A', () => {
    const flags = parseKeyspaceEventFlags('AK');
    expect(keyspaceEventsFlagsToString(flags)).toBe('AK');
  });

  it('does not use A when not all type flags present', () => {
    const flags = parseKeyspaceEventFlags('Kg$l');
    const result = keyspaceEventsFlagsToString(flags);
    expect(result).not.toContain('A');
    expect(result).toContain('K');
    expect(result).toContain('g');
    expect(result).toContain('$');
    expect(result).toContain('l');
  });

  it('normalizes order — type flags before K/E/m', () => {
    const flags = parseKeyspaceEventFlags('Kg');
    expect(keyspaceEventsFlagsToString(flags)).toBe('gK');
  });

  it('deduplicates via round-trip', () => {
    const flags = parseKeyspaceEventFlags('KKKggg');
    expect(keyspaceEventsFlagsToString(flags)).toBe('gK');
  });

  it('serializes d and n when A is not active', () => {
    const flags = parseKeyspaceEventFlags('Kgdn');
    const result = keyspaceEventsFlagsToString(flags);
    expect(result).toBe('gdnK');
  });

  it('drops d and n when A is active (matches Redis)', () => {
    // When all A-type flags are set, d and n are in the else branch
    // and get dropped — this matches Redis behavior
    const flags = parseKeyspaceEventFlags('AKdn');
    expect(keyspaceEventsFlagsToString(flags)).toBe('AK');
  });

  it('serializes m always (outside else branch)', () => {
    const flags = parseKeyspaceEventFlags('AKm');
    expect(keyspaceEventsFlagsToString(flags)).toBe('AKm');
  });

  it('serializes module flag alone', () => {
    expect(keyspaceEventsFlagsToString(EVENT_FLAGS.MODULE)).toBe('d');
  });

  it('preserves canonical order for all individual flags', () => {
    const flags = parseKeyspaceEventFlags('mEnK$gd');
    const result = keyspaceEventsFlagsToString(flags);
    expect(result).toBe('g$dnKEm');
  });
});

describe('normalizeKeyspaceEventConfig', () => {
  it('returns empty for empty', () => {
    expect(normalizeKeyspaceEventConfig('')).toBe('');
  });

  it('returns null for invalid chars', () => {
    expect(normalizeKeyspaceEventConfig('KQ')).toBeNull();
  });

  it('normalizes valid config', () => {
    expect(normalizeKeyspaceEventConfig('KKKggg')).toBe('gK');
  });

  it('normalizes to A when all type flags present', () => {
    expect(normalizeKeyspaceEventConfig('Kg$lshzxet')).toBe('AK');
  });

  it('normalizes to A and drops d when all type flags plus module', () => {
    // Redis drops d and n when A is used (they are inside the else branch)
    expect(normalizeKeyspaceEventConfig('Kg$lshzxetd')).toBe('AK');
  });
});

describe('notifyKeyspaceEvent', () => {
  let pubsub: PubSubManager;
  let config: ConfigStore;
  let messages: { clientId: number; reply: Reply }[];

  beforeEach(() => {
    pubsub = new PubSubManager();
    config = new ConfigStore();
    messages = [];
    pubsub.setSender((clientId, reply) => {
      messages.push({ clientId, reply });
    });
  });

  it('does nothing when notify-keyspace-events is empty', () => {
    pubsub.subscribe(1, '__keyspace@0__:mykey');
    notifyKeyspaceEvent(config, pubsub, EVENT_FLAGS.GENERIC, 'del', 'mykey', 0);
    expect(messages).toHaveLength(0);
  });

  it('does nothing when event type is not enabled in config', () => {
    config.set('notify-keyspace-events', 'Kg'); // only generic events
    pubsub.subscribe(1, '__keyspace@0__:mykey');
    // STRING type event — not enabled
    notifyKeyspaceEvent(config, pubsub, EVENT_FLAGS.STRING, 'set', 'mykey', 0);
    expect(messages).toHaveLength(0);
  });

  it('publishes to keyspace channel when K flag is set', () => {
    config.set('notify-keyspace-events', 'Kg');
    pubsub.subscribe(1, '__keyspace@0__:mykey');
    notifyKeyspaceEvent(config, pubsub, EVENT_FLAGS.GENERIC, 'del', 'mykey', 0);
    expect(messages).toHaveLength(1);
    expect(messages[0]).toBeDefined();
    const reply = messages[0]?.reply;
    expect(reply).toEqual(
      expect.objectContaining({
        kind: 'array',
        value: expect.arrayContaining([
          expect.objectContaining({ kind: 'bulk', value: 'message' }),
          expect.objectContaining({
            kind: 'bulk',
            value: '__keyspace@0__:mykey',
          }),
          expect.objectContaining({ kind: 'bulk', value: 'del' }),
        ]),
      })
    );
  });

  it('publishes to keyevent channel when E flag is set', () => {
    config.set('notify-keyspace-events', 'Eg');
    pubsub.subscribe(1, '__keyevent@0__:del');
    notifyKeyspaceEvent(config, pubsub, EVENT_FLAGS.GENERIC, 'del', 'mykey', 0);
    expect(messages).toHaveLength(1);
    expect(messages[0]).toBeDefined();
    const reply = messages[0]?.reply;
    expect(reply).toEqual(
      expect.objectContaining({
        kind: 'array',
        value: expect.arrayContaining([
          expect.objectContaining({ kind: 'bulk', value: 'message' }),
          expect.objectContaining({
            kind: 'bulk',
            value: '__keyevent@0__:del',
          }),
          expect.objectContaining({ kind: 'bulk', value: 'mykey' }),
        ]),
      })
    );
  });

  it('publishes to both keyspace and keyevent when KE flags set', () => {
    config.set('notify-keyspace-events', 'KEg');
    pubsub.subscribe(1, '__keyspace@0__:mykey');
    pubsub.subscribe(2, '__keyevent@0__:del');
    notifyKeyspaceEvent(config, pubsub, EVENT_FLAGS.GENERIC, 'del', 'mykey', 0);
    expect(messages).toHaveLength(2);
  });

  it('uses correct db index in channel name', () => {
    config.set('notify-keyspace-events', 'Kg');
    pubsub.subscribe(1, '__keyspace@5__:mykey');
    notifyKeyspaceEvent(config, pubsub, EVENT_FLAGS.GENERIC, 'del', 'mykey', 5);
    expect(messages).toHaveLength(1);
  });

  it('handles A flag enabling all standard type events', () => {
    config.set('notify-keyspace-events', 'KA');
    pubsub.subscribe(1, '__keyspace@0__:mykey');

    notifyKeyspaceEvent(config, pubsub, EVENT_FLAGS.STRING, 'set', 'mykey', 0);
    expect(messages).toHaveLength(1);

    messages.length = 0;
    notifyKeyspaceEvent(config, pubsub, EVENT_FLAGS.LIST, 'lpush', 'mykey', 0);
    expect(messages).toHaveLength(1);

    messages.length = 0;
    notifyKeyspaceEvent(config, pubsub, EVENT_FLAGS.HASH, 'hset', 'mykey', 0);
    expect(messages).toHaveLength(1);

    messages.length = 0;
    notifyKeyspaceEvent(
      config,
      pubsub,
      EVENT_FLAGS.SORTEDSET,
      'zadd',
      'mykey',
      0
    );
    expect(messages).toHaveLength(1);

    messages.length = 0;
    notifyKeyspaceEvent(
      config,
      pubsub,
      EVENT_FLAGS.EXPIRED,
      'expired',
      'mykey',
      0
    );
    expect(messages).toHaveLength(1);

    // A does NOT include module (d) — must be enabled separately
    messages.length = 0;
    notifyKeyspaceEvent(
      config,
      pubsub,
      EVENT_FLAGS.MODULE,
      'module-event',
      'mykey',
      0
    );
    expect(messages).toHaveLength(0);
  });

  it('delivers to pattern subscribers', () => {
    config.set('notify-keyspace-events', 'Kg');
    pubsub.psubscribe(1, '__keyspace@0__:*');
    notifyKeyspaceEvent(config, pubsub, EVENT_FLAGS.GENERIC, 'del', 'mykey', 0);
    expect(messages).toHaveLength(1);
    expect(messages[0]).toBeDefined();
    const reply = messages[0]?.reply;
    // Pattern match delivers pmessage format
    expect(reply).toEqual(
      expect.objectContaining({
        kind: 'array',
        value: expect.arrayContaining([
          expect.objectContaining({ kind: 'bulk', value: 'pmessage' }),
        ]),
      })
    );
  });

  it('does not publish when no subscribers exist', () => {
    config.set('notify-keyspace-events', 'KEg');
    // No subscribers — publish still runs but no messages sent
    notifyKeyspaceEvent(config, pubsub, EVENT_FLAGS.GENERIC, 'del', 'mykey', 0);
    expect(messages).toHaveLength(0);
  });

  it('handles string ($) event type', () => {
    config.set('notify-keyspace-events', 'K$');
    pubsub.subscribe(1, '__keyspace@0__:mykey');
    notifyKeyspaceEvent(config, pubsub, EVENT_FLAGS.STRING, 'set', 'mykey', 0);
    expect(messages).toHaveLength(1);
  });

  it('handles list (l) event type', () => {
    config.set('notify-keyspace-events', 'El');
    pubsub.subscribe(1, '__keyevent@0__:lpush');
    notifyKeyspaceEvent(config, pubsub, EVENT_FLAGS.LIST, 'lpush', 'mykey', 0);
    expect(messages).toHaveLength(1);
  });

  it('handles set (s) event type', () => {
    config.set('notify-keyspace-events', 'Ks');
    pubsub.subscribe(1, '__keyspace@0__:mykey');
    notifyKeyspaceEvent(config, pubsub, EVENT_FLAGS.SET, 'sadd', 'mykey', 0);
    expect(messages).toHaveLength(1);
  });

  it('handles hash (h) event type', () => {
    config.set('notify-keyspace-events', 'Kh');
    pubsub.subscribe(1, '__keyspace@0__:mykey');
    notifyKeyspaceEvent(config, pubsub, EVENT_FLAGS.HASH, 'hset', 'mykey', 0);
    expect(messages).toHaveLength(1);
  });

  it('handles sorted set (z) event type', () => {
    config.set('notify-keyspace-events', 'Kz');
    pubsub.subscribe(1, '__keyspace@0__:mykey');
    notifyKeyspaceEvent(
      config,
      pubsub,
      EVENT_FLAGS.SORTEDSET,
      'zadd',
      'mykey',
      0
    );
    expect(messages).toHaveLength(1);
  });

  it('handles expired (x) event type', () => {
    config.set('notify-keyspace-events', 'Ex');
    pubsub.subscribe(1, '__keyevent@0__:expired');
    notifyKeyspaceEvent(
      config,
      pubsub,
      EVENT_FLAGS.EXPIRED,
      'expired',
      'mykey',
      0
    );
    expect(messages).toHaveLength(1);
  });

  it('handles evicted (e) event type', () => {
    config.set('notify-keyspace-events', 'Ee');
    pubsub.subscribe(1, '__keyevent@0__:evicted');
    notifyKeyspaceEvent(
      config,
      pubsub,
      EVENT_FLAGS.EVICTED,
      'evicted',
      'mykey',
      0
    );
    expect(messages).toHaveLength(1);
  });

  it('handles stream (t) event type', () => {
    config.set('notify-keyspace-events', 'Kt');
    pubsub.subscribe(1, '__keyspace@0__:mykey');
    notifyKeyspaceEvent(config, pubsub, EVENT_FLAGS.STREAM, 'xadd', 'mykey', 0);
    expect(messages).toHaveLength(1);
  });

  it('handles key miss (m) event type', () => {
    config.set('notify-keyspace-events', 'Em');
    pubsub.subscribe(1, '__keyevent@0__:keymiss');
    notifyKeyspaceEvent(
      config,
      pubsub,
      EVENT_FLAGS.KEY_MISS,
      'keymiss',
      'mykey',
      0
    );
    expect(messages).toHaveLength(1);
  });

  it('handles new key (n) event type', () => {
    config.set('notify-keyspace-events', 'Kn');
    pubsub.subscribe(1, '__keyspace@0__:mykey');
    notifyKeyspaceEvent(config, pubsub, EVENT_FLAGS.NEW, 'new', 'mykey', 0);
    expect(messages).toHaveLength(1);
  });

  it('config normalization rejects invalid characters', () => {
    const err = config.set('notify-keyspace-events', 'KQ');
    expect(err).not.toBeNull();
    expect(err).toContain('Invalid argument');
  });

  it('config normalizes and round-trips through flags', () => {
    config.set('notify-keyspace-events', 'KKKggg');
    const result = config.get('notify-keyspace-events');
    expect(result[1]).toBe('gK');
  });

  it('config collapses to A when all type flags present', () => {
    config.set('notify-keyspace-events', 'KEg$lshzxet');
    const result = config.get('notify-keyspace-events');
    expect(result[1]).toBe('AKE');
  });

  it('config collapses to A and drops d/n when all flags plus extras', () => {
    config.set('notify-keyspace-events', 'KEg$lshzxetdn');
    const result = config.get('notify-keyspace-events');
    // d and n are inside the else branch in Redis — dropped when A is used
    expect(result[1]).toBe('AKE');
  });

  it('does nothing when only type flags set without K or E', () => {
    // "At least K or E must be set for any notifications"
    config.set('notify-keyspace-events', 'g$lsh');
    pubsub.subscribe(1, '__keyspace@0__:mykey');
    pubsub.subscribe(2, '__keyevent@0__:set');
    notifyKeyspaceEvent(config, pubsub, EVENT_FLAGS.STRING, 'set', 'mykey', 0);
    expect(messages).toHaveLength(0);
  });

  it('does nothing when A set without K or E', () => {
    config.set('notify-keyspace-events', 'A');
    pubsub.subscribe(1, '__keyspace@0__:mykey');
    notifyKeyspaceEvent(config, pubsub, EVENT_FLAGS.GENERIC, 'del', 'mykey', 0);
    expect(messages).toHaveLength(0);
  });

  it('enables notifications after CONFIG SET and disables after reset', () => {
    config.set('notify-keyspace-events', 'Kg');
    pubsub.subscribe(1, '__keyspace@0__:mykey');

    notifyKeyspaceEvent(config, pubsub, EVENT_FLAGS.GENERIC, 'del', 'mykey', 0);
    expect(messages).toHaveLength(1);

    // Disable notifications
    messages.length = 0;
    config.set('notify-keyspace-events', '');
    notifyKeyspaceEvent(config, pubsub, EVENT_FLAGS.GENERIC, 'del', 'mykey', 0);
    expect(messages).toHaveLength(0);
  });

  it('selectively enables only configured event types', () => {
    // Enable only hash events on keyspace channel
    config.set('notify-keyspace-events', 'Kh');
    pubsub.subscribe(1, '__keyspace@0__:mykey');

    // Hash event — should fire
    notifyKeyspaceEvent(config, pubsub, EVENT_FLAGS.HASH, 'hset', 'mykey', 0);
    expect(messages).toHaveLength(1);

    // String event — should not fire ($ not enabled)
    messages.length = 0;
    notifyKeyspaceEvent(config, pubsub, EVENT_FLAGS.STRING, 'set', 'mykey', 0);
    expect(messages).toHaveLength(0);

    // List event — should not fire (l not enabled)
    notifyKeyspaceEvent(config, pubsub, EVENT_FLAGS.LIST, 'lpush', 'mykey', 0);
    expect(messages).toHaveLength(0);
  });

  it('handles module (d) event type', () => {
    config.set('notify-keyspace-events', 'Kd');
    pubsub.subscribe(1, '__keyspace@0__:mykey');
    notifyKeyspaceEvent(
      config,
      pubsub,
      EVENT_FLAGS.MODULE,
      'module-event',
      'mykey',
      0
    );
    expect(messages).toHaveLength(1);
  });

  it('handles A with module events (A does not include d)', () => {
    // A doesn't include d, so module events should NOT fire with just KA
    config.set('notify-keyspace-events', 'KA');
    pubsub.subscribe(1, '__keyspace@0__:mykey');
    notifyKeyspaceEvent(
      config,
      pubsub,
      EVENT_FLAGS.MODULE,
      'module-event',
      'mykey',
      0
    );
    expect(messages).toHaveLength(0);
  });
});
