import { describe, it, expect, beforeEach } from 'vitest';
import { RedisEngine } from '../engine.ts';
import { ConfigStore } from '../../config-store.ts';
import type { CommandContext, Reply } from '../types.ts';
import { ClientState } from '../../server/client-state.ts';
import { notify, EVENT_FLAGS } from './notify.ts';

interface CapturedMsg {
  clientId: number;
  reply: Reply;
}

describe('notify helper', () => {
  let engine: RedisEngine;
  let config: ConfigStore;
  let messages: CapturedMsg[];

  beforeEach(() => {
    engine = new RedisEngine({ clock: () => 1000 });
    config = new ConfigStore();
    messages = [];
    engine.pubsub.setSender((clientId, reply) => {
      messages.push({ clientId, reply });
    });
  });

  function createCtx(dbIndex = 0): CommandContext {
    const client = new ClientState(1, 0);
    client.dbIndex = dbIndex;
    return {
      db: engine.db(dbIndex),
      engine,
      client,
      config,
      pubsub: engine.pubsub,
    };
  }

  it('emits to keyspace channel when K flag is set', () => {
    config.set('notify-keyspace-events', 'K$');
    engine.pubsub.subscribe(1, '__keyspace@0__:mykey');

    notify(createCtx(), EVENT_FLAGS.STRING, 'set', 'mykey');

    expect(messages).toHaveLength(1);
  });

  it('emits to keyevent channel when E flag is set', () => {
    config.set('notify-keyspace-events', 'E$');
    engine.pubsub.subscribe(1, '__keyevent@0__:set');

    notify(createCtx(), EVENT_FLAGS.STRING, 'set', 'mykey');

    expect(messages).toHaveLength(1);
  });

  it('uses client dbIndex for channel', () => {
    config.set('notify-keyspace-events', 'KEg');
    engine.pubsub.subscribe(1, '__keyspace@3__:mykey');

    notify(createCtx(3), EVENT_FLAGS.GENERIC, 'del', 'mykey');

    expect(messages).toHaveLength(1);
  });

  it('no-ops when config is not available', () => {
    const ctx: CommandContext = {
      db: engine.db(0),
      engine,
    };
    // Should not throw
    notify(ctx, EVENT_FLAGS.STRING, 'set', 'mykey');
    expect(messages).toHaveLength(0);
  });

  it('no-ops when notifications are disabled', () => {
    config.set('notify-keyspace-events', '');
    engine.pubsub.subscribe(1, '__keyspace@0__:mykey');

    notify(createCtx(), EVENT_FLAGS.STRING, 'set', 'mykey');

    expect(messages).toHaveLength(0);
  });
});
