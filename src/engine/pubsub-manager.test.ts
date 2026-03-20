import { describe, it, expect } from 'vitest';
import { PubSubManager } from './pubsub-manager.ts';
import type { Reply } from './types.ts';

describe('PubSubManager', () => {
  describe('subscribe', () => {
    it('subscribes a client to a channel', () => {
      const mgr = new PubSubManager();
      const added = mgr.subscribe(1, 'news');
      expect(added).toBe(true);
      expect(mgr.channelCount(1)).toBe(1);
    });

    it('returns false for duplicate subscription', () => {
      const mgr = new PubSubManager();
      mgr.subscribe(1, 'news');
      const added = mgr.subscribe(1, 'news');
      expect(added).toBe(false);
      expect(mgr.channelCount(1)).toBe(1);
    });

    it('tracks multiple channels per client', () => {
      const mgr = new PubSubManager();
      mgr.subscribe(1, 'ch1');
      mgr.subscribe(1, 'ch2');
      mgr.subscribe(1, 'ch3');
      expect(mgr.channelCount(1)).toBe(3);
    });

    it('tracks multiple clients per channel', () => {
      const mgr = new PubSubManager();
      mgr.subscribe(1, 'news');
      mgr.subscribe(2, 'news');
      mgr.subscribe(3, 'news');
      expect(mgr.subscribers('news').size).toBe(3);
    });
  });

  describe('unsubscribe', () => {
    it('unsubscribes a client from a channel', () => {
      const mgr = new PubSubManager();
      mgr.subscribe(1, 'news');
      const removed = mgr.unsubscribe(1, 'news');
      expect(removed).toBe(true);
      expect(mgr.channelCount(1)).toBe(0);
      expect(mgr.subscribers('news').size).toBe(0);
    });

    it('returns false when not subscribed', () => {
      const mgr = new PubSubManager();
      const removed = mgr.unsubscribe(1, 'news');
      expect(removed).toBe(false);
    });

    it('cleans up empty channel sets', () => {
      const mgr = new PubSubManager();
      mgr.subscribe(1, 'news');
      mgr.unsubscribe(1, 'news');
      expect(mgr.subscribers('news').size).toBe(0);
    });
  });

  describe('unsubscribeAll', () => {
    it('unsubscribes from all channels', () => {
      const mgr = new PubSubManager();
      mgr.subscribe(1, 'ch1');
      mgr.subscribe(1, 'ch2');
      mgr.subscribe(1, 'ch3');
      const removed = mgr.unsubscribeAll(1);
      expect(removed).toHaveLength(3);
      expect(mgr.channelCount(1)).toBe(0);
    });

    it('returns empty array when no subscriptions', () => {
      const mgr = new PubSubManager();
      const removed = mgr.unsubscribeAll(1);
      expect(removed).toHaveLength(0);
    });

    it('does not affect other clients', () => {
      const mgr = new PubSubManager();
      mgr.subscribe(1, 'news');
      mgr.subscribe(2, 'news');
      mgr.unsubscribeAll(1);
      expect(mgr.subscribers('news').size).toBe(1);
      expect(mgr.channelCount(2)).toBe(1);
    });
  });

  describe('channelCount', () => {
    it('returns 0 for unknown client', () => {
      const mgr = new PubSubManager();
      expect(mgr.channelCount(999)).toBe(0);
    });
  });

  describe('clientSubscriptions', () => {
    it('returns subscribed channels', () => {
      const mgr = new PubSubManager();
      mgr.subscribe(1, 'ch1');
      mgr.subscribe(1, 'ch2');
      const subs = mgr.clientSubscriptions(1);
      expect(subs.has('ch1')).toBe(true);
      expect(subs.has('ch2')).toBe(true);
      expect(subs.size).toBe(2);
    });

    it('returns empty set for unknown client', () => {
      const mgr = new PubSubManager();
      expect(mgr.clientSubscriptions(999).size).toBe(0);
    });
  });

  describe('publish', () => {
    it('returns 0 when no subscribers', () => {
      const mgr = new PubSubManager();
      expect(mgr.publish('news', 'hello')).toBe(0);
    });

    it('delivers message to channel subscribers and returns count', () => {
      const mgr = new PubSubManager();
      const sent: { clientId: number; reply: Reply }[] = [];
      mgr.setSender((clientId, reply) => sent.push({ clientId, reply }));

      mgr.subscribe(1, 'news');
      mgr.subscribe(2, 'news');

      const count = mgr.publish('news', 'hello');
      expect(count).toBe(2);
      expect(sent).toHaveLength(2);
    });

    it('sends correct message format to channel subscribers', () => {
      const mgr = new PubSubManager();
      const sent: { clientId: number; reply: Reply }[] = [];
      mgr.setSender((clientId, reply) => sent.push({ clientId, reply }));

      mgr.subscribe(1, 'news');
      mgr.publish('news', 'hello world');

      expect(sent).toHaveLength(1);
      expect(sent[0]?.reply).toEqual({
        kind: 'array',
        value: [
          { kind: 'bulk', value: 'message' },
          { kind: 'bulk', value: 'news' },
          { kind: 'bulk', value: 'hello world' },
        ],
      });
    });

    it('does not deliver to clients subscribed to other channels', () => {
      const mgr = new PubSubManager();
      const sent: { clientId: number; reply: Reply }[] = [];
      mgr.setSender((clientId, reply) => sent.push({ clientId, reply }));

      mgr.subscribe(1, 'sports');
      mgr.subscribe(2, 'news');

      const count = mgr.publish('news', 'hello');
      expect(count).toBe(1);
      expect(sent).toHaveLength(1);
      expect(sent[0]?.clientId).toBe(2);
    });

    it('returns 0 when no sender is set', () => {
      const mgr = new PubSubManager();
      mgr.subscribe(1, 'news');
      // no sender registered — publish should still return count
      const count = mgr.publish('news', 'hello');
      expect(count).toBe(1);
    });

    it('delivers to multiple channels independently', () => {
      const mgr = new PubSubManager();
      const sent: { clientId: number; reply: Reply }[] = [];
      mgr.setSender((clientId, reply) => sent.push({ clientId, reply }));

      mgr.subscribe(1, 'ch1');
      mgr.subscribe(1, 'ch2');
      mgr.subscribe(2, 'ch1');

      const count = mgr.publish('ch1', 'msg');
      expect(count).toBe(2);
      expect(sent).toHaveLength(2);
    });

    it('does not deliver after unsubscribe', () => {
      const mgr = new PubSubManager();
      const sent: { clientId: number; reply: Reply }[] = [];
      mgr.setSender((clientId, reply) => sent.push({ clientId, reply }));

      mgr.subscribe(1, 'news');
      mgr.unsubscribe(1, 'news');

      const count = mgr.publish('news', 'hello');
      expect(count).toBe(0);
      expect(sent).toHaveLength(0);
    });
  });
});
