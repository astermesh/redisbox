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

  describe('psubscribe', () => {
    it('subscribes a client to a pattern', () => {
      const mgr = new PubSubManager();
      const added = mgr.psubscribe(1, 'news.*');
      expect(added).toBe(true);
      expect(mgr.patternCount(1)).toBe(1);
    });

    it('returns false for duplicate pattern subscription', () => {
      const mgr = new PubSubManager();
      mgr.psubscribe(1, 'news.*');
      const added = mgr.psubscribe(1, 'news.*');
      expect(added).toBe(false);
      expect(mgr.patternCount(1)).toBe(1);
    });

    it('tracks multiple patterns per client', () => {
      const mgr = new PubSubManager();
      mgr.psubscribe(1, 'news.*');
      mgr.psubscribe(1, 'sports.*');
      mgr.psubscribe(1, 'weather.*');
      expect(mgr.patternCount(1)).toBe(3);
    });

    it('tracks multiple clients per pattern', () => {
      const mgr = new PubSubManager();
      mgr.psubscribe(1, 'news.*');
      mgr.psubscribe(2, 'news.*');
      mgr.psubscribe(3, 'news.*');
      expect(mgr.patternSubscribers('news.*').size).toBe(3);
    });
  });

  describe('punsubscribe', () => {
    it('unsubscribes a client from a pattern', () => {
      const mgr = new PubSubManager();
      mgr.psubscribe(1, 'news.*');
      const removed = mgr.punsubscribe(1, 'news.*');
      expect(removed).toBe(true);
      expect(mgr.patternCount(1)).toBe(0);
      expect(mgr.patternSubscribers('news.*').size).toBe(0);
    });

    it('returns false when not subscribed', () => {
      const mgr = new PubSubManager();
      const removed = mgr.punsubscribe(1, 'news.*');
      expect(removed).toBe(false);
    });

    it('cleans up empty pattern sets', () => {
      const mgr = new PubSubManager();
      mgr.psubscribe(1, 'news.*');
      mgr.punsubscribe(1, 'news.*');
      expect(mgr.patternSubscribers('news.*').size).toBe(0);
    });
  });

  describe('punsubscribeAll', () => {
    it('unsubscribes from all patterns', () => {
      const mgr = new PubSubManager();
      mgr.psubscribe(1, 'p1');
      mgr.psubscribe(1, 'p2');
      mgr.psubscribe(1, 'p3');
      const removed = mgr.punsubscribeAll(1);
      expect(removed).toHaveLength(3);
      expect(mgr.patternCount(1)).toBe(0);
    });

    it('returns empty array when no pattern subscriptions', () => {
      const mgr = new PubSubManager();
      const removed = mgr.punsubscribeAll(1);
      expect(removed).toHaveLength(0);
    });

    it('does not affect other clients', () => {
      const mgr = new PubSubManager();
      mgr.psubscribe(1, 'news.*');
      mgr.psubscribe(2, 'news.*');
      mgr.punsubscribeAll(1);
      expect(mgr.patternSubscribers('news.*').size).toBe(1);
      expect(mgr.patternCount(2)).toBe(1);
    });
  });

  describe('subscriptionCount', () => {
    it('returns combined channel and pattern count', () => {
      const mgr = new PubSubManager();
      mgr.subscribe(1, 'news');
      mgr.subscribe(1, 'sports');
      mgr.psubscribe(1, 'weather.*');
      expect(mgr.subscriptionCount(1)).toBe(3);
    });

    it('returns 0 for unknown client', () => {
      const mgr = new PubSubManager();
      expect(mgr.subscriptionCount(999)).toBe(0);
    });
  });

  describe('pattern matching via publish', () => {
    it('matches * wildcard (any string)', () => {
      const mgr = new PubSubManager();
      const sent: { clientId: number; reply: Reply }[] = [];
      mgr.setSender((clientId, reply) => sent.push({ clientId, reply }));

      mgr.psubscribe(1, 'news.*');
      const count = mgr.publish('news.breaking', 'hello');
      expect(count).toBe(1);
    });

    it('matches ? wildcard (single char)', () => {
      const mgr = new PubSubManager();
      const sent: { clientId: number; reply: Reply }[] = [];
      mgr.setSender((clientId, reply) => sent.push({ clientId, reply }));

      mgr.psubscribe(1, 'ch?');
      expect(mgr.publish('ch1', 'msg')).toBe(1);
      expect(mgr.publish('ch12', 'msg')).toBe(0);
    });

    it('matches [abc] character class', () => {
      const mgr = new PubSubManager();
      const sent: { clientId: number; reply: Reply }[] = [];
      mgr.setSender((clientId, reply) => sent.push({ clientId, reply }));

      mgr.psubscribe(1, 'ch[abc]');
      expect(mgr.publish('cha', 'msg')).toBe(1);
      expect(mgr.publish('chd', 'msg')).toBe(0);
    });

    it('matches [^abc] negated character class', () => {
      const mgr = new PubSubManager();
      const sent: { clientId: number; reply: Reply }[] = [];
      mgr.setSender((clientId, reply) => sent.push({ clientId, reply }));

      mgr.psubscribe(1, 'ch[^abc]');
      expect(mgr.publish('chd', 'msg')).toBe(1);
      expect(mgr.publish('cha', 'msg')).toBe(0);
    });

    it('matches escaped special characters with backslash', () => {
      const mgr = new PubSubManager();
      const sent: { clientId: number; reply: Reply }[] = [];
      mgr.setSender((clientId, reply) => sent.push({ clientId, reply }));

      mgr.psubscribe(1, 'ch\\*');
      expect(mgr.publish('ch*', 'msg')).toBe(1);
      expect(mgr.publish('chabc', 'msg')).toBe(0);
    });

    it('matches exact string without wildcards', () => {
      const mgr = new PubSubManager();
      const sent: { clientId: number; reply: Reply }[] = [];
      mgr.setSender((clientId, reply) => sent.push({ clientId, reply }));

      mgr.psubscribe(1, 'news');
      expect(mgr.publish('news', 'msg')).toBe(1);
      expect(mgr.publish('news2', 'msg')).toBe(0);
    });

    it('* matches empty string', () => {
      const mgr = new PubSubManager();
      const sent: { clientId: number; reply: Reply }[] = [];
      mgr.setSender((clientId, reply) => sent.push({ clientId, reply }));

      mgr.psubscribe(1, 'news*');
      expect(mgr.publish('news', 'msg')).toBe(1);
      expect(mgr.publish('news.extra', 'msg')).toBe(1);
    });

    it('matches [a-z] character range', () => {
      const mgr = new PubSubManager();
      const sent: { clientId: number; reply: Reply }[] = [];
      mgr.setSender((clientId, reply) => sent.push({ clientId, reply }));

      mgr.psubscribe(1, 'ch[a-z]');
      expect(mgr.publish('chm', 'msg')).toBe(1);
      expect(mgr.publish('ch1', 'msg')).toBe(0);
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

    it('delivers pmessage to pattern subscribers', () => {
      const mgr = new PubSubManager();
      const sent: { clientId: number; reply: Reply }[] = [];
      mgr.setSender((clientId, reply) => sent.push({ clientId, reply }));

      mgr.psubscribe(1, 'news.*');
      const count = mgr.publish('news.breaking', 'alert');
      expect(count).toBe(1);
      expect(sent).toHaveLength(1);
      expect(sent[0]?.reply).toEqual({
        kind: 'array',
        value: [
          { kind: 'bulk', value: 'pmessage' },
          { kind: 'bulk', value: 'news.*' },
          { kind: 'bulk', value: 'news.breaking' },
          { kind: 'bulk', value: 'alert' },
        ],
      });
    });

    it('delivers to both channel and pattern subscribers', () => {
      const mgr = new PubSubManager();
      const sent: { clientId: number; reply: Reply }[] = [];
      mgr.setSender((clientId, reply) => sent.push({ clientId, reply }));

      mgr.subscribe(1, 'news');
      mgr.psubscribe(2, 'new*');

      const count = mgr.publish('news', 'hello');
      expect(count).toBe(2);
      expect(sent).toHaveLength(2);
    });

    it('client with both channel and pattern subscription receives message twice', () => {
      const mgr = new PubSubManager();
      const sent: { clientId: number; reply: Reply }[] = [];
      mgr.setSender((clientId, reply) => sent.push({ clientId, reply }));

      mgr.subscribe(1, 'news');
      mgr.psubscribe(1, 'new*');

      const count = mgr.publish('news', 'hello');
      // Redis sends both: one as "message" and one as "pmessage"
      expect(count).toBe(2);
      expect(sent).toHaveLength(2);

      const messageReply = sent.find(
        (s) =>
          s.reply.kind === 'array' &&
          s.reply.value[0]?.kind === 'bulk' &&
          s.reply.value[0].value === 'message'
      );
      const pmessageReply = sent.find(
        (s) =>
          s.reply.kind === 'array' &&
          s.reply.value[0]?.kind === 'bulk' &&
          s.reply.value[0].value === 'pmessage'
      );
      expect(messageReply).toBeDefined();
      expect(pmessageReply).toBeDefined();
    });

    it('does not deliver pattern message after punsubscribe', () => {
      const mgr = new PubSubManager();
      const sent: { clientId: number; reply: Reply }[] = [];
      mgr.setSender((clientId, reply) => sent.push({ clientId, reply }));

      mgr.psubscribe(1, 'news.*');
      mgr.punsubscribe(1, 'news.*');

      const count = mgr.publish('news.breaking', 'hello');
      expect(count).toBe(0);
      expect(sent).toHaveLength(0);
    });

    it('multiple patterns can match the same channel', () => {
      const mgr = new PubSubManager();
      const sent: { clientId: number; reply: Reply }[] = [];
      mgr.setSender((clientId, reply) => sent.push({ clientId, reply }));

      mgr.psubscribe(1, 'n*');
      mgr.psubscribe(1, 'new*');

      const count = mgr.publish('news', 'hello');
      // Each matching pattern sends a separate pmessage
      expect(count).toBe(2);
      expect(sent).toHaveLength(2);
    });
  });

  describe('totalPatterns', () => {
    it('returns count of unique patterns with subscribers', () => {
      const mgr = new PubSubManager();
      mgr.psubscribe(1, 'p1');
      mgr.psubscribe(2, 'p1');
      mgr.psubscribe(1, 'p2');
      expect(mgr.totalPatterns).toBe(2);
    });
  });

  describe('removeClient', () => {
    it('removes all channel and pattern subscriptions', () => {
      const mgr = new PubSubManager();
      mgr.subscribe(1, 'ch1');
      mgr.subscribe(1, 'ch2');
      mgr.psubscribe(1, 'p1');
      mgr.psubscribe(1, 'p2');

      mgr.removeClient(1);

      expect(mgr.channelCount(1)).toBe(0);
      expect(mgr.patternCount(1)).toBe(0);
      expect(mgr.subscriptionCount(1)).toBe(0);
      expect(mgr.subscribers('ch1').size).toBe(0);
      expect(mgr.patternSubscribers('p1').size).toBe(0);
    });

    it('does not affect other clients', () => {
      const mgr = new PubSubManager();
      mgr.subscribe(1, 'ch1');
      mgr.subscribe(2, 'ch1');
      mgr.psubscribe(1, 'p1');
      mgr.psubscribe(2, 'p1');

      mgr.removeClient(1);

      expect(mgr.channelCount(2)).toBe(1);
      expect(mgr.patternCount(2)).toBe(1);
      expect(mgr.subscribers('ch1').size).toBe(1);
      expect(mgr.patternSubscribers('p1').size).toBe(1);
    });

    it('is safe to call for unknown client', () => {
      const mgr = new PubSubManager();
      expect(() => mgr.removeClient(999)).not.toThrow();
    });

    it('prevents message delivery after removal', () => {
      const mgr = new PubSubManager();
      const sent: { clientId: number; reply: Reply }[] = [];
      mgr.setSender((clientId, reply) => sent.push({ clientId, reply }));

      mgr.subscribe(1, 'news');
      mgr.psubscribe(1, 'new*');
      mgr.removeClient(1);

      expect(mgr.publish('news', 'hello')).toBe(0);
      expect(sent).toHaveLength(0);
    });
  });
});
