/**
 * Server-wide Pub/Sub subscription manager.
 *
 * Maintains bidirectional indexes for both channel and pattern subscriptions:
 *   channel → Set<clientId>   /   clientId → Set<channel>
 *   pattern → Set<clientId>   /   clientId → Set<pattern>
 *
 * Handles message delivery via a registered sender callback.
 */

import type { Reply } from './types.ts';
import { arrayReply, bulkReply } from './types.ts';

export type MessageSender = (clientId: number, reply: Reply) => void;

/**
 * Match a string against a Redis glob-style pattern.
 *
 * Supports: * (any string including empty), ? (any single char),
 * [abc] (character class), [^abc] / [!abc] (negated), [a-z] (ranges),
 * \ (escape next character).
 */
export function matchPattern(pattern: string, str: string): boolean {
  let pi = 0;
  let si = 0;

  // Backtracking state for * wildcard
  let starPi = -1;
  let starSi = -1;

  while (si < str.length) {
    if (pi < pattern.length && pattern[pi] === '\\') {
      // Escaped character — must match literally
      pi++;
      if (pi < pattern.length && pattern[pi] === str[si]) {
        pi++;
        si++;
      } else {
        // Backtrack
        if (starPi >= 0) {
          pi = starPi + 1;
          starSi++;
          si = starSi;
        } else {
          return false;
        }
      }
    } else if (pi < pattern.length && pattern[pi] === '*') {
      starPi = pi;
      starSi = si;
      pi++;
    } else if (pi < pattern.length && pattern[pi] === '?') {
      pi++;
      si++;
    } else if (pi < pattern.length && pattern[pi] === '[') {
      // Character class
      pi++;
      let negate = false;
      if (pi < pattern.length && (pattern[pi] === '^' || pattern[pi] === '!')) {
        negate = true;
        pi++;
      }

      let matched = false;
      let classEnd = false;
      while (pi < pattern.length && !classEnd) {
        if (pattern[pi] === ']') {
          classEnd = true;
          break;
        }
        if (pattern[pi] === '\\' && pi + 1 < pattern.length) {
          pi++;
          if (pattern[pi] === str[si]) matched = true;
          pi++;
        } else if (
          pi + 2 < pattern.length &&
          pattern[pi + 1] === '-' &&
          pattern[pi + 2] !== ']'
        ) {
          const lo = pattern.charCodeAt(pi);
          const hi = pattern.charCodeAt(pi + 2);
          const ch = str.charCodeAt(si);
          if (ch >= lo && ch <= hi) matched = true;
          pi += 3;
        } else {
          if (pattern[pi] === str[si]) matched = true;
          pi++;
        }
      }

      if (!classEnd) return false; // Unterminated [
      pi++; // skip ]

      if (negate) matched = !matched;

      if (matched) {
        si++;
      } else {
        if (starPi >= 0) {
          pi = starPi + 1;
          starSi++;
          si = starSi;
        } else {
          return false;
        }
      }
    } else if (pi < pattern.length && pattern[pi] === str[si]) {
      pi++;
      si++;
    } else {
      // Mismatch — backtrack to last *
      if (starPi >= 0) {
        pi = starPi + 1;
        starSi++;
        si = starSi;
      } else {
        return false;
      }
    }
  }

  // Consume remaining * in pattern
  while (pi < pattern.length && pattern[pi] === '*') {
    pi++;
  }

  return pi === pattern.length;
}

export class PubSubManager {
  /** channel name → set of subscribed client IDs */
  private readonly channelSubscribers = new Map<string, Set<number>>();

  /** client ID → set of subscribed channel names */
  private readonly clientChannels = new Map<number, Set<string>>();

  /** pattern → set of subscribed client IDs */
  private readonly patternSubs = new Map<string, Set<number>>();

  /** client ID → set of subscribed patterns */
  private readonly clientPatterns = new Map<number, Set<string>>();

  /** callback to deliver push messages to clients */
  private sender: MessageSender | null = null;

  /**
   * Subscribe a client to a channel.
   * @returns true if the client was newly subscribed, false if already subscribed
   */
  subscribe(clientId: number, channel: string): boolean {
    let channels = this.clientChannels.get(clientId);
    if (!channels) {
      channels = new Set();
      this.clientChannels.set(clientId, channels);
    }

    if (channels.has(channel)) {
      return false;
    }

    channels.add(channel);

    let subscribers = this.channelSubscribers.get(channel);
    if (!subscribers) {
      subscribers = new Set();
      this.channelSubscribers.set(channel, subscribers);
    }
    subscribers.add(clientId);

    return true;
  }

  /**
   * Unsubscribe a client from a channel.
   * @returns true if the client was subscribed (and is now removed), false if not subscribed
   */
  unsubscribe(clientId: number, channel: string): boolean {
    const channels = this.clientChannels.get(clientId);
    if (!channels || !channels.has(channel)) {
      return false;
    }

    channels.delete(channel);
    if (channels.size === 0) {
      this.clientChannels.delete(clientId);
    }

    const subscribers = this.channelSubscribers.get(channel);
    if (subscribers) {
      subscribers.delete(clientId);
      if (subscribers.size === 0) {
        this.channelSubscribers.delete(channel);
      }
    }

    return true;
  }

  /**
   * Unsubscribe a client from all channels.
   * @returns the list of channels the client was subscribed to
   */
  unsubscribeAll(clientId: number): string[] {
    const channels = this.clientChannels.get(clientId);
    if (!channels || channels.size === 0) {
      return [];
    }

    const removed = [...channels];
    for (const channel of removed) {
      const subscribers = this.channelSubscribers.get(channel);
      if (subscribers) {
        subscribers.delete(clientId);
        if (subscribers.size === 0) {
          this.channelSubscribers.delete(channel);
        }
      }
    }

    this.clientChannels.delete(clientId);
    return removed;
  }

  /**
   * Get the total number of channel subscriptions for a client.
   * This does NOT include pattern subscriptions.
   */
  channelCount(clientId: number): number {
    return this.clientChannels.get(clientId)?.size ?? 0;
  }

  /**
   * Get the channels a client is subscribed to.
   */
  clientSubscriptions(clientId: number): ReadonlySet<string> {
    return this.clientChannels.get(clientId) ?? new Set();
  }

  /**
   * Get the set of client IDs subscribed to a channel.
   */
  subscribers(channel: string): ReadonlySet<number> {
    return this.channelSubscribers.get(channel) ?? new Set();
  }

  // --- Pattern subscriptions ---

  /**
   * Subscribe a client to a pattern.
   * @returns true if the client was newly subscribed, false if already subscribed
   */
  psubscribe(clientId: number, pattern: string): boolean {
    let patterns = this.clientPatterns.get(clientId);
    if (!patterns) {
      patterns = new Set();
      this.clientPatterns.set(clientId, patterns);
    }

    if (patterns.has(pattern)) {
      return false;
    }

    patterns.add(pattern);

    let subs = this.patternSubs.get(pattern);
    if (!subs) {
      subs = new Set();
      this.patternSubs.set(pattern, subs);
    }
    subs.add(clientId);

    return true;
  }

  /**
   * Unsubscribe a client from a pattern.
   * @returns true if the client was subscribed (and is now removed), false if not subscribed
   */
  punsubscribe(clientId: number, pattern: string): boolean {
    const patterns = this.clientPatterns.get(clientId);
    if (!patterns || !patterns.has(pattern)) {
      return false;
    }

    patterns.delete(pattern);
    if (patterns.size === 0) {
      this.clientPatterns.delete(clientId);
    }

    const subs = this.patternSubs.get(pattern);
    if (subs) {
      subs.delete(clientId);
      if (subs.size === 0) {
        this.patternSubs.delete(pattern);
      }
    }

    return true;
  }

  /**
   * Unsubscribe a client from all patterns.
   * @returns the list of patterns the client was subscribed to
   */
  punsubscribeAll(clientId: number): string[] {
    const patterns = this.clientPatterns.get(clientId);
    if (!patterns || patterns.size === 0) {
      return [];
    }

    const removed = [...patterns];
    for (const pattern of removed) {
      const subs = this.patternSubs.get(pattern);
      if (subs) {
        subs.delete(clientId);
        if (subs.size === 0) {
          this.patternSubs.delete(pattern);
        }
      }
    }

    this.clientPatterns.delete(clientId);
    return removed;
  }

  /**
   * Get the total number of pattern subscriptions for a client.
   */
  patternCount(clientId: number): number {
    return this.clientPatterns.get(clientId)?.size ?? 0;
  }

  /**
   * Get the patterns a client is subscribed to.
   */
  clientPatternSubscriptions(clientId: number): ReadonlySet<string> {
    return this.clientPatterns.get(clientId) ?? new Set();
  }

  /**
   * Get the set of client IDs subscribed to a pattern.
   */
  patternSubscribers(pattern: string): ReadonlySet<number> {
    return this.patternSubs.get(pattern) ?? new Set();
  }

  /**
   * Get the total number of subscriptions (channels + patterns) for a client.
   * This is the count returned in SUBSCRIBE/UNSUBSCRIBE/PSUBSCRIBE/PUNSUBSCRIBE replies.
   */
  subscriptionCount(clientId: number): number {
    return this.channelCount(clientId) + this.patternCount(clientId);
  }

  /**
   * Register a callback for delivering push messages to clients.
   */
  setSender(sender: MessageSender): void {
    this.sender = sender;
  }

  /**
   * Publish a message to a channel.
   * Delivers to all channel subscribers and pattern subscribers.
   * @returns the number of clients that received the message
   */
  publish(channel: string, message: string): number {
    let count = 0;

    // Deliver to channel subscribers
    const subs = this.channelSubscribers.get(channel);
    if (subs && subs.size > 0) {
      const reply = arrayReply([
        bulkReply('message'),
        bulkReply(channel),
        bulkReply(message),
      ]);

      for (const clientId of subs) {
        this.sender?.(clientId, reply);
        count++;
      }
    }

    // Deliver to pattern subscribers
    for (const [pattern, patternClients] of this.patternSubs) {
      if (matchPattern(pattern, channel)) {
        const reply = arrayReply([
          bulkReply('pmessage'),
          bulkReply(pattern),
          bulkReply(channel),
          bulkReply(message),
        ]);

        for (const clientId of patternClients) {
          this.sender?.(clientId, reply);
          count++;
        }
      }
    }

    return count;
  }

  /**
   * Total number of active channels (channels with at least one subscriber).
   */
  get totalChannels(): number {
    return this.channelSubscribers.size;
  }

  /**
   * Total number of active patterns (patterns with at least one subscriber).
   */
  get totalPatterns(): number {
    return this.patternSubs.size;
  }
}
