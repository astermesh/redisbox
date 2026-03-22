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
import { matchGlob } from './glob-pattern.ts';

export type MessageSender = (clientId: number, reply: Reply) => void;

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
   * Remove a client completely — unsubscribe from all channels and patterns.
   * Should be called when a client disconnects.
   */
  removeClient(clientId: number): void {
    this.unsubscribeAll(clientId);
    this.punsubscribeAll(clientId);
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
      if (matchGlob(pattern, channel)) {
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

  /**
   * Get all active channel names (channels with at least one subscriber).
   * If a pattern is provided, only channels matching the glob pattern are returned.
   */
  activeChannels(pattern?: string): string[] {
    const channels = [...this.channelSubscribers.keys()];
    if (!pattern) return channels;
    return channels.filter((ch) => matchGlob(pattern, ch));
  }

  /**
   * Get subscriber count for specific channels.
   * Returns an array of [channel, count] pairs.
   */
  numSub(channels: string[]): [string, number][] {
    return channels.map((ch) => [
      ch,
      this.channelSubscribers.get(ch)?.size ?? 0,
    ]);
  }

  /**
   * Get total number of unique pattern subscriptions across all clients.
   * Note: this counts unique patterns, not total client-pattern pairs.
   */
  numPat(): number {
    return this.patternSubs.size;
  }
}
