/**
 * Server-wide Pub/Sub subscription manager.
 *
 * Maintains a bidirectional index:
 *   channel → Set<clientId>
 *   clientId → Set<channel>
 *
 * Handles message delivery via a registered sender callback.
 */

import type { Reply } from './types.ts';
import { arrayReply, bulkReply } from './types.ts';

export type MessageSender = (clientId: number, reply: Reply) => void;

export class PubSubManager {
  /** channel name → set of subscribed client IDs */
  private readonly channelSubscribers = new Map<string, Set<number>>();

  /** client ID → set of subscribed channel names */
  private readonly clientChannels = new Map<number, Set<string>>();

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

  /**
   * Register a callback for delivering push messages to clients.
   */
  setSender(sender: MessageSender): void {
    this.sender = sender;
  }

  /**
   * Publish a message to a channel.
   * Delivers to all channel subscribers (and pattern subscribers once added).
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

    // Pattern subscriber delivery will be added by T03 (PSUBSCRIBE)

    return count;
  }

  /**
   * Total number of active channels (channels with at least one subscriber).
   */
  get totalChannels(): number {
    return this.channelSubscribers.size;
  }
}
