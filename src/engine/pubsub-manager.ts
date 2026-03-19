/**
 * Server-wide Pub/Sub subscription manager.
 *
 * Maintains a bidirectional index:
 *   channel → Set<clientId>
 *   clientId → Set<channel>
 *
 * This class only tracks subscriptions. Message delivery is handled
 * separately (T02: PUBLISH and message delivery).
 */

export class PubSubManager {
  /** channel name → set of subscribed client IDs */
  private readonly channelSubscribers = new Map<string, Set<number>>();

  /** client ID → set of subscribed channel names */
  private readonly clientChannels = new Map<number, Set<string>>();

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
}
