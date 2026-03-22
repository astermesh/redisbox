/**
 * Latency monitor manager.
 *
 * Records latency spikes for server events when they exceed the
 * latency-monitor-threshold config (in milliseconds).
 *
 * Matches Redis behavior:
 * - Each event maintains a circular buffer of up to 160 samples
 * - Samples are [timestamp-sec, latency-ms] pairs
 * - Tracks all-time max latency per event
 */

/** Max number of samples stored per event */
const MAX_SAMPLES = 160;

export interface LatencySample {
  /** Unix timestamp (seconds) when the sample was recorded */
  timestamp: number;
  /** Latency duration in milliseconds */
  latency: number;
}

interface EventData {
  /** Circular buffer of samples */
  samples: LatencySample[];
  /** All-time maximum latency for this event */
  max: number;
}

export class LatencyManager {
  private events = new Map<string, EventData>();

  /**
   * Record a latency event if it exceeds the threshold.
   *
   * @param event - event name (e.g. "command", "fast-command", "expire-cycle")
   * @param latencyMs - measured latency in milliseconds
   * @param thresholdMs - latency-monitor-threshold config value; 0 = disabled
   * @param timestampSec - unix timestamp in seconds
   */
  record(
    event: string,
    latencyMs: number,
    thresholdMs: number,
    timestampSec: number
  ): void {
    // Threshold 0 means monitoring is disabled
    if (thresholdMs <= 0) return;

    // Only record if latency exceeds threshold
    if (latencyMs < thresholdMs) return;

    let data = this.events.get(event);
    if (!data) {
      data = { samples: [], max: 0 };
      this.events.set(event, data);
    }

    data.samples.push({ timestamp: timestampSec, latency: latencyMs });

    // Evict oldest when exceeding max samples
    if (data.samples.length > MAX_SAMPLES) {
      data.samples.shift();
    }

    if (latencyMs > data.max) {
      data.max = latencyMs;
    }
  }

  /**
   * Return latest sample for all events.
   * Each entry: [event-name, timestamp-of-latest, latest-latency-ms, all-time-max-ms]
   */
  latest(): {
    event: string;
    timestamp: number;
    latest: number;
    max: number;
  }[] {
    const result: {
      event: string;
      timestamp: number;
      latest: number;
      max: number;
    }[] = [];

    for (const [event, data] of this.events) {
      if (data.samples.length === 0) continue;
      const last = data.samples.at(-1);
      if (!last) continue;
      result.push({
        event,
        timestamp: last.timestamp,
        latest: last.latency,
        max: data.max,
      });
    }

    return result;
  }

  /**
   * Return all samples for a specific event.
   * Returns empty array if event doesn't exist.
   */
  history(event: string): LatencySample[] {
    const data = this.events.get(event);
    if (!data) return [];
    return data.samples.slice();
  }

  /**
   * Reset latency data for specified events (or all if none specified).
   * Returns number of events that were reset.
   */
  reset(events?: string[]): number {
    if (!events || events.length === 0) {
      const count = this.events.size;
      this.events.clear();
      return count;
    }

    let count = 0;
    for (const event of events) {
      if (this.events.delete(event)) {
        count++;
      }
    }
    return count;
  }

  /** Return all known event names */
  eventNames(): string[] {
    return Array.from(this.events.keys());
  }

  /** Check if an event has recorded samples */
  has(event: string): boolean {
    const data = this.events.get(event);
    return data !== undefined && data.samples.length > 0;
  }
}
