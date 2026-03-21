/**
 * Slowlog manager.
 *
 * Records commands whose execution time exceeds slowlog-log-slower-than
 * microseconds. Maintains a bounded FIFO buffer of slowlog-max-len entries.
 */

export interface SlowlogEntry {
  /** Unique auto-incrementing ID */
  id: number;
  /** Unix timestamp (seconds) when the command was logged */
  timestamp: number;
  /** Execution duration in microseconds */
  duration: number;
  /** Command and its arguments */
  args: string[];
  /** Client IP:port or empty string */
  clientAddr: string;
  /** Client name (from CLIENT SETNAME) or empty string */
  clientName: string;
}

export class SlowlogManager {
  private entries: SlowlogEntry[] = [];
  private nextId = 0;

  /**
   * Record a command if its duration exceeds the threshold.
   *
   * @param durationUs - execution time in microseconds
   * @param thresholdUs - slowlog-log-slower-than value; -1 disables logging, 0 logs everything
   * @param maxLen - maximum number of entries to keep
   * @param timestampSec - unix timestamp in seconds
   * @param args - command and arguments
   * @param clientAddr - client address string
   * @param clientName - client name
   */
  record(
    durationUs: number,
    thresholdUs: number,
    maxLen: number,
    timestampSec: number,
    args: string[],
    clientAddr: string,
    clientName: string
  ): void {
    // Negative threshold disables slowlog
    if (thresholdUs < 0) return;

    // Only record if duration exceeds threshold (0 means log everything)
    if (durationUs < thresholdUs) return;

    const entry: SlowlogEntry = {
      id: this.nextId++,
      timestamp: timestampSec,
      duration: durationUs,
      args,
      clientAddr,
      clientName,
    };

    // Prepend (newest first, matching Redis behavior)
    this.entries.unshift(entry);

    // Trim to max length
    if (this.entries.length > maxLen) {
      this.entries.length = maxLen;
    }
  }

  /** Return entries, optionally limited to count. Newest first. */
  get(count?: number): SlowlogEntry[] {
    if (count === undefined || count < 0) {
      return this.entries.slice();
    }
    return this.entries.slice(0, count);
  }

  /** Return number of entries */
  len(): number {
    return this.entries.length;
  }

  /** Clear all entries */
  reset(): void {
    this.entries = [];
  }
}
