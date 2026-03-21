/**
 * Slowlog manager.
 *
 * Records commands whose execution time exceeds slowlog-log-slower-than
 * microseconds. Maintains a bounded FIFO buffer of slowlog-max-len entries.
 *
 * Matches Redis behavior:
 * - Arguments are truncated to SLOWLOG_ENTRY_MAX_ARGC (32) entries
 * - Individual argument strings are truncated to SLOWLOG_ENTRY_MAX_STRING (128) bytes
 */

/** Max number of arguments stored per slowlog entry (Redis: SLOWLOG_ENTRY_MAX_ARGC) */
const MAX_ARGC = 32;
/** Max string length per argument (Redis: SLOWLOG_ENTRY_MAX_STRING) */
const MAX_STRING = 128;

export interface SlowlogEntry {
  /** Unique auto-incrementing ID */
  id: number;
  /** Unix timestamp (seconds) when the command was logged */
  timestamp: number;
  /** Execution duration in microseconds */
  duration: number;
  /** Command and its arguments (truncated per Redis rules) */
  args: string[];
  /** Client IP:port or empty string */
  clientAddr: string;
  /** Client name (from CLIENT SETNAME) or empty string */
  clientName: string;
}

/**
 * Truncate argument list to match Redis slowlog behavior.
 * - Max 32 args; extra args replaced with "... (N more arguments)"
 * - Each arg truncated to 128 bytes; excess replaced with "... (N more bytes)"
 */
function truncateArgs(args: string[]): string[] {
  let result: string[];
  if (args.length > MAX_ARGC) {
    result = args.slice(0, MAX_ARGC - 1);
    result.push(`... (${args.length - MAX_ARGC + 1} more arguments)`);
  } else {
    result = args.slice();
  }

  for (let i = 0; i < result.length; i++) {
    const arg = result[i] ?? '';
    if (arg.length > MAX_STRING) {
      result[i] =
        arg.slice(0, MAX_STRING) +
        `... (${arg.length - MAX_STRING} more bytes)`;
    }
  }

  return result;
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
      args: truncateArgs(args),
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
    if (count === undefined) {
      return this.entries.slice(0, 10);
    }
    if (count < 0) {
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
