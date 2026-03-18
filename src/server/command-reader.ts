/**
 * Unified command reader supporting both RESP multibulk and inline protocols.
 *
 * Performs per-command protocol detection: checks the first byte of each
 * command to determine whether it's a RESP multibulk (*) or an inline
 * command, matching real Redis networking.c behavior.
 */

import { parseInlineCommand } from '../resp/inline-parser.ts';

export type CommandCallback = (args: string[]) => void;

const CR = 0x0d;
const LF = 0x0a;
const STAR = 0x2a; // *
const DOLLAR = 0x24; // $

const MAX_BULK_LEN = 512 * 1024 * 1024; // 512 MB

export class CommandReader {
  private buffer: Buffer = Buffer.alloc(0);
  private offset = 0;
  private readonly callback: CommandCallback;

  constructor(callback: CommandCallback) {
    this.callback = callback;
  }

  /** Feed incoming data into the reader. */
  write(data: Buffer | Uint8Array): void {
    const buf = Buffer.isBuffer(data) ? data : Buffer.from(data);
    if (buf.length === 0) return;

    if (this.offset > 0) {
      this.buffer = Buffer.concat([this.buffer.subarray(this.offset), buf]);
    } else if (this.buffer.length > 0) {
      this.buffer = Buffer.concat([this.buffer, buf]);
    } else {
      this.buffer = buf;
    }
    this.offset = 0;
    this.process();
  }

  /** Reset reader state, discarding any buffered data. */
  reset(): void {
    this.buffer = Buffer.alloc(0);
    this.offset = 0;
  }

  private process(): void {
    while (this.offset < this.buffer.length) {
      const firstByte = this.buffer[this.offset] as number;

      if (firstByte === CR || firstByte === LF) {
        // Skip bare CR/LF (telnet clients sometimes send these)
        this.offset++;
        continue;
      }

      if (firstByte === STAR) {
        const args = this.parseMultibulk();
        if (args === undefined) break; // incomplete
        if (args.length > 0) {
          this.callback(args);
        }
      } else {
        // Inline command
        const result = parseInlineCommand(this.buffer, this.offset);
        if (!result) break; // incomplete
        this.offset += result.bytesConsumed;
        if (result.args.length > 0) {
          this.callback(result.args.map((b) => b.toString('utf8')));
        }
      }
    }

    // Compact buffer when fully consumed
    if (this.offset >= this.buffer.length) {
      this.buffer = Buffer.alloc(0);
      this.offset = 0;
    }
  }

  private parseMultibulk(): string[] | undefined {
    const savedOffset = this.offset;

    // Read "*N\r\n"
    const countStr = this.readLine();
    if (countStr === undefined) {
      this.offset = savedOffset;
      return undefined;
    }

    const count = parseInt(countStr, 10);
    if (isNaN(count) || count < 0) {
      throw new Error('Protocol error: invalid multibulk length');
    }

    if (count === 0) return [];

    const args: string[] = [];
    for (let i = 0; i < count; i++) {
      const arg = this.readBulkString();
      if (arg === undefined) {
        this.offset = savedOffset;
        return undefined;
      }
      args.push(arg);
    }

    return args;
  }

  /**
   * Read a CRLF-terminated line, skipping the leading type marker byte.
   * Returns the line content (without marker or CRLF), or undefined if
   * the line is incomplete.
   */
  private readLine(): string | undefined {
    const start = this.offset + 1; // skip type marker
    const idx = this.buffer.indexOf('\r\n', start);
    if (idx === -1) return undefined;
    const line = this.buffer.toString('utf8', start, idx);
    this.offset = idx + 2;
    return line;
  }

  private readBulkString(): string | undefined {
    if (this.offset >= this.buffer.length) return undefined;

    const marker = this.buffer[this.offset] as number;
    if (marker !== DOLLAR) {
      throw new Error(
        `Protocol error: expected '$', got '${String.fromCharCode(marker)}'`
      );
    }

    const savedOffset = this.offset;
    const lenStr = this.readLine();
    if (lenStr === undefined) {
      this.offset = savedOffset;
      return undefined;
    }

    const len = parseInt(lenStr, 10);
    if (isNaN(len) || len < 0 || len > MAX_BULK_LEN) {
      throw new Error('Protocol error: invalid bulk length');
    }

    // Need len bytes + \r\n
    if (this.offset + len + 2 > this.buffer.length) {
      this.offset = savedOffset;
      return undefined;
    }

    const value = this.buffer.toString('utf8', this.offset, this.offset + len);
    this.offset += len + 2;
    return value;
  }
}
