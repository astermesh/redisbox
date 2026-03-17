/**
 * RESP2 streaming parser.
 *
 * Incrementally parses RESP2 protocol data, emitting parsed values
 * via a callback. Handles partial buffers across multiple data events.
 */

import type { RespValue, RespCallback } from './types.ts';
export type { RespValue, RespCallback };

const PLUS = 0x2b; // +
const MINUS = 0x2d; // -
const COLON = 0x3a; // :
const DOLLAR = 0x24; // $
const STAR = 0x2a; // *

const MAX_BULK_LEN = 512 * 1024 * 1024; // 512 MB

export class RespParser {
  private buffer: Buffer = Buffer.alloc(0);
  private offset = 0;
  private readonly callback: RespCallback;

  constructor(callback: RespCallback) {
    this.callback = callback;
  }

  /** Feed incoming data into the parser. */
  write(data: Buffer | Uint8Array): void {
    const buf = Buffer.isBuffer(data) ? data : Buffer.from(data);

    if (this.offset > 0) {
      this.buffer = Buffer.concat([this.buffer.subarray(this.offset), buf]);
      this.offset = 0;
    } else if (this.buffer.length > 0) {
      this.buffer = Buffer.concat([this.buffer, buf]);
    } else {
      this.buffer = buf;
    }

    this.parse();
  }

  /** Reset parser state. */
  reset(): void {
    this.buffer = Buffer.alloc(0);
    this.offset = 0;
  }

  private parse(): void {
    while (this.offset < this.buffer.length) {
      const value = this.readValue();
      if (value === undefined) {
        break;
      }
      this.emit(value);
    }

    // compact buffer
    if (this.offset > 0) {
      if (this.offset >= this.buffer.length) {
        this.buffer = Buffer.alloc(0);
        this.offset = 0;
      }
      // leave partial data for next write
    }
  }

  private readValue(): RespValue | undefined {
    if (this.offset >= this.buffer.length) return undefined;

    // bounds already checked above
    const type = this.buffer[this.offset] as number;

    switch (type) {
      case PLUS:
        return this.readSimpleString();
      case MINUS:
        return this.readError();
      case COLON:
        return this.readInteger();
      case DOLLAR:
        return this.readBulkString();
      case STAR:
        return this.readArray();
      default:
        throw new Error(
          `Protocol error: unexpected byte '${String.fromCharCode(type)}' (0x${type.toString(16)})`
        );
    }
  }

  private readLine(): string | undefined {
    const start = this.offset + 1; // skip type byte
    const idx = this.buffer.indexOf('\r\n', start);
    if (idx === -1) return undefined;
    const line = this.buffer.toString('utf8', start, idx);
    this.offset = idx + 2;
    return line;
  }

  private readSimpleString(): RespValue | undefined {
    const savedOffset = this.offset;
    const line = this.readLine();
    if (line === undefined) {
      this.offset = savedOffset;
      return undefined;
    }
    return { type: 'simple', value: line };
  }

  private readError(): RespValue | undefined {
    const savedOffset = this.offset;
    const line = this.readLine();
    if (line === undefined) {
      this.offset = savedOffset;
      return undefined;
    }
    return { type: 'error', value: line };
  }

  private readInteger(): RespValue | undefined {
    const savedOffset = this.offset;
    const line = this.readLine();
    if (line === undefined) {
      this.offset = savedOffset;
      return undefined;
    }
    const value = parseInt(line, 10);
    if (isNaN(value)) {
      throw new Error(`Protocol error: invalid integer '${line}'`);
    }
    return { type: 'integer', value };
  }

  private readBulkString(): RespValue | undefined {
    const savedOffset = this.offset;
    const line = this.readLine();
    if (line === undefined) {
      this.offset = savedOffset;
      return undefined;
    }

    const len = parseInt(line, 10);

    if (isNaN(len)) {
      throw new Error(`Protocol error: invalid bulk length '${line}'`);
    }

    // null bulk string
    if (len === -1) {
      return { type: 'bulk', value: null };
    }

    if (len < 0 || len > MAX_BULK_LEN) {
      throw new Error(`Protocol error: invalid bulk length ${len}`);
    }

    // need len bytes + \r\n
    if (this.offset + len + 2 > this.buffer.length) {
      this.offset = savedOffset;
      return undefined;
    }

    if (
      this.buffer[this.offset + len] !== 0x0d ||
      this.buffer[this.offset + len + 1] !== 0x0a
    ) {
      throw new Error('Protocol error: bulk string terminator is not CRLF');
    }

    const value = Buffer.alloc(len);
    this.buffer.copy(value, 0, this.offset, this.offset + len);
    this.offset += len + 2; // skip data + \r\n
    return { type: 'bulk', value };
  }

  private readArray(): RespValue | undefined {
    const savedOffset = this.offset;
    const line = this.readLine();
    if (line === undefined) {
      this.offset = savedOffset;
      return undefined;
    }

    const count = parseInt(line, 10);

    if (isNaN(count)) {
      throw new Error(`Protocol error: invalid array length '${line}'`);
    }

    // null array
    if (count === -1) {
      return { type: 'array', value: null };
    }

    if (count < 0) {
      throw new Error(`Protocol error: invalid array length ${count}`);
    }

    // empty array
    if (count === 0) {
      return { type: 'array', value: [] };
    }

    const items: RespValue[] = [];
    for (let i = 0; i < count; i++) {
      const item = this.readValue();
      if (item === undefined) {
        this.offset = savedOffset;
        return undefined;
      }
      items.push(item);
    }

    return { type: 'array', value: items };
  }

  private emit(value: RespValue): void {
    this.callback(value);
  }
}
