/**
 * RESP2 serializer.
 *
 * Encodes Redis response values into RESP2 wire format,
 * byte-for-byte matching real Redis output.
 */

import type { RespValue } from './resp-parser.ts';

const CRLF = Buffer.from('\r\n');

/** Encode a RESP value to wire format. */
export function serialize(value: RespValue): Buffer {
  switch (value.type) {
    case 'simple':
      return Buffer.from(`+${value.value}\r\n`);

    case 'error':
      return Buffer.from(`-${value.value}\r\n`);

    case 'integer':
      return Buffer.from(`:${value.value}\r\n`);

    case 'bulk':
      return serializeBulk(value.value);

    case 'array':
      return serializeArray(value.value);
  }
}

function serializeBulk(value: Buffer | null): Buffer {
  if (value === null) {
    return Buffer.from('$-1\r\n');
  }
  const header = Buffer.from(`$${value.length}\r\n`);
  return Buffer.concat([header, value, CRLF]);
}

function serializeArray(value: RespValue[] | null): Buffer {
  if (value === null) {
    return Buffer.from('*-1\r\n');
  }
  const parts: Buffer[] = [Buffer.from(`*${value.length}\r\n`)];
  for (const item of value) {
    parts.push(serialize(item));
  }
  return Buffer.concat(parts);
}

// Convenience helpers for common response patterns

export function simpleString(value: string): Buffer {
  return Buffer.from(`+${value}\r\n`);
}

export function error(value: string): Buffer {
  return Buffer.from(`-${value}\r\n`);
}

export function integer(value: number): Buffer {
  return Buffer.from(`:${value}\r\n`);
}

export function bulkString(value: Buffer | string | null): Buffer {
  if (value === null) {
    return Buffer.from('$-1\r\n');
  }
  const buf = typeof value === 'string' ? Buffer.from(value) : value;
  const header = Buffer.from(`$${buf.length}\r\n`);
  return Buffer.concat([header, buf, CRLF]);
}

export function nullBulk(): Buffer {
  return Buffer.from('$-1\r\n');
}

export function nullArray(): Buffer {
  return Buffer.from('*-1\r\n');
}

export function array(values: RespValue[]): Buffer {
  return serializeArray(values);
}

export function ok(): Buffer {
  return Buffer.from('+OK\r\n');
}

export function pong(): Buffer {
  return Buffer.from('+PONG\r\n');
}
