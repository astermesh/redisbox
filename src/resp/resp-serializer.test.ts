import { describe, it, expect } from 'vitest';
import {
  serialize,
  simpleString,
  error,
  integer,
  bulkString,
  nullBulk,
  nullArray,
  array,
  ok,
  pong,
} from './resp-serializer.ts';
import type { RespValue } from './resp-parser.ts';

describe('serialize', () => {
  it('serializes simple string', () => {
    const result = serialize({ type: 'simple', value: 'OK' });
    expect(result).toEqual(Buffer.from('+OK\r\n'));
  });

  it('serializes error', () => {
    const result = serialize({
      type: 'error',
      value: 'ERR unknown',
    });
    expect(result).toEqual(Buffer.from('-ERR unknown\r\n'));
  });

  it('serializes integer', () => {
    const result = serialize({ type: 'integer', value: 42 });
    expect(result).toEqual(Buffer.from(':42\r\n'));
  });

  it('serializes negative integer', () => {
    const result = serialize({ type: 'integer', value: -1 });
    expect(result).toEqual(Buffer.from(':-1\r\n'));
  });

  it('serializes bulk string', () => {
    const result = serialize({
      type: 'bulk',
      value: Buffer.from('hello'),
    });
    expect(result).toEqual(Buffer.from('$5\r\nhello\r\n'));
  });

  it('serializes empty bulk string', () => {
    const result = serialize({
      type: 'bulk',
      value: Buffer.alloc(0),
    });
    expect(result).toEqual(Buffer.from('$0\r\n\r\n'));
  });

  it('serializes null bulk string', () => {
    const result = serialize({ type: 'bulk', value: null });
    expect(result).toEqual(Buffer.from('$-1\r\n'));
  });

  it('serializes null array', () => {
    const result = serialize({ type: 'array', value: null });
    expect(result).toEqual(Buffer.from('*-1\r\n'));
  });

  it('serializes empty array', () => {
    const result = serialize({ type: 'array', value: [] });
    expect(result).toEqual(Buffer.from('*0\r\n'));
  });

  it('serializes array of bulk strings', () => {
    const items: RespValue[] = [
      { type: 'bulk', value: Buffer.from('SET') },
      { type: 'bulk', value: Buffer.from('key') },
      { type: 'bulk', value: Buffer.from('val') },
    ];
    const result = serialize({ type: 'array', value: items });
    expect(result).toEqual(
      Buffer.from('*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$3\r\nval\r\n')
    );
  });

  it('serializes nested arrays', () => {
    const inner: RespValue = {
      type: 'array',
      value: [
        { type: 'integer', value: 1 },
        { type: 'integer', value: 2 },
      ],
    };
    const result = serialize({
      type: 'array',
      value: [inner],
    });
    expect(result).toEqual(Buffer.from('*1\r\n*2\r\n:1\r\n:2\r\n'));
  });

  it('serializes bulk string with binary data', () => {
    const data = Buffer.from([0x00, 0x0d, 0x0a, 0xff]);
    const result = serialize({ type: 'bulk', value: data });
    expect(result).toEqual(
      Buffer.concat([Buffer.from('$4\r\n'), data, Buffer.from('\r\n')])
    );
  });
});

describe('convenience helpers', () => {
  it('simpleString', () => {
    expect(simpleString('OK')).toEqual(Buffer.from('+OK\r\n'));
  });

  it('simpleString throws on embedded CR', () => {
    expect(() => simpleString('bad\rvalue')).toThrow();
  });

  it('simpleString throws on embedded LF', () => {
    expect(() => simpleString('bad\nvalue')).toThrow();
  });

  it('error throws on embedded CRLF', () => {
    expect(() => error('ERR bad\r\nvalue')).toThrow();
  });

  it('error', () => {
    expect(error('ERR test')).toEqual(Buffer.from('-ERR test\r\n'));
  });

  it('integer', () => {
    expect(integer(100)).toEqual(Buffer.from(':100\r\n'));
  });

  it('bulkString from string', () => {
    expect(bulkString('hi')).toEqual(Buffer.from('$2\r\nhi\r\n'));
  });

  it('bulkString from Buffer', () => {
    expect(bulkString(Buffer.from('hi'))).toEqual(Buffer.from('$2\r\nhi\r\n'));
  });

  it('bulkString null', () => {
    expect(bulkString(null)).toEqual(Buffer.from('$-1\r\n'));
  });

  it('nullBulk', () => {
    expect(nullBulk()).toEqual(Buffer.from('$-1\r\n'));
  });

  it('nullArray', () => {
    expect(nullArray()).toEqual(Buffer.from('*-1\r\n'));
  });

  it('array', () => {
    const items: RespValue[] = [{ type: 'integer', value: 1 }];
    expect(array(items)).toEqual(Buffer.from('*1\r\n:1\r\n'));
  });

  it('ok', () => {
    expect(ok()).toEqual(Buffer.from('+OK\r\n'));
  });

  it('pong', () => {
    expect(pong()).toEqual(Buffer.from('+PONG\r\n'));
  });
});
