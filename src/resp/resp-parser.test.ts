import { describe, it, expect, beforeEach } from 'vitest';
import { RespParser, RespValue } from './resp-parser.ts';

function bulkValue(v: RespValue): Buffer | null {
  expect(v.type).toBe('bulk');
  return (v as Extract<RespValue, { type: 'bulk' }>).value;
}

function arrayValue(v: RespValue): RespValue[] | null {
  expect(v.type).toBe('array');
  return (v as Extract<RespValue, { type: 'array' }>).value;
}

describe('RespParser', () => {
  let results: RespValue[];
  let parser: RespParser;

  beforeEach(() => {
    results = [];
    parser = new RespParser((value) => results.push(value));
  });

  function result(i: number): RespValue {
    expect(results.length).toBeGreaterThan(i);
    return results[i] as RespValue;
  }

  describe('simple strings', () => {
    it('parses simple string', () => {
      parser.write(Buffer.from('+OK\r\n'));
      expect(results).toEqual([{ type: 'simple', value: 'OK' }]);
    });

    it('parses empty simple string', () => {
      parser.write(Buffer.from('+\r\n'));
      expect(results).toEqual([{ type: 'simple', value: '' }]);
    });

    it('parses simple string with spaces', () => {
      parser.write(Buffer.from('+hello world\r\n'));
      expect(results).toEqual([{ type: 'simple', value: 'hello world' }]);
    });
  });

  describe('errors', () => {
    it('parses error', () => {
      parser.write(Buffer.from('-ERR unknown command\r\n'));
      expect(results).toEqual([
        { type: 'error', value: 'ERR unknown command' },
      ]);
    });

    it('parses WRONGTYPE error', () => {
      parser.write(
        Buffer.from(
          '-WRONGTYPE Operation against a key holding the wrong kind of value\r\n'
        )
      );
      expect(results).toEqual([
        {
          type: 'error',
          value:
            'WRONGTYPE Operation against a key holding the wrong kind of value',
        },
      ]);
    });
  });

  describe('integers', () => {
    it('parses positive integer', () => {
      parser.write(Buffer.from(':1000\r\n'));
      expect(results).toEqual([{ type: 'integer', value: 1000 }]);
    });

    it('parses zero', () => {
      parser.write(Buffer.from(':0\r\n'));
      expect(results).toEqual([{ type: 'integer', value: 0 }]);
    });

    it('parses negative integer', () => {
      parser.write(Buffer.from(':-5\r\n'));
      expect(results).toEqual([{ type: 'integer', value: -5 }]);
    });
  });

  describe('bulk strings', () => {
    it('parses bulk string', () => {
      parser.write(Buffer.from('$5\r\nhello\r\n'));
      expect(bulkValue(result(0))).toEqual(Buffer.from('hello'));
    });

    it('parses null bulk string', () => {
      parser.write(Buffer.from('$-1\r\n'));
      expect(results).toEqual([{ type: 'bulk', value: null }]);
    });

    it('parses empty bulk string', () => {
      parser.write(Buffer.from('$0\r\n\r\n'));
      const val = bulkValue(result(0));
      expect(val).not.toBeNull();
      expect(val).toHaveLength(0);
    });

    it('handles binary data with \\r\\n in bulk string', () => {
      const data = Buffer.from('$6\r\nhe\r\nlo\r\n');
      parser.write(data);
      expect(bulkValue(result(0))).toEqual(Buffer.from('he\r\nlo'));
    });

    it('handles null bytes in bulk string', () => {
      const payload = Buffer.from([0x68, 0x00, 0x69]); // h\0i
      const header = Buffer.from('$3\r\n');
      const trailer = Buffer.from('\r\n');
      parser.write(Buffer.concat([header, payload, trailer]));
      expect(bulkValue(result(0))).toEqual(payload);
    });
  });

  describe('arrays', () => {
    it('parses empty array', () => {
      parser.write(Buffer.from('*0\r\n'));
      expect(results).toEqual([{ type: 'array', value: [] }]);
    });

    it('parses null array', () => {
      parser.write(Buffer.from('*-1\r\n'));
      expect(results).toEqual([{ type: 'array', value: null }]);
    });

    it('parses array of bulk strings', () => {
      parser.write(Buffer.from('*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n'));
      const arr = arrayValue(result(0)) as RespValue[];
      expect(arr).toHaveLength(2);
      expect(bulkValue(arr[0] as RespValue)).toEqual(Buffer.from('GET'));
      expect(bulkValue(arr[1] as RespValue)).toEqual(Buffer.from('key'));
    });

    it('parses mixed type array', () => {
      parser.write(Buffer.from('*3\r\n:1\r\n+OK\r\n$4\r\ntest\r\n'));
      const arr = arrayValue(result(0)) as RespValue[];
      expect(arr).toHaveLength(3);
      expect(arr[0]).toEqual({ type: 'integer', value: 1 });
      expect(arr[1]).toEqual({ type: 'simple', value: 'OK' });
      expect((arr[2] as RespValue).type).toBe('bulk');
    });

    it('parses nested arrays', () => {
      parser.write(Buffer.from('*2\r\n*2\r\n:1\r\n:2\r\n*2\r\n:3\r\n:4\r\n'));
      const outer = arrayValue(result(0)) as RespValue[];
      expect(outer).toHaveLength(2);

      expect(arrayValue(outer[0] as RespValue)).toEqual([
        { type: 'integer', value: 1 },
        { type: 'integer', value: 2 },
      ]);

      expect(arrayValue(outer[1] as RespValue)).toEqual([
        { type: 'integer', value: 3 },
        { type: 'integer', value: 4 },
      ]);
    });

    it('parses array with null elements', () => {
      parser.write(Buffer.from('*3\r\n$3\r\nfoo\r\n$-1\r\n$3\r\nbar\r\n'));
      const arr = arrayValue(result(0)) as RespValue[];
      expect(arr).toHaveLength(3);
      expect(arr[1]).toEqual({ type: 'bulk', value: null });
    });
  });

  describe('partial buffer handling', () => {
    it('handles command split across chunks', () => {
      parser.write(Buffer.from('+HE'));
      expect(results).toHaveLength(0);
      parser.write(Buffer.from('LLO\r\n'));
      expect(results).toEqual([{ type: 'simple', value: 'HELLO' }]);
    });

    it('handles bulk string split across chunks', () => {
      parser.write(Buffer.from('$5\r\nhel'));
      expect(results).toHaveLength(0);
      parser.write(Buffer.from('lo\r\n'));
      expect(bulkValue(result(0))).toEqual(Buffer.from('hello'));
    });

    it('handles array split across chunks', () => {
      parser.write(Buffer.from('*2\r\n$3\r\nGET\r\n'));
      expect(results).toHaveLength(0);
      parser.write(Buffer.from('$3\r\nkey\r\n'));
      expect(results).toHaveLength(1);
    });

    it('handles header split at \\r\\n boundary', () => {
      parser.write(Buffer.from('$5\r'));
      expect(results).toHaveLength(0);
      parser.write(Buffer.from('\nhello\r\n'));
      expect(results).toHaveLength(1);
    });
  });

  describe('pipelined commands', () => {
    it('parses multiple commands from single buffer', () => {
      parser.write(Buffer.from('+OK\r\n:42\r\n$3\r\nfoo\r\n'));
      expect(results).toHaveLength(3);
      expect(result(0)).toEqual({ type: 'simple', value: 'OK' });
      expect(result(1)).toEqual({ type: 'integer', value: 42 });
      expect(result(2).type).toBe('bulk');
    });

    it('parses pipelined arrays', () => {
      parser.write(
        Buffer.from(
          '*2\r\n$3\r\nSET\r\n$3\r\nkey\r\n*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n'
        )
      );
      expect(results).toHaveLength(2);
    });
  });

  describe('error handling', () => {
    it('throws on unexpected type byte', () => {
      expect(() => {
        parser.write(Buffer.from('!invalid\r\n'));
      }).toThrow('Protocol error');
    });
  });

  describe('reset', () => {
    it('clears parser state', () => {
      parser.write(Buffer.from('+HEL'));
      parser.reset();
      parser.write(Buffer.from('+OK\r\n'));
      expect(results).toEqual([{ type: 'simple', value: 'OK' }]);
    });
  });
});
