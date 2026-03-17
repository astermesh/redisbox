import { describe, it, expect } from 'vitest';
import {
  parseInlineCommand,
  isInlineCommand,
  InlineParseResult,
} from './inline-parser.ts';

function parse(input: string, offset?: number): InlineParseResult {
  const result = parseInlineCommand(Buffer.from(input), offset);
  expect(result).toBeDefined();
  return result as InlineParseResult;
}

function args(input: string): string[] {
  return parse(input).args.map((b) => b.toString());
}

describe('parseInlineCommand', () => {
  describe('basic parsing', () => {
    it('parses simple command', () => {
      const r = parse('PING\r\n');
      expect(r.args.map((b) => b.toString())).toEqual(['PING']);
      expect(r.bytesConsumed).toBe(6);
    });

    it('parses command with arguments', () => {
      expect(args('SET key value\r\n')).toEqual(['SET', 'key', 'value']);
    });

    it('parses with \\n only (no \\r)', () => {
      const r = parse('PING\n');
      expect(r.args.map((b) => b.toString())).toEqual(['PING']);
      expect(r.bytesConsumed).toBe(5);
    });

    it('returns undefined for incomplete line', () => {
      const result = parseInlineCommand(Buffer.from('SET key'));
      expect(result).toBeUndefined();
    });

    it('handles empty line', () => {
      const r = parse('\r\n');
      expect(r.args).toEqual([]);
      expect(r.bytesConsumed).toBe(2);
    });

    it('handles multiple spaces between arguments', () => {
      expect(args('SET  key  value\r\n')).toEqual(['SET', 'key', 'value']);
    });

    it('handles tabs between arguments', () => {
      expect(args('SET\tkey\tvalue\r\n')).toEqual(['SET', 'key', 'value']);
    });
  });

  describe('double-quoted strings', () => {
    it('parses double-quoted argument', () => {
      expect(args('SET "hello world" value\r\n')).toEqual([
        'SET',
        'hello world',
        'value',
      ]);
    });

    it('handles escape sequences', () => {
      const r = parse('SET "line1\\nline2" val\r\n');
      expect(r.args[1]).toEqual(Buffer.from('line1\nline2'));
    });

    it('handles \\r escape', () => {
      expect(parse('SET "a\\rb" val\r\n').args[1]).toEqual(Buffer.from('a\rb'));
    });

    it('handles \\t escape', () => {
      expect(parse('SET "a\\tb" val\r\n').args[1]).toEqual(Buffer.from('a\tb'));
    });

    it('handles \\a escape (bell)', () => {
      expect(parse('SET "a\\ab" val\r\n').args[1]).toEqual(
        Buffer.from('a\x07b')
      );
    });

    it('handles \\b escape (backspace)', () => {
      expect(parse('SET "a\\bb" val\r\n').args[1]).toEqual(
        Buffer.from('a\x08b')
      );
    });

    it('handles \\\\ escape', () => {
      expect(parse('SET "a\\\\b" val\r\n').args[1]).toEqual(
        Buffer.from('a\\b')
      );
    });

    it('handles \\" escape', () => {
      expect(parse('SET "a\\"b" val\r\n').args[1]).toEqual(Buffer.from('a"b'));
    });

    it('handles \\xNN hex escape', () => {
      expect(parse('SET "\\x00\\xff" val\r\n').args[1]).toEqual(
        Buffer.from([0x00, 0xff])
      );
    });

    it('empty double-quoted string', () => {
      const r = parse('SET "" val\r\n');
      expect(r.args[1]).toEqual(Buffer.from(''));
      expect(r.args[1]).toHaveLength(0);
    });

    it('throws on unbalanced double quotes', () => {
      expect(() =>
        parseInlineCommand(Buffer.from('SET "unclosed\r\n'))
      ).toThrow('unbalanced quotes');
    });
  });

  describe('single-quoted strings', () => {
    it('parses single-quoted argument (literal)', () => {
      expect(args("SET 'hello world' val\r\n")).toEqual([
        'SET',
        'hello world',
        'val',
      ]);
    });

    it('does not process \\n in single quotes', () => {
      expect(parse("SET 'a\\nb' val\r\n").args[1]).toEqual(
        Buffer.from('a\\nb')
      );
    });

    it('handles \\\\ escape in single quotes', () => {
      expect(parse("SET 'a\\\\b' val\r\n").args[1]).toEqual(
        Buffer.from('a\\b')
      );
    });

    it("handles \\' escape in single quotes", () => {
      expect(parse("SET 'a\\'b' val\r\n").args[1]).toEqual(Buffer.from("a'b"));
    });

    it('empty single-quoted string', () => {
      const r = parse("SET '' val\r\n");
      expect(r.args[1]).toEqual(Buffer.from(''));
      expect(r.args[1]).toHaveLength(0);
    });

    it('throws on unbalanced single quotes', () => {
      expect(() =>
        parseInlineCommand(Buffer.from("SET 'unclosed\r\n"))
      ).toThrow('unbalanced quotes');
    });
  });

  describe('max inline length', () => {
    it('throws on too long inline request', () => {
      const longLine = 'A'.repeat(65537);
      expect(() => parseInlineCommand(Buffer.from(longLine))).toThrow(
        'too big inline request'
      );
    });
  });

  describe('offset parameter', () => {
    it('parses from given offset', () => {
      const r = parse('XXXPING\r\n', 3);
      expect(r.args.map((b) => b.toString())).toEqual(['PING']);
      expect(r.bytesConsumed).toBe(6);
    });
  });
});

describe('isInlineCommand', () => {
  it('returns true for non-* first byte', () => {
    expect(isInlineCommand('P'.charCodeAt(0))).toBe(true);
    expect(isInlineCommand('S'.charCodeAt(0))).toBe(true);
    expect(isInlineCommand('+'.charCodeAt(0))).toBe(true);
  });

  it('returns false for * first byte', () => {
    expect(isInlineCommand('*'.charCodeAt(0))).toBe(false);
  });
});
