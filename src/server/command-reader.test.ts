import { describe, it, expect } from 'vitest';
import { CommandReader } from './command-reader.ts';

describe('CommandReader', () => {
  describe('RESP multibulk parsing', () => {
    it('parses a single RESP command', () => {
      const commands: string[][] = [];
      const reader = new CommandReader((args) => commands.push(args));

      reader.write(Buffer.from('*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n'));

      expect(commands).toEqual([['GET', 'foo']]);
    });

    it('parses RESP command with multiple arguments', () => {
      const commands: string[][] = [];
      const reader = new CommandReader((args) => commands.push(args));

      reader.write(
        Buffer.from('*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n')
      );

      expect(commands).toEqual([['SET', 'foo', 'bar']]);
    });

    it('parses multiple pipelined RESP commands in one write', () => {
      const commands: string[][] = [];
      const reader = new CommandReader((args) => commands.push(args));

      reader.write(
        Buffer.from(
          '*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n' +
            '*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n'
        )
      );

      expect(commands).toEqual([
        ['SET', 'foo', 'bar'],
        ['GET', 'foo'],
      ]);
    });

    it('handles partial data split across multiple writes', () => {
      const commands: string[][] = [];
      const reader = new CommandReader((args) => commands.push(args));

      reader.write(Buffer.from('*2\r\n$3\r\nGE'));
      expect(commands).toEqual([]);

      reader.write(Buffer.from('T\r\n$3\r\nfoo\r\n'));
      expect(commands).toEqual([['GET', 'foo']]);
    });

    it('handles split at CRLF boundary', () => {
      const commands: string[][] = [];
      const reader = new CommandReader((args) => commands.push(args));

      reader.write(Buffer.from('*1\r\n$4\r\nPING\r'));
      expect(commands).toEqual([]);

      reader.write(Buffer.from('\n'));
      expect(commands).toEqual([['PING']]);
    });

    it('handles split in the middle of bulk string length line', () => {
      const commands: string[][] = [];
      const reader = new CommandReader((args) => commands.push(args));

      reader.write(Buffer.from('*1\r\n$'));
      expect(commands).toEqual([]);

      reader.write(Buffer.from('4\r\nPING\r\n'));
      expect(commands).toEqual([['PING']]);
    });

    it('handles split between count and first bulk string', () => {
      const commands: string[][] = [];
      const reader = new CommandReader((args) => commands.push(args));

      reader.write(Buffer.from('*2\r\n'));
      expect(commands).toEqual([]);

      reader.write(Buffer.from('$3\r\nGET\r\n$3\r\nfoo\r\n'));
      expect(commands).toEqual([['GET', 'foo']]);
    });

    it('handles zero-count multibulk (empty command)', () => {
      const commands: string[][] = [];
      const reader = new CommandReader((args) => commands.push(args));

      reader.write(Buffer.from('*0\r\n*1\r\n$4\r\nPING\r\n'));

      // *0 is an empty command — should not emit
      expect(commands).toEqual([['PING']]);
    });

    it('handles binary-safe bulk strings', () => {
      const commands: string[][] = [];
      const reader = new CommandReader((args) => commands.push(args));

      // SET key with value containing \r\n (6 bytes: h, e, \r, \n, l, o)
      reader.write(
        Buffer.from('*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$6\r\nhe\r\nlo\r\n')
      );

      expect(commands).toEqual([['SET', 'key', 'he\r\nlo']]);
    });

    it('throws on invalid multibulk count', () => {
      const noop = () => {
        /* error callback */
      };
      const reader = new CommandReader(noop);

      expect(() => {
        reader.write(Buffer.from('*abc\r\n'));
      }).toThrow('Protocol error');
    });

    it('silently accepts *-1 null multibulk (matches Redis behavior)', () => {
      const commands: string[][] = [];
      const reader = new CommandReader((args) => commands.push(args));

      reader.write(Buffer.from('*-1\r\n*1\r\n$4\r\nPING\r\n'));

      // *-1 is silently ignored (Redis returns C_OK with no command)
      expect(commands).toEqual([['PING']]);
    });

    it('silently accepts any negative multibulk count', () => {
      const commands: string[][] = [];
      const reader = new CommandReader((args) => commands.push(args));

      reader.write(Buffer.from('*-2\r\n*1\r\n$4\r\nPING\r\n'));

      expect(commands).toEqual([['PING']]);
    });

    it('throws on non-$ marker in multibulk body', () => {
      const noop = () => {
        /* error callback */
      };
      const reader = new CommandReader(noop);

      expect(() => {
        reader.write(Buffer.from('*1\r\n+OK\r\n'));
      }).toThrow('Protocol error');
    });

    it('throws on invalid bulk string length', () => {
      const noop = () => {
        /* error callback */
      };
      const reader = new CommandReader(noop);

      expect(() => {
        reader.write(Buffer.from('*1\r\n$abc\r\n'));
      }).toThrow('Protocol error');
    });

    it('handles three pipelined commands arriving byte-by-byte', () => {
      const commands: string[][] = [];
      const reader = new CommandReader((args) => commands.push(args));

      const data = Buffer.from(
        '*1\r\n$4\r\nPING\r\n' +
          '*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n' +
          '*2\r\n$3\r\nGET\r\n$1\r\na\r\n'
      );

      for (let i = 0; i < data.length; i++) {
        reader.write(data.subarray(i, i + 1));
      }

      expect(commands).toEqual([['PING'], ['SET', 'a', '1'], ['GET', 'a']]);
    });
  });

  describe('inline parsing', () => {
    it('parses a single inline command', () => {
      const commands: string[][] = [];
      const reader = new CommandReader((args) => commands.push(args));

      reader.write(Buffer.from('PING\r\n'));

      expect(commands).toEqual([['PING']]);
    });

    it('parses inline command with arguments', () => {
      const commands: string[][] = [];
      const reader = new CommandReader((args) => commands.push(args));

      reader.write(Buffer.from('SET foo bar\r\n'));

      expect(commands).toEqual([['SET', 'foo', 'bar']]);
    });

    it('parses multiple pipelined inline commands', () => {
      const commands: string[][] = [];
      const reader = new CommandReader((args) => commands.push(args));

      reader.write(Buffer.from('SET foo bar\r\nGET foo\r\n'));

      expect(commands).toEqual([
        ['SET', 'foo', 'bar'],
        ['GET', 'foo'],
      ]);
    });

    it('handles partial inline data across writes', () => {
      const commands: string[][] = [];
      const reader = new CommandReader((args) => commands.push(args));

      reader.write(Buffer.from('SET foo'));
      expect(commands).toEqual([]);

      reader.write(Buffer.from(' bar\r\n'));
      expect(commands).toEqual([['SET', 'foo', 'bar']]);
    });

    it('skips bare CR/LF between commands', () => {
      const commands: string[][] = [];
      const reader = new CommandReader((args) => commands.push(args));

      reader.write(Buffer.from('\r\n\r\nPING\r\n'));

      expect(commands).toEqual([['PING']]);
    });

    it('handles LF-only line endings', () => {
      const commands: string[][] = [];
      const reader = new CommandReader((args) => commands.push(args));

      reader.write(Buffer.from('PING\nGET foo\n'));

      expect(commands).toEqual([['PING'], ['GET', 'foo']]);
    });

    it('handles inline quoted strings', () => {
      const commands: string[][] = [];
      const reader = new CommandReader((args) => commands.push(args));

      reader.write(Buffer.from('SET foo "hello world"\r\n'));

      expect(commands).toEqual([['SET', 'foo', 'hello world']]);
    });
  });

  describe('mixed protocol', () => {
    it('handles inline followed by RESP', () => {
      const commands: string[][] = [];
      const reader = new CommandReader((args) => commands.push(args));

      reader.write(Buffer.from('PING\r\n*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n'));

      expect(commands).toEqual([['PING'], ['GET', 'foo']]);
    });

    it('handles RESP followed by inline', () => {
      const commands: string[][] = [];
      const reader = new CommandReader((args) => commands.push(args));

      reader.write(Buffer.from('*1\r\n$4\r\nPING\r\nGET foo\r\n'));

      expect(commands).toEqual([['PING'], ['GET', 'foo']]);
    });

    it('handles interleaved protocols across writes', () => {
      const commands: string[][] = [];
      const reader = new CommandReader((args) => commands.push(args));

      reader.write(Buffer.from('PING\r\n'));
      reader.write(Buffer.from('*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n'));
      reader.write(Buffer.from('SET bar baz\r\n'));

      expect(commands).toEqual([
        ['PING'],
        ['GET', 'foo'],
        ['SET', 'bar', 'baz'],
      ]);
    });
  });

  describe('reset', () => {
    it('clears internal buffer and allows fresh parsing', () => {
      const commands: string[][] = [];
      const reader = new CommandReader((args) => commands.push(args));

      // Write partial data
      reader.write(Buffer.from('*2\r\n$3\r\nGE'));
      reader.reset();

      // Write a fresh complete command
      reader.write(Buffer.from('*1\r\n$4\r\nPING\r\n'));
      expect(commands).toEqual([['PING']]);
    });
  });

  describe('edge cases', () => {
    it('handles empty write', () => {
      const commands: string[][] = [];
      const reader = new CommandReader((args) => commands.push(args));

      reader.write(Buffer.alloc(0));
      expect(commands).toEqual([]);
    });

    it('handles zero-length bulk string', () => {
      const commands: string[][] = [];
      const reader = new CommandReader((args) => commands.push(args));

      reader.write(Buffer.from('*2\r\n$3\r\nSET\r\n$0\r\n\r\n'));

      expect(commands).toEqual([['SET', '']]);
    });

    it('processes commands after partial + complete sequence', () => {
      const commands: string[][] = [];
      const reader = new CommandReader((args) => commands.push(args));

      // Partial first command, complete second command in same write
      reader.write(Buffer.from('*2\r\n$3\r\nGET\r\n'));
      expect(commands).toEqual([]);

      reader.write(
        Buffer.from('$3\r\nfoo\r\n*2\r\n$3\r\nGET\r\n$3\r\nbar\r\n')
      );
      expect(commands).toEqual([
        ['GET', 'foo'],
        ['GET', 'bar'],
      ]);
    });
  });
});
