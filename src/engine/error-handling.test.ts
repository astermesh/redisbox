import { describe, it, expect, beforeEach } from 'vitest';
import { CommandDispatcher, createTransactionState } from './command-dispatcher.ts';
import type { TransactionState } from './command-dispatcher.ts';
import { createCommandTable } from './command-registry.ts';
import { RedisEngine } from './engine.ts';
import type { CommandContext, Reply } from './types.ts';
import {
  errorReply,
  wrongArityError,
  unknownCommandError,
  unknownSubcommandError,
  invalidExpireTimeError,
  WRONGTYPE_ERR,
  SYNTAX_ERR,
  NOT_INTEGER_ERR,
  NOT_FLOAT_ERR,
  OVERFLOW_ERR,
  INF_NAN_ERR,
  STRING_EXCEEDS_512MB_ERR,
  OFFSET_OUT_OF_RANGE_ERR,
  NO_SUCH_KEY_ERR,
} from './types.ts';

/**
 * Error handling and response formatting tests.
 *
 * Ensures all error messages are byte-identical to real Redis.
 * Each error constant and helper is tested for exact format parity.
 */

// --- Error constant format tests ---

describe('error constants match Redis format', () => {
  it('WRONGTYPE error', () => {
    expect(WRONGTYPE_ERR).toEqual({
      kind: 'error',
      prefix: 'WRONGTYPE',
      message: 'Operation against a key holding the wrong kind of value',
    });
  });

  it('syntax error', () => {
    expect(SYNTAX_ERR).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'syntax error',
    });
  });

  it('not integer error', () => {
    expect(NOT_INTEGER_ERR).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'value is not an integer or out of range',
    });
  });

  it('not float error', () => {
    expect(NOT_FLOAT_ERR).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'value is not a valid float',
    });
  });

  it('overflow error', () => {
    expect(OVERFLOW_ERR).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'increment or decrement would overflow',
    });
  });

  it('infinity/NaN error', () => {
    expect(INF_NAN_ERR).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'increment would produce NaN or Infinity',
    });
  });

  it('string exceeds 512MB error', () => {
    expect(STRING_EXCEEDS_512MB_ERR).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'string exceeds maximum allowed size (512MB)',
    });
  });

  it('offset out of range error', () => {
    expect(OFFSET_OUT_OF_RANGE_ERR).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'offset is out of range',
    });
  });

  it('no such key error', () => {
    expect(NO_SUCH_KEY_ERR).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'no such key',
    });
  });
});

// --- Error helper format tests ---

describe('error helpers match Redis format', () => {
  it('wrongArityError produces correct format', () => {
    expect(wrongArityError('get')).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: "wrong number of arguments for 'get' command",
    });
  });

  it('wrongArityError with subcommand', () => {
    expect(wrongArityError('object|encoding')).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: "wrong number of arguments for 'object|encoding' command",
    });
  });

  it('unknownCommandError with no args', () => {
    expect(unknownCommandError('BADCMD', [])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: "unknown command 'BADCMD', with args beginning with: ",
    });
  });

  it('unknownCommandError with args', () => {
    expect(unknownCommandError('BADCMD', ['arg1', 'arg2'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message:
        "unknown command 'BADCMD', with args beginning with: 'arg1' 'arg2'",
    });
  });

  it('unknownCommandError preserves original case', () => {
    const err = unknownCommandError('BadCmd', ['x']) as {
      kind: 'error';
      message: string;
    };
    expect(err.message).toContain("'BadCmd'");
  });

  it('unknownSubcommandError produces correct format', () => {
    expect(unknownSubcommandError('object', 'badcmd')).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message:
        "unknown subcommand or wrong number of arguments for 'object|badcmd' command",
    });
  });

  it('invalidExpireTimeError produces correct format', () => {
    expect(invalidExpireTimeError('set')).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: "invalid expire time in 'set' command",
    });
  });

  it('invalidExpireTimeError for different commands', () => {
    for (const cmd of ['set', 'setex', 'psetex', 'getex']) {
      const err = invalidExpireTimeError(cmd);
      expect(err).toEqual({
        kind: 'error',
        prefix: 'ERR',
        message: `invalid expire time in '${cmd}' command`,
      });
    }
  });
});

// --- Integration tests: errors through the dispatcher ---

describe('error responses through command dispatcher', () => {
  let dispatcher: CommandDispatcher;
  let state: TransactionState;
  let ctx: CommandContext;

  beforeEach(() => {
    const table = createCommandTable();
    dispatcher = new CommandDispatcher(table);
    state = createTransactionState();
    const now = 1000;
    const engine = new RedisEngine({ clock: () => now, rng: () => 0.5 });
    ctx = { db: engine.db(0), engine };
  });

  function dispatch(args: string[]): Reply {
    return dispatcher.dispatch(state, ctx, args);
  }

  describe('wrong number of arguments', () => {
    it('GET with no key', () => {
      expect(dispatch(['GET'])).toEqual(wrongArityError('get'));
    });

    it('GET with too many args', () => {
      expect(dispatch(['GET', 'a', 'b'])).toEqual(wrongArityError('get'));
    });

    it('SET with too few args', () => {
      expect(dispatch(['SET', 'k'])).toEqual(wrongArityError('set'));
    });

    it('RENAME with too few args', () => {
      expect(dispatch(['RENAME', 'src'])).toEqual(wrongArityError('rename'));
    });

    it('DEL with no keys', () => {
      expect(dispatch(['DEL'])).toEqual(wrongArityError('del'));
    });

    it('OBJECT ENCODING with no key', () => {
      const result = dispatch(['OBJECT', 'ENCODING']);
      expect(result).toEqual(wrongArityError('object|encoding'));
    });

    it('error message uses lowercase command name', () => {
      const result = dispatch(['GET']) as { kind: 'error'; message: string };
      expect(result.message).toBe(
        "wrong number of arguments for 'get' command"
      );
    });
  });

  describe('unknown command', () => {
    it('completely unknown command', () => {
      expect(dispatch(['BADCMD'])).toEqual(unknownCommandError('BADCMD', []));
    });

    it('unknown command with args', () => {
      expect(dispatch(['BADCMD', 'a', 'b'])).toEqual(
        unknownCommandError('BADCMD', ['a', 'b'])
      );
    });

    it('empty command array', () => {
      expect(dispatch([])).toEqual(unknownCommandError('', []));
    });

    it('preserves original command case in error', () => {
      const result = dispatch(['BadCmd', 'x']) as {
        kind: 'error';
        message: string;
      };
      expect(result.message).toContain("'BadCmd'");
    });
  });

  describe('WRONGTYPE error', () => {
    it('GET on a list key', () => {
      ctx.db.set('k', 'list', 'quicklist', ['a', 'b']);
      expect(dispatch(['GET', 'k'])).toEqual(WRONGTYPE_ERR);
    });

    it('GET on a set key', () => {
      ctx.db.set('k', 'set', 'hashtable', new Set(['a']));
      expect(dispatch(['GET', 'k'])).toEqual(WRONGTYPE_ERR);
    });

    it('GET on a hash key', () => {
      ctx.db.set('k', 'hash', 'hashtable', new Map([['f', 'v']]));
      expect(dispatch(['GET', 'k'])).toEqual(WRONGTYPE_ERR);
    });

    it('APPEND on a non-string key', () => {
      ctx.db.set('k', 'list', 'quicklist', ['a']);
      expect(dispatch(['APPEND', 'k', 'val'])).toEqual(WRONGTYPE_ERR);
    });

    it('STRLEN on a non-string key', () => {
      ctx.db.set('k', 'list', 'quicklist', ['a']);
      expect(dispatch(['STRLEN', 'k'])).toEqual(WRONGTYPE_ERR);
    });

    it('INCR on a non-string key', () => {
      ctx.db.set('k', 'list', 'quicklist', ['a']);
      expect(dispatch(['INCR', 'k'])).toEqual(WRONGTYPE_ERR);
    });

    it('error has WRONGTYPE prefix, not ERR', () => {
      ctx.db.set('k', 'list', 'quicklist', ['a']);
      const result = dispatch(['GET', 'k']) as {
        kind: 'error';
        prefix: string;
      };
      expect(result.prefix).toBe('WRONGTYPE');
    });
  });

  describe('not integer error', () => {
    it('INCR on non-integer string', () => {
      ctx.db.set('k', 'string', 'raw', 'hello');
      expect(dispatch(['INCR', 'k'])).toEqual(NOT_INTEGER_ERR);
    });

    it('INCRBY with non-integer increment', () => {
      ctx.db.set('k', 'string', 'int', '10');
      expect(dispatch(['INCRBY', 'k', 'abc'])).toEqual(NOT_INTEGER_ERR);
    });

    it('DECRBY with non-integer decrement', () => {
      ctx.db.set('k', 'string', 'int', '10');
      expect(dispatch(['DECRBY', 'k', 'xyz'])).toEqual(NOT_INTEGER_ERR);
    });

    it('EXPIRE with non-integer seconds', () => {
      ctx.db.set('k', 'string', 'raw', 'v');
      expect(dispatch(['EXPIRE', 'k', 'abc'])).toEqual(NOT_INTEGER_ERR);
    });
  });

  describe('syntax error', () => {
    it('SET with invalid flag', () => {
      expect(dispatch(['SET', 'k', 'v', 'BADFLAG'])).toEqual(SYNTAX_ERR);
    });

    it('COPY with invalid option', () => {
      expect(dispatch(['COPY', 'src', 'dst', 'BADFLAG'])).toEqual(SYNTAX_ERR);
    });
  });

  describe('overflow error', () => {
    it('INCR at INT64_MAX', () => {
      ctx.db.set('k', 'string', 'int', '9223372036854775807');
      expect(dispatch(['INCR', 'k'])).toEqual(OVERFLOW_ERR);
    });

    it('DECR at INT64_MIN', () => {
      ctx.db.set('k', 'string', 'int', '-9223372036854775808');
      expect(dispatch(['DECR', 'k'])).toEqual(OVERFLOW_ERR);
    });
  });

  describe('invalid expire time', () => {
    it('SET EX with zero', () => {
      const result = dispatch(['SET', 'k', 'v', 'EX', '0']);
      expect(result).toEqual(invalidExpireTimeError('set'));
    });

    it('SET EX with negative', () => {
      const result = dispatch(['SET', 'k', 'v', 'EX', '-1']);
      expect(result).toEqual(invalidExpireTimeError('set'));
    });

    it('SETEX with zero seconds', () => {
      const result = dispatch(['SETEX', 'k', '0', 'v']);
      expect(result).toEqual(invalidExpireTimeError('setex'));
    });

    it('PSETEX with negative ms', () => {
      const result = dispatch(['PSETEX', 'k', '-1', 'v']);
      expect(result).toEqual(invalidExpireTimeError('psetex'));
    });
  });

  describe('subscribe mode error', () => {
    it('rejects non-subscribe commands with correct message', () => {
      state.subscribed = true;
      const result = dispatch(['GET', 'k']) as {
        kind: 'error';
        prefix: string;
        message: string;
      };
      expect(result.prefix).toBe('ERR');
      expect(result.message).toBe(
        "Can't execute 'get': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context"
      );
    });

    it('lowercases command name in error', () => {
      state.subscribed = true;
      const result = dispatch(['SET', 'k', 'v']) as {
        kind: 'error';
        message: string;
      };
      expect(result.message).toContain("'set'");
    });
  });

  describe('MULTI mode errors', () => {
    it('nested MULTI returns correct error', () => {
      state.inMulti = true;
      expect(dispatch(['MULTI'])).toEqual(
        errorReply('ERR', 'MULTI calls can not be nested')
      );
    });

    it('WATCH inside MULTI returns correct error', () => {
      state.inMulti = true;
      expect(dispatch(['WATCH', 'k'])).toEqual(
        errorReply('ERR', 'WATCH inside MULTI is not allowed')
      );
    });

    it('unknown command in MULTI flags dirty', () => {
      state.inMulti = true;
      const result = dispatch(['BADCMD']);
      expect(result.kind).toBe('error');
      expect(state.multiDirty).toBe(true);
    });

    it('arity error in MULTI flags dirty', () => {
      state.inMulti = true;
      const result = dispatch(['GET']);
      expect(result.kind).toBe('error');
      expect(state.multiDirty).toBe(true);
    });
  });

  describe('no such key error', () => {
    it('RENAME with nonexistent source', () => {
      expect(dispatch(['RENAME', 'nosuch', 'dst'])).toEqual(NO_SUCH_KEY_ERR);
    });

    it('RENAMENX with nonexistent source', () => {
      expect(dispatch(['RENAMENX', 'nosuch', 'dst'])).toEqual(NO_SUCH_KEY_ERR);
    });
  });

  describe('INCRBYFLOAT errors', () => {
    it('non-float value returns not-float error', () => {
      ctx.db.set('k', 'string', 'raw', 'hello');
      expect(dispatch(['INCRBYFLOAT', 'k', '1.0'])).toEqual(NOT_FLOAT_ERR);
    });

    it('non-float increment returns not-float error', () => {
      ctx.db.set('k', 'string', 'int', '10');
      expect(dispatch(['INCRBYFLOAT', 'k', 'abc'])).toEqual(NOT_FLOAT_ERR);
    });

    it('inf increment returns NaN/Infinity error', () => {
      ctx.db.set('k', 'string', 'int', '10');
      expect(dispatch(['INCRBYFLOAT', 'k', 'inf'])).toEqual(INF_NAN_ERR);
    });
  });

  describe('SETRANGE errors', () => {
    it('negative offset returns offset out of range', () => {
      expect(dispatch(['SETRANGE', 'k', '-1', 'v'])).toEqual(
        OFFSET_OUT_OF_RANGE_ERR
      );
    });

    it('non-integer offset returns not integer error', () => {
      expect(dispatch(['SETRANGE', 'k', 'abc', 'v'])).toEqual(NOT_INTEGER_ERR);
    });
  });

  describe('GETRANGE errors', () => {
    it('non-integer start returns not integer error', () => {
      expect(dispatch(['GETRANGE', 'k', 'abc', '5'])).toEqual(NOT_INTEGER_ERR);
    });

    it('non-integer end returns not integer error', () => {
      expect(dispatch(['GETRANGE', 'k', '0', 'xyz'])).toEqual(NOT_INTEGER_ERR);
    });
  });
});

// --- Error constant identity tests (shared references) ---

describe('error constants are shared references', () => {
  it('WRONGTYPE_ERR is the same object on every access', () => {
    expect(WRONGTYPE_ERR).toBe(WRONGTYPE_ERR);
  });

  it('SYNTAX_ERR is the same object on every access', () => {
    expect(SYNTAX_ERR).toBe(SYNTAX_ERR);
  });

  it('NOT_INTEGER_ERR is the same object on every access', () => {
    expect(NOT_INTEGER_ERR).toBe(NOT_INTEGER_ERR);
  });
});
