import { describe, it, expect } from 'vitest';
import { createCommandTable } from '../command-registry.ts';
import type { CommandTable } from '../command-table.ts';
import * as cmd from './command.ts';
import type { Reply } from '../types.ts';

function table(): CommandTable {
  return createCommandTable();
}

function asArray(reply: Reply): Reply[] {
  expect(reply.kind).toBe('array');
  return (reply as { kind: 'array'; value: Reply[] }).value;
}

function asInteger(reply: Reply): number {
  expect(reply.kind).toBe('integer');
  return (reply as { kind: 'integer'; value: number }).value;
}

function asBulk(reply: Reply): string | null {
  expect(reply.kind).toBe('bulk');
  return (reply as { kind: 'bulk'; value: string | null }).value;
}

function asStatus(reply: Reply): string {
  expect(reply.kind).toBe('status');
  return (reply as { kind: 'status'; value: string }).value;
}

function asError(reply: Reply): { prefix: string; message: string } {
  expect(reply.kind).toBe('error');
  return reply as { kind: 'error'; prefix: string; message: string };
}

function at(arr: Reply[], index: number): Reply {
  const v = arr[index];
  expect(v).toBeDefined();
  return v as Reply;
}

describe('COMMAND (no subcommand)', () => {
  it('returns an array of command info entries', () => {
    const t = table();
    const result = cmd.command(t);
    const arr = asArray(result);
    expect(arr.length).toBe(t.size);
  });

  it('each entry has 10 elements matching Redis 7 format', () => {
    const t = table();
    const result = cmd.command(t);
    const arr = asArray(result);
    const entry = asArray(at(arr, 0));
    expect(entry.length).toBe(10);
    expect(at(entry, 0).kind).toBe('bulk'); // name
    expect(at(entry, 1).kind).toBe('integer'); // arity
    expect(at(entry, 2).kind).toBe('array'); // flags
    expect(at(entry, 3).kind).toBe('integer'); // firstKey
    expect(at(entry, 4).kind).toBe('integer'); // lastKey
    expect(at(entry, 5).kind).toBe('integer'); // keyStep
    expect(at(entry, 6).kind).toBe('array'); // categories
    expect(at(entry, 7).kind).toBe('array'); // tips
    expect(at(entry, 8).kind).toBe('array'); // key specs
    expect(at(entry, 9).kind).toBe('array'); // subcommands
  });

  it('flags are returned as status replies (matching Redis)', () => {
    const t = table();
    const result = cmd.command(t);
    const arr = asArray(result);
    // Find GET which has flags: readonly, fast
    const getEntry = arr.find((e) => {
      const inner = asArray(e);
      return asBulk(at(inner, 0)) === 'get';
    });
    expect(getEntry).toBeDefined();
    const entry = asArray(getEntry as Reply);
    const flags = asArray(at(entry, 2));
    expect(flags.length).toBeGreaterThan(0);
    for (const flag of flags) {
      expect(flag.kind).toBe('status');
    }
    const flagValues = flags.map((f) => asStatus(f));
    expect(flagValues).toContain('readonly');
    expect(flagValues).toContain('fast');
  });

  it('categories are returned as bulk replies', () => {
    const t = table();
    const result = cmd.command(t);
    const arr = asArray(result);
    const getEntry = arr.find((e) => {
      const inner = asArray(e);
      return asBulk(at(inner, 0)) === 'get';
    });
    expect(getEntry).toBeDefined();
    const entry = asArray(getEntry as Reply);
    const categories = asArray(at(entry, 6));
    expect(categories.length).toBeGreaterThan(0);
    for (const cat of categories) {
      expect(cat.kind).toBe('bulk');
    }
  });

  it('includes correct metadata for GET command', () => {
    const t = table();
    const result = cmd.command(t);
    const arr = asArray(result);
    const getEntry = arr.find((e) => {
      const inner = asArray(e);
      return asBulk(at(inner, 0)) === 'get';
    });
    expect(getEntry).toBeDefined();
    const entry = asArray(getEntry as Reply);
    expect(asBulk(at(entry, 0))).toBe('get');
    expect(asInteger(at(entry, 1))).toBe(2);
    expect(asInteger(at(entry, 3))).toBe(1);
    expect(asInteger(at(entry, 4))).toBe(1);
    expect(asInteger(at(entry, 5))).toBe(1);
  });
});

describe('COMMAND COUNT', () => {
  it('returns the total number of commands', () => {
    const t = table();
    const result = cmd.commandCount(t, []);
    expect(asInteger(result)).toBe(t.size);
  });

  it('rejects extra arguments', () => {
    const t = table();
    const result = cmd.commandCount(t, ['extra']);
    expect(result.kind).toBe('error');
  });
});

describe('COMMAND LIST', () => {
  it('returns all command names with no filter', () => {
    const t = table();
    const result = cmd.commandList(t, []);
    const names = asArray(result).map((r) => asBulk(r));
    expect(names.length).toBe(t.size);
    expect(names).toContain('get');
    expect(names).toContain('set');
    expect(names).toContain('ping');
    expect(names).toContain('command');
  });

  it('filters by FILTERBY ACLCAT', () => {
    const t = table();
    const result = cmd.commandList(t, ['FILTERBY', 'ACLCAT', 'hash']);
    const names = asArray(result).map((r) => asBulk(r));
    expect(names).toContain('hset');
    expect(names).toContain('hget');
    expect(names).not.toContain('get');
  });

  it('filters by FILTERBY PATTERN', () => {
    const t = table();
    const result = cmd.commandList(t, ['FILTERBY', 'PATTERN', 'h*']);
    const names = asArray(result).map((r) => asBulk(r));
    expect(names).toContain('hset');
    expect(names).toContain('hget');
    expect(names).not.toContain('get');
  });

  it('FILTERBY MODULE returns empty array (no modules)', () => {
    const t = table();
    const result = cmd.commandList(t, ['FILTERBY', 'MODULE', 'mymod']);
    const names = asArray(result);
    expect(names.length).toBe(0);
  });

  it('rejects invalid filter type after FILTERBY', () => {
    const t = table();
    const result = cmd.commandList(t, ['FILTERBY', 'INVALID', 'value']);
    expect(result.kind).toBe('error');
    expect(asError(result).message).toBe('syntax error');
  });

  it('rejects missing FILTERBY keyword', () => {
    const t = table();
    const result = cmd.commandList(t, ['ACLCAT', 'hash']);
    expect(result.kind).toBe('error');
    expect(asError(result).message).toBe('syntax error');
  });

  it('rejects FILTERBY without enough args', () => {
    const t = table();
    const result = cmd.commandList(t, ['FILTERBY']);
    expect(result.kind).toBe('error');
    expect(asError(result).message).toBe('syntax error');
  });

  it('rejects FILTERBY with only filter type (no value)', () => {
    const t = table();
    const result = cmd.commandList(t, ['FILTERBY', 'ACLCAT']);
    expect(result.kind).toBe('error');
    expect(asError(result).message).toBe('syntax error');
  });

  it('rejects extra args after FILTERBY value', () => {
    const t = table();
    const result = cmd.commandList(t, ['FILTERBY', 'ACLCAT', 'hash', 'extra']);
    expect(result.kind).toBe('error');
  });

  it('FILTERBY keyword is case-insensitive', () => {
    const t = table();
    const result = cmd.commandList(t, ['filterby', 'ACLCAT', 'hash']);
    const names = asArray(result).map((r) => asBulk(r));
    expect(names).toContain('hset');
  });
});

describe('COMMAND INFO', () => {
  it('returns info for known commands', () => {
    const t = table();
    const result = cmd.commandInfo(t, ['get', 'set']);
    const arr = asArray(result);
    expect(arr.length).toBe(2);

    const getEntry = asArray(at(arr, 0));
    expect(asBulk(at(getEntry, 0))).toBe('get');
    expect(asInteger(at(getEntry, 1))).toBe(2);

    const setEntry = asArray(at(arr, 1));
    expect(asBulk(at(setEntry, 0))).toBe('set');
    expect(asInteger(at(setEntry, 1))).toBe(-3);
  });

  it('returns null for unknown commands', () => {
    const t = table();
    const result = cmd.commandInfo(t, ['nonexistent']);
    const arr = asArray(result);
    expect(arr.length).toBe(1);
    expect(at(arr, 0).kind).toBe('bulk');
    expect(asBulk(at(arr, 0))).toBeNull();
  });

  it('mixes known and unknown', () => {
    const t = table();
    const result = cmd.commandInfo(t, ['get', 'fakecmd', 'ping']);
    const arr = asArray(result);
    expect(arr.length).toBe(3);
    expect(asArray(at(arr, 0))[0]?.kind).toBe('bulk');
    expect(asBulk(at(arr, 1))).toBeNull();
    expect(asArray(at(arr, 2))[0]?.kind).toBe('bulk');
  });

  it('rejects zero arguments', () => {
    const t = table();
    const result = cmd.commandInfo(t, []);
    expect(result.kind).toBe('error');
  });
});

describe('COMMAND DOCS', () => {
  it('returns docs for all commands when no args', () => {
    const t = table();
    const result = cmd.commandDocs(t, []);
    const arr = asArray(result);
    expect(arr.length).toBe(t.size * 2);
  });

  it('returns docs for specific commands', () => {
    const t = table();
    const result = cmd.commandDocs(t, ['get']);
    const arr = asArray(result);
    expect(arr.length).toBe(2);
    expect(asBulk(at(arr, 0))).toBe('get');
    expect(at(arr, 1).kind).toBe('array');
  });

  it('skips unknown commands', () => {
    const t = table();
    const result = cmd.commandDocs(t, ['nonexistent']);
    const arr = asArray(result);
    expect(arr.length).toBe(0);
  });
});

describe('COMMAND GETKEYS', () => {
  it('extracts keys from GET', () => {
    const t = table();
    const result = cmd.commandGetkeys(t, ['get', 'mykey']);
    const keys = asArray(result).map((r) => asBulk(r));
    expect(keys).toEqual(['mykey']);
  });

  it('extracts keys from MSET (key-step=2)', () => {
    const t = table();
    const result = cmd.commandGetkeys(t, ['mset', 'k1', 'v1', 'k2', 'v2']);
    const keys = asArray(result).map((r) => asBulk(r));
    expect(keys).toEqual(['k1', 'k2']);
  });

  it('extracts keys from DEL (variadic)', () => {
    const t = table();
    const result = cmd.commandGetkeys(t, ['del', 'a', 'b', 'c']);
    const keys = asArray(result).map((r) => asBulk(r));
    expect(keys).toEqual(['a', 'b', 'c']);
  });

  it('returns exact Redis error for unknown command', () => {
    const t = table();
    const result = cmd.commandGetkeys(t, ['fakecmd', 'arg1']);
    const err = asError(result);
    expect(err.prefix).toBe('ERR');
    expect(err.message).toBe('Invalid command specified');
  });

  it('returns exact Redis error for command with no keys (PING)', () => {
    const t = table();
    const result = cmd.commandGetkeys(t, ['ping']);
    const err = asError(result);
    expect(err.prefix).toBe('ERR');
    expect(err.message).toBe('The command has no key arguments');
  });

  it('returns exact Redis error with wrong arity', () => {
    const t = table();
    const result = cmd.commandGetkeys(t, ['get']);
    const err = asError(result);
    expect(err.prefix).toBe('ERR');
    expect(err.message).toBe(
      'Invalid number of arguments specified for command'
    );
  });

  it('rejects zero arguments', () => {
    const t = table();
    const result = cmd.commandGetkeys(t, []);
    expect(result.kind).toBe('error');
  });
});

describe('COMMAND HELP', () => {
  it('returns an array of help strings', () => {
    const result = cmd.commandHelp();
    const arr = asArray(result);
    expect(arr.length).toBeGreaterThan(0);
    expect(asBulk(at(arr, 0))).toContain('COMMAND');
  });
});

describe('COMMAND dispatch', () => {
  it('routes to COMMAND (no args)', () => {
    const t = table();
    const result = cmd.commandDispatch(t, []);
    const arr = asArray(result);
    expect(arr.length).toBe(t.size);
  });

  it('routes to COUNT', () => {
    const t = table();
    const result = cmd.commandDispatch(t, ['COUNT']);
    expect(asInteger(result)).toBe(t.size);
  });

  it('routes to LIST', () => {
    const t = table();
    const result = cmd.commandDispatch(t, ['LIST']);
    const names = asArray(result).map((r) => asBulk(r));
    expect(names.length).toBe(t.size);
  });

  it('routes to INFO', () => {
    const t = table();
    const result = cmd.commandDispatch(t, ['INFO', 'get']);
    const arr = asArray(result);
    expect(arr.length).toBe(1);
  });

  it('routes to DOCS', () => {
    const t = table();
    const result = cmd.commandDispatch(t, ['DOCS', 'get']);
    const arr = asArray(result);
    expect(arr.length).toBe(2);
  });

  it('routes to GETKEYS', () => {
    const t = table();
    const result = cmd.commandDispatch(t, ['GETKEYS', 'get', 'mykey']);
    const keys = asArray(result).map((r) => asBulk(r));
    expect(keys).toEqual(['mykey']);
  });

  it('routes to HELP', () => {
    const t = table();
    const result = cmd.commandDispatch(t, ['HELP']);
    const arr = asArray(result);
    expect(arr.length).toBeGreaterThan(0);
  });

  it('returns error for unknown subcommand', () => {
    const t = table();
    const result = cmd.commandDispatch(t, ['INVALID']);
    expect(result.kind).toBe('error');
    expect((result as { kind: 'error'; message: string }).message).toContain(
      'unknown subcommand'
    );
  });

  it('subcommand matching is case-insensitive', () => {
    const t = table();
    const result = cmd.commandDispatch(t, ['count']);
    expect(asInteger(result)).toBe(t.size);
  });
});
