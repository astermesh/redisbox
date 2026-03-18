import { describe, it, expect } from 'vitest';
import * as cmd from './connection.ts';

describe('PING', () => {
  it('returns PONG with no arguments', () => {
    expect(cmd.ping([])).toEqual({ kind: 'status', value: 'PONG' });
  });

  it('returns bulk string with one argument', () => {
    expect(cmd.ping(['hello'])).toEqual({ kind: 'bulk', value: 'hello' });
  });

  it('echoes empty string argument', () => {
    expect(cmd.ping([''])).toEqual({ kind: 'bulk', value: '' });
  });
});

describe('ECHO', () => {
  it('returns the argument as bulk string', () => {
    expect(cmd.echo(['hello'])).toEqual({ kind: 'bulk', value: 'hello' });
  });

  it('returns empty string', () => {
    expect(cmd.echo([''])).toEqual({ kind: 'bulk', value: '' });
  });

  it('returns argument with spaces', () => {
    expect(cmd.echo(['hello world'])).toEqual({
      kind: 'bulk',
      value: 'hello world',
    });
  });
});

describe('QUIT', () => {
  it('returns OK', () => {
    expect(cmd.quit()).toEqual({ kind: 'status', value: 'OK' });
  });
});

describe('RESET', () => {
  it('returns RESET status', () => {
    expect(cmd.reset()).toEqual({ kind: 'status', value: 'RESET' });
  });
});
