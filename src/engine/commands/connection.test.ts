import { describe, it, expect } from 'vitest';
import * as cmd from './connection.ts';
import { RedisEngine } from '../engine.ts';
import { ClientState } from '../../server/client-state.ts';
import { ConfigStore } from '../../config-store.ts';
import type { CommandContext } from '../types.ts';

function createCtx(opts?: { clientId?: number; config?: ConfigStore }): {
  ctx: CommandContext;
  client: ClientState;
} {
  const engine = new RedisEngine({ clock: () => 1000 });
  const client = new ClientState(opts?.clientId ?? 42, 500);
  return {
    ctx: {
      db: engine.db(0),
      engine,
      client,
      config: opts?.config,
      acl: engine.acl,
    },
    client,
  };
}

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

  it('rejects more than one argument', () => {
    const result = cmd.ping(['a', 'b']);
    expect(result).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: "wrong number of arguments for 'ping' command",
    });
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

describe('HELLO', () => {
  it('returns server info without arguments', () => {
    const { ctx } = createCtx({ clientId: 7 });
    const result = cmd.hello(ctx, []);
    expect(result).toEqual({
      kind: 'array',
      value: [
        { kind: 'bulk', value: 'server' },
        { kind: 'bulk', value: 'redis' },
        { kind: 'bulk', value: 'version' },
        { kind: 'bulk', value: '7.2.0' },
        { kind: 'bulk', value: 'proto' },
        { kind: 'integer', value: 2 },
        { kind: 'bulk', value: 'id' },
        { kind: 'integer', value: 7 },
        { kind: 'bulk', value: 'mode' },
        { kind: 'bulk', value: 'standalone' },
        { kind: 'bulk', value: 'role' },
        { kind: 'bulk', value: 'master' },
        { kind: 'bulk', value: 'modules' },
        { kind: 'array', value: [] },
      ],
    });
  });

  it('HELLO 2 succeeds and returns proto=2', () => {
    const { ctx } = createCtx({ clientId: 10 });
    const result = cmd.hello(ctx, ['2']);
    expect(result.kind).toBe('array');
    if (result.kind === 'array') {
      expect(result.value[5]).toEqual({ kind: 'integer', value: 2 });
    }
  });

  it('HELLO 3 returns NOPROTO error', () => {
    const { ctx } = createCtx();
    const result = cmd.hello(ctx, ['3']);
    expect(result).toEqual({
      kind: 'error',
      prefix: 'NOPROTO',
      message: 'sorry, this protocol version is not supported',
    });
  });

  it('HELLO with unsupported version (0) returns NOPROTO', () => {
    const { ctx } = createCtx();
    const result = cmd.hello(ctx, ['0']);
    expect(result).toEqual({
      kind: 'error',
      prefix: 'NOPROTO',
      message: 'unsupported protocol version',
    });
  });

  it('HELLO with unsupported version (1) returns NOPROTO', () => {
    const { ctx } = createCtx();
    const result = cmd.hello(ctx, ['1']);
    expect(result).toEqual({
      kind: 'error',
      prefix: 'NOPROTO',
      message: 'unsupported protocol version',
    });
  });

  it('HELLO with non-integer version returns error', () => {
    const { ctx } = createCtx();
    const result = cmd.hello(ctx, ['abc']);
    expect(result).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'Protocol version is not an integer or out of range',
    });
  });

  it('HELLO with AUTH option authenticates and returns server info', () => {
    const config = new ConfigStore();
    config.set('requirepass', 'secret');
    const { ctx, client } = createCtx({ config, clientId: 5 });
    client.authenticated = false;

    const result = cmd.hello(ctx, ['2', 'AUTH', 'default', 'secret']);
    expect(result.kind).toBe('array');
    expect(client.authenticated).toBe(true);
  });

  it('HELLO with AUTH and wrong password returns error', () => {
    const config = new ConfigStore();
    config.set('requirepass', 'secret');
    const { ctx, client } = createCtx({ config, clientId: 5 });
    client.authenticated = false;

    const result = cmd.hello(ctx, ['2', 'AUTH', 'default', 'wrong']);
    expect(result).toEqual({
      kind: 'error',
      prefix: 'WRONGPASS',
      message: 'invalid username-password pair or user is disabled.',
    });
    expect(client.authenticated).toBe(false);
  });

  it('HELLO with AUTH when no password is set succeeds (nopass user)', () => {
    const config = new ConfigStore();
    const { ctx, client } = createCtx({ config, clientId: 5 });
    client.authenticated = false;

    const result = cmd.hello(ctx, ['2', 'AUTH', 'default', 'pass']);
    expect(result.kind).toBe('array');
    expect(client.authenticated).toBe(true);
  });

  it('HELLO with SETNAME option sets client name', () => {
    const { ctx, client } = createCtx({ clientId: 5 });
    const result = cmd.hello(ctx, ['2', 'SETNAME', 'myconn']);
    expect(result.kind).toBe('array');
    expect(client.name).toBe('myconn');
  });

  it('HELLO with both AUTH and SETNAME', () => {
    const config = new ConfigStore();
    config.set('requirepass', 'pass123');
    const { ctx, client } = createCtx({ config, clientId: 5 });
    client.authenticated = false;

    const result = cmd.hello(ctx, [
      '2',
      'AUTH',
      'default',
      'pass123',
      'SETNAME',
      'conn1',
    ]);
    expect(result.kind).toBe('array');
    expect(client.authenticated).toBe(true);
    expect(client.name).toBe('conn1');
  });

  it('HELLO with invalid AUTH still fails even with valid SETNAME', () => {
    const config = new ConfigStore();
    config.set('requirepass', 'pass123');
    const { ctx, client } = createCtx({ config, clientId: 5 });
    client.authenticated = false;

    const result = cmd.hello(ctx, [
      '2',
      'AUTH',
      'default',
      'wrong',
      'SETNAME',
      'conn1',
    ]);
    expect(result.kind).toBe('error');
    expect(client.name).toBe('');
    expect(client.authenticated).toBe(false);
  });

  it('HELLO without client still returns server info (no id)', () => {
    const engine = new RedisEngine({ clock: () => 1000 });
    const ctx: CommandContext = {
      db: engine.db(0),
      engine,
    };
    const result = cmd.hello(ctx, []);
    expect(result.kind).toBe('array');
    if (result.kind === 'array') {
      expect(result.value[7]).toEqual({ kind: 'integer', value: 0 });
    }
  });

  it('HELLO with unknown option returns syntax error', () => {
    const { ctx } = createCtx();
    const result = cmd.hello(ctx, ['2', 'UNKNOWN', 'value']);
    expect(result).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'Unrecognized HELLO option: UNKNOWN',
    });
  });

  it('HELLO AUTH with missing arguments returns syntax error', () => {
    const { ctx } = createCtx();
    const result = cmd.hello(ctx, ['2', 'AUTH', 'user']);
    expect(result).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: "Syntax error in HELLO option 'AUTH'",
    });
  });

  it('HELLO SETNAME with missing argument returns syntax error', () => {
    const { ctx } = createCtx();
    const result = cmd.hello(ctx, ['2', 'SETNAME']);
    expect(result).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: "Syntax error in HELLO option 'SETNAME'",
    });
  });

  it('HELLO with negative version returns NOPROTO', () => {
    const { ctx } = createCtx();
    const result = cmd.hello(ctx, ['-1']);
    expect(result).toEqual({
      kind: 'error',
      prefix: 'NOPROTO',
      message: 'unsupported protocol version',
    });
  });

  it('HELLO with AUTH as first arg returns version parse error', () => {
    const config = new ConfigStore();
    config.set('requirepass', 'secret');
    const { ctx } = createCtx({ config });
    const result = cmd.hello(ctx, ['AUTH', 'default', 'secret']);
    expect(result).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'Protocol version is not an integer or out of range',
    });
  });

  it('HELLO with SETNAME as first arg returns version parse error', () => {
    const { ctx } = createCtx();
    const result = cmd.hello(ctx, ['SETNAME', 'myconn']);
    expect(result).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'Protocol version is not an integer or out of range',
    });
  });

  it('HELLO with empty string arg returns version parse error', () => {
    const { ctx } = createCtx();
    const result = cmd.hello(ctx, ['']);
    expect(result).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'Protocol version is not an integer or out of range',
    });
  });

  it('HELLO with float version returns version parse error', () => {
    const { ctx } = createCtx();
    const result = cmd.hello(ctx, ['2.5']);
    expect(result).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'Protocol version is not an integer or out of range',
    });
  });
});

describe('AUTH', () => {
  it('returns OK with correct password', () => {
    const config = new ConfigStore();
    config.set('requirepass', 'secret');
    const { ctx, client } = createCtx({ config });
    client.authenticated = false;

    const result = cmd.auth(ctx, ['secret']);
    expect(result).toEqual({ kind: 'status', value: 'OK' });
    expect(client.authenticated).toBe(true);
  });

  it('returns WRONGPASS with incorrect password', () => {
    const config = new ConfigStore();
    config.set('requirepass', 'secret');
    const { ctx, client } = createCtx({ config });

    const result = cmd.auth(ctx, ['wrong']);
    expect(result).toEqual({
      kind: 'error',
      prefix: 'WRONGPASS',
      message: 'invalid username-password pair or user is disabled.',
    });
    expect(client.authenticated).toBe(false);
  });

  it('returns error when no password is set', () => {
    const config = new ConfigStore();
    const { ctx } = createCtx({ config });

    const result = cmd.auth(ctx, ['anypass']);
    expect(result).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message:
        'Client sent AUTH, but no password is set. Did you mean ACL SETUSER with >password?',
    });
  });

  it('accepts AUTH with username and password (default user)', () => {
    const config = new ConfigStore();
    config.set('requirepass', 'pass123');
    const { ctx, client } = createCtx({ config });
    client.authenticated = false;

    const result = cmd.auth(ctx, ['default', 'pass123']);
    expect(result).toEqual({ kind: 'status', value: 'OK' });
    expect(client.authenticated).toBe(true);
  });

  it('rejects AUTH with non-default username', () => {
    const config = new ConfigStore();
    config.set('requirepass', 'pass123');
    const { ctx } = createCtx({ config });

    const result = cmd.auth(ctx, ['otheruser', 'pass123']);
    expect(result).toEqual({
      kind: 'error',
      prefix: 'WRONGPASS',
      message: 'invalid username-password pair or user is disabled.',
    });
  });

  it('rejects AUTH with default username but wrong password', () => {
    const config = new ConfigStore();
    config.set('requirepass', 'pass123');
    const { ctx } = createCtx({ config });

    const result = cmd.auth(ctx, ['default', 'wrong']);
    expect(result).toEqual({
      kind: 'error',
      prefix: 'WRONGPASS',
      message: 'invalid username-password pair or user is disabled.',
    });
  });

  it('works without config (no password set)', () => {
    const { ctx } = createCtx();

    const result = cmd.auth(ctx, ['anypass']);
    expect(result).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message:
        'Client sent AUTH, but no password is set. Did you mean ACL SETUSER with >password?',
    });
  });

  it('works without client object', () => {
    const config = new ConfigStore();
    config.set('requirepass', 'secret');
    const engine = new RedisEngine({ clock: () => 1000 });
    const ctx: CommandContext = {
      db: engine.db(0),
      engine,
      config,
      acl: engine.acl,
    };

    const result = cmd.auth(ctx, ['secret']);
    expect(result).toEqual({ kind: 'status', value: 'OK' });
  });

  it('AUTH username password succeeds when user has nopass (2-arg form)', () => {
    const { ctx, client } = createCtx();
    client.authenticated = false;

    // No requirepass → default user has nopass
    // 2-arg form should succeed (real Redis: ACLAuthenticateUser succeeds)
    const result = cmd.auth(ctx, ['default', 'anypass']);
    expect(result).toEqual({ kind: 'status', value: 'OK' });
    expect(client.authenticated).toBe(true);
  });

  it('AUTH password returns no-password error when default user has nopass (1-arg form)', () => {
    const { ctx } = createCtx();

    // No requirepass → default user has nopass
    // 1-arg form should return "no password set" error (real Redis short-circuit)
    const result = cmd.auth(ctx, ['anypass']);
    expect(result).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message:
        'Client sent AUTH, but no password is set. Did you mean ACL SETUSER with >password?',
    });
  });

  it('rejects AUTH when default user is disabled', () => {
    const config = new ConfigStore();
    config.set('requirepass', 'secret');
    const { ctx, client } = createCtx({ config });
    client.authenticated = false;

    // Disable the default user via ACL store
    const acl = ctx.acl;
    if (acl) {
      acl.getDefaultUser().enabled = false;
    }

    const result = cmd.auth(ctx, ['secret']);
    expect(result).toEqual({
      kind: 'error',
      prefix: 'WRONGPASS',
      message: 'invalid username-password pair or user is disabled.',
    });
    expect(client.authenticated).toBe(false);
  });
});
