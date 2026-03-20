import { describe, it, expect, beforeEach } from 'vitest';
import { aclDispatch } from './acl.ts';
import { RedisEngine } from '../engine.ts';
import { createCommandTable } from '../command-registry.ts';
import type { CommandContext } from '../types.ts';
import { ClientState } from '../../server/client-state.ts';

function makeCtx(engine: RedisEngine, client?: ClientState): CommandContext {
  return {
    db: engine.db(0),
    engine,
    client,
    commandTable: createCommandTable(),
    acl: engine.acl,
  };
}

/** Helper: get user or throw (only in tests where we know user exists). */
function mustGetUser(engine: RedisEngine, username: string) {
  const user = engine.acl.getUser(username);
  if (!user) throw new Error(`Expected user '${username}' to exist`);
  return user;
}

describe('ACL commands', () => {
  let engine: RedisEngine;
  let client: ClientState;
  let ctx: CommandContext;

  beforeEach(() => {
    engine = new RedisEngine({ rng: () => 0.5 });
    client = new ClientState(1, engine.clock());
    client.authenticated = true;
    ctx = makeCtx(engine, client);
  });

  // -------------------------------------------------------------------------
  // ACL WHOAMI
  // -------------------------------------------------------------------------

  describe('ACL WHOAMI', () => {
    it('returns default username', () => {
      const reply = aclDispatch(ctx, ['WHOAMI']);
      expect(reply).toEqual({ kind: 'bulk', value: 'default' });
    });

    it('returns authenticated username', () => {
      client.username = 'alice';
      const reply = aclDispatch(ctx, ['WHOAMI']);
      expect(reply).toEqual({ kind: 'bulk', value: 'alice' });
    });

    it('returns default when no client', () => {
      const noClientCtx = makeCtx(engine);
      const reply = aclDispatch(noClientCtx, ['WHOAMI']);
      expect(reply).toEqual({ kind: 'bulk', value: 'default' });
    });
  });

  // -------------------------------------------------------------------------
  // ACL SETUSER
  // -------------------------------------------------------------------------

  describe('ACL SETUSER', () => {
    it('creates a new user with default disabled state', () => {
      const reply = aclDispatch(ctx, ['SETUSER', 'alice']);
      expect(reply).toEqual({ kind: 'status', value: 'OK' });

      const user = mustGetUser(engine, 'alice');
      expect(user.enabled).toBe(false);
      expect(user.nopass).toBe(false);
      expect(user.allCommands).toBe(false);
      expect(user.allKeys).toBe(false);
      expect(user.allChannels).toBe(false);
    });

    it('applies on/off rules', () => {
      aclDispatch(ctx, ['SETUSER', 'alice', 'on']);
      expect(mustGetUser(engine, 'alice').enabled).toBe(true);

      aclDispatch(ctx, ['SETUSER', 'alice', 'off']);
      expect(mustGetUser(engine, 'alice').enabled).toBe(false);
    });

    it('applies password rules', () => {
      aclDispatch(ctx, ['SETUSER', 'alice', 'on', '>pass1', '>pass2']);
      const user = mustGetUser(engine, 'alice');
      expect(user.getPasswords()).toEqual(['pass1', 'pass2']);
      expect(user.nopass).toBe(false);
    });

    it('removes password with <', () => {
      aclDispatch(ctx, ['SETUSER', 'alice', '>pass1', '>pass2']);
      aclDispatch(ctx, ['SETUSER', 'alice', '<pass1']);
      expect(mustGetUser(engine, 'alice').getPasswords()).toEqual(['pass2']);
    });

    it('returns error when removing nonexistent password', () => {
      aclDispatch(ctx, ['SETUSER', 'alice', '>pass1']);
      const reply = aclDispatch(ctx, ['SETUSER', 'alice', '<nope']);
      expect(reply.kind).toBe('error');
    });

    it('applies nopass rule', () => {
      aclDispatch(ctx, ['SETUSER', 'alice', '>pass1', 'nopass']);
      const user = mustGetUser(engine, 'alice');
      expect(user.nopass).toBe(true);
      expect(user.hasPasswords()).toBe(false);
    });

    it('applies resetpass rule', () => {
      aclDispatch(ctx, ['SETUSER', 'alice', '>pass1', 'resetpass']);
      const user = mustGetUser(engine, 'alice');
      expect(user.nopass).toBe(false);
      expect(user.hasPasswords()).toBe(false);
    });

    it('applies allcommands rule', () => {
      aclDispatch(ctx, ['SETUSER', 'alice', 'allcommands']);
      expect(mustGetUser(engine, 'alice').allCommands).toBe(true);
    });

    it('applies +@all rule', () => {
      aclDispatch(ctx, ['SETUSER', 'alice', '+@all']);
      expect(mustGetUser(engine, 'alice').allCommands).toBe(true);
    });

    it('applies nocommands / -@all rule', () => {
      aclDispatch(ctx, ['SETUSER', 'alice', 'allcommands', 'nocommands']);
      expect(mustGetUser(engine, 'alice').allCommands).toBe(false);

      aclDispatch(ctx, ['SETUSER', 'alice', '+@all', '-@all']);
      expect(mustGetUser(engine, 'alice').allCommands).toBe(false);
    });

    it('applies allkeys / ~* rule', () => {
      aclDispatch(ctx, ['SETUSER', 'alice', 'allkeys']);
      expect(mustGetUser(engine, 'alice').allKeys).toBe(true);

      aclDispatch(ctx, ['SETUSER', 'alice', 'resetkeys', '~*']);
      expect(mustGetUser(engine, 'alice').allKeys).toBe(true);
    });

    it('applies allchannels / &* rule', () => {
      aclDispatch(ctx, ['SETUSER', 'alice', 'allchannels']);
      expect(mustGetUser(engine, 'alice').allChannels).toBe(true);

      aclDispatch(ctx, ['SETUSER', 'alice', 'resetchannels', '&*']);
      expect(mustGetUser(engine, 'alice').allChannels).toBe(true);
    });

    it('applies reset rule', () => {
      aclDispatch(ctx, [
        'SETUSER',
        'alice',
        'on',
        '>pass',
        'allcommands',
        'allkeys',
        'allchannels',
      ]);
      aclDispatch(ctx, ['SETUSER', 'alice', 'reset']);
      const user = mustGetUser(engine, 'alice');
      expect(user.enabled).toBe(false);
      expect(user.nopass).toBe(false);
      expect(user.allCommands).toBe(false);
      expect(user.allKeys).toBe(false);
      expect(user.allChannels).toBe(false);
    });

    it('modifies existing default user', () => {
      const reply = aclDispatch(ctx, ['SETUSER', 'default', '>newpass']);
      expect(reply).toEqual({ kind: 'status', value: 'OK' });
      expect(engine.acl.getDefaultUser().getPasswords()).toContain('newpass');
    });

    it('returns error for invalid rule', () => {
      const reply = aclDispatch(ctx, ['SETUSER', 'alice', 'invalidrule']);
      expect(reply.kind).toBe('error');
    });

    it('returns error with no username', () => {
      const reply = aclDispatch(ctx, ['SETUSER']);
      expect(reply.kind).toBe('error');
    });

    it('applies multiple rules atomically', () => {
      aclDispatch(ctx, [
        'SETUSER',
        'alice',
        'on',
        '>secret',
        'allcommands',
        'allkeys',
        '&*',
      ]);
      const user = mustGetUser(engine, 'alice');
      expect(user.enabled).toBe(true);
      expect(user.allCommands).toBe(true);
      expect(user.allKeys).toBe(true);
      expect(user.allChannels).toBe(true);
      expect(user.getPasswords()).toEqual(['secret']);
    });
  });

  // -------------------------------------------------------------------------
  // ACL DELUSER
  // -------------------------------------------------------------------------

  describe('ACL DELUSER', () => {
    it('deletes an existing user', () => {
      aclDispatch(ctx, ['SETUSER', 'alice', 'on']);
      const reply = aclDispatch(ctx, ['DELUSER', 'alice']);
      expect(reply).toEqual({ kind: 'integer', value: 1 });
      expect(engine.acl.getUser('alice')).toBeUndefined();
    });

    it('returns 0 for non-existent user', () => {
      const reply = aclDispatch(ctx, ['DELUSER', 'ghost']);
      expect(reply).toEqual({ kind: 'integer', value: 0 });
    });

    it('cannot delete default user', () => {
      const reply = aclDispatch(ctx, ['DELUSER', 'default']);
      expect(reply.kind).toBe('error');
    });

    it('deletes multiple users', () => {
      aclDispatch(ctx, ['SETUSER', 'alice', 'on']);
      aclDispatch(ctx, ['SETUSER', 'bob', 'on']);
      const reply = aclDispatch(ctx, ['DELUSER', 'alice', 'bob']);
      expect(reply).toEqual({ kind: 'integer', value: 2 });
    });

    it('returns error with no username', () => {
      const reply = aclDispatch(ctx, ['DELUSER']);
      expect(reply.kind).toBe('error');
    });
  });

  // -------------------------------------------------------------------------
  // ACL GETUSER
  // -------------------------------------------------------------------------

  describe('ACL GETUSER', () => {
    it('returns nil for non-existent user', () => {
      const reply = aclDispatch(ctx, ['GETUSER', 'ghost']);
      expect(reply).toEqual({ kind: 'bulk', value: null });
    });

    it('returns default user info', () => {
      const reply = aclDispatch(ctx, ['GETUSER', 'default']);
      expect(reply.kind).toBe('array');
      if (reply.kind !== 'array') return;

      // flags
      expect(reply.value[0]).toEqual({ kind: 'bulk', value: 'flags' });
      const flags = reply.value[1];
      expect(flags).toBeDefined();
      if (flags?.kind === 'array') {
        const flagValues = flags.value.map(
          (f) => (f as { value: string }).value
        );
        expect(flagValues).toContain('on');
        expect(flagValues).toContain('nopass');
        expect(flagValues).toContain('allkeys');
        expect(flagValues).toContain('allcommands');
        expect(flagValues).toContain('allchannels');
      }

      // commands
      expect(reply.value[4]).toEqual({ kind: 'bulk', value: 'commands' });
      expect(reply.value[5]).toEqual({ kind: 'bulk', value: '+@all' });
    });

    it('returns user with password hashes', () => {
      aclDispatch(ctx, ['SETUSER', 'alice', 'on', '>mypassword']);
      const reply = aclDispatch(ctx, ['GETUSER', 'alice']);
      expect(reply.kind).toBe('array');
      if (reply.kind !== 'array') return;

      // passwords field should contain SHA256 hashes
      const passwords = reply.value[3];
      if (passwords?.kind === 'array') {
        expect(passwords.value.length).toBe(1);
        const hash = (passwords.value[0] as { value: string }).value;
        expect(hash.length).toBe(64); // SHA256 hex = 64 chars
      }
    });

    it('returns selectors as empty array', () => {
      const reply = aclDispatch(ctx, ['GETUSER', 'default']);
      if (reply.kind !== 'array') return;
      expect(reply.value[10]).toEqual({ kind: 'bulk', value: 'selectors' });
      expect(reply.value[11]).toEqual({ kind: 'array', value: [] });
    });
  });

  // -------------------------------------------------------------------------
  // ACL LIST
  // -------------------------------------------------------------------------

  describe('ACL LIST', () => {
    it('returns default user entry', () => {
      const reply = aclDispatch(ctx, ['LIST']);
      expect(reply.kind).toBe('array');
      if (reply.kind !== 'array') return;
      expect(reply.value.length).toBeGreaterThanOrEqual(1);
      const entry = (reply.value[0] as { value: string }).value;
      expect(entry).toContain('user default on');
      expect(entry).toContain('nopass');
      expect(entry).toContain('~*');
      expect(entry).toContain('&*');
      expect(entry).toContain('+@all');
    });

    it('includes created users', () => {
      aclDispatch(ctx, [
        'SETUSER',
        'alice',
        'on',
        '>pass',
        '~*',
        '&*',
        '+@all',
      ]);
      const reply = aclDispatch(ctx, ['LIST']);
      if (reply.kind !== 'array') return;
      expect(reply.value.length).toBe(2);
      const aliceEntry = (reply.value[1] as { value: string }).value;
      expect(aliceEntry).toContain('user alice on');
    });
  });

  // -------------------------------------------------------------------------
  // ACL CAT
  // -------------------------------------------------------------------------

  describe('ACL CAT', () => {
    it('returns all categories without arguments', () => {
      const reply = aclDispatch(ctx, ['CAT']);
      expect(reply.kind).toBe('array');
      if (reply.kind !== 'array') return;
      const cats = reply.value.map((r) => (r as { value: string }).value);
      expect(cats).toContain('keyspace');
      expect(cats).toContain('read');
      expect(cats).toContain('write');
      expect(cats).toContain('string');
      expect(cats).toContain('hash');
      expect(cats).toContain('list');
      expect(cats).toContain('set');
      expect(cats).toContain('sortedset');
      expect(cats).toContain('admin');
      expect(cats).toContain('fast');
      expect(cats).toContain('slow');
      expect(cats).toContain('connection');
      expect(cats).toContain('transaction');
      expect(cats).toContain('pubsub');
      expect(cats).toContain('dangerous');
      expect(cats).toContain('bitmap');
      expect(cats).toContain('stream');
      expect(cats).toContain('generic');
      expect(cats).toContain('hyperloglog');
      expect(cats).toContain('geo');
      expect(cats).toContain('blocking');
      expect(cats).toContain('scripting');
    });

    it('returns sorted categories', () => {
      const reply = aclDispatch(ctx, ['CAT']);
      if (reply.kind !== 'array') return;
      const cats = reply.value.map((r) => (r as { value: string }).value);
      const sorted = [...cats].sort();
      expect(cats).toEqual(sorted);
    });

    it('returns commands for a category', () => {
      const reply = aclDispatch(ctx, ['CAT', 'string']);
      expect(reply.kind).toBe('array');
      if (reply.kind !== 'array') return;
      const cmds = reply.value.map((r) => (r as { value: string }).value);
      expect(cmds).toContain('get');
      expect(cmds).toContain('set');
    });

    it('returns commands for connection category', () => {
      const reply = aclDispatch(ctx, ['CAT', 'connection']);
      if (reply.kind !== 'array') return;
      const cmds = reply.value.map((r) => (r as { value: string }).value);
      expect(cmds).toContain('ping');
      expect(cmds).toContain('auth');
    });

    it('returns error for unknown category', () => {
      const reply = aclDispatch(ctx, ['CAT', 'nonexistent']);
      expect(reply.kind).toBe('error');
    });

    it('is case-insensitive', () => {
      const reply = aclDispatch(ctx, ['CAT', 'STRING']);
      expect(reply.kind).toBe('array');
    });
  });

  // -------------------------------------------------------------------------
  // ACL LOG
  // -------------------------------------------------------------------------

  describe('ACL LOG', () => {
    it('returns empty array when no log entries', () => {
      const reply = aclDispatch(ctx, ['LOG']);
      expect(reply).toEqual({ kind: 'array', value: [] });
    });

    it('returns log entries', () => {
      engine.acl.addLogEntry('auth', 'toplevel', 'AUTH', 'alice', '', 1000);
      const reply = aclDispatch(ctx, ['LOG']);
      expect(reply.kind).toBe('array');
      if (reply.kind !== 'array') return;
      expect(reply.value.length).toBe(1);
    });

    it('returns limited entries with count', () => {
      engine.acl.addLogEntry('auth', 'toplevel', 'AUTH', 'a', '', 1000);
      engine.acl.addLogEntry('command', 'toplevel', 'GET', 'b', '', 2000);
      const reply = aclDispatch(ctx, ['LOG', '1']);
      if (reply.kind !== 'array') return;
      expect(reply.value.length).toBe(1);
    });

    it('resets log with RESET', () => {
      engine.acl.addLogEntry('auth', 'toplevel', 'AUTH', 'a', '', 1000);
      const reply = aclDispatch(ctx, ['LOG', 'RESET']);
      expect(reply).toEqual({ kind: 'status', value: 'OK' });
      expect(engine.acl.getLog()).toEqual([]);
    });

    it('returns error for invalid count', () => {
      const reply = aclDispatch(ctx, ['LOG', '-1']);
      expect(reply.kind).toBe('error');
    });
  });

  // -------------------------------------------------------------------------
  // ACL GENPASS
  // -------------------------------------------------------------------------

  describe('ACL GENPASS', () => {
    it('returns 64-char hex string by default (256 bits)', () => {
      const reply = aclDispatch(ctx, ['GENPASS']);
      expect(reply.kind).toBe('bulk');
      if (reply.kind !== 'bulk') return;
      expect(reply.value).toHaveLength(64);
      expect(reply.value).toMatch(/^[0-9a-f]+$/);
    });

    it('returns custom bit length', () => {
      const reply = aclDispatch(ctx, ['GENPASS', '128']);
      expect(reply.kind).toBe('bulk');
      if (reply.kind !== 'bulk') return;
      expect(reply.value).toHaveLength(32); // 128/4 = 32 hex chars
    });

    it('returns error for invalid bits', () => {
      expect(aclDispatch(ctx, ['GENPASS', '0']).kind).toBe('error');
      expect(aclDispatch(ctx, ['GENPASS', '6145']).kind).toBe('error');
      expect(aclDispatch(ctx, ['GENPASS', 'abc']).kind).toBe('error');
    });
  });

  // -------------------------------------------------------------------------
  // ACL LOAD / ACL SAVE (stubs)
  // -------------------------------------------------------------------------

  describe('ACL LOAD', () => {
    it('returns error about no ACL file', () => {
      const reply = aclDispatch(ctx, ['LOAD']);
      expect(reply.kind).toBe('error');
    });
  });

  describe('ACL SAVE', () => {
    it('returns error about no ACL file', () => {
      const reply = aclDispatch(ctx, ['SAVE']);
      expect(reply.kind).toBe('error');
    });
  });

  // -------------------------------------------------------------------------
  // ACL DRYRUN
  // -------------------------------------------------------------------------

  describe('ACL DRYRUN', () => {
    it('returns OK for user with all permissions', () => {
      const reply = aclDispatch(ctx, ['DRYRUN', 'default', 'get', 'mykey']);
      expect(reply).toEqual({ kind: 'status', value: 'OK' });
    });

    it('returns error for non-existent user', () => {
      const reply = aclDispatch(ctx, ['DRYRUN', 'ghost', 'get']);
      expect(reply.kind).toBe('error');
    });

    it('returns permission denied for user without commands', () => {
      aclDispatch(ctx, ['SETUSER', 'limited', 'on', '>pass']);
      const reply = aclDispatch(ctx, ['DRYRUN', 'limited', 'get', 'key']);
      expect(reply.kind).toBe('bulk');
      if (reply.kind !== 'bulk') return;
      expect(reply.value).toContain('no permissions');
    });

    it('returns OK for user with allcommands and allkeys', () => {
      aclDispatch(ctx, [
        'SETUSER',
        'alice',
        'on',
        '>pass',
        'allcommands',
        'allkeys',
        '&*',
      ]);
      const reply = aclDispatch(ctx, ['DRYRUN', 'alice', 'get', 'mykey']);
      expect(reply).toEqual({ kind: 'status', value: 'OK' });
    });

    it('returns error with insufficient args', () => {
      const reply = aclDispatch(ctx, ['DRYRUN', 'default']);
      expect(reply.kind).toBe('error');
    });
  });

  // -------------------------------------------------------------------------
  // ACL HELP
  // -------------------------------------------------------------------------

  describe('ACL HELP', () => {
    it('returns array of help strings', () => {
      const reply = aclDispatch(ctx, ['HELP']);
      expect(reply.kind).toBe('array');
      if (reply.kind !== 'array') return;
      expect(reply.value.length).toBeGreaterThan(0);
    });
  });

  // -------------------------------------------------------------------------
  // Unknown subcommand
  // -------------------------------------------------------------------------

  describe('unknown subcommand', () => {
    it('returns error for unknown subcommand', () => {
      const reply = aclDispatch(ctx, ['NOSUCH']);
      expect(reply.kind).toBe('error');
    });
  });
});
