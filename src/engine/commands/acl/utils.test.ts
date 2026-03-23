import { describe, it, expect, beforeEach } from 'vitest';
import {
  ACL_CATEGORIES,
  userFlags,
  userPasswordHashes,
  userCommandsString,
  userKeysString,
  userChannelsString,
  formatUserForList,
  makeSubSpec,
} from './utils.ts';
import { RedisEngine } from '../../engine.ts';
import type { AclUser } from '../../acl-store.ts';
import { OK } from '../../types.ts';

describe('ACL utils', () => {
  let engine: RedisEngine;
  let user: AclUser;

  beforeEach(() => {
    engine = new RedisEngine();
    user = engine.acl.createOrGetUser('alice');
  });

  describe('ACL_CATEGORIES', () => {
    it('is sorted', () => {
      const sorted = [...ACL_CATEGORIES].sort();
      expect(ACL_CATEGORIES).toEqual(sorted);
    });

    it('contains expected categories', () => {
      expect(ACL_CATEGORIES).toContain('keyspace');
      expect(ACL_CATEGORIES).toContain('read');
      expect(ACL_CATEGORIES).toContain('write');
      expect(ACL_CATEGORIES).toContain('string');
      expect(ACL_CATEGORIES).toContain('admin');
      expect(ACL_CATEGORIES).toContain('scripting');
    });
  });

  describe('userFlags', () => {
    it('returns off for disabled user', () => {
      const reply = userFlags(user);
      expect(reply.kind).toBe('array');
      if (reply.kind !== 'array') return;
      const values = reply.value.map((f) => (f as { value: string }).value);
      expect(values).toContain('off');
      expect(values).not.toContain('on');
    });

    it('returns on for enabled user', () => {
      user.enabled = true;
      const reply = userFlags(user);
      if (reply.kind !== 'array') return;
      const values = reply.value.map((f) => (f as { value: string }).value);
      expect(values).toContain('on');
    });

    it('includes allkeys when set', () => {
      user.allKeys = true;
      const reply = userFlags(user);
      if (reply.kind !== 'array') return;
      const values = reply.value.map((f) => (f as { value: string }).value);
      expect(values).toContain('allkeys');
    });

    it('includes allchannels when set', () => {
      user.allChannels = true;
      const reply = userFlags(user);
      if (reply.kind !== 'array') return;
      const values = reply.value.map((f) => (f as { value: string }).value);
      expect(values).toContain('allchannels');
    });

    it('includes allcommands when set', () => {
      user.allCommands = true;
      const reply = userFlags(user);
      if (reply.kind !== 'array') return;
      const values = reply.value.map((f) => (f as { value: string }).value);
      expect(values).toContain('allcommands');
    });

    it('includes nopass when set', () => {
      user.setNopass();
      const reply = userFlags(user);
      if (reply.kind !== 'array') return;
      const values = reply.value.map((f) => (f as { value: string }).value);
      expect(values).toContain('nopass');
    });
  });

  describe('userPasswordHashes', () => {
    it('returns empty array for user without passwords', () => {
      const reply = userPasswordHashes(user);
      expect(reply).toEqual({ kind: 'array', value: [] });
    });

    it('returns #-prefixed SHA256 hashes', () => {
      user.addPassword('secret');
      const reply = userPasswordHashes(user);
      if (reply.kind !== 'array') return;
      expect(reply.value.length).toBe(1);
      const hash = (reply.value[0] as { value: string }).value;
      expect(hash).toMatch(/^#[0-9a-f]{64}$/);
    });

    it('returns multiple hashes for multiple passwords', () => {
      user.addPassword('pass1');
      user.addPassword('pass2');
      const reply = userPasswordHashes(user);
      if (reply.kind !== 'array') return;
      expect(reply.value.length).toBe(2);
    });
  });

  describe('userCommandsString', () => {
    it('returns +@all when allCommands is true', () => {
      user.allCommands = true;
      expect(userCommandsString(user)).toBe('+@all');
    });

    it('returns -@all when allCommands is false', () => {
      expect(userCommandsString(user)).toBe('-@all');
    });
  });

  describe('userKeysString', () => {
    it('returns ~* when allKeys is true', () => {
      user.allKeys = true;
      expect(userKeysString(user)).toBe('~*');
    });

    it('returns empty string when allKeys is false', () => {
      expect(userKeysString(user)).toBe('');
    });
  });

  describe('userChannelsString', () => {
    it('returns &* when allChannels is true', () => {
      user.allChannels = true;
      expect(userChannelsString(user)).toBe('&*');
    });

    it('returns empty string when allChannels is false', () => {
      expect(userChannelsString(user)).toBe('');
    });
  });

  describe('formatUserForList', () => {
    it('formats disabled user with no passwords', () => {
      const result = formatUserForList(user);
      expect(result).toBe(
        'user alice off resetpass resetkeys resetchannels -@all'
      );
    });

    it('formats enabled user with nopass and full permissions', () => {
      user.enabled = true;
      user.setNopass();
      user.allCommands = true;
      user.allKeys = true;
      user.allChannels = true;
      const result = formatUserForList(user);
      expect(result).toBe('user alice on nopass ~* &* +@all');
    });

    it('formats user with password hashes', () => {
      user.enabled = true;
      user.addPassword('secret');
      const result = formatUserForList(user);
      expect(result).toMatch(
        /^user alice on #[0-9a-f]{64} resetkeys resetchannels -@all$/
      );
    });

    it('formats user with multiple passwords', () => {
      user.addPassword('pass1');
      user.addPassword('pass2');
      const result = formatUserForList(user);
      const hashes = result.match(/#[0-9a-f]{64}/g);
      expect(hashes).toHaveLength(2);
    });
  });

  describe('makeSubSpec', () => {
    it('creates a CommandSpec with given name, handler, and arity', () => {
      const handler = () => OK;
      const spec = makeSubSpec('setuser', handler, -3);
      expect(spec.name).toBe('setuser');
      expect(spec.handler).toBe(handler);
      expect(spec.arity).toBe(-3);
    });

    it('sets admin flags and categories', () => {
      const spec = makeSubSpec('test', () => OK, 2);
      expect(spec.flags).toEqual(['admin', 'loading', 'stale']);
      expect(spec.categories).toEqual(['@admin', '@slow', '@dangerous']);
    });

    it('sets key indices to 0', () => {
      const spec = makeSubSpec('test', () => OK, 2);
      expect(spec.firstKey).toBe(0);
      expect(spec.lastKey).toBe(0);
      expect(spec.keyStep).toBe(0);
    });
  });
});
