import { describe, it, expect, beforeEach } from 'vitest';
import { applyRule } from './rules.ts';
import { RedisEngine } from '../../engine.ts';
import type { AclUser } from '../../acl-store.ts';

describe('applyRule', () => {
  let engine: RedisEngine;
  let user: AclUser;

  beforeEach(() => {
    engine = new RedisEngine();
    user = engine.acl.createOrGetUser('testuser');
  });

  describe('basic state rules', () => {
    it('enables user with "on"', () => {
      expect(applyRule(user, 'on')).toBeNull();
      expect(user.enabled).toBe(true);
    });

    it('disables user with "off"', () => {
      user.enabled = true;
      expect(applyRule(user, 'off')).toBeNull();
      expect(user.enabled).toBe(false);
    });
  });

  describe('password rules', () => {
    it('adds password with >password', () => {
      expect(applyRule(user, '>mypass')).toBeNull();
      expect(user.getPasswords()).toContain('mypass');
    });

    it('removes password with <password', () => {
      applyRule(user, '>mypass');
      expect(applyRule(user, '<mypass')).toBeNull();
      expect(user.getPasswords()).not.toContain('mypass');
    });

    it('returns error when removing nonexistent password', () => {
      const err = applyRule(user, '<nope');
      expect(err).toContain('no such password');
    });

    it('sets nopass', () => {
      applyRule(user, '>mypass');
      expect(applyRule(user, 'nopass')).toBeNull();
      expect(user.nopass).toBe(true);
      expect(user.hasPasswords()).toBe(false);
    });

    it('resets passwords with resetpass', () => {
      applyRule(user, '>mypass');
      expect(applyRule(user, 'resetpass')).toBeNull();
      expect(user.nopass).toBe(false);
      expect(user.hasPasswords()).toBe(false);
    });
  });

  describe('command rules', () => {
    it('grants all commands with allcommands', () => {
      expect(applyRule(user, 'allcommands')).toBeNull();
      expect(user.allCommands).toBe(true);
    });

    it('revokes all commands with nocommands', () => {
      user.allCommands = true;
      expect(applyRule(user, 'nocommands')).toBeNull();
      expect(user.allCommands).toBe(false);
    });

    it('grants all commands with +@all', () => {
      expect(applyRule(user, '+@all')).toBeNull();
      expect(user.allCommands).toBe(true);
    });

    it('revokes all commands with -@all', () => {
      user.allCommands = true;
      expect(applyRule(user, '-@all')).toBeNull();
      expect(user.allCommands).toBe(false);
    });

    it('accepts +command without error', () => {
      expect(applyRule(user, '+get')).toBeNull();
    });

    it('accepts -command without error', () => {
      expect(applyRule(user, '-get')).toBeNull();
    });

    it('accepts +@category without error', () => {
      expect(applyRule(user, '+@read')).toBeNull();
    });

    it('accepts -@category without error', () => {
      expect(applyRule(user, '-@write')).toBeNull();
    });
  });

  describe('key rules', () => {
    it('grants all keys with allkeys', () => {
      expect(applyRule(user, 'allkeys')).toBeNull();
      expect(user.allKeys).toBe(true);
    });

    it('resets keys with resetkeys', () => {
      user.allKeys = true;
      expect(applyRule(user, 'resetkeys')).toBeNull();
      expect(user.allKeys).toBe(false);
    });

    it('grants all keys with ~*', () => {
      expect(applyRule(user, '~*')).toBeNull();
      expect(user.allKeys).toBe(true);
    });

    it('accepts ~pattern without error', () => {
      expect(applyRule(user, '~prefix:*')).toBeNull();
    });

    it('accepts %R~ pattern without error', () => {
      expect(applyRule(user, '%R~*')).toBeNull();
    });
  });

  describe('channel rules', () => {
    it('grants all channels with allchannels', () => {
      expect(applyRule(user, 'allchannels')).toBeNull();
      expect(user.allChannels).toBe(true);
    });

    it('resets channels with resetchannels', () => {
      user.allChannels = true;
      expect(applyRule(user, 'resetchannels')).toBeNull();
      expect(user.allChannels).toBe(false);
    });

    it('grants all channels with &*', () => {
      expect(applyRule(user, '&*')).toBeNull();
      expect(user.allChannels).toBe(true);
    });

    it('accepts &pattern without error', () => {
      expect(applyRule(user, '&mychannel')).toBeNull();
    });
  });

  describe('reset rule', () => {
    it('resets user to defaults', () => {
      applyRule(user, 'on');
      applyRule(user, '>pass');
      applyRule(user, 'allcommands');
      applyRule(user, 'allkeys');
      applyRule(user, 'allchannels');

      expect(applyRule(user, 'reset')).toBeNull();
      expect(user.enabled).toBe(false);
      expect(user.nopass).toBe(false);
      expect(user.allCommands).toBe(false);
      expect(user.allKeys).toBe(false);
      expect(user.allChannels).toBe(false);
    });
  });

  describe('hash rules', () => {
    it('accepts #hash without error', () => {
      expect(applyRule(user, '#abc123')).toBeNull();
    });

    it('accepts !hash without error', () => {
      expect(applyRule(user, '!abc123')).toBeNull();
    });
  });

  describe('invalid rules', () => {
    it('returns error for unknown rule', () => {
      const err = applyRule(user, 'invalidrule');
      expect(err).toContain('Syntax error');
    });
  });

  describe('combined rules', () => {
    it('applies multiple rules in sequence', () => {
      expect(applyRule(user, 'on')).toBeNull();
      expect(applyRule(user, '>secret')).toBeNull();
      expect(applyRule(user, '+@all')).toBeNull();
      expect(applyRule(user, '~*')).toBeNull();
      expect(applyRule(user, '&*')).toBeNull();

      expect(user.enabled).toBe(true);
      expect(user.getPasswords()).toContain('secret');
      expect(user.allCommands).toBe(true);
      expect(user.allKeys).toBe(true);
      expect(user.allChannels).toBe(true);
    });

    it('later rules override earlier ones', () => {
      applyRule(user, '+@all');
      applyRule(user, '-@all');
      expect(user.allCommands).toBe(false);

      applyRule(user, '-@all');
      applyRule(user, '+@all');
      expect(user.allCommands).toBe(true);
    });
  });
});
