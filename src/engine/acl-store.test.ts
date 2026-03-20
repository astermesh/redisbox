import { describe, it, expect } from 'vitest';
import { AclUser, AclStore } from './acl-store.ts';

// ---------------------------------------------------------------------------
// AclUser
// ---------------------------------------------------------------------------

describe('AclUser', () => {
  describe('construction', () => {
    it('stores the username', () => {
      const user = new AclUser('alice');
      expect(user.username).toBe('alice');
    });

    it('defaults to enabled, nopass, all permissions', () => {
      const user = new AclUser('bob');
      expect(user.enabled).toBe(true);
      expect(user.nopass).toBe(true);
      expect(user.allCommands).toBe(true);
      expect(user.allKeys).toBe(true);
      expect(user.allChannels).toBe(true);
    });
  });

  describe('password management', () => {
    it('addPassword stores password and clears nopass', () => {
      const user = new AclUser('u');
      user.addPassword('secret');
      expect(user.nopass).toBe(false);
      expect(user.hasPasswords()).toBe(true);
      expect(user.getPasswords()).toEqual(['secret']);
    });

    it('supports multiple passwords', () => {
      const user = new AclUser('u');
      user.addPassword('p1');
      user.addPassword('p2');
      expect(user.getPasswords()).toEqual(['p1', 'p2']);
    });

    it('removePassword removes a single password', () => {
      const user = new AclUser('u');
      user.addPassword('p1');
      user.addPassword('p2');
      expect(user.removePassword('p1')).toBe(true);
      expect(user.getPasswords()).toEqual(['p2']);
    });

    it('removePassword returns false for non-existent password', () => {
      const user = new AclUser('u');
      user.addPassword('p1');
      expect(user.removePassword('nope')).toBe(false);
    });

    it('clearPasswords removes all passwords', () => {
      const user = new AclUser('u');
      user.addPassword('p1');
      user.addPassword('p2');
      user.clearPasswords();
      expect(user.hasPasswords()).toBe(false);
      expect(user.getPasswords()).toEqual([]);
    });

    it('setNopass clears passwords and sets nopass', () => {
      const user = new AclUser('u');
      user.addPassword('p1');
      user.setNopass();
      expect(user.nopass).toBe(true);
      expect(user.hasPasswords()).toBe(false);
    });

    it('resetPassword replaces all passwords with one', () => {
      const user = new AclUser('u');
      user.addPassword('old1');
      user.addPassword('old2');
      user.resetPassword('new');
      expect(user.getPasswords()).toEqual(['new']);
      expect(user.nopass).toBe(false);
    });
  });

  describe('validatePassword', () => {
    it('accepts any password when nopass is true', () => {
      const user = new AclUser('u');
      expect(user.validatePassword('anything')).toBe(true);
      expect(user.validatePassword('')).toBe(true);
    });

    it('accepts a stored password', () => {
      const user = new AclUser('u');
      user.addPassword('secret');
      expect(user.validatePassword('secret')).toBe(true);
    });

    it('rejects wrong password', () => {
      const user = new AclUser('u');
      user.addPassword('secret');
      expect(user.validatePassword('wrong')).toBe(false);
    });

    it('accepts any of multiple stored passwords', () => {
      const user = new AclUser('u');
      user.addPassword('p1');
      user.addPassword('p2');
      expect(user.validatePassword('p1')).toBe(true);
      expect(user.validatePassword('p2')).toBe(true);
      expect(user.validatePassword('p3')).toBe(false);
    });

    it('rejects empty string when passwords are set', () => {
      const user = new AclUser('u');
      user.addPassword('secret');
      expect(user.validatePassword('')).toBe(false);
    });
  });

  describe('resetToDefaults', () => {
    it('restores user to initial state', () => {
      const user = new AclUser('u');
      user.addPassword('p');
      user.enabled = false;
      user.allCommands = false;
      user.allKeys = false;
      user.allChannels = false;

      user.resetToDefaults();

      expect(user.enabled).toBe(true);
      expect(user.nopass).toBe(true);
      expect(user.hasPasswords()).toBe(false);
      expect(user.allCommands).toBe(true);
      expect(user.allKeys).toBe(true);
      expect(user.allChannels).toBe(true);
    });
  });
});

// ---------------------------------------------------------------------------
// AclStore
// ---------------------------------------------------------------------------

describe('AclStore', () => {
  describe('default user', () => {
    it('creates default user on construction', () => {
      const store = new AclStore();
      expect(store.hasUser('default')).toBe(true);
    });

    it('default user is enabled with nopass and all permissions', () => {
      const store = new AclStore();
      const user = store.getDefaultUser();
      expect(user.username).toBe('default');
      expect(user.enabled).toBe(true);
      expect(user.nopass).toBe(true);
      expect(user.allCommands).toBe(true);
      expect(user.allKeys).toBe(true);
      expect(user.allChannels).toBe(true);
    });

    it('getUser returns default user', () => {
      const store = new AclStore();
      expect(store.getUser('default')).toBe(store.getDefaultUser());
    });
  });

  describe('getUser', () => {
    it('returns undefined for non-existent user', () => {
      const store = new AclStore();
      expect(store.getUser('alice')).toBeUndefined();
    });
  });

  describe('usernames', () => {
    it('returns default user', () => {
      const store = new AclStore();
      expect(store.usernames()).toEqual(['default']);
    });
  });

  describe('syncRequirePass', () => {
    it('sets password on default user when non-empty', () => {
      const store = new AclStore();
      store.syncRequirePass('secret');
      const user = store.getDefaultUser();
      expect(user.nopass).toBe(false);
      expect(user.getPasswords()).toEqual(['secret']);
    });

    it('restores nopass when empty string', () => {
      const store = new AclStore();
      store.syncRequirePass('secret');
      store.syncRequirePass('');
      const user = store.getDefaultUser();
      expect(user.nopass).toBe(true);
      expect(user.hasPasswords()).toBe(false);
    });

    it('replaces previous password', () => {
      const store = new AclStore();
      store.syncRequirePass('old');
      store.syncRequirePass('new');
      const user = store.getDefaultUser();
      expect(user.getPasswords()).toEqual(['new']);
    });
  });

  describe('authenticate', () => {
    it('accepts default user with nopass (any password)', () => {
      const store = new AclStore();
      expect(store.authenticate('default', 'anything')).toBe(true);
    });

    it('accepts default user with correct password after syncRequirePass', () => {
      const store = new AclStore();
      store.syncRequirePass('secret');
      expect(store.authenticate('default', 'secret')).toBe(true);
    });

    it('rejects default user with wrong password', () => {
      const store = new AclStore();
      store.syncRequirePass('secret');
      expect(store.authenticate('default', 'wrong')).toBe(false);
    });

    it('rejects non-existent user', () => {
      const store = new AclStore();
      expect(store.authenticate('nobody', 'pass')).toBe(false);
    });

    it('rejects disabled user even with correct password', () => {
      const store = new AclStore();
      store.syncRequirePass('secret');
      store.getDefaultUser().enabled = false;
      expect(store.authenticate('default', 'secret')).toBe(false);
    });
  });
});
