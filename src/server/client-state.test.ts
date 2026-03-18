import { describe, it, expect } from 'vitest';
import { ClientState, ClientStateStore } from './client-state.ts';

describe('ClientState', () => {
  describe('initial state', () => {
    it('has correct defaults', () => {
      const state = new ClientState(42, 1000);

      expect(state.id).toBe(42);
      expect(state.dbIndex).toBe(0);
      expect(state.name).toBe('');
      expect(state.createdAt).toBe(1000);
      expect(state.lastCommandTime).toBe(0);
      expect(state.lastCommand).toBe('');
    });

    it('has all flags set to false initially', () => {
      const state = new ClientState(1, 0);

      expect(state.flagMulti).toBe(false);
      expect(state.flagBlocked).toBe(false);
      expect(state.flagSubscribed).toBe(false);
    });
  });

  describe('database selection', () => {
    it('allows changing selected database', () => {
      const state = new ClientState(1, 0);
      state.dbIndex = 5;
      expect(state.dbIndex).toBe(5);
    });
  });

  describe('name', () => {
    it('allows setting and getting client name', () => {
      const state = new ClientState(1, 0);
      state.name = 'my-client';
      expect(state.name).toBe('my-client');
    });
  });

  describe('flags', () => {
    it('allows setting multi flag', () => {
      const state = new ClientState(1, 0);
      state.flagMulti = true;
      expect(state.flagMulti).toBe(true);
    });

    it('allows setting blocked flag', () => {
      const state = new ClientState(1, 0);
      state.flagBlocked = true;
      expect(state.flagBlocked).toBe(true);
    });

    it('allows setting subscribed flag', () => {
      const state = new ClientState(1, 0);
      state.flagSubscribed = true;
      expect(state.flagSubscribed).toBe(true);
    });

    it('flags are independent', () => {
      const state = new ClientState(1, 0);
      state.flagMulti = true;
      state.flagBlocked = true;

      expect(state.flagMulti).toBe(true);
      expect(state.flagBlocked).toBe(true);
      expect(state.flagSubscribed).toBe(false);

      state.flagMulti = false;
      expect(state.flagMulti).toBe(false);
      expect(state.flagBlocked).toBe(true);
    });
  });

  describe('last command tracking', () => {
    it('tracks last command and time', () => {
      const state = new ClientState(1, 0);
      state.lastCommand = 'GET';
      state.lastCommandTime = 5000;

      expect(state.lastCommand).toBe('GET');
      expect(state.lastCommandTime).toBe(5000);
    });
  });

  describe('flagsString', () => {
    it('returns N for no special flags', () => {
      const state = new ClientState(1, 0);
      expect(state.flagsString()).toBe('N');
    });

    it('returns x for multi', () => {
      const state = new ClientState(1, 0);
      state.flagMulti = true;
      expect(state.flagsString()).toBe('x');
    });

    it('returns b for blocked', () => {
      const state = new ClientState(1, 0);
      state.flagBlocked = true;
      expect(state.flagsString()).toBe('b');
    });

    it('returns P for subscribed', () => {
      const state = new ClientState(1, 0);
      state.flagSubscribed = true;
      expect(state.flagsString()).toBe('P');
    });

    it('combines multiple flags', () => {
      const state = new ClientState(1, 0);
      state.flagMulti = true;
      state.flagSubscribed = true;
      expect(state.flagsString()).toBe('xP');
    });

    it('combines all flags', () => {
      const state = new ClientState(1, 0);
      state.flagMulti = true;
      state.flagBlocked = true;
      state.flagSubscribed = true;
      expect(state.flagsString()).toBe('xbP');
    });
  });
});

describe('ClientStateStore', () => {
  describe('create and get', () => {
    it('creates client state with given id and timestamp', () => {
      const store = new ClientStateStore();
      const state = store.create(1, 1000);

      expect(state.id).toBe(1);
      expect(state.createdAt).toBe(1000);
    });

    it('retrieves created state by id', () => {
      const store = new ClientStateStore();
      const created = store.create(1, 1000);
      const retrieved = store.get(1);

      expect(retrieved).toBe(created);
    });

    it('returns undefined for unknown id', () => {
      const store = new ClientStateStore();
      expect(store.get(999)).toBeUndefined();
    });
  });

  describe('remove', () => {
    it('removes client state by id', () => {
      const store = new ClientStateStore();
      store.create(1, 1000);
      store.remove(1);

      expect(store.get(1)).toBeUndefined();
    });

    it('returns true when removing existing client', () => {
      const store = new ClientStateStore();
      store.create(1, 1000);
      expect(store.remove(1)).toBe(true);
    });

    it('returns false when removing non-existent client', () => {
      const store = new ClientStateStore();
      expect(store.remove(999)).toBe(false);
    });
  });

  describe('has', () => {
    it('returns true for existing client', () => {
      const store = new ClientStateStore();
      store.create(1, 1000);
      expect(store.has(1)).toBe(true);
    });

    it('returns false for non-existent client', () => {
      const store = new ClientStateStore();
      expect(store.has(999)).toBe(false);
    });

    it('returns false after removal', () => {
      const store = new ClientStateStore();
      store.create(1, 1000);
      store.remove(1);
      expect(store.has(1)).toBe(false);
    });
  });

  describe('size', () => {
    it('returns 0 for empty store', () => {
      const store = new ClientStateStore();
      expect(store.size).toBe(0);
    });

    it('tracks number of clients', () => {
      const store = new ClientStateStore();
      store.create(1, 1000);
      store.create(2, 2000);
      store.create(3, 3000);
      expect(store.size).toBe(3);
    });

    it('decrements on removal', () => {
      const store = new ClientStateStore();
      store.create(1, 1000);
      store.create(2, 2000);
      store.remove(1);
      expect(store.size).toBe(1);
    });
  });

  describe('all', () => {
    it('returns empty iterator for empty store', () => {
      const store = new ClientStateStore();
      expect([...store.all()]).toEqual([]);
    });

    it('iterates over all client states', () => {
      const store = new ClientStateStore();
      store.create(1, 1000);
      store.create(2, 2000);

      const all = [...store.all()];
      expect(all.length).toBe(2);
      expect(all.map((s) => s.id).sort()).toEqual([1, 2]);
    });
  });

  describe('multiple clients', () => {
    it('manages multiple independent clients', () => {
      const store = new ClientStateStore();
      const c1 = store.create(1, 1000);
      const c2 = store.create(2, 2000);

      c1.dbIndex = 3;
      c2.dbIndex = 7;
      c1.name = 'client-1';

      const r1 = store.get(1);
      const r2 = store.get(2);
      expect(r1).toBeDefined();
      expect(r2).toBeDefined();
      expect(r1?.dbIndex).toBe(3);
      expect(r2?.dbIndex).toBe(7);
      expect(r1?.name).toBe('client-1');
      expect(r2?.name).toBe('');
    });
  });
});
