import { Database } from './database.ts';
import type { EngineDeps } from './types.ts';
import { PubSubManager } from './pubsub-manager.ts';
import { BlockingManager } from './blocking-manager.ts';
import { TimeoutManager } from './timeout-manager.ts';
import { estimateKeyMemory } from './memory.ts';
import { AclStore } from './acl-store.ts';
import { SlowlogManager } from './slowlog.ts';
import { LatencyManager } from './latency.ts';
import { IbiHookManager } from './hooks/ibi.ts';
import { ObiHookManager } from './hooks/obi.ts';

const NUM_DATABASES = 16;

const defaultDeps: EngineDeps = {
  clock: () => Date.now(),
  rng: () => Math.random(),
};

export class RedisEngine {
  readonly databases: Database[];
  readonly clock: () => number;
  readonly rng: () => number;
  readonly pubsub = new PubSubManager();
  readonly blocking = new BlockingManager();
  readonly timeouts: TimeoutManager;
  readonly acl = new AclStore();
  readonly slowlog = new SlowlogManager();
  readonly latency = new LatencyManager();
  readonly ibi = new IbiHookManager();
  readonly obi: ObiHookManager;
  readonly startTime: number;

  constructor(deps?: Partial<EngineDeps>) {
    const resolved = { ...defaultDeps, ...deps };
    this.obi = new ObiHookManager({
      clock: resolved.clock,
      rng: resolved.rng,
    });
    this.clock = () => this.obi.clock();
    this.rng = () => this.obi.rng();
    this.startTime = this.clock();
    this.timeouts = new TimeoutManager(this.blocking, this.clock);

    this.databases = [];
    for (let i = 0; i < NUM_DATABASES; i++) {
      const db = new Database(this.clock);
      db.setRng(this.rng);
      this.databases.push(db);
    }
  }

  db(index: number): Database {
    const d = this.databases[index];
    if (!d) {
      throw new Error(`Database index out of range: ${index}`);
    }
    return d;
  }

  /**
   * Estimate total memory used by all databases in bytes.
   * Iterates all keys and sums per-key memory estimates.
   */
  usedMemory(): number {
    let total = 0;
    for (const db of this.databases) {
      for (const [key, entry] of db.entriesIterator()) {
        const hasExpiry = db.getExpiry(key) !== undefined;
        total += estimateKeyMemory(key, entry, hasExpiry);
      }
    }
    return total;
  }
}
