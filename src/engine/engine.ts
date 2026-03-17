import { Database } from './database.ts';
import type { EngineDeps } from './types.ts';

const NUM_DATABASES = 16;

const defaultDeps: EngineDeps = {
  clock: () => Date.now(),
  rng: () => Math.random(),
};

export class RedisEngine {
  readonly databases: Database[];
  readonly clock: () => number;
  readonly rng: () => number;

  constructor(deps?: Partial<EngineDeps>) {
    const resolved = { ...defaultDeps, ...deps };
    this.clock = resolved.clock;
    this.rng = resolved.rng;

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
}
