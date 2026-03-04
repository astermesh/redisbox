import type { RedisBoxOptions } from './types.ts';

const DEFAULT_OPTIONS: Required<RedisBoxOptions> = {
  mode: 'auto',
  port: 0,
  host: '127.0.0.1',
};

export class RedisBox {
  readonly options: Required<RedisBoxOptions>;

  constructor(options?: RedisBoxOptions) {
    this.options = { ...DEFAULT_OPTIONS, ...options };
  }
}

export function createRedisBox(options?: RedisBoxOptions): RedisBox {
  return new RedisBox(options);
}
