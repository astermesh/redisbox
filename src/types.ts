export interface RedisBoxOptions {
  /** Operating mode: 'proxy' (real Redis subprocess), 'engine' (pure JS), 'auto' */
  mode?: 'proxy' | 'engine' | 'auto';
  /** Port to listen on (0 = random) */
  port?: number;
  /** Host to bind to */
  host?: string;
}
