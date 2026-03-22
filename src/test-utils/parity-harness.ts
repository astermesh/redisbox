/**
 * Dual-backend test harness for parity verification.
 *
 * Runs the same Redis commands against both RedisBox (in-process TCP
 * server) and a real Redis instance, then compares responses to verify
 * exact behavioral parity.
 */

import { TcpServer } from '../server/tcp-server.ts';
import { ClientConnection } from '../server/client-connection.ts';
import { ClientStateStore } from '../server/client-state.ts';
import { RedisEngine } from '../engine/engine.ts';
import { createCommandTable } from '../engine/command-registry.ts';
import { CommandDispatcher } from '../engine/command-dispatcher.ts';
import { ConfigStore } from '../config-store.ts';
import { RespClient, canConnect } from './resp-client.ts';
import type { RespValue } from '../resp/types.ts';

const DEFAULT_REDIS_HOST = '127.0.0.1';
const DEFAULT_REDIS_PORT = 6379;

export interface ParityHarnessOptions {
  /** Real Redis host (default: 127.0.0.1) */
  redisHost?: string;
  /** Real Redis port (default: 6379) */
  redisPort?: number;
}

export interface SideEffects {
  exists: RespValue;
  type: RespValue;
  ttl: RespValue;
  encoding: RespValue;
}

export class ParityHarness {
  /** RedisBox client */
  readonly box: RespClient;
  /** Real Redis client */
  readonly redis: RespClient;

  private server: TcpServer | null = null;
  private engine: RedisEngine | null = null;

  private constructor(box: RespClient, redis: RespClient) {
    this.box = box;
    this.redis = redis;
  }

  /**
   * Create and start a parity harness.
   *
   * Starts a RedisBox TCP server on a random port and connects clients
   * to both RedisBox and the real Redis instance. Returns null if real
   * Redis is not available.
   */
  static async create(
    options?: ParityHarnessOptions
  ): Promise<ParityHarness | null> {
    const redisHost = options?.redisHost ?? DEFAULT_REDIS_HOST;
    const redisPort = options?.redisPort ?? DEFAULT_REDIS_PORT;

    // Check if real Redis is available
    const available = await canConnect(redisHost, redisPort);
    if (!available) {
      return null;
    }

    // Start RedisBox TCP server
    const engine = new RedisEngine();
    const table = createCommandTable();
    const dispatcher = new CommandDispatcher(table);
    const clientStore = new ClientStateStore();
    const config = new ConfigStore();

    const server = new TcpServer({ port: 0, host: '127.0.0.1' });
    server.on('connection', (id, socket) => {
      const clientState = clientStore.create(id, Date.now());
      new ClientConnection({
        socket,
        clientState,
        clientStore,
        engine,
        dispatcher,
        config,
      });
    });

    await server.start();

    // Connect clients
    const boxClient = new RespClient({
      host: '127.0.0.1',
      port: server.port,
    });
    const redisClient = new RespClient({ host: redisHost, port: redisPort });

    await boxClient.connect();
    await redisClient.connect();

    const harness = new ParityHarness(boxClient, redisClient);
    harness.server = server;
    harness.engine = engine;

    return harness;
  }

  /**
   * Flush both backends (FLUSHALL) for test isolation.
   */
  async flush(): Promise<void> {
    await Promise.all([
      this.box.command('FLUSHALL'),
      this.redis.command('FLUSHALL'),
    ]);
  }

  /**
   * Tear down the harness: disconnect clients and stop the server.
   */
  async teardown(): Promise<void> {
    await Promise.all([this.box.disconnect(), this.redis.disconnect()]);
    if (this.server) {
      await this.server.stop();
      this.server = null;
    }
    this.engine = null;
  }

  /**
   * Execute a command on both backends and return both responses.
   */
  async exec(
    cmd: string,
    ...args: string[]
  ): Promise<{ box: RespValue; redis: RespValue }> {
    const [boxReply, redisReply] = await Promise.all([
      this.box.command(cmd, ...args),
      this.redis.command(cmd, ...args),
    ]);
    return { box: boxReply, redis: redisReply };
  }

  /**
   * Execute a command on both backends and assert identical responses.
   *
   * Performs exact comparison — suitable for deterministic commands.
   * Returns the response from both backends.
   */
  async compareCommand(
    cmd: string,
    ...args: string[]
  ): Promise<{ box: RespValue; redis: RespValue }> {
    const result = await this.exec(cmd, ...args);
    assertRespEqual(result.box, result.redis, `${cmd} ${args.join(' ')}`);
    return result;
  }

  /**
   * Execute a command on both backends and assert responses are identical
   * when treated as unordered collections.
   *
   * Suitable for commands like SMEMBERS, KEYS, HGETALL where element
   * order is not guaranteed.
   */
  async compareUnordered(
    cmd: string,
    ...args: string[]
  ): Promise<{ box: RespValue; redis: RespValue }> {
    const result = await this.exec(cmd, ...args);
    assertRespUnordered(result.box, result.redis, `${cmd} ${args.join(' ')}`);
    return result;
  }

  /**
   * Execute a command on both backends and assert responses have the
   * same type and structure, but not necessarily the same values.
   *
   * Suitable for non-deterministic commands like RANDOMKEY, SRANDMEMBER
   * with negative count.
   */
  async compareStructure(
    cmd: string,
    ...args: string[]
  ): Promise<{ box: RespValue; redis: RespValue }> {
    const result = await this.exec(cmd, ...args);
    assertRespStructure(result.box, result.redis, `${cmd} ${args.join(' ')}`);
    return result;
  }

  /**
   * Compare side effects of a key on both backends.
   *
   * Checks EXISTS, TYPE, TTL, and OBJECT ENCODING to verify that the
   * key state is identical after a mutation.
   */
  async compareSideEffects(key: string): Promise<{
    box: SideEffects;
    redis: SideEffects;
  }> {
    const [boxEffects, redisEffects] = await Promise.all([
      this.getSideEffects(this.box, key),
      this.getSideEffects(this.redis, key),
    ]);

    assertRespEqual(
      boxEffects.exists,
      redisEffects.exists,
      `side-effect EXISTS ${key}`
    );
    assertRespEqual(
      boxEffects.type,
      redisEffects.type,
      `side-effect TYPE ${key}`
    );
    assertRespEqual(boxEffects.ttl, redisEffects.ttl, `side-effect TTL ${key}`);
    assertRespEqual(
      boxEffects.encoding,
      redisEffects.encoding,
      `side-effect OBJECT ENCODING ${key}`
    );

    return { box: boxEffects, redis: redisEffects };
  }

  private async getSideEffects(
    client: RespClient,
    key: string
  ): Promise<SideEffects> {
    const [exists, type, ttl, encoding] = await Promise.all([
      client.command('EXISTS', key),
      client.command('TYPE', key),
      client.command('TTL', key),
      client.command('OBJECT', 'ENCODING', key),
    ]);
    return { exists, type, ttl, encoding };
  }
}

// ============================================================================
// Comparison utilities
// ============================================================================

/**
 * Normalize a RespValue to a plain JS structure for deep comparison.
 */
export function normalizeResp(value: RespValue): unknown {
  switch (value.type) {
    case 'simple':
      return { type: 'simple', value: value.value };
    case 'error':
      return { type: 'error', value: value.value };
    case 'integer':
      return { type: 'integer', value: Number(value.value) };
    case 'bulk':
      return {
        type: 'bulk',
        value: value.value === null ? null : value.value.toString('utf8'),
      };
    case 'array':
      if (value.value === null) {
        return { type: 'array', value: null };
      }
      return {
        type: 'array',
        value: value.value.map(normalizeResp),
      };
  }
}

/**
 * Assert two RESP values are identical.
 */
export function assertRespEqual(
  actual: RespValue,
  expected: RespValue,
  context = ''
): void {
  const a = normalizeResp(actual);
  const e = normalizeResp(expected);
  const prefix = context ? `[${context}] ` : '';

  if (JSON.stringify(a) !== JSON.stringify(e)) {
    throw new Error(
      `${prefix}RESP mismatch:\n` +
        `  RedisBox: ${JSON.stringify(a)}\n` +
        `  Redis:    ${JSON.stringify(e)}`
    );
  }
}

/**
 * Assert two RESP values are identical when treated as unordered
 * collections. Both must be arrays of the same length with the same
 * elements (in any order).
 */
export function assertRespUnordered(
  actual: RespValue,
  expected: RespValue,
  context = ''
): void {
  const prefix = context ? `[${context}] ` : '';

  // Both must be the same type
  if (actual.type !== expected.type) {
    throw new Error(
      `${prefix}type mismatch: ${actual.type} vs ${expected.type}`
    );
  }

  // Non-array types: exact match
  if (actual.type !== 'array' || expected.type !== 'array') {
    assertRespEqual(actual, expected, context);
    return;
  }

  // Null arrays
  if (actual.value === null && expected.value === null) return;
  if (actual.value === null || expected.value === null) {
    throw new Error(`${prefix}one array is null, other is not`);
  }

  // Length check
  if (actual.value.length !== expected.value.length) {
    throw new Error(
      `${prefix}array length mismatch: ${actual.value.length} vs ${expected.value.length}`
    );
  }

  // Sort both and compare
  const sortedActual = actual.value
    .map(normalizeResp)
    .map((v) => JSON.stringify(v))
    .sort();
  const sortedExpected = expected.value
    .map(normalizeResp)
    .map((v) => JSON.stringify(v))
    .sort();

  for (let i = 0; i < sortedActual.length; i++) {
    if (sortedActual[i] !== sortedExpected[i]) {
      throw new Error(
        `${prefix}unordered mismatch at sorted index ${i}:\n` +
          `  RedisBox: ${sortedActual[i]}\n` +
          `  Redis:    ${sortedExpected[i]}`
      );
    }
  }
}

/**
 * Assert two RESP values have the same type and structure, without
 * comparing actual values. For arrays, checks that both are arrays
 * of the same length with elements of matching types.
 */
export function assertRespStructure(
  actual: RespValue,
  expected: RespValue,
  context = ''
): void {
  const prefix = context ? `[${context}] ` : '';

  if (actual.type !== expected.type) {
    throw new Error(
      `${prefix}type mismatch: ${actual.type} vs ${expected.type}`
    );
  }

  if (actual.type === 'array' && expected.type === 'array') {
    if (actual.value === null && expected.value === null) return;
    if (actual.value === null || expected.value === null) {
      throw new Error(`${prefix}one array is null, other is not`);
    }

    if (actual.value.length !== expected.value.length) {
      throw new Error(
        `${prefix}array length mismatch: ${actual.value.length} vs ${expected.value.length}`
      );
    }

    for (let i = 0; i < actual.value.length; i++) {
      const a = actual.value[i];
      const e = expected.value[i];
      if (a && e) {
        assertRespStructure(a, e, `${context}[${i}]`);
      }
    }
  }
}

/**
 * Format a RespValue for human-readable display.
 */
export function formatResp(value: RespValue): string {
  switch (value.type) {
    case 'simple':
      return `+${value.value}`;
    case 'error':
      return `-${value.value}`;
    case 'integer':
      return `:${value.value}`;
    case 'bulk':
      return value.value === null
        ? '(nil)'
        : `"${value.value.toString('utf8')}"`;
    case 'array':
      if (value.value === null) return '(nil array)';
      if (value.value.length === 0) return '(empty array)';
      return `[${value.value.map(formatResp).join(', ')}]`;
  }
}
