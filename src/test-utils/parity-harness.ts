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
import {
  assertRespEqual,
  assertRespUnordered,
  assertRespStructure,
} from './resp-comparison.ts';

export {
  normalizeResp,
  assertRespEqual,
  assertRespUnordered,
  assertRespStructure,
  formatResp,
} from './resp-comparison.ts';

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
