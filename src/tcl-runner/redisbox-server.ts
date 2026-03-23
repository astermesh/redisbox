/**
 * Standalone RedisBox TCP server for external testing.
 *
 * Starts a RedisBox engine behind a TCP server so external tools
 * (like the Redis TCL test suite) can connect via the Redis protocol.
 */

import { TcpServer } from '../server/tcp-server.ts';
import { ClientConnection } from '../server/client-connection.ts';
import { ClientStateStore } from '../server/client-state.ts';
import { RedisEngine } from '../engine/engine.ts';
import { createCommandTable } from '../engine/command-registry.ts';
import { CommandDispatcher } from '../engine/command-dispatcher.ts';
import { ConfigStore } from '../config-store.ts';

export interface RedisBoxServerOptions {
  /** Port to listen on (0 = random available port) */
  port?: number;
  /** Host to bind to (default: 127.0.0.1) */
  host?: string;
}

export interface RedisBoxServer {
  /** Actual port the server is listening on */
  readonly port: number;
  /** Host the server is bound to */
  readonly host: string;
  /** Stop the server and clean up */
  stop(): Promise<void>;
}

/**
 * Start a standalone RedisBox TCP server.
 *
 * Returns a handle with the actual port and a stop function.
 */
export async function startRedisBoxServer(
  options?: RedisBoxServerOptions
): Promise<RedisBoxServer> {
  const host = options?.host ?? '127.0.0.1';
  const port = options?.port ?? 0;

  const engine = new RedisEngine();
  const table = createCommandTable();
  const dispatcher = new CommandDispatcher(table);
  const clientStore = new ClientStateStore();
  const config = new ConfigStore();

  const server = new TcpServer({ port, host });

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

  return {
    get port() {
      return server.port;
    },
    host,
    stop: () => server.stop(),
  };
}
