import { describe, it, expect, afterEach } from 'vitest';
import * as net from 'node:net';
import { TcpServer } from './tcp-server.ts';

function connect(port: number, host = '127.0.0.1'): Promise<net.Socket> {
  return new Promise((resolve, reject) => {
    const socket = net.createConnection({ port, host }, () => resolve(socket));
    socket.on('error', reject);
  });
}

describe('TcpServer', () => {
  let server: TcpServer | undefined;

  afterEach(async () => {
    if (server) {
      await server.stop();
      server = undefined;
    }
  });

  describe('listening', () => {
    it('listens on a random port when port is 0', async () => {
      server = new TcpServer({ port: 0, host: '127.0.0.1' });
      await server.start();

      const port = server.port;
      expect(port).toBeGreaterThan(0);

      // verify we can connect
      const client = await connect(port);
      client.destroy();
    });

    it('listens on a specified port', async () => {
      // first find a free port
      const tmp = new TcpServer({ port: 0, host: '127.0.0.1' });
      await tmp.start();
      const freePort = tmp.port;
      await tmp.stop();

      server = new TcpServer({ port: freePort, host: '127.0.0.1' });
      await server.start();

      expect(server.port).toBe(freePort);

      const client = await connect(freePort);
      client.destroy();
    });

    it('listens on the specified host', async () => {
      server = new TcpServer({ port: 0, host: '127.0.0.1' });
      await server.start();

      const client = await connect(server.port, '127.0.0.1');
      client.destroy();
    });

    it('reports listening state', async () => {
      server = new TcpServer({ port: 0, host: '127.0.0.1' });
      expect(server.listening).toBe(false);

      await server.start();
      expect(server.listening).toBe(true);

      await server.stop();
      expect(server.listening).toBe(false);
      server = undefined;
    });

    it('rejects start if already listening', async () => {
      server = new TcpServer({ port: 0, host: '127.0.0.1' });
      await server.start();
      await expect(server.start()).rejects.toThrow();
    });
  });

  describe('connections', () => {
    it('accepts a single connection', async () => {
      server = new TcpServer({ port: 0, host: '127.0.0.1' });
      await server.start();

      const client = await connect(server.port);
      // give server a moment to register the connection
      await delay(50);

      expect(server.connectionCount).toBe(1);
      client.destroy();
    });

    it('accepts multiple concurrent connections', async () => {
      server = new TcpServer({ port: 0, host: '127.0.0.1' });
      await server.start();

      const clients = await Promise.all([
        connect(server.port),
        connect(server.port),
        connect(server.port),
      ]);

      await delay(50);
      expect(server.connectionCount).toBe(3);

      for (const c of clients) c.destroy();
    });

    it('assigns unique client IDs to each connection', async () => {
      server = new TcpServer({ port: 0, host: '127.0.0.1' });
      await server.start();

      const clients = await Promise.all([
        connect(server.port),
        connect(server.port),
        connect(server.port),
      ]);

      await delay(50);

      const ids = server.clientIds;
      expect(ids.length).toBe(3);
      // all IDs are unique
      expect(new Set(ids).size).toBe(3);

      for (const c of clients) c.destroy();
    });

    it('emits "connection" event with client id and socket', async () => {
      server = new TcpServer({ port: 0, host: '127.0.0.1' });
      await server.start();

      const connectionPromise = new Promise<{ id: number; socket: net.Socket }>(
        (resolve) => {
          server?.on('connection', (id, socket) => resolve({ id, socket }));
        }
      );

      const client = await connect(server.port);
      const { id, socket } = await connectionPromise;

      expect(typeof id).toBe('number');
      expect(socket).toBeInstanceOf(net.Socket);
      expect(socket.remoteAddress).toBeDefined();

      client.destroy();
    });
  });

  describe('disconnection and cleanup', () => {
    it('removes connection on client close', async () => {
      server = new TcpServer({ port: 0, host: '127.0.0.1' });
      await server.start();

      const client = await connect(server.port);
      await delay(50);
      expect(server.connectionCount).toBe(1);

      client.destroy();
      await delay(100);
      expect(server.connectionCount).toBe(0);
    });

    it('emits "disconnection" event with client id', async () => {
      server = new TcpServer({ port: 0, host: '127.0.0.1' });
      await server.start();

      let connectedId = -1;
      server.on('connection', (id) => {
        connectedId = id;
      });

      const disconnectPromise = new Promise<number>((resolve) => {
        server?.on('disconnection', (id) => resolve(id));
      });

      const client = await connect(server.port);
      await delay(50);

      client.destroy();
      const disconnectedId = await disconnectPromise;

      expect(disconnectedId).toBe(connectedId);
    });

    it('cleans up all connections on server stop', async () => {
      server = new TcpServer({ port: 0, host: '127.0.0.1' });
      await server.start();

      const clients = await Promise.all([
        connect(server.port),
        connect(server.port),
      ]);

      await delay(50);
      expect(server.connectionCount).toBe(2);

      await server.stop();
      expect(server.connectionCount).toBe(0);

      // give clients time to receive the FIN
      await delay(100);

      // clients should be disconnected
      for (const c of clients) {
        expect(c.destroyed).toBe(true);
      }

      server = undefined;
    });

    it('emits disconnection events for all clients on server stop', async () => {
      server = new TcpServer({ port: 0, host: '127.0.0.1' });
      await server.start();

      const clients = await Promise.all([
        connect(server.port),
        connect(server.port),
        connect(server.port),
      ]);

      await delay(50);

      const disconnectedIds: number[] = [];
      server.on('disconnection', (id) => disconnectedIds.push(id));

      const connectedIds = [...server.clientIds];
      await server.stop();

      expect(disconnectedIds.length).toBe(3);
      expect(disconnectedIds.sort()).toEqual(connectedIds.sort());

      for (const c of clients) c.destroy();
      server = undefined;
    });

    it('handles rapid connect/disconnect', async () => {
      server = new TcpServer({ port: 0, host: '127.0.0.1' });
      await server.start();

      for (let i = 0; i < 10; i++) {
        const client = await connect(server.port);
        client.destroy();
      }

      await delay(200);
      expect(server.connectionCount).toBe(0);
    });
  });

  describe('error handling', () => {
    it('handles socket errors without crashing the server', async () => {
      server = new TcpServer({ port: 0, host: '127.0.0.1' });
      await server.start();

      const client = await connect(server.port);
      await delay(50);

      // force an error by resetting the connection
      client.resetAndDestroy();
      await delay(100);

      expect(server.listening).toBe(true);
      expect(server.connectionCount).toBe(0);

      // server should still accept new connections
      const client2 = await connect(server.port);
      await delay(50);
      expect(server.connectionCount).toBe(1);
      client2.destroy();
    });
  });

  describe('stop', () => {
    it('can be called multiple times safely', async () => {
      server = new TcpServer({ port: 0, host: '127.0.0.1' });
      await server.start();
      await server.stop();
      await server.stop(); // should not throw
      server = undefined;
    });

    it('can be restarted after stop', async () => {
      server = new TcpServer({ port: 0, host: '127.0.0.1' });
      await server.start();
      const port1 = server.port;

      const client1 = await connect(port1);
      await delay(50);
      expect(server.connectionCount).toBe(1);
      client1.destroy();

      await server.stop();
      expect(server.listening).toBe(false);

      await server.start();
      expect(server.listening).toBe(true);

      const client2 = await connect(server.port);
      await delay(50);
      expect(server.connectionCount).toBe(1);
      client2.destroy();
    });

    it('rejects new connections after stop', async () => {
      server = new TcpServer({ port: 0, host: '127.0.0.1' });
      await server.start();
      const port = server.port;

      await server.stop();
      server = undefined;

      await expect(connect(port)).rejects.toThrow();
    });
  });
});

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
