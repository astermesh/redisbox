import { describe, it, expect, afterEach } from 'vitest';
import * as net from 'node:net';
import { TcpServer } from './tcp-server.ts';
import { ClientConnection } from './client-connection.ts';
import { ClientStateStore } from './client-state.ts';
import { RedisEngine } from '../engine/engine.ts';
import { createCommandTable } from '../engine/command-registry.ts';
import { CommandDispatcher } from '../engine/command-dispatcher.ts';

function connect(port: number): Promise<net.Socket> {
  return new Promise((resolve, reject) => {
    const socket = net.createConnection({ port, host: '127.0.0.1' }, () =>
      resolve(socket)
    );
    socket.on('error', reject);
  });
}

function readBytes(socket: net.Socket, n: number): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    let total = 0;
    const timeout = setTimeout(() => {
      socket.removeListener('data', onData);
      reject(new Error(`Timed out waiting for ${n} bytes (got ${total})`));
    }, 5000);

    function onData(data: Buffer) {
      chunks.push(data);
      total += data.length;
      if (total >= n) {
        clearTimeout(timeout);
        socket.removeListener('data', onData);
        resolve(Buffer.concat(chunks).subarray(0, n));
      }
    }

    socket.on('data', onData);
  });
}

function readResponse(socket: net.Socket): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      socket.removeListener('data', onData);
      reject(new Error('Timed out waiting for response'));
    }, 5000);

    function onData(data: Buffer) {
      clearTimeout(timeout);
      socket.removeListener('data', onData);
      resolve(data);
    }

    socket.on('data', onData);
  });
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

interface TestContext {
  server: TcpServer;
  engine: RedisEngine;
  clientStore: ClientStateStore;
}

function createTestServer(): TestContext {
  const engine = new RedisEngine();
  const table = createCommandTable();
  const dispatcher = new CommandDispatcher(table);
  const clientStore = new ClientStateStore();

  const server = new TcpServer({ port: 0, host: '127.0.0.1' });
  server.on('connection', (id, socket) => {
    const clientState = clientStore.create(id, Date.now());
    new ClientConnection({ socket, clientState, engine, dispatcher });
  });

  return { server, engine, clientStore };
}

describe('ClientConnection', () => {
  let ctx: TestContext | undefined;

  afterEach(async () => {
    if (ctx) {
      await ctx.server.stop();
      ctx = undefined;
    }
  });

  async function start(): Promise<TestContext> {
    ctx = createTestServer();
    await ctx.server.start();
    return ctx;
  }

  describe('RESP protocol', () => {
    it('executes SET and returns +OK', async () => {
      const { server } = await start();

      const client = await connect(server.port);
      const responsePromise = readResponse(client);

      client.write(
        Buffer.from('*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n')
      );

      const response = await responsePromise;
      expect(response.toString()).toBe('+OK\r\n');

      client.destroy();
    });

    it('executes SET then GET and returns correct value', async () => {
      const { server } = await start();

      const client = await connect(server.port);

      client.write(
        Buffer.from(
          '*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n' +
            '*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n'
        )
      );

      const expected = '+OK\r\n$3\r\nbar\r\n';
      const response = await readBytes(client, expected.length);
      expect(response.toString()).toBe(expected);

      client.destroy();
    });

    it('returns nil bulk for non-existent key', async () => {
      const { server } = await start();

      const client = await connect(server.port);
      const responsePromise = readResponse(client);

      client.write(Buffer.from('*2\r\n$3\r\nGET\r\n$11\r\nnonexistent\r\n'));

      const response = await responsePromise;
      expect(response.toString()).toBe('$-1\r\n');

      client.destroy();
    });

    it('returns error for unknown command', async () => {
      const { server } = await start();

      const client = await connect(server.port);
      const responsePromise = readResponse(client);

      client.write(Buffer.from('*1\r\n$7\r\nUNKNOWN\r\n'));

      const response = await responsePromise;
      expect(response.toString()).toMatch(/^-ERR unknown command 'UNKNOWN'/);

      client.destroy();
    });

    it('returns integer reply for EXISTS', async () => {
      const { server } = await start();

      const client = await connect(server.port);

      client.write(
        Buffer.from(
          '*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n' +
            '*2\r\n$6\r\nEXISTS\r\n$1\r\nk\r\n'
        )
      );

      const expected = '+OK\r\n:1\r\n';
      const response = await readBytes(client, expected.length);
      expect(response.toString()).toBe(expected);

      client.destroy();
    });

    it('returns array reply for KEYS', async () => {
      const { server } = await start();

      const client = await connect(server.port);

      client.write(
        Buffer.from(
          '*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n' +
            '*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$1\r\n2\r\n' +
            '*2\r\n$4\r\nKEYS\r\n$1\r\n*\r\n'
        )
      );

      // +OK\r\n+OK\r\n*2\r\n$1\r\na\r\n$1\r\nb\r\n (or b then a)
      const expected = '+OK\r\n+OK\r\n*2\r\n$1\r\na\r\n$1\r\nb\r\n';
      const responseData = await readBytes(client, expected.length);
      const text = responseData.toString();

      expect(text.startsWith('+OK\r\n+OK\r\n*2\r\n')).toBe(true);
      expect(text).toContain('$1\r\na\r\n');
      expect(text).toContain('$1\r\nb\r\n');

      client.destroy();
    });
  });

  describe('inline protocol', () => {
    it('executes inline SET command', async () => {
      const { server } = await start();

      const client = await connect(server.port);
      const responsePromise = readResponse(client);

      client.write(Buffer.from('SET foo bar\r\n'));

      const response = await responsePromise;
      expect(response.toString()).toBe('+OK\r\n');

      client.destroy();
    });

    it('executes inline GET command', async () => {
      const { server } = await start();

      const client = await connect(server.port);

      client.write(Buffer.from('SET foo bar\r\nGET foo\r\n'));

      const expected = '+OK\r\n$3\r\nbar\r\n';
      const response = await readBytes(client, expected.length);
      expect(response.toString()).toBe(expected);

      client.destroy();
    });

    it('handles inline command with quoted arguments', async () => {
      const { server } = await start();

      const client = await connect(server.port);

      client.write(Buffer.from('SET mykey "hello world"\r\nGET mykey\r\n'));

      const expected = '+OK\r\n$11\r\nhello world\r\n';
      const response = await readBytes(client, expected.length);
      expect(response.toString()).toBe(expected);

      client.destroy();
    });
  });

  describe('pipelining', () => {
    it('maintains strict response ordering for pipelined commands', async () => {
      const { server } = await start();

      const client = await connect(server.port);

      client.write(
        Buffer.from(
          '*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n' +
            '*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$1\r\n2\r\n' +
            '*2\r\n$3\r\nGET\r\n$1\r\na\r\n' +
            '*2\r\n$3\r\nGET\r\n$1\r\nb\r\n'
        )
      );

      const expected = '+OK\r\n+OK\r\n$1\r\n1\r\n$1\r\n2\r\n';
      const response = await readBytes(client, expected.length);
      expect(response.toString()).toBe(expected);

      client.destroy();
    });

    it('handles many pipelined commands', async () => {
      const { server } = await start();

      const client = await connect(server.port);

      let payload = '';
      for (let i = 0; i < 100; i++) {
        const key = `k${i}`;
        const val = `v${i}`;
        payload += `*3\r\n$3\r\nSET\r\n$${key.length}\r\n${key}\r\n$${val.length}\r\n${val}\r\n`;
      }

      client.write(Buffer.from(payload));

      const expected = '+OK\r\n'.repeat(100);
      const response = await readBytes(client, expected.length);
      expect(response.toString()).toBe(expected);

      client.destroy();
    });

    it('handles pipelined commands split across TCP segments', async () => {
      const { server } = await start();

      const client = await connect(server.port);

      const full = '*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n';
      const mid = Math.floor(full.length / 2);

      client.write(Buffer.from(full.slice(0, mid)));
      await delay(20);
      client.write(Buffer.from(full.slice(mid)));

      const response = await readResponse(client);
      expect(response.toString()).toBe('+OK\r\n');

      client.destroy();
    });
  });

  describe('mixed protocol pipelining', () => {
    it('handles inline and RESP commands in same pipeline', async () => {
      const { server } = await start();

      const client = await connect(server.port);

      client.write(
        Buffer.from('SET foo bar\r\n' + '*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n')
      );

      const expected = '+OK\r\n$3\r\nbar\r\n';
      const response = await readBytes(client, expected.length);
      expect(response.toString()).toBe(expected);

      client.destroy();
    });
  });

  describe('protocol error handling', () => {
    it('sends error and closes connection on protocol error', async () => {
      const { server } = await start();

      const client = await connect(server.port);

      client.write(Buffer.from('*abc\r\n'));

      const response = await readResponse(client);
      expect(response.toString()).toMatch(/^-ERR/);

      const closed = await Promise.race([
        new Promise<boolean>((resolve) =>
          client.on('close', () => resolve(true))
        ),
        new Promise<boolean>((resolve) =>
          setTimeout(() => resolve(false), 2000)
        ),
      ]);
      expect(closed).toBe(true);

      client.destroy();
    });
  });

  describe('multiple clients', () => {
    it('shares database between clients', async () => {
      const { server } = await start();

      const client1 = await connect(server.port);
      const client2 = await connect(server.port);
      await delay(50);

      client1.write(Buffer.from('*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n'));
      const r1 = await readResponse(client1);
      expect(r1.toString()).toBe('+OK\r\n');

      client2.write(Buffer.from('*2\r\n$3\r\nGET\r\n$1\r\na\r\n'));
      const r2 = await readResponse(client2);
      expect(r2.toString()).toBe('$1\r\n1\r\n');

      client1.destroy();
      client2.destroy();
    });
  });

  describe('backpressure', () => {
    it('completes all responses even under heavy pipelining', async () => {
      const { server } = await start();

      const client = await connect(server.port);

      let payload = '';
      const count = 500;
      for (let i = 0; i < count; i++) {
        const key = `key${i}`;
        const val = `val${i}`;
        payload += `*3\r\n$3\r\nSET\r\n$${key.length}\r\n${key}\r\n$${val.length}\r\n${val}\r\n`;
      }

      client.write(Buffer.from(payload));

      const expected = '+OK\r\n'.repeat(count);
      const response = await readBytes(client, expected.length);
      expect(response.toString()).toBe(expected);

      client.destroy();
    });
  });

  describe('client state tracking', () => {
    it('updates lastCommand on command execution', async () => {
      const { server, clientStore } = await start();

      const connPromise = new Promise<number>((resolve) => {
        server.on('connection', (id) => resolve(id));
      });

      const client = await connect(server.port);
      const clientId = await connPromise;
      await delay(50);

      client.write(
        Buffer.from('*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n')
      );
      await readResponse(client);

      const state = clientStore.get(clientId);
      expect(state?.lastCommand).toBe('SET');
      expect(state?.lastCommandTime).toBeGreaterThan(0);

      client.destroy();
    });
  });
});
