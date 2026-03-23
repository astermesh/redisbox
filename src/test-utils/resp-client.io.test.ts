/**
 * Integration tests for RespClient.
 *
 * Tests the RESP client against a live RedisBox TCP server.
 */

import { describe, it, expect, afterEach } from 'vitest';
import { TcpServer } from '../server/tcp-server.ts';
import { ClientConnection } from '../server/client-connection.ts';
import { ClientStateStore } from '../server/client-state.ts';
import { RedisEngine } from '../engine/engine.ts';
import { createCommandTable } from '../engine/command-registry.ts';
import { CommandDispatcher } from '../engine/command-dispatcher.ts';
import { RespClient } from './resp-client.ts';

interface TestContext {
  server: TcpServer;
  engine: RedisEngine;
  port: number;
}

async function startServer(): Promise<TestContext> {
  const engine = new RedisEngine();
  const table = createCommandTable();
  const dispatcher = new CommandDispatcher(table);
  const clientStore = new ClientStateStore();

  const server = new TcpServer({ port: 0, host: '127.0.0.1' });
  server.on('connection', (id, socket) => {
    const clientState = clientStore.create(id, Date.now());
    new ClientConnection({ socket, clientState, engine, dispatcher });
  });

  await server.start();
  return { server, engine, port: server.port };
}

describe('RespClient', () => {
  let ctx: TestContext | undefined;
  let client: RespClient | undefined;

  afterEach(async () => {
    if (client) {
      await client.disconnect();
      client = undefined;
    }
    if (ctx) {
      await ctx.server.stop();
      ctx = undefined;
    }
  });

  it('connects and sends PING', async () => {
    ctx = await startServer();
    client = new RespClient({ host: '127.0.0.1', port: ctx.port });
    await client.connect();
    expect(client.connected).toBe(true);

    const reply = await client.command('PING');
    expect(reply).toEqual({ type: 'simple', value: 'PONG' });
  });

  it('sends SET and GET', async () => {
    ctx = await startServer();
    client = new RespClient({ host: '127.0.0.1', port: ctx.port });
    await client.connect();

    const setReply = await client.command('SET', 'mykey', 'myvalue');
    expect(setReply).toEqual({ type: 'simple', value: 'OK' });

    const getReply = await client.command('GET', 'mykey');
    expect(getReply.type).toBe('bulk');
    expect(getReply.type === 'bulk' && getReply.value?.toString()).toBe(
      'myvalue'
    );
  });

  it('returns integer replies', async () => {
    ctx = await startServer();
    client = new RespClient({ host: '127.0.0.1', port: ctx.port });
    await client.connect();

    await client.command('SET', 'counter', '10');
    const reply = await client.command('INCR', 'counter');
    expect(reply).toEqual({ type: 'integer', value: 11 });
  });

  it('returns null bulk for missing key', async () => {
    ctx = await startServer();
    client = new RespClient({ host: '127.0.0.1', port: ctx.port });
    await client.connect();

    const reply = await client.command('GET', 'nonexistent');
    expect(reply).toEqual({ type: 'bulk', value: null });
  });

  it('returns error for invalid commands', async () => {
    ctx = await startServer();
    client = new RespClient({ host: '127.0.0.1', port: ctx.port });
    await client.connect();

    const reply = await client.command('SET', 'key');
    expect(reply.type).toBe('error');
  });

  it('returns array replies', async () => {
    ctx = await startServer();
    client = new RespClient({ host: '127.0.0.1', port: ctx.port });
    await client.connect();

    await client.command('RPUSH', 'list', 'a', 'b', 'c');
    const reply = await client.command('LRANGE', 'list', '0', '-1');
    expect(reply.type).toBe('array');
    if (reply.type === 'array' && reply.value) {
      expect(reply.value.length).toBe(3);
    }
  });

  it('handles multiple sequential commands', async () => {
    ctx = await startServer();
    client = new RespClient({ host: '127.0.0.1', port: ctx.port });
    await client.connect();

    for (let i = 0; i < 10; i++) {
      await client.command('SET', `key${i}`, `val${i}`);
    }

    for (let i = 0; i < 10; i++) {
      const reply = await client.command('GET', `key${i}`);
      expect(reply.type).toBe('bulk');
      if (reply.type === 'bulk') {
        expect(reply.value?.toString()).toBe(`val${i}`);
      }
    }
  });

  it('reports connected status correctly', async () => {
    ctx = await startServer();
    client = new RespClient({ host: '127.0.0.1', port: ctx.port });
    expect(client.connected).toBe(false);

    await client.connect();
    expect(client.connected).toBe(true);

    await client.disconnect();
    expect(client.connected).toBe(false);
    client = undefined;
  });

  it('throws when sending command while not connected', async () => {
    client = new RespClient({ host: '127.0.0.1', port: 1 });
    await expect(client.command('PING')).rejects.toThrow('Not connected');
    client = undefined;
  });

  it('handles binary-safe values', async () => {
    ctx = await startServer();
    client = new RespClient({ host: '127.0.0.1', port: ctx.port });
    await client.connect();

    const binaryVal = 'hello\x00world';
    await client.command('SET', 'binkey', binaryVal);
    const reply = await client.command('GET', 'binkey');
    expect(reply.type).toBe('bulk');
    if (reply.type === 'bulk') {
      expect(reply.value?.toString()).toBe(binaryVal);
    }
  });
});
