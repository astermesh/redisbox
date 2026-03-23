import { describe, it, expect, afterEach } from 'vitest';
import { startRedisBoxServer, type RedisBoxServer } from './redisbox-server.ts';
import { RespClient } from '../test-utils/resp-client.ts';

describe('startRedisBoxServer', () => {
  let server: RedisBoxServer | null = null;

  afterEach(async () => {
    if (server) {
      await server.stop();
      server = null;
    }
  });

  it('starts on a random port when port is 0', async () => {
    server = await startRedisBoxServer({ port: 0 });
    expect(server.port).toBeGreaterThan(0);
    expect(server.host).toBe('127.0.0.1');
  });

  it('responds to PING via RESP protocol', async () => {
    server = await startRedisBoxServer({ port: 0 });

    const client = new RespClient({ host: '127.0.0.1', port: server.port });
    await client.connect();

    const reply = await client.command('PING');
    expect(reply).toEqual({ type: 'simple', value: 'PONG' });

    await client.disconnect();
  });

  it('supports SET and GET commands', async () => {
    server = await startRedisBoxServer({ port: 0 });

    const client = new RespClient({ host: '127.0.0.1', port: server.port });
    await client.connect();

    const setReply = await client.command('SET', 'testkey', 'testvalue');
    expect(setReply).toEqual({ type: 'simple', value: 'OK' });

    const getReply = await client.command('GET', 'testkey');
    expect(getReply).toEqual({ type: 'bulk', value: Buffer.from('testvalue') });

    await client.disconnect();
  });

  it('stops cleanly', async () => {
    server = await startRedisBoxServer({ port: 0 });
    const port = server.port;

    await server.stop();
    server = null;

    // Verify server is no longer accepting connections
    const client = new RespClient({
      host: '127.0.0.1',
      port,
      timeout: 500,
    });

    await expect(client.connect()).rejects.toThrow();
  });
});
