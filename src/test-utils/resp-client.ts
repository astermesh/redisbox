/**
 * Minimal RESP2 client for testing.
 *
 * Connects to a Redis-compatible server via TCP, sends commands using
 * the RESP2 multibulk protocol, and returns parsed responses.
 */

import * as net from 'node:net';
import { RespParser } from '../resp/resp-parser.ts';
import type { RespValue } from '../resp/types.ts';

const DEFAULT_TIMEOUT = 5000;

export interface RespClientOptions {
  host: string;
  port: number;
  timeout?: number;
}

export class RespClient {
  private socket: net.Socket | null = null;
  private parser: RespParser | null = null;
  private pending: {
    resolve: (value: RespValue) => void;
    reject: (err: Error) => void;
  }[] = [];
  private readonly host: string;
  private readonly port: number;
  private readonly timeout: number;

  constructor(options: RespClientOptions) {
    this.host = options.host;
    this.port = options.port;
    this.timeout = options.timeout ?? DEFAULT_TIMEOUT;
  }

  /** Connect to the server. */
  async connect(): Promise<void> {
    if (this.socket) return;

    return new Promise((resolve, reject) => {
      const socket = net.createConnection(
        { port: this.port, host: this.host },
        () => {
          this.socket = socket;
          resolve();
        }
      );

      this.parser = new RespParser((value: RespValue) => {
        const entry = this.pending.shift();
        if (entry) {
          entry.resolve(value);
        }
      });

      const parser = this.parser;
      socket.on('data', (data: Buffer) => {
        parser.write(data);
      });

      socket.on('error', (err) => {
        // Reject all pending commands
        for (const entry of this.pending) {
          entry.reject(err);
        }
        this.pending = [];
        if (!this.socket) {
          reject(err);
        }
      });

      socket.on('close', () => {
        for (const entry of this.pending) {
          entry.reject(new Error('Connection closed'));
        }
        this.pending = [];
        this.socket = null;
      });

      const timer = setTimeout(() => {
        socket.destroy();
        reject(new Error(`Connection timeout after ${this.timeout}ms`));
      }, this.timeout);

      socket.once('connect', () => clearTimeout(timer));
    });
  }

  /** Send a command and wait for the response. */
  async command(cmd: string, ...args: string[]): Promise<RespValue> {
    if (!this.socket) {
      throw new Error('Not connected');
    }

    const parts = [cmd, ...args];
    const segments: string[] = [`*${parts.length}\r\n`];
    for (const part of parts) {
      const buf = Buffer.from(part);
      segments.push(`$${buf.length}\r\n`);
      segments.push(part);
      segments.push('\r\n');
    }

    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        // Remove from pending
        const idx = this.pending.findIndex((e) => e.resolve === resolve);
        if (idx !== -1) this.pending.splice(idx, 1);
        reject(
          new Error(
            `Command timeout after ${this.timeout}ms: ${cmd} ${args.join(' ')}`
          )
        );
      }, this.timeout);

      this.pending.push({
        resolve: (value) => {
          clearTimeout(timer);
          resolve(value);
        },
        reject: (err) => {
          clearTimeout(timer);
          reject(err);
        },
      });

      const sock = this.socket;
      if (sock) {
        sock.write(segments.join(''));
      }
    });
  }

  /** Disconnect from the server. */
  async disconnect(): Promise<void> {
    if (!this.socket) return;

    const socket = this.socket;
    return new Promise((resolve) => {
      socket.once('close', () => {
        this.socket = null;
        resolve();
      });
      socket.destroy();
    });
  }

  /** Check if connected. */
  get connected(): boolean {
    return this.socket !== null && !this.socket.destroyed;
  }
}

/**
 * Try to connect to a Redis server.
 * Returns true if connection succeeds, false otherwise.
 */
export async function canConnect(
  host: string,
  port: number,
  timeout = 1000
): Promise<boolean> {
  return new Promise((resolve) => {
    const socket = net.createConnection({ port, host }, () => {
      socket.destroy();
      resolve(true);
    });
    socket.on('error', () => resolve(false));
    const timer = setTimeout(() => {
      socket.destroy();
      resolve(false);
    }, timeout);
    socket.once('connect', () => clearTimeout(timer));
  });
}
