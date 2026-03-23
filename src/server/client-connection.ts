/**
 * Per-client connection handler.
 *
 * Wires the RESP/inline command reader to the command dispatcher and
 * serializes replies back to the socket. Maintains strict response
 * ordering for pipelined commands and handles TCP backpressure.
 */

import type * as net from 'node:net';
import { CommandReader } from './command-reader.ts';
import type { CommandDispatcher } from '../engine/command-dispatcher.ts';
import { createTransactionState } from '../engine/command-dispatcher.ts';
import type { TransactionState } from '../engine/command-dispatcher.ts';
import type { Reply, CommandContext } from '../engine/types.ts';
import type { RespValue } from '../resp/types.ts';
import * as serializer from '../resp/resp-serializer.ts';
import type { ClientState, ClientStateStore } from './client-state.ts';
import type { RedisEngine } from '../engine/engine.ts';
import type { ConfigStore } from '../config-store.ts';
import type { EvictionManager } from '../engine/memory/eviction-manager.ts';

/** Convert an engine Reply to a RESP wire-format RespValue. */
function replyToRespValue(reply: Reply): RespValue {
  switch (reply.kind) {
    case 'status':
      return { type: 'simple', value: reply.value };
    case 'error':
      return { type: 'error', value: `${reply.prefix} ${reply.message}` };
    case 'integer':
      return { type: 'integer', value: reply.value };
    case 'bulk':
      return reply.value === null
        ? { type: 'bulk', value: null }
        : { type: 'bulk', value: Buffer.from(reply.value) };
    case 'array':
      return { type: 'array', value: reply.value.map(replyToRespValue) };
    case 'nil-array':
      return { type: 'array', value: null };
    case 'multi':
      // multi replies are handled in serializeReply; this branch should not be reached
      return { type: 'array', value: reply.value.map(replyToRespValue) };
  }
}

/** Serialize an engine Reply to a RESP wire-format Buffer. */
export function serializeReply(reply: Reply): Buffer {
  if (reply.kind === 'multi') {
    return Buffer.concat(reply.value.map(serializeReply));
  }
  return serializer.serialize(replyToRespValue(reply));
}

export interface ClientConnectionOptions {
  socket: net.Socket;
  clientState: ClientState;
  clientStore?: ClientStateStore;
  engine: RedisEngine;
  dispatcher: CommandDispatcher;
  config?: ConfigStore;
  eviction?: EvictionManager;
}

export class ClientConnection {
  private readonly socket: net.Socket;
  private readonly reader: CommandReader;
  private readonly dispatcher: CommandDispatcher;
  private readonly clientState: ClientState;
  private readonly clientStore?: ClientStateStore;
  private readonly dispatcherState: TransactionState;
  private readonly engine: RedisEngine;
  private readonly config?: ConfigStore;
  private readonly eviction?: EvictionManager;
  private paused = false;
  private closed = false;

  constructor(options: ClientConnectionOptions) {
    this.socket = options.socket;
    this.clientState = options.clientState;
    this.clientStore = options.clientStore;
    this.engine = options.engine;
    this.dispatcher = options.dispatcher;
    this.config = options.config;
    this.eviction = options.eviction;
    this.dispatcherState = createTransactionState();

    this.reader = new CommandReader((args) => this.handleCommand(args));

    this.socket.on('data', (data: Buffer) => this.onData(data));
    this.socket.on('drain', () => this.onDrain());
  }

  private onData(data: Buffer): void {
    if (this.closed) return;
    try {
      this.reader.write(data);
    } catch (err) {
      this.closed = true;
      const msg = err instanceof Error ? err.message : 'Protocol error';
      this.socket.end(serializer.error(`ERR ${msg}`));
      this.socket.destroy();
    }
  }

  private handleCommand(args: string[]): void {
    this.clientState.lastCommand = args[0] ?? '';
    this.clientState.lastCommandTime = this.engine.clock();

    const ctx: CommandContext = {
      db: this.engine.db(this.clientState.dbIndex),
      engine: this.engine,
      client: this.clientState,
      config: this.config,
      clientStore: this.clientStore,
      pubsub: this.engine.pubsub,
      blocking: this.engine.blocking,
      acl: this.engine.acl,
      eviction: this.eviction,
      ibi: this.engine.ibi,
    };

    const reply = this.dispatcher.dispatch(this.dispatcherState, ctx, args);
    const buf = serializeReply(reply);
    const flushed = this.socket.write(buf);

    if (!flushed && !this.paused) {
      this.paused = true;
      this.socket.pause();
    }
  }

  private onDrain(): void {
    if (this.paused) {
      this.paused = false;
      this.socket.resume();
    }
  }

  /**
   * Send a push reply to this client (used for pub/sub message delivery).
   */
  sendReply(reply: Reply): void {
    if (this.closed) return;
    const buf = serializeReply(reply);
    const flushed = this.socket.write(buf);
    if (!flushed && !this.paused) {
      this.paused = true;
      this.socket.pause();
    }
  }
}
