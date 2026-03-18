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
import { createClientState as createDispatcherState } from '../engine/command-dispatcher.ts';
import type { ClientState as DispatcherClientState } from '../engine/command-dispatcher.ts';
import type { Reply, CommandContext } from '../engine/types.ts';
import type { RespValue } from '../resp/types.ts';
import * as serializer from '../resp/resp-serializer.ts';
import type { ClientState } from './client-state.ts';
import type { RedisEngine } from '../engine/engine.ts';

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
  }
}

/** Serialize an engine Reply to a RESP wire-format Buffer. */
export function serializeReply(reply: Reply): Buffer {
  return serializer.serialize(replyToRespValue(reply));
}

export interface ClientConnectionOptions {
  socket: net.Socket;
  clientState: ClientState;
  engine: RedisEngine;
  dispatcher: CommandDispatcher;
}

export class ClientConnection {
  private readonly socket: net.Socket;
  private readonly reader: CommandReader;
  private readonly dispatcher: CommandDispatcher;
  private readonly clientState: ClientState;
  private readonly dispatcherState: DispatcherClientState;
  private readonly engine: RedisEngine;
  private paused = false;

  constructor(options: ClientConnectionOptions) {
    this.socket = options.socket;
    this.clientState = options.clientState;
    this.engine = options.engine;
    this.dispatcher = options.dispatcher;
    this.dispatcherState = createDispatcherState();

    this.reader = new CommandReader((args) => this.handleCommand(args));

    this.socket.on('data', (data: Buffer) => this.onData(data));
    this.socket.on('drain', () => this.onDrain());
  }

  private onData(data: Buffer): void {
    try {
      this.reader.write(data);
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Protocol error';
      this.socket.end(serializer.error(`ERR ${msg}`));
    }
  }

  private handleCommand(args: string[]): void {
    this.clientState.lastCommand = args[0] ?? '';
    this.clientState.lastCommandTime = this.engine.clock();

    const ctx: CommandContext = {
      db: this.engine.db(this.clientState.dbIndex),
      engine: this.engine,
      client: this.clientState,
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
}
