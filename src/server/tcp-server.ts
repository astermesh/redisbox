import * as net from 'node:net';
import { EventEmitter } from 'node:events';

export interface TcpServerOptions {
  port: number;
  host: string;
}

export interface TcpServerEvents {
  connection: (id: number, socket: net.Socket) => void;
  disconnection: (id: number) => void;
}

export class TcpServer extends EventEmitter {
  private readonly host: string;
  private readonly requestedPort: number;
  private server: net.Server | null = null;
  private nextClientId = 1;
  private readonly connections = new Map<number, net.Socket>();

  constructor(options: TcpServerOptions) {
    super();
    this.host = options.host;
    this.requestedPort = options.port;
  }

  get listening(): boolean {
    return this.server?.listening ?? false;
  }

  get port(): number {
    const addr = this.server?.address();
    if (addr && typeof addr === 'object') {
      return addr.port;
    }
    return 0;
  }

  get connectionCount(): number {
    return this.connections.size;
  }

  get clientIds(): number[] {
    return [...this.connections.keys()];
  }

  override on<K extends keyof TcpServerEvents>(
    event: K,
    listener: TcpServerEvents[K]
  ): this;
  override on(event: string, listener: (...args: unknown[]) => void): this;
  override on(event: string, listener: (...args: unknown[]) => void): this {
    return super.on(event, listener);
  }

  override emit<K extends keyof TcpServerEvents>(
    event: K,
    ...args: Parameters<TcpServerEvents[K]>
  ): boolean;
  override emit(event: string, ...args: unknown[]): boolean;
  override emit(event: string, ...args: unknown[]): boolean {
    return super.emit(event, ...args);
  }

  start(): Promise<void> {
    if (this.server?.listening) {
      return Promise.reject(new Error('Server is already listening'));
    }

    return new Promise((resolve, reject) => {
      const server = net.createServer((socket) => this.handleConnection(socket));

      server.on('error', (err) => {
        reject(err);
      });

      server.listen(this.requestedPort, this.host, () => {
        this.server = server;
        // replace one-time error handler with ongoing one
        server.removeAllListeners('error');
        server.on('error', (err) => {
          this.emit('error' as string, err);
        });
        resolve();
      });
    });
  }

  stop(): Promise<void> {
    if (!this.server) {
      return Promise.resolve();
    }

    // destroy all active connections and emit disconnection events
    for (const [id, socket] of this.connections) {
      this.connections.delete(id);
      this.emit('disconnection', id);
      socket.destroy();
    }

    const srv = this.server;
    return new Promise((resolve) => {
      srv.close(() => {
        this.server = null;
        resolve();
      });
    });
  }

  private handleConnection(socket: net.Socket): void {
    const id = this.nextClientId++;
    this.connections.set(id, socket);

    this.emit('connection', id, socket);

    const cleanup = () => {
      if (this.connections.has(id)) {
        this.connections.delete(id);
        this.emit('disconnection', id);
      }
    };

    socket.on('close', cleanup);
    socket.on('error', () => {
      // error is handled by triggering close, which calls cleanup
      // we just need to prevent unhandled error crashes
    });
  }
}
