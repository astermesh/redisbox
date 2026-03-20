export type TrackingMode = 'normal' | 'bcast' | 'optin' | 'optout';

export class ClientState {
  readonly id: number;
  readonly createdAt: number;

  dbIndex = 0;
  name = '';
  username = 'default';
  lastCommand = '';
  lastCommandTime = 0;

  flagMulti = false;
  flagBlocked = false;
  flagSubscribed = false;
  authenticated = false;

  noEvict = false;
  noTouch = false;

  tracking = false;
  trackingMode: TrackingMode | null = null;
  trackingRedirect = 0;
  trackingPrefixes: string[] = [];
  trackingNoloop = false;

  constructor(id: number, createdAt: number) {
    this.id = id;
    this.createdAt = createdAt;
  }

  /** Redis-compatible flag string (e.g. "N", "x", "bP", "xbP") */
  flagsString(): string {
    let flags = '';
    if (this.flagMulti) flags += 'x';
    if (this.flagBlocked) flags += 'b';
    if (this.flagSubscribed) flags += 'P';
    if (this.tracking) flags += 't';
    return flags || 'N';
  }
}

export class ClientStateStore {
  private readonly clients = new Map<number, ClientState>();

  create(id: number, createdAt: number): ClientState {
    const state = new ClientState(id, createdAt);
    this.clients.set(id, state);
    return state;
  }

  get(id: number): ClientState | undefined {
    return this.clients.get(id);
  }

  has(id: number): boolean {
    return this.clients.has(id);
  }

  remove(id: number): boolean {
    return this.clients.delete(id);
  }

  get size(): number {
    return this.clients.size;
  }

  all(): IterableIterator<ClientState> {
    return this.clients.values();
  }
}
