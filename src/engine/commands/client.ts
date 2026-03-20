import type { Reply, CommandContext } from '../types.ts';
import {
  bulkReply,
  integerReply,
  arrayReply,
  errorReply,
  OK,
  NIL,
  unknownSubcommandError,
} from '../types.ts';
import type { CommandSpec } from '../command-table.ts';
import type {
  ClientState,
  ClientStateStore,
  TrackingMode,
} from '../../server/client-state.ts';

// --- CLIENT SETNAME validation ---

function isValidClientName(name: string): boolean {
  for (let i = 0; i < name.length; i++) {
    const code = name.charCodeAt(i);
    if (code < 33 || code > 126) return false;
  }
  return true;
}

// --- CLIENT LIST format ---

function formatClientInfo(client: ClientState, clock: () => number): string {
  const now = clock();
  const age = Math.floor((now - client.createdAt) / 1000);
  const idle =
    client.lastCommandTime === 0
      ? age
      : Math.floor((now - client.lastCommandTime) / 1000);

  const multi = client.flagMulti ? '0' : '-1';

  return (
    `id=${client.id}` +
    ` addr=127.0.0.1:0` +
    ` laddr=127.0.0.1:0` +
    ` fd=0` +
    ` name=${client.name}` +
    ` age=${age}` +
    ` idle=${idle}` +
    ` flags=${client.flagsString()}` +
    ` db=${client.dbIndex}` +
    ` sub=0` +
    ` psub=0` +
    ` ssub=0` +
    ` multi=${multi}` +
    ` watch=0` +
    ` qbuf=0` +
    ` qbuf-free=0` +
    ` argv-mem=0` +
    ` multi-mem=0` +
    ` rbs=0` +
    ` rbp=0` +
    ` obl=0` +
    ` oll=0` +
    ` omem=0` +
    ` tot-mem=0` +
    ` events=r` +
    ` cmd=${client.lastCommand || 'NULL'}` +
    ` user=default` +
    ` redir=${client.trackingRedirect}` +
    ` resp=2` +
    ` lib-name=` +
    ` lib-ver=`
  );
}

// --- Subcommand implementations ---

export function clientId(client: ClientState | undefined): Reply {
  return integerReply(client?.id ?? 0);
}

export function clientGetname(client: ClientState | undefined): Reply {
  const name = client?.name ?? '';
  return name === '' ? NIL : bulkReply(name);
}

export function clientSetname(
  client: ClientState | undefined,
  args: string[]
): Reply {
  const name = args[0] ?? '';

  if (!isValidClientName(name)) {
    return errorReply(
      'ERR',
      'Client names cannot contain spaces, newlines or special characters.'
    );
  }

  if (client) {
    client.name = name;
  }
  return OK;
}

export function clientList(
  clientStore: ClientStateStore | undefined,
  client: ClientState | undefined,
  clock: () => number,
  args: string[]
): Reply {
  // Parse optional TYPE filter
  let typeFilter: string | null = null;
  for (let i = 0; i < args.length; i++) {
    const arg = (args[i] ?? '').toUpperCase();
    if (arg === 'TYPE') {
      const val = args[i + 1];
      if (val === undefined) {
        return errorReply('ERR', 'syntax error');
      }
      const lower = val.toLowerCase();
      if (
        lower !== 'normal' &&
        lower !== 'master' &&
        lower !== 'replica' &&
        lower !== 'pubsub' &&
        lower !== 'slave'
      ) {
        return errorReply('ERR', `Unknown client type '${val}'`);
      }
      typeFilter = lower;
      i++; // skip value
    } else if (arg === 'ID') {
      // CLIENT LIST ID id1 id2 ... — skip parsing, we filter below
      break;
    } else {
      return errorReply('ERR', 'syntax error');
    }
  }

  // Parse optional ID filter
  let idFilter: Set<number> | null = null;
  for (let i = 0; i < args.length; i++) {
    if ((args[i] ?? '').toUpperCase() === 'ID') {
      idFilter = new Set();
      for (let j = i + 1; j < args.length; j++) {
        const n = parseInt(args[j] ?? '', 10);
        if (isNaN(n)) {
          return errorReply('ERR', 'syntax error');
        }
        idFilter.add(n);
      }
      if (idFilter.size === 0) {
        return errorReply('ERR', 'syntax error');
      }
      break;
    }
  }

  const clients: ClientState[] = [];
  if (clientStore) {
    for (const c of clientStore.all()) {
      if (idFilter && !idFilter.has(c.id)) continue;
      if (typeFilter === 'pubsub' && !c.flagSubscribed) continue;
      if (typeFilter === 'normal' && c.flagSubscribed) continue;
      clients.push(c);
    }
  } else if (client) {
    if (!idFilter || idFilter.has(client.id)) {
      if (typeFilter === 'pubsub' && !client.flagSubscribed) {
        // filtered out
      } else if (typeFilter === 'normal' && client.flagSubscribed) {
        // filtered out
      } else {
        clients.push(client);
      }
    }
  }

  const lines = clients.map((c) => formatClientInfo(c, clock));
  return bulkReply(lines.length > 0 ? lines.join('\n') + '\n' : '');
}

export function clientInfo(
  client: ClientState | undefined,
  clock: () => number
): Reply {
  if (!client) {
    return bulkReply('');
  }
  return bulkReply(formatClientInfo(client, clock) + '\n');
}

export function clientKill(
  clientStore: ClientStateStore | undefined,
  callingClient: ClientState | undefined,
  args: string[]
): Reply {
  if (args.length === 0) {
    return errorReply(
      'ERR',
      "wrong number of arguments for 'client|kill' command"
    );
  }

  // Old-style: CLIENT KILL addr:port
  if (args.length === 1) {
    // In our emulator, we don't have real addresses, so always return error
    return errorReply('ERR', 'No such client');
  }

  // New-style: CLIENT KILL [ID id] [ADDR addr] [LADDR addr] [USER user] [SKIPME yes|no]
  let killed = 0;
  let targetId: number | null = null;
  let skipMe = true;

  for (let i = 0; i < args.length; i += 2) {
    const filter = (args[i] ?? '').toUpperCase();
    const value = args[i + 1];

    if (value === undefined) {
      return errorReply('ERR', 'syntax error');
    }

    switch (filter) {
      case 'ID': {
        const n = parseInt(value, 10);
        if (isNaN(n)) {
          return errorReply(
            'ERR',
            'client-id is not an integer or out of range'
          );
        }
        targetId = n;
        break;
      }
      case 'SKIPME': {
        const lower = value.toLowerCase();
        if (lower === 'yes') skipMe = true;
        else if (lower === 'no') skipMe = false;
        else return errorReply('ERR', 'syntax error');
        break;
      }
      case 'ADDR':
      case 'LADDR':
      case 'USER':
      case 'MAXAGE':
        // Accepted filters but not applicable in emulator
        break;
      default:
        return errorReply('ERR', `syntax error`);
    }
  }

  if (clientStore && targetId !== null) {
    const target = clientStore.get(targetId);
    if (target) {
      const isMe =
        callingClient !== undefined && target.id === callingClient.id;
      if (!isMe || !skipMe) {
        clientStore.remove(targetId);
        killed++;
      }
    }
  }

  return integerReply(killed);
}

export function clientPause(): Reply {
  return OK;
}

export function clientUnpause(): Reply {
  return OK;
}

export function clientReply(args: string[]): Reply {
  const mode = (args[0] ?? '').toUpperCase();
  if (mode !== 'ON' && mode !== 'OFF' && mode !== 'SKIP') {
    return errorReply('ERR', 'syntax error');
  }
  return OK;
}

export function clientNoEvict(
  client: ClientState | undefined,
  args: string[]
): Reply {
  const flag = (args[0] ?? '').toUpperCase();
  if (flag !== 'ON' && flag !== 'OFF') {
    return errorReply('ERR', 'syntax error');
  }
  if (client) {
    client.noEvict = flag === 'ON';
  }
  return OK;
}

export function clientNoTouch(
  client: ClientState | undefined,
  args: string[]
): Reply {
  const flag = (args[0] ?? '').toUpperCase();
  if (flag !== 'ON' && flag !== 'OFF') {
    return errorReply('ERR', 'syntax error');
  }
  if (client) {
    client.noTouch = flag === 'ON';
  }
  return OK;
}

export function clientTracking(
  client: ClientState | undefined,
  clientStore: ClientStateStore | undefined,
  args: string[]
): Reply {
  if (args.length === 0) {
    return errorReply(
      'ERR',
      "wrong number of arguments for 'client|tracking' command"
    );
  }

  const toggle = (args[0] ?? '').toUpperCase();

  if (toggle === 'OFF') {
    if (client) {
      client.tracking = false;
      client.trackingMode = null;
      client.trackingRedirect = 0;
      client.trackingPrefixes = [];
      client.trackingNoloop = false;
    }
    return OK;
  }

  if (toggle !== 'ON') {
    return errorReply('ERR', 'Invalid TRACKING option, must be ON or OFF');
  }

  // Parse options
  let redirect = 0;
  let bcast = false;
  let optin = false;
  let optout = false;
  let noloop = false;
  const prefixes: string[] = [];

  for (let i = 1; i < args.length; i++) {
    const opt = (args[i] ?? '').toUpperCase();
    switch (opt) {
      case 'REDIRECT': {
        const val = args[++i];
        if (val === undefined) {
          return errorReply('ERR', 'syntax error');
        }
        const n = parseInt(val, 10);
        if (isNaN(n)) {
          return errorReply('ERR', 'value is not an integer or out of range');
        }
        if (n <= 0) {
          return errorReply('ERR', 'Invalid client ID for REDIRECT option');
        }
        if (clientStore && !clientStore.has(n)) {
          return errorReply(
            'ERR',
            'The client ID you want redirect to does not exist'
          );
        }
        redirect = n;
        break;
      }
      case 'BCAST':
        bcast = true;
        break;
      case 'PREFIX': {
        const val = args[++i];
        if (val === undefined) {
          return errorReply('ERR', 'syntax error');
        }
        prefixes.push(val);
        break;
      }
      case 'OPTIN':
        optin = true;
        break;
      case 'OPTOUT':
        optout = true;
        break;
      case 'NOLOOP':
        noloop = true;
        break;
      default:
        return errorReply('ERR', `Unrecognized option '${args[i]}'`);
    }
  }

  // Validation
  if (optin && optout) {
    return errorReply(
      'ERR',
      "You can't use both OPTIN and OPTOUT at the same time"
    );
  }

  if (prefixes.length > 0 && !bcast) {
    return errorReply('ERR', 'PREFIX option requires BCAST mode to be enabled');
  }

  // RESP2 mode: require REDIRECT unless BCAST
  if (redirect === 0 && !bcast) {
    // In RESP2, tracking without REDIRECT requires BCAST mode
    // For our emulator, we allow it (like RESP3 mode)
  }

  if (client) {
    client.tracking = true;
    let mode: TrackingMode = 'normal';
    if (bcast) mode = 'bcast';
    else if (optin) mode = 'optin';
    else if (optout) mode = 'optout';
    client.trackingMode = mode;
    client.trackingRedirect = redirect;
    client.trackingPrefixes = prefixes;
    client.trackingNoloop = noloop;
  }

  return OK;
}

export function clientCaching(
  client: ClientState | undefined,
  args: string[]
): Reply {
  const mode = (args[0] ?? '').toUpperCase();
  if (mode !== 'YES' && mode !== 'NO') {
    return errorReply('ERR', 'syntax error');
  }

  if (!client || !client.tracking) {
    return errorReply(
      'ERR',
      'CLIENT CACHING can be called only when the client is in tracking mode with OPTIN or OPTOUT mode enabled'
    );
  }

  if (mode === 'YES' && client.trackingMode !== 'optin') {
    return errorReply(
      'ERR',
      'CLIENT CACHING YES is only valid when tracking is enabled in OPTIN mode.'
    );
  }

  if (mode === 'NO' && client.trackingMode !== 'optout') {
    return errorReply(
      'ERR',
      'CLIENT CACHING NO is only valid when tracking is enabled in OPTOUT mode.'
    );
  }

  return OK;
}

export function clientTrackinginfo(client: ClientState | undefined): Reply {
  if (!client) {
    return arrayReply([
      bulkReply('flags'),
      arrayReply([bulkReply('off')]),
      bulkReply('redirect'),
      integerReply(0),
      bulkReply('prefixes'),
      arrayReply([]),
    ]);
  }

  const flags: Reply[] = [];
  if (!client.tracking) {
    flags.push(bulkReply('off'));
  } else {
    flags.push(bulkReply('on'));
    if (client.trackingMode === 'bcast') flags.push(bulkReply('bcast'));
    if (client.trackingMode === 'optin') flags.push(bulkReply('optin'));
    if (client.trackingMode === 'optout') flags.push(bulkReply('optout'));
    if (client.trackingNoloop) flags.push(bulkReply('noloop'));
  }

  const prefixes = client.trackingPrefixes.map((p) => bulkReply(p));

  return arrayReply([
    bulkReply('flags'),
    arrayReply(flags),
    bulkReply('redirect'),
    integerReply(client.trackingRedirect),
    bulkReply('prefixes'),
    arrayReply(prefixes),
  ]);
}

export function clientGetredir(client: ClientState | undefined): Reply {
  if (!client || !client.tracking) {
    return integerReply(-1);
  }
  return integerReply(client.trackingRedirect);
}

const CLIENT_HELP_LINES = [
  'CLIENT <subcommand> [<arg> [value] [opt] ...]. subcommands are:',
  'CACHING (YES|NO)',
  '    Instruct the server whether to track or not the keys in the next command.',
  'GETNAME',
  '    Return the name of the current connection.',
  'GETREDIR',
  '    Return the client ID of the tracking notification redirection.',
  'ID',
  '    Return the ID of the current connection.',
  'INFO',
  '    Return information about the current client connection.',
  'KILL <option> ...',
  '    Kill connections. Options are:',
  '    ADDR (<ip>:<port>|<unixsocket>:0)',
  '        Kill the connection at <ip>:<port>.',
  '    ID <client-id>',
  '        Kill the connection with <client-id>.',
  '    USER <username>',
  '        Kill connections authenticated by <username>.',
  '    SKIPME (YES|NO)',
  '        Skip the calling client.',
  'LIST [TYPE (NORMAL|MASTER|REPLICA|PUBSUB)] [ID <id> [<id> ...]]',
  '    Return information about client connections.',
  'NO-EVICT (ON|OFF)',
  '    Set client eviction mode for the current connection.',
  'NO-TOUCH (ON|OFF)',
  '    Set client no-touch mode for the current connection.',
  'PAUSE <timeout> [WRITE|ALL]',
  '    Suspend all Redis clients.',
  'REPLY (ON|OFF|SKIP)',
  '    Control the replies sent to the current connection.',
  'SETNAME <name>',
  '    Set the name of the current connection.',
  'TRACKING (ON|OFF) [REDIRECT <id>] [BCAST] [PREFIX <prefix>] [OPTIN] [OPTOUT] [NOLOOP]',
  '    Enable or disable server-assisted client-side caching.',
  'TRACKINGINFO',
  '    Return the tracking info of the current connection.',
  'UNPAUSE',
  '    Resume processing of all paused clients.',
  'HELP',
  '    Print this help.',
];

export function clientHelp(): Reply {
  return arrayReply(CLIENT_HELP_LINES.map((l) => bulkReply(l)));
}

// --- Main dispatch ---

export function client(ctx: CommandContext, args: string[]): Reply {
  if (args.length === 0) {
    return unknownSubcommandError('client', '');
  }

  const sub = args[0] ?? '';
  const subUpper = sub.toUpperCase();
  const rest = args.slice(1);

  switch (subUpper) {
    case 'ID':
      return clientId(ctx.client);
    case 'GETNAME':
      return clientGetname(ctx.client);
    case 'SETNAME':
      if (rest.length !== 1) {
        return errorReply(
          'ERR',
          "wrong number of arguments for 'client|setname' command"
        );
      }
      return clientSetname(ctx.client, rest);
    case 'LIST':
      return clientList(ctx.clientStore, ctx.client, ctx.engine.clock, rest);
    case 'INFO':
      return clientInfo(ctx.client, ctx.engine.clock);
    case 'KILL':
      return clientKill(ctx.clientStore, ctx.client, rest);
    case 'PAUSE':
      return clientPause();
    case 'UNPAUSE':
      return clientUnpause();
    case 'REPLY':
      if (rest.length !== 1) {
        return errorReply(
          'ERR',
          "wrong number of arguments for 'client|reply' command"
        );
      }
      return clientReply(rest);
    case 'NO-EVICT':
      if (rest.length !== 1) {
        return errorReply(
          'ERR',
          "wrong number of arguments for 'client|no-evict' command"
        );
      }
      return clientNoEvict(ctx.client, rest);
    case 'NO-TOUCH':
      if (rest.length !== 1) {
        return errorReply(
          'ERR',
          "wrong number of arguments for 'client|no-touch' command"
        );
      }
      return clientNoTouch(ctx.client, rest);
    case 'TRACKING':
      return clientTracking(ctx.client, ctx.clientStore, rest);
    case 'CACHING':
      if (rest.length !== 1) {
        return errorReply(
          'ERR',
          "wrong number of arguments for 'client|caching' command"
        );
      }
      return clientCaching(ctx.client, rest);
    case 'TRACKINGINFO':
      return clientTrackinginfo(ctx.client);
    case 'GETREDIR':
      return clientGetredir(ctx.client);
    case 'HELP':
      return clientHelp();
    default:
      return unknownSubcommandError('client', sub.toLowerCase());
  }
}

export const specs: CommandSpec[] = [
  {
    name: 'client',
    handler: (ctx, args) => client(ctx, args),
    arity: -2,
    flags: ['admin', 'noscript', 'loading', 'stale'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@admin', '@slow', '@connection'],
    subcommands: [
      {
        name: 'id',
        handler: (ctx) => clientId(ctx.client),
        arity: 2,
        flags: ['fast', 'loading', 'stale'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@slow', '@connection'],
      },
      {
        name: 'getname',
        handler: (ctx) => clientGetname(ctx.client),
        arity: 2,
        flags: ['fast', 'loading', 'stale'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@slow', '@connection'],
      },
      {
        name: 'setname',
        handler: (ctx, args) => clientSetname(ctx.client, args.slice(1)),
        arity: 3,
        flags: ['fast', 'loading', 'stale'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@slow', '@connection'],
      },
      {
        name: 'list',
        handler: (ctx, args) =>
          clientList(
            ctx.clientStore,
            ctx.client,
            ctx.engine.clock,
            args.slice(1)
          ),
        arity: -2,
        flags: ['admin', 'loading', 'stale'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@admin', '@slow', '@connection'],
      },
      {
        name: 'info',
        handler: (ctx) => clientInfo(ctx.client, ctx.engine.clock),
        arity: 2,
        flags: ['loading', 'stale'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@slow', '@connection'],
      },
      {
        name: 'kill',
        handler: (ctx, args) =>
          clientKill(ctx.clientStore, ctx.client, args.slice(1)),
        arity: -3,
        flags: ['admin', 'loading', 'stale'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@admin', '@slow', '@connection'],
      },
      {
        name: 'pause',
        handler: () => clientPause(),
        arity: -3,
        flags: ['admin', 'loading', 'stale'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@admin', '@slow', '@connection'],
      },
      {
        name: 'unpause',
        handler: () => clientUnpause(),
        arity: 2,
        flags: ['admin', 'loading', 'stale'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@admin', '@slow', '@connection'],
      },
      {
        name: 'reply',
        handler: (ctx, args) => clientReply(args.slice(1)),
        arity: 3,
        flags: ['fast', 'loading', 'stale'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@slow', '@connection'],
      },
      {
        name: 'no-evict',
        handler: (ctx, args) => clientNoEvict(ctx.client, args.slice(1)),
        arity: 3,
        flags: ['admin', 'loading', 'stale'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@admin', '@slow', '@connection'],
      },
      {
        name: 'no-touch',
        handler: (ctx, args) => clientNoTouch(ctx.client, args.slice(1)),
        arity: 3,
        flags: ['admin', 'loading', 'stale'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@admin', '@slow', '@connection'],
      },
      {
        name: 'tracking',
        handler: (ctx, args) =>
          clientTracking(ctx.client, ctx.clientStore, args.slice(1)),
        arity: -3,
        flags: ['loading', 'stale'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@slow', '@connection'],
      },
      {
        name: 'caching',
        handler: (ctx, args) => clientCaching(ctx.client, args.slice(1)),
        arity: 3,
        flags: ['loading', 'stale'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@slow', '@connection'],
      },
      {
        name: 'trackinginfo',
        handler: (ctx) => clientTrackinginfo(ctx.client),
        arity: 2,
        flags: ['loading', 'stale'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@slow', '@connection'],
      },
      {
        name: 'getredir',
        handler: (ctx) => clientGetredir(ctx.client),
        arity: 2,
        flags: ['loading', 'stale'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@slow', '@connection'],
      },
      {
        name: 'help',
        handler: () => clientHelp(),
        arity: 2,
        flags: ['loading', 'stale'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@slow', '@connection'],
      },
    ],
  },
];
