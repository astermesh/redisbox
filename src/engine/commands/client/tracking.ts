import type { Reply } from '../../types.ts';
import {
  bulkReply,
  integerReply,
  arrayReply,
  errorReply,
  OK,
} from '../../types.ts';
import type {
  ClientState,
  ClientStateStore,
  TrackingMode,
} from '../../../server/client-state.ts';

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
