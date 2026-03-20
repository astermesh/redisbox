import type { Reply, CommandContext } from '../types.ts';
import {
  statusReply,
  bulkReply,
  integerReply,
  arrayReply,
  errorReply,
  wrongArityError,
  OK,
  EMPTY_ARRAY,
} from '../types.ts';
import type { CommandSpec } from '../command-table.ts';

const PONG: Reply = statusReply('PONG');
const RESET_REPLY: Reply = statusReply('RESET');

const WRONGPASS_ERR = errorReply(
  'WRONGPASS',
  'invalid username-password pair or user is disabled.'
);

const NO_PASSWORD_ERR = errorReply(
  'ERR',
  'Client sent AUTH, but no password is set. Did you mean ACL SETUSER with >password?'
);

export function ping(args: string[]): Reply {
  if (args.length > 1) return wrongArityError('ping');
  if (args.length === 0) return PONG;
  return bulkReply(args[0] ?? '');
}

export function echo(args: string[]): Reply {
  return bulkReply(args[0] ?? '');
}

export function quit(): Reply {
  return OK;
}

export function reset(): Reply {
  return RESET_REPLY;
}

// ---------------------------------------------------------------------------
// Helper: get requirepass from config
// ---------------------------------------------------------------------------

function getRequirePass(ctx: CommandContext): string {
  if (!ctx.config) return '';
  const result = ctx.config.get('requirepass');
  return result[1] ?? '';
}

// ---------------------------------------------------------------------------
// Helper: sync ACL store with requirepass config and validate credentials
// ---------------------------------------------------------------------------

function syncAcl(ctx: CommandContext): void {
  if (ctx.acl) {
    ctx.acl.syncRequirePass(getRequirePass(ctx));
  }
}

function validateAuth(
  ctx: CommandContext,
  username: string,
  password: string
): Reply | null {
  syncAcl(ctx);

  if (ctx.acl) {
    const user = ctx.acl.getUser(username);

    if (!user || !user.enabled || !user.validatePassword(password)) {
      return WRONGPASS_ERR;
    }

    if (ctx.client) {
      ctx.client.authenticated = true;
      ctx.client.username = username;
    }
    return null;
  }

  // Legacy fallback (no ACL store)
  const requirepass = getRequirePass(ctx);

  if (!requirepass) {
    return NO_PASSWORD_ERR;
  }

  if (username !== 'default' || password !== requirepass) {
    return WRONGPASS_ERR;
  }

  if (ctx.client) {
    ctx.client.authenticated = true;
  }
  return null;
}

// ---------------------------------------------------------------------------
// HELLO
// ---------------------------------------------------------------------------

function buildHelloResponse(clientId: number): Reply {
  return arrayReply([
    bulkReply('server'),
    bulkReply('redis'),
    bulkReply('version'),
    bulkReply('7.2.0'),
    bulkReply('proto'),
    integerReply(2),
    bulkReply('id'),
    integerReply(clientId),
    bulkReply('mode'),
    bulkReply('standalone'),
    bulkReply('role'),
    bulkReply('master'),
    bulkReply('modules'),
    EMPTY_ARRAY,
  ]);
}

export function hello(ctx: CommandContext, args: string[]): Reply {
  let i = 0;

  // Parse optional protocol version — if any args are present, the first
  // one MUST be a protocol version number (Redis 7.x behaviour).
  if (i < args.length) {
    const versionStr = args[i] ?? '';
    const version = Number(versionStr);

    if (!Number.isInteger(version) || versionStr === '') {
      return errorReply(
        'ERR',
        'Protocol version is not an integer or out of range'
      );
    }

    if (version === 3) {
      return errorReply(
        'NOPROTO',
        'sorry, this protocol version is not supported'
      );
    }

    if (version !== 2) {
      return errorReply('NOPROTO', 'unsupported protocol version');
    }

    i++;
  }

  // Parse options: AUTH and SETNAME
  let authError: Reply | null = null;
  let clientName: string | undefined;

  while (i < args.length) {
    const option = (args[i] ?? '').toUpperCase();

    if (option === 'AUTH') {
      if (i + 2 >= args.length) {
        return errorReply('ERR', "Syntax error in HELLO option 'AUTH'");
      }
      const username = args[i + 1] ?? '';
      const password = args[i + 2] ?? '';
      authError = validateAuth(ctx, username, password);
      i += 3;
    } else if (option === 'SETNAME') {
      if (i + 1 >= args.length) {
        return errorReply('ERR', "Syntax error in HELLO option 'SETNAME'");
      }
      clientName = args[i + 1] ?? '';
      i += 2;
    } else {
      return errorReply('ERR', `Unrecognized HELLO option: ${option}`);
    }
  }

  // If AUTH failed, return auth error without applying SETNAME
  if (authError) {
    return authError;
  }

  // Apply SETNAME
  if (clientName !== undefined && ctx.client) {
    ctx.client.name = clientName;
  }

  const clientId = ctx.client?.id ?? 0;
  return buildHelloResponse(clientId);
}

// ---------------------------------------------------------------------------
// AUTH
// ---------------------------------------------------------------------------

export function auth(ctx: CommandContext, args: string[]): Reply {
  let username = 'default';
  let password: string;

  if (args.length === 1) {
    password = args[0] ?? '';
  } else {
    username = args[0] ?? '';
    password = args[1] ?? '';
  }

  syncAcl(ctx);

  if (ctx.acl) {
    // Old-style AUTH <password>: if default user has nopass, return
    // "no password set" error — matches real Redis short-circuit.
    if (args.length === 1) {
      const defaultUser = ctx.acl.getDefaultUser();
      if (defaultUser.nopass) {
        return NO_PASSWORD_ERR;
      }
    }

    const user = ctx.acl.getUser(username);

    if (!user || !user.enabled || !user.validatePassword(password)) {
      if (ctx.client) {
        ctx.client.authenticated = false;
      }
      return WRONGPASS_ERR;
    }

    if (ctx.client) {
      ctx.client.authenticated = true;
      ctx.client.username = username;
    }
    return OK;
  }

  // Legacy fallback (no ACL store)
  const requirepass = getRequirePass(ctx);

  if (!requirepass) {
    return NO_PASSWORD_ERR;
  }

  if (username !== 'default' || password !== requirepass) {
    if (ctx.client) {
      ctx.client.authenticated = false;
    }
    return WRONGPASS_ERR;
  }

  if (ctx.client) {
    ctx.client.authenticated = true;
  }
  return OK;
}

export const specs: CommandSpec[] = [
  {
    name: 'ping',
    handler: (_ctx, args) => ping(args),
    arity: -1,
    flags: ['fast', 'stale', 'loading'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@fast', '@connection'],
  },
  {
    name: 'echo',
    handler: (_ctx, args) => echo(args),
    arity: 2,
    flags: ['fast', 'stale', 'loading'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@fast', '@connection'],
  },
  {
    name: 'quit',
    handler: () => quit(),
    arity: -1,
    flags: ['fast', 'noscript', 'stale', 'loading', 'noauth'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@fast', '@connection'],
  },
  {
    name: 'reset',
    handler: () => reset(),
    arity: 1,
    flags: ['fast', 'noscript', 'loading', 'stale', 'noauth'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@fast', '@connection'],
  },
  {
    name: 'hello',
    handler: (ctx, args) => hello(ctx, args),
    arity: -1,
    flags: ['fast', 'loading', 'stale', 'noauth'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@fast', '@connection'],
  },
  {
    name: 'auth',
    handler: (ctx, args) => auth(ctx, args),
    arity: -2,
    flags: ['fast', 'loading', 'stale', 'noauth', 'noscript'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@fast', '@connection'],
  },
];
