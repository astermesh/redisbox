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
// Helper: validate auth credentials
// ---------------------------------------------------------------------------

function validateAuth(
  ctx: CommandContext,
  username: string,
  password: string
): Reply | null {
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
