import type { Reply } from '../../types.ts';
import { bulkReply, arrayReply } from '../../types.ts';
import type { CommandSpec } from '../../command-table.ts';
import type { AclUser } from '../../acl-store.ts';
import { sha256 } from '../../sha256.ts';

// ---------------------------------------------------------------------------
// Redis ACL categories — the full set returned by ACL CAT in Redis 7.x
// ---------------------------------------------------------------------------

export const ACL_CATEGORIES = [
  'keyspace',
  'read',
  'write',
  'set',
  'sortedset',
  'list',
  'hash',
  'string',
  'bitmap',
  'hyperloglog',
  'geo',
  'stream',
  'pubsub',
  'admin',
  'fast',
  'slow',
  'blocking',
  'dangerous',
  'connection',
  'transaction',
  'scripting',
  'generic',
].sort();

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

export function getAcl(ctx: import('../../types.ts').CommandContext) {
  return ctx.acl;
}

/** Format a user's flags array for ACL GETUSER. */
export function userFlags(user: AclUser): Reply {
  const flags: string[] = [];
  flags.push(user.enabled ? 'on' : 'off');
  if (user.allKeys) flags.push('allkeys');
  if (user.allChannels) flags.push('allchannels');
  if (user.allCommands) flags.push('allcommands');
  if (user.nopass) flags.push('nopass');
  return arrayReply(flags.map((f) => bulkReply(f)));
}

/** Format a user's password hashes for ACL GETUSER (each prefixed with #). */
export function userPasswordHashes(user: AclUser): Reply {
  const passwords = user.getPasswords();
  return arrayReply(passwords.map((p) => bulkReply(`#${sha256(p)}`)));
}

/** Format the commands string for ACL GETUSER / ACL LIST. */
export function userCommandsString(user: AclUser): string {
  if (user.allCommands) return '+@all';
  return '-@all';
}

/** Format keys pattern for ACL GETUSER / ACL LIST. */
export function userKeysString(user: AclUser): string {
  if (user.allKeys) return '~*';
  return '';
}

/** Format channels pattern for ACL GETUSER / ACL LIST. */
export function userChannelsString(user: AclUser): string {
  if (user.allChannels) return '&*';
  return '';
}

/** Format a user for ACL LIST output. */
export function formatUserForList(user: AclUser): string {
  const parts: string[] = ['user', user.username];

  parts.push(user.enabled ? 'on' : 'off');

  // Passwords
  const passwords = user.getPasswords();
  if (user.nopass) {
    parts.push('nopass');
  } else if (passwords.length === 0) {
    parts.push('resetpass');
  } else {
    for (const p of passwords) {
      parts.push(`#${sha256(p)}`);
    }
  }

  // Keys
  if (user.allKeys) {
    parts.push('~*');
  } else {
    parts.push('resetkeys');
  }

  // Channels
  if (user.allChannels) {
    parts.push('&*');
  } else {
    parts.push('resetchannels');
  }

  // Commands
  parts.push(userCommandsString(user));

  return parts.join(' ');
}

export function makeSubSpec(
  name: string,
  handler: CommandSpec['handler'],
  arity: number
): CommandSpec {
  return {
    name,
    handler,
    arity,
    flags: ['admin', 'loading', 'stale'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@admin', '@slow', '@dangerous'],
  };
}
