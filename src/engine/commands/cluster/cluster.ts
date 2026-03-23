import type { Reply, CommandContext } from '../../types.ts';
import type { CommandSpec } from '../../command-table.ts';
import {
  bulkReply,
  integerReply,
  arrayReply,
  errorReply,
  OK,
  EMPTY_ARRAY,
  unknownSubcommandError,
} from '../../types.ts';
import { keySlot } from './keyslot.ts';

// --- Consistent node ID (40-char hex, generated once per engine) ---

const NODE_ID = '0'.repeat(40);

// --- CLUSTER INFO response ---

const CLUSTER_INFO_LINES = [
  'cluster_state:ok',
  'cluster_slots_assigned:0',
  'cluster_slots_ok:0',
  'cluster_slots_pfail:0',
  'cluster_slots_fail:0',
  'cluster_known_nodes:0',
  'cluster_size:0',
  'cluster_current_epoch:0',
  'cluster_my_epoch:0',
  'cluster_stats_messages_sent:0',
  'cluster_stats_messages_received:0',
  'total_cluster_links_buffer_limit_exceeded:0',
].join('\r\n');

// --- Subcommand implementations ---

export function clusterInfo(): Reply {
  return bulkReply(CLUSTER_INFO_LINES);
}

export function clusterMyid(): Reply {
  return bulkReply(NODE_ID);
}

export function clusterKeyslot(args: string[]): Reply {
  if (args.length !== 1) {
    return errorReply(
      'ERR',
      "wrong number of arguments for 'cluster|keyslot' command"
    );
  }
  return integerReply(keySlot(args[0] ?? ''));
}

export function clusterNodes(): Reply {
  return bulkReply(`${NODE_ID} :0@0 myself,master - 0 0 0 connected 0-16383\n`);
}

export function clusterSlots(): Reply {
  return EMPTY_ARRAY;
}

export function clusterShards(): Reply {
  return EMPTY_ARRAY;
}

export function clusterCountkeysinslot(
  ctx: CommandContext,
  args: string[]
): Reply {
  if (args.length !== 1) {
    return errorReply(
      'ERR',
      "wrong number of arguments for 'cluster|countkeysinslot' command"
    );
  }
  const slot = parseInt(args[0] ?? '', 10);
  if (isNaN(slot)) {
    return errorReply('ERR', 'value is not an integer or out of range');
  }
  if (slot < 0 || slot > 16383) {
    return errorReply('ERR', 'Invalid slot');
  }
  let count = 0;
  for (const key of ctx.db.keys()) {
    if (keySlot(key) === slot) count++;
  }
  return integerReply(count);
}

export function clusterGetkeysinslot(
  ctx: CommandContext,
  args: string[]
): Reply {
  if (args.length !== 2) {
    return errorReply(
      'ERR',
      "wrong number of arguments for 'cluster|getkeysinslot' command"
    );
  }
  const slot = parseInt(args[0] ?? '', 10);
  const maxkeys = parseInt(args[1] ?? '', 10);
  if (isNaN(slot) || isNaN(maxkeys)) {
    return errorReply('ERR', 'value is not an integer or out of range');
  }
  if (slot < 0 || slot > 16383 || maxkeys < 0) {
    return errorReply('ERR', 'Invalid slot or number of keys');
  }
  const result: Reply[] = [];
  for (const key of ctx.db.keys()) {
    if (result.length >= maxkeys) break;
    if (keySlot(key) === slot) {
      result.push(bulkReply(key));
    }
  }
  return arrayReply(result);
}

export function clusterReset(args: string[]): Reply {
  if (args.length > 1) {
    return errorReply(
      'ERR',
      "wrong number of arguments for 'cluster|reset' command"
    );
  }
  if (args.length === 1) {
    const mode = (args[0] ?? '').toUpperCase();
    if (mode !== 'HARD' && mode !== 'SOFT') {
      return errorReply('ERR', 'syntax error');
    }
  }
  return OK;
}

const CLUSTER_HELP_LINES = [
  'CLUSTER <subcommand> [<arg> [value] [opt] ...]. subcommands are:',
  'COUNTKEYSINSLOT <slot>',
  '    Return the number of keys in <slot>.',
  'GETKEYSINSLOT <slot> <count>',
  '    Return key names stored by current node in a slot.',
  'INFO',
  '    Return information about the cluster.',
  'KEYSLOT <key>',
  '    Return the hash slot for <key>.',
  'MYID',
  '    Return the node id.',
  'NODES',
  '    Return cluster configuration of nodes.',
  'RESET [HARD|SOFT]',
  '    Reset a node.',
  'SHARDS',
  '    Return information about slot allocation for each shard.',
  'SLOTS',
  '    Return information about slots allocation.',
  'HELP',
  '    Print this help.',
];

export function clusterHelp(): Reply {
  return arrayReply(CLUSTER_HELP_LINES.map((l) => bulkReply(l)));
}

// --- Stub subcommands ---

export function clusterSetslot(): Reply {
  return OK;
}

export function clusterAddslots(): Reply {
  return OK;
}

export function clusterDelslots(): Reply {
  return OK;
}

export function clusterFlushslots(): Reply {
  return OK;
}

export function clusterFailover(): Reply {
  return OK;
}

export function clusterReplicate(): Reply {
  return OK;
}

export function clusterSaveconfig(): Reply {
  return OK;
}

export function clusterSetConfigEpoch(): Reply {
  return OK;
}

export function clusterMeet(): Reply {
  return OK;
}

export function clusterForget(): Reply {
  return OK;
}

export function clusterLinks(): Reply {
  return EMPTY_ARRAY;
}

// --- Main dispatch ---

export function cluster(ctx: CommandContext, args: string[]): Reply {
  if (args.length === 0) {
    return unknownSubcommandError('cluster', '');
  }

  const sub = args[0] ?? '';
  const subUpper = sub.toUpperCase();
  const rest = args.slice(1);

  switch (subUpper) {
    case 'INFO':
      return clusterInfo();
    case 'MYID':
      return clusterMyid();
    case 'KEYSLOT':
      return clusterKeyslot(rest);
    case 'NODES':
      return clusterNodes();
    case 'SLOTS':
      return clusterSlots();
    case 'SHARDS':
      return clusterShards();
    case 'COUNTKEYSINSLOT':
      return clusterCountkeysinslot(ctx, rest);
    case 'GETKEYSINSLOT':
      return clusterGetkeysinslot(ctx, rest);
    case 'RESET':
      return clusterReset(rest);
    case 'HELP':
      return clusterHelp();
    case 'SETSLOT':
      return clusterSetslot();
    case 'ADDSLOTS':
      return clusterAddslots();
    case 'DELSLOTS':
      return clusterDelslots();
    case 'FLUSHSLOTS':
      return clusterFlushslots();
    case 'FAILOVER':
      return clusterFailover();
    case 'REPLICATE':
      return clusterReplicate();
    case 'SAVECONFIG':
      return clusterSaveconfig();
    case 'SET-CONFIG-EPOCH':
      return clusterSetConfigEpoch();
    case 'MEET':
      return clusterMeet();
    case 'FORGET':
      return clusterForget();
    case 'LINKS':
      return clusterLinks();
    default:
      return unknownSubcommandError('cluster', sub.toLowerCase());
  }
}

export const specs: CommandSpec[] = [
  {
    name: 'cluster',
    handler: (ctx, args) => cluster(ctx, args),
    arity: -2,
    flags: ['admin'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@admin', '@slow'],
    subcommands: [
      {
        name: 'info',
        handler: () => clusterInfo(),
        arity: 2,
        flags: ['stale', 'loading'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@slow'],
      },
      {
        name: 'myid',
        handler: () => clusterMyid(),
        arity: 2,
        flags: ['stale', 'loading'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@slow'],
      },
      {
        name: 'keyslot',
        handler: (_ctx, args) => clusterKeyslot(args.slice(1)),
        arity: 3,
        flags: ['stale', 'loading'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@slow'],
      },
      {
        name: 'nodes',
        handler: () => clusterNodes(),
        arity: 2,
        flags: ['stale', 'loading'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@slow'],
      },
      {
        name: 'slots',
        handler: () => clusterSlots(),
        arity: 2,
        flags: ['stale', 'loading'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@slow'],
      },
      {
        name: 'shards',
        handler: () => clusterShards(),
        arity: 2,
        flags: ['stale', 'loading'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@slow'],
      },
      {
        name: 'countkeysinslot',
        handler: (ctx, args) => clusterCountkeysinslot(ctx, args.slice(1)),
        arity: 3,
        flags: ['stale', 'loading'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@slow'],
      },
      {
        name: 'getkeysinslot',
        handler: (ctx, args) => clusterGetkeysinslot(ctx, args.slice(1)),
        arity: 4,
        flags: ['stale', 'loading'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@slow'],
      },
      {
        name: 'reset',
        handler: (_ctx, args) => clusterReset(args.slice(1)),
        arity: -2,
        flags: ['admin'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@admin', '@slow'],
      },
      {
        name: 'help',
        handler: () => clusterHelp(),
        arity: 2,
        flags: ['stale', 'loading'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@slow'],
      },
      {
        name: 'links',
        handler: () => clusterLinks(),
        arity: 2,
        flags: ['stale', 'loading'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@slow'],
      },
      {
        name: 'setslot',
        handler: () => clusterSetslot(),
        arity: -4,
        flags: ['admin'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@admin', '@slow'],
      },
      {
        name: 'addslots',
        handler: () => clusterAddslots(),
        arity: -3,
        flags: ['admin'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@admin', '@slow'],
      },
      {
        name: 'delslots',
        handler: () => clusterDelslots(),
        arity: -3,
        flags: ['admin'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@admin', '@slow'],
      },
      {
        name: 'flushslots',
        handler: () => clusterFlushslots(),
        arity: 2,
        flags: ['admin'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@admin', '@slow'],
      },
      {
        name: 'failover',
        handler: () => clusterFailover(),
        arity: -2,
        flags: ['admin'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@admin', '@slow'],
      },
      {
        name: 'replicate',
        handler: () => clusterReplicate(),
        arity: 3,
        flags: ['admin'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@admin', '@slow'],
      },
      {
        name: 'saveconfig',
        handler: () => clusterSaveconfig(),
        arity: 2,
        flags: ['admin'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@admin', '@slow'],
      },
      {
        name: 'set-config-epoch',
        handler: () => clusterSetConfigEpoch(),
        arity: 3,
        flags: ['admin'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@admin', '@slow'],
      },
      {
        name: 'meet',
        handler: () => clusterMeet(),
        arity: -4,
        flags: ['admin'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@admin', '@slow'],
      },
      {
        name: 'forget',
        handler: () => clusterForget(),
        arity: 3,
        flags: ['admin'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@admin', '@slow'],
      },
    ],
  },
];
