import type { Reply, CommandContext } from '../types.ts';
import {
  bulkReply,
  integerReply,
  arrayReply,
  errorReply,
  OK,
  EMPTY_ARRAY,
  unknownSubcommandError,
} from '../types.ts';

// --- CRC16-CCITT lookup table ---

const CRC16_TABLE = new Uint16Array(256);

(function buildTable() {
  for (let i = 0; i < 256; i++) {
    let crc = i << 8;
    for (let j = 0; j < 8; j++) {
      if (crc & 0x8000) {
        crc = ((crc << 1) ^ 0x1021) & 0xffff;
      } else {
        crc = (crc << 1) & 0xffff;
      }
    }
    CRC16_TABLE[i] = crc;
  }
})();

/**
 * Compute CRC16-CCITT for a string (same algorithm as Redis).
 */
function crc16(data: string): number {
  let crc = 0;
  for (let i = 0; i < data.length; i++) {
    const idx = ((crc >> 8) ^ data.charCodeAt(i)) & 0xff;
    crc = ((crc << 8) ^ (CRC16_TABLE[idx] ?? 0)) & 0xffff;
  }
  return crc;
}

/**
 * Extract the hash tag from a key (content between first { and next }).
 * If no valid hash tag exists, the entire key is used.
 */
function extractHashTag(key: string): string {
  const start = key.indexOf('{');
  if (start === -1) return key;
  const end = key.indexOf('}', start + 1);
  if (end === -1 || end === start + 1) return key;
  return key.substring(start + 1, end);
}

/**
 * Compute the hash slot for a key (0-16383).
 */
export function keySlot(key: string): number {
  return crc16(extractHashTag(key)) & 16383;
}

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
  if (isNaN(slot) || slot < 0 || slot > 16383) {
    return errorReply('ERR', 'Invalid or out of range slot');
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
  if (isNaN(slot) || slot < 0 || slot > 16383) {
    return errorReply('ERR', 'Invalid or out of range slot');
  }
  const count = parseInt(args[1] ?? '', 10);
  if (isNaN(count) || count < 0) {
    return errorReply('ERR', 'value is not an integer or out of range');
  }
  const result: Reply[] = [];
  for (const key of ctx.db.keys()) {
    if (result.length >= count) break;
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
