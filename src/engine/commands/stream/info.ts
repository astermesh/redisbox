import type { Database } from '../../database.ts';
import type { Reply } from '../../types.ts';
import type { CommandContext } from '../../types.ts';
import {
  arrayReply,
  bulkReply,
  errorReply,
  integerReply,
  SYNTAX_ERR,
} from '../../types.ts';
import { RedisStream, streamIdToString } from '../../stream.ts';
import { getStream, entryToReply } from './utils.ts';

function xinfoStream(db: Database, args: string[]): Reply {
  if (args.length < 1) {
    return errorReply(
      'ERR',
      "wrong number of arguments for 'xinfo|stream' command"
    );
  }

  const key = args[0] as string;
  const lookup = getStream(db, key);
  if (lookup.error) return lookup.error;
  if (!lookup.stream) {
    return errorReply('ERR', 'no such key');
  }

  const stream = lookup.stream;

  // Check for FULL option
  let full = false;
  let fullCount = 10; // default for FULL
  let i = 1;
  while (i < args.length) {
    const upper = (args[i] as string).toUpperCase();
    if (upper === 'FULL') {
      full = true;
      i++;
      if (i < args.length && (args[i] as string).toUpperCase() === 'COUNT') {
        i++;
        if (i >= args.length) return SYNTAX_ERR;
        const n = Number(args[i]);
        if (!Number.isInteger(n) || n < 0) {
          return errorReply('ERR', 'value is not an integer or out of range');
        }
        fullCount = n;
        i++;
      }
    } else {
      return SYNTAX_ERR;
    }
  }

  if (full) {
    return xinfoStreamFull(stream, fullCount);
  }

  // Standard XINFO STREAM response
  const firstEntry = stream.firstEntry();
  const lastEntry = stream.lastEntry();

  const result: Reply[] = [
    bulkReply('length'),
    integerReply(stream.length),
    bulkReply('radix-tree-keys'),
    integerReply(1),
    bulkReply('radix-tree-nodes'),
    integerReply(2),
    bulkReply('last-generated-id'),
    bulkReply(stream.lastIdString),
    bulkReply('max-deleted-entry-id'),
    bulkReply(streamIdToString(stream.maxDeletedEntryId)),
    bulkReply('entries-added'),
    integerReply(stream.entriesAdded),
    bulkReply('recorded-first-entry-id'),
    bulkReply(streamIdToString(stream.recordedFirstEntryId)),
    bulkReply('groups'),
    integerReply(stream.groups.size),
    bulkReply('first-entry'),
    firstEntry ? entryToReply(firstEntry) : bulkReply(null),
    bulkReply('last-entry'),
    lastEntry ? entryToReply(lastEntry) : bulkReply(null),
  ];

  return arrayReply(result);
}

function xinfoStreamFull(stream: RedisStream, count: number): Reply {
  const entries = stream.getEntries();
  const limitedEntries = count === 0 ? entries : entries.slice(0, count);

  const groupReplies: Reply[] = [];
  for (const [, group] of stream.groups) {
    const pelEntries: Reply[] = [];
    let pelCount = 0;
    for (const [, pe] of group.pel) {
      if (count > 0 && pelCount >= count) break;
      pelEntries.push(
        arrayReply([
          bulkReply(pe.entryId),
          bulkReply(pe.consumer),
          integerReply(pe.deliveryTime),
          integerReply(pe.deliveryCount),
        ])
      );
      pelCount++;
    }

    const consumerReplies: Reply[] = [];
    for (const [, consumer] of group.consumers) {
      const cPelEntries: Reply[] = [];
      let cPelCount = 0;
      for (const [, pe] of consumer.pending) {
        if (count > 0 && cPelCount >= count) break;
        cPelEntries.push(
          arrayReply([
            bulkReply(pe.entryId),
            integerReply(pe.deliveryTime),
            integerReply(pe.deliveryCount),
          ])
        );
        cPelCount++;
      }

      consumerReplies.push(
        arrayReply([
          bulkReply('name'),
          bulkReply(consumer.name),
          bulkReply('seen-time'),
          integerReply(consumer.seenTime),
          bulkReply('active-time'),
          integerReply(consumer.activeTime),
          bulkReply('pel-count'),
          integerReply(consumer.pending.size),
          bulkReply('pel'),
          arrayReply(cPelEntries),
        ])
      );
    }

    groupReplies.push(
      arrayReply([
        bulkReply('name'),
        bulkReply(group.name),
        bulkReply('last-delivered-id'),
        bulkReply(streamIdToString(group.lastDeliveredId)),
        bulkReply('entries-read'),
        integerReply(group.entriesRead),
        bulkReply('pel-count'),
        integerReply(group.pel.size),
        bulkReply('pel'),
        arrayReply(pelEntries),
        bulkReply('consumers'),
        arrayReply(consumerReplies),
      ])
    );
  }

  const result: Reply[] = [
    bulkReply('length'),
    integerReply(stream.length),
    bulkReply('radix-tree-keys'),
    integerReply(1),
    bulkReply('radix-tree-nodes'),
    integerReply(2),
    bulkReply('last-generated-id'),
    bulkReply(stream.lastIdString),
    bulkReply('max-deleted-entry-id'),
    bulkReply(streamIdToString(stream.maxDeletedEntryId)),
    bulkReply('entries-added'),
    integerReply(stream.entriesAdded),
    bulkReply('recorded-first-entry-id'),
    bulkReply(stream.firstEntry()?.id ?? '0-0'),
    bulkReply('entries'),
    arrayReply(limitedEntries.map(entryToReply)),
    bulkReply('groups'),
    arrayReply(groupReplies),
  ];

  return arrayReply(result);
}

function xinfoGroups(db: Database, args: string[]): Reply {
  if (args.length < 1) {
    return errorReply(
      'ERR',
      "wrong number of arguments for 'xinfo|groups' command"
    );
  }

  const key = args[0] as string;
  const lookup = getStream(db, key);
  if (lookup.error) return lookup.error;
  if (!lookup.stream) {
    return errorReply('ERR', 'no such key');
  }

  const stream = lookup.stream;
  const result: Reply[] = [];

  for (const [, group] of stream.groups) {
    result.push(
      arrayReply([
        bulkReply('name'),
        bulkReply(group.name),
        bulkReply('consumers'),
        integerReply(group.consumers.size),
        bulkReply('pending'),
        integerReply(group.pel.size),
        bulkReply('last-delivered-id'),
        bulkReply(streamIdToString(group.lastDeliveredId)),
        bulkReply('entries-read'),
        integerReply(group.entriesRead),
        bulkReply('lag'),
        integerReply(Math.max(0, stream.entriesAdded - group.entriesRead)),
      ])
    );
  }

  return arrayReply(result);
}

function xinfoConsumers(db: Database, clockMs: number, args: string[]): Reply {
  if (args.length < 2) {
    return errorReply(
      'ERR',
      "wrong number of arguments for 'xinfo|consumers' command"
    );
  }

  const key = args[0] as string;
  const groupName = args[1] as string;

  const lookup = getStream(db, key);
  if (lookup.error) return lookup.error;
  if (!lookup.stream) {
    return errorReply('ERR', 'no such key');
  }

  const group = lookup.stream.getGroup(groupName);
  if (!group) {
    return errorReply(
      'NOGROUP',
      "No such consumer group '" + groupName + "' for key name '" + key + "'"
    );
  }

  const result: Reply[] = [];
  for (const [, consumer] of group.consumers) {
    const idle = clockMs - consumer.seenTime;
    const inactive =
      consumer.activeTime > 0 ? clockMs - consumer.activeTime : -1;
    result.push(
      arrayReply([
        bulkReply('name'),
        bulkReply(consumer.name),
        bulkReply('pending'),
        integerReply(consumer.pending.size),
        bulkReply('idle'),
        integerReply(Math.max(0, idle)),
        bulkReply('inactive'),
        integerReply(inactive >= 0 ? inactive : -1),
      ])
    );
  }

  return arrayReply(result);
}

export function xinfo(ctx: CommandContext, args: string[]): Reply {
  if (args.length === 0) {
    return errorReply('ERR', "wrong number of arguments for 'xinfo' command");
  }

  const subcommand = (args[0] as string).toUpperCase();
  const subArgs = args.slice(1);

  switch (subcommand) {
    case 'STREAM':
      return xinfoStream(ctx.db, subArgs);
    case 'GROUPS':
      return xinfoGroups(ctx.db, subArgs);
    case 'CONSUMERS':
      return xinfoConsumers(ctx.db, ctx.engine.clock(), subArgs);
    case 'HELP':
      return arrayReply([
        bulkReply(
          'XINFO <subcommand> [<arg> [value] [opt] ...]. Subcommands are:'
        ),
        bulkReply('CONSUMERS <key> <groupname>'),
        bulkReply('    Return list of consumers for a consumer group.'),
        bulkReply('GROUPS <key>'),
        bulkReply('    Return list of consumer groups for a stream.'),
        bulkReply('STREAM <key> [FULL [COUNT <count>]]'),
        bulkReply('    Return information about the stream stored at <key>.'),
        bulkReply('HELP'),
        bulkReply('    Return this help message.'),
      ]);
    default:
      return errorReply(
        'ERR',
        `unknown subcommand or wrong number of arguments for 'xinfo|${args[0]}' command`
      );
  }
}
