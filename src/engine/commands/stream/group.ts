import type { Database } from '../../database.ts';
import type { Reply } from '../../types.ts';
import type { CommandContext } from '../../types.ts';
import { errorReply, integerReply, OK, SYNTAX_ERR } from '../../types.ts';
import { RedisStream, parseStreamId } from '../../stream.ts';
import type { StreamId } from '../../stream.ts';
import { notify, EVENT_FLAGS } from '../../notify.ts';
import { getStream, INVALID_STREAM_ID_ERR } from './utils.ts';

function xgroupCreate(db: Database, args: string[]): Reply {
  // XGROUP CREATE key groupname id-or-$ [MKSTREAM] [ENTRIESREAD entries-read]
  if (args.length < 3) {
    return errorReply(
      'ERR',
      "wrong number of arguments for 'xgroup|create' command"
    );
  }

  const key = args[0] as string;
  const groupName = args[1] as string;
  const idArg = args[2] as string;
  let mkstream = false;
  let entriesRead = -1;

  let i = 3;
  while (i < args.length) {
    const upper = (args[i] as string).toUpperCase();
    if (upper === 'MKSTREAM') {
      mkstream = true;
      i++;
    } else if (upper === 'ENTRIESREAD') {
      i++;
      const val = args[i];
      if (val === undefined) return SYNTAX_ERR;
      const n = Number(val);
      if (!Number.isInteger(n) || n < 0) {
        return errorReply('ERR', 'value is not an integer or out of range');
      }
      entriesRead = n;
      i++;
    } else {
      return SYNTAX_ERR;
    }
  }

  const existing = getStream(db, key);
  if (existing.error) return existing.error;

  let stream = existing.stream;

  if (!stream) {
    if (!mkstream) {
      return errorReply(
        'ERR',
        'The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.'
      );
    }
    stream = new RedisStream();
    db.set(key, 'stream', 'stream', stream);
  }

  // Parse the ID
  let lastDeliveredId: StreamId;
  if (idArg === '$') {
    lastDeliveredId = stream.lastId;
  } else if (idArg === '0') {
    lastDeliveredId = { ms: 0, seq: 0 };
  } else {
    const parsed = parseStreamId(idArg);
    if (!parsed) return INVALID_STREAM_ID_ERR;
    lastDeliveredId = parsed;
  }

  const created = stream.createGroup(
    groupName,
    lastDeliveredId,
    entriesRead >= 0 ? entriesRead : 0
  );
  if (!created) {
    return errorReply('BUSYGROUP', 'Consumer Group name already exists');
  }

  return OK;
}

function xgroupSetid(db: Database, args: string[]): Reply {
  // XGROUP SETID key groupname id-or-$ [ENTRIESREAD entries-read]
  if (args.length < 3) {
    return errorReply(
      'ERR',
      "wrong number of arguments for 'xgroup|setid' command"
    );
  }

  const key = args[0] as string;
  const groupName = args[1] as string;
  const idArg = args[2] as string;
  let entriesRead = -1;

  let i = 3;
  while (i < args.length) {
    const upper = (args[i] as string).toUpperCase();
    if (upper === 'ENTRIESREAD') {
      i++;
      const val = args[i];
      if (val === undefined) return SYNTAX_ERR;
      const n = Number(val);
      if (!Number.isInteger(n) || n < 0) {
        return errorReply('ERR', 'value is not an integer or out of range');
      }
      entriesRead = n;
      i++;
    } else {
      return SYNTAX_ERR;
    }
  }

  const existing = getStream(db, key);
  if (existing.error) return existing.error;
  if (!existing.stream) {
    return errorReply(
      'ERR',
      'The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.'
    );
  }

  const stream = existing.stream;
  const group = stream.getGroup(groupName);
  if (!group) {
    return errorReply(
      'NOGROUP',
      "No such consumer group '" + groupName + "' for key name '" + key + "'"
    );
  }

  let newId: StreamId;
  if (idArg === '$') {
    newId = stream.lastId;
  } else if (idArg === '0') {
    newId = { ms: 0, seq: 0 };
  } else {
    const parsed = parseStreamId(idArg);
    if (!parsed) return INVALID_STREAM_ID_ERR;
    newId = parsed;
  }

  stream.setGroupId(groupName, newId, entriesRead >= 0 ? entriesRead : 0);
  return OK;
}

function xgroupDestroy(db: Database, args: string[]): Reply {
  // XGROUP DESTROY key groupname
  if (args.length < 2) {
    return errorReply(
      'ERR',
      "wrong number of arguments for 'xgroup|destroy' command"
    );
  }

  const key = args[0] as string;
  const groupName = args[1] as string;

  const existing = getStream(db, key);
  if (existing.error) return existing.error;
  if (!existing.stream) {
    return errorReply(
      'ERR',
      'The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.'
    );
  }

  const destroyed = existing.stream.destroyGroup(groupName);
  return integerReply(destroyed ? 1 : 0);
}

function xgroupDelconsumer(db: Database, args: string[]): Reply {
  // XGROUP DELCONSUMER key groupname consumername
  if (args.length < 3) {
    return errorReply(
      'ERR',
      "wrong number of arguments for 'xgroup|delconsumer' command"
    );
  }

  const key = args[0] as string;
  const groupName = args[1] as string;
  const consumerName = args[2] as string;

  const existing = getStream(db, key);
  if (existing.error) return existing.error;
  if (!existing.stream) {
    return errorReply(
      'ERR',
      'The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.'
    );
  }

  const group = existing.stream.getGroup(groupName);
  if (!group) {
    return errorReply(
      'NOGROUP',
      "No such consumer group '" + groupName + "' for key name '" + key + "'"
    );
  }

  const pendingCount = existing.stream.deleteConsumer(groupName, consumerName);
  return integerReply(pendingCount ?? 0);
}

function xgroupCreateconsumer(db: Database, args: string[]): Reply {
  // XGROUP CREATECONSUMER key groupname consumername
  if (args.length < 3) {
    return errorReply(
      'ERR',
      "wrong number of arguments for 'xgroup|createconsumer' command"
    );
  }

  const key = args[0] as string;
  const groupName = args[1] as string;
  const consumerName = args[2] as string;

  const existing = getStream(db, key);
  if (existing.error) return existing.error;
  if (!existing.stream) {
    return errorReply(
      'ERR',
      'The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.'
    );
  }

  const result = existing.stream.createConsumer(groupName, consumerName);
  if (result === null) {
    return errorReply(
      'NOGROUP',
      "No such consumer group '" + groupName + "' for key name '" + key + "'"
    );
  }
  return integerReply(result);
}

export function xgroup(ctx: CommandContext, args: string[]): Reply {
  if (args.length === 0) {
    return errorReply('ERR', "wrong number of arguments for 'xgroup' command");
  }

  const subcommand = (args[0] as string).toUpperCase();
  const subArgs = args.slice(1);

  switch (subcommand) {
    case 'CREATE': {
      const reply = xgroupCreate(ctx.db, subArgs);
      if (reply === OK) {
        notify(ctx, EVENT_FLAGS.STREAM, 'xgroup-create', subArgs[0] ?? '');
      }
      return reply;
    }
    case 'SETID': {
      const reply = xgroupSetid(ctx.db, subArgs);
      if (reply === OK) {
        notify(ctx, EVENT_FLAGS.STREAM, 'xgroup-setid', subArgs[0] ?? '');
      }
      return reply;
    }
    case 'DESTROY': {
      const reply = xgroupDestroy(ctx.db, subArgs);
      if (reply.kind === 'integer' && (reply.value as number) === 1) {
        notify(ctx, EVENT_FLAGS.STREAM, 'xgroup-destroy', subArgs[0] ?? '');
      }
      return reply;
    }
    case 'DELCONSUMER': {
      const reply = xgroupDelconsumer(ctx.db, subArgs);
      if (reply.kind === 'integer') {
        notify(ctx, EVENT_FLAGS.STREAM, 'xgroup-delconsumer', subArgs[0] ?? '');
      }
      return reply;
    }
    case 'CREATECONSUMER': {
      const reply = xgroupCreateconsumer(ctx.db, subArgs);
      if (reply.kind === 'integer') {
        notify(
          ctx,
          EVENT_FLAGS.STREAM,
          'xgroup-createconsumer',
          subArgs[0] ?? ''
        );
      }
      return reply;
    }
    default:
      return errorReply(
        'ERR',
        `unknown subcommand or wrong number of arguments for 'xgroup|${args[0]}' command`
      );
  }
}
