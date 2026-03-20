import { CommandTable, toDefinition } from './command-table.ts';
import type { CommandSpec } from './command-table.ts';
import { specs as connectionSpecs } from './commands/connection.ts';
import { specs as databaseSpecs } from './commands/database.ts';
import { specs as clientSpecs } from './commands/client.ts';
import { specs as transactionSpecs } from './commands/transaction.ts';
import { specs as genericSpecs } from './commands/generic.ts';
import { specs as memorySpecs } from './commands/memory.ts';
import { specs as ttlSpecs } from './commands/ttl.ts';
import { specs as scanSpecs } from './commands/scan.ts';
import { specs as sortSpecs } from './commands/sort.ts';
import { specs as stringSpecs } from './commands/string.ts';
import { specs as incrSpecs } from './commands/incr.ts';
import { specs as bitmapSpecs } from './commands/bitmap.ts';
import { specs as hashSpecs } from './commands/hash.ts';
import { specs as hashTtlSpecs } from './commands/hash-ttl.ts';
import { specs as listSpecs } from './commands/list.ts';
import { specs as blockingListSpecs } from './commands/blocking-list.ts';
import { specs as setSpecs } from './commands/set.ts';
import { specs as sortedSetSpecs } from './commands/sorted-set.ts';
import { specs as pubsubSpecs } from './commands/pubsub.ts';
import { specs as infoSpecs } from './commands/info.ts';
import { specs as clusterSpecs } from './commands/cluster.ts';
import { specs as commandSpecs } from './commands/command.ts';
import { specs as streamSpecs } from './commands/stream.ts';
import { specs as aclSpecs } from './commands/acl.ts';
import { specs as hyperloglogSpecs } from './commands/hyperloglog.ts';
import { specs as replicationSpecs } from './commands/replication.ts';

const allSpecs: CommandSpec[] = [
  ...connectionSpecs,
  ...databaseSpecs,
  ...clientSpecs,
  ...transactionSpecs,
  ...genericSpecs,
  ...memorySpecs,
  ...ttlSpecs,
  ...scanSpecs,
  ...sortSpecs,
  ...stringSpecs,
  ...incrSpecs,
  ...bitmapSpecs,
  ...hashSpecs,
  ...hashTtlSpecs,
  ...listSpecs,
  ...blockingListSpecs,
  ...setSpecs,
  ...sortedSetSpecs,
  ...pubsubSpecs,
  ...infoSpecs,
  ...clusterSpecs,
  ...commandSpecs,
  ...streamSpecs,
  ...aclSpecs,
  ...hyperloglogSpecs,
  ...replicationSpecs,
];

export function createCommandTable(): CommandTable {
  const table = new CommandTable();
  for (const spec of allSpecs) {
    table.register(toDefinition(spec));
  }
  return table;
}
