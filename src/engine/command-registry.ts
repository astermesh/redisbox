import { CommandTable, toDefinition } from './command-table.ts';
import type { CommandSpec } from './command-table.ts';
import { specs as connectionSpecs } from './commands/connection.ts';
import { specs as databaseSpecs } from './commands/database.ts';
import { specs as clientSpecs } from './commands/client/index.ts';
import { specs as transactionSpecs } from './commands/transaction.ts';
import { specs as genericSpecs } from './commands/generic.ts';
import { specs as memorySpecs } from './commands/memory.ts';
import { specs as ttlSpecs } from './commands/ttl.ts';
import { specs as scanSpecs } from './commands/scan.ts';
import { specs as sortSpecs } from './commands/sort.ts';
import { specs as stringSpecs } from './commands/string/index.ts';
import { specs as incrSpecs } from './commands/incr.ts';
import { specs as bitmapSpecs } from './commands/bitmap/index.ts';
import { specs as hashSpecs } from './commands/hash/index.ts';
import { specs as listSpecs } from './commands/list/index.ts';
import { specs as blockingListSpecs } from './commands/list/blocking.ts';
import { specs as blockingSortedSetSpecs } from './commands/sorted-set/blocking.ts';
import { specs as setSpecs } from './commands/set/index.ts';
import { specs as sortedSetSpecs } from './commands/sorted-set/index.ts';
import { specs as pubsubSpecs } from './commands/pubsub/index.ts';
import { specs as infoSpecs } from './commands/info.ts';
import { specs as clusterSpecs } from './commands/cluster/index.ts';
import { specs as commandSpecs } from './commands/command.ts';
import { specs as streamSpecs } from './commands/stream/index.ts';
import { specs as aclSpecs } from './commands/acl/index.ts';
import { specs as hyperloglogSpecs } from './commands/hyperloglog/index.ts';
import { specs as geoSpecs } from './commands/geo/index.ts';
import { specs as replicationSpecs } from './commands/replication.ts';
import { specs as slowlogSpecs } from './commands/slowlog.ts';
import { specs as latencySpecs } from './commands/latency.ts';
import { specs as persistenceSpecs } from './commands/persistence.ts';
import { specs as scriptingSpecs } from './commands/scripting.ts';
import { specs as functionsSpecs } from './commands/functions.ts';
import { specs as serverSpecs } from './commands/server.ts';

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
  ...listSpecs,
  ...blockingListSpecs,
  ...blockingSortedSetSpecs,
  ...setSpecs,
  ...sortedSetSpecs,
  ...pubsubSpecs,
  ...infoSpecs,
  ...clusterSpecs,
  ...commandSpecs,
  ...streamSpecs,
  ...aclSpecs,
  ...hyperloglogSpecs,
  ...geoSpecs,
  ...replicationSpecs,
  ...slowlogSpecs,
  ...latencySpecs,
  ...persistenceSpecs,
  ...scriptingSpecs,
  ...functionsSpecs,
  ...serverSpecs,
];

export function createCommandTable(): CommandTable {
  const table = new CommandTable();
  for (const spec of allSpecs) {
    table.register(toDefinition(spec));
  }
  return table;
}
