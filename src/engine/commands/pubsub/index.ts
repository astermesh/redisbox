import { specs as pubsubSpecs } from './pubsub.ts';
import { specs as patternSpecs } from './pattern.ts';
import { specs as shardSpecs } from './shard.ts';
import { specs as introspectionSpecs } from './introspection.ts';

export const specs = [
  ...pubsubSpecs,
  ...patternSpecs,
  ...introspectionSpecs,
  ...shardSpecs,
];
