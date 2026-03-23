import { specs as hashSpecs } from './hash.ts';
import { specs as fieldTtlSpecs } from './field-ttl.ts';

export const specs = [...hashSpecs, ...fieldTtlSpecs];
