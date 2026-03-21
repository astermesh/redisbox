/**
 * IBI (Inbound Box Interface) hooks for SimBox integration.
 *
 * Fires on every inbound Redis command:
 * - `redis:command` — generic hook on every command
 * - Per-command-family hooks based on command categories
 */

import type { CommandFlag } from '../command-table.ts';
import type { Reply } from '../types.ts';
import { AsyncHook } from './hook.ts';

/** Context passed to IBI hooks. */
export interface CommandHookCtx {
  /** Uppercase command name (e.g. 'SET', 'HGET'). */
  command: string;
  /** Command arguments (excluding the command name itself). */
  args: readonly string[];
  /** Client ID (0 when no client is associated). */
  clientId: number;
  /** Current database index. */
  db: number;
  /** Command metadata. */
  meta: CommandHookMeta;
}

/** Metadata about the command being executed. */
export interface CommandHookMeta {
  categories: ReadonlySet<string>;
  flags: ReadonlySet<CommandFlag>;
}

/** All IBI hook event names. */
export type IbiHookName =
  | 'redis:command'
  | 'redis:string:read'
  | 'redis:string:write'
  | 'redis:hash:read'
  | 'redis:hash:write'
  | 'redis:list:read'
  | 'redis:list:write'
  | 'redis:set:read'
  | 'redis:set:write'
  | 'redis:zset:read'
  | 'redis:zset:write'
  | 'redis:stream:read'
  | 'redis:stream:write'
  | 'redis:pubsub'
  | 'redis:tx'
  | 'redis:script'
  | 'redis:key'
  | 'redis:server'
  | 'redis:connection';

const ALL_HOOK_NAMES: IbiHookName[] = [
  'redis:command',
  'redis:string:read',
  'redis:string:write',
  'redis:hash:read',
  'redis:hash:write',
  'redis:list:read',
  'redis:list:write',
  'redis:set:read',
  'redis:set:write',
  'redis:zset:read',
  'redis:zset:write',
  'redis:stream:read',
  'redis:stream:write',
  'redis:pubsub',
  'redis:tx',
  'redis:script',
  'redis:key',
  'redis:server',
  'redis:connection',
];

/**
 * Map a Redis @category to a base IBI hook family name (without :read/:write suffix).
 * Data-type categories need a read/write suffix appended separately.
 */
const CATEGORY_TO_FAMILY = new Map<string, string>([
  ['@string', 'redis:string'],
  ['@hash', 'redis:hash'],
  ['@list', 'redis:list'],
  ['@set', 'redis:set'],
  ['@sortedset', 'redis:zset'],
  ['@stream', 'redis:stream'],
  // Bitmap and HyperLogLog are string-encoded in Redis
  ['@bitmap', 'redis:string'],
  ['@hyperloglog', 'redis:string'],
  ['@pubsub', 'redis:pubsub'],
  ['@transaction', 'redis:tx'],
  ['@scripting', 'redis:script'],
  ['@keyspace', 'redis:key'],
  ['@connection', 'redis:connection'],
]);

/** Families that split into :read and :write variants. */
const READ_WRITE_FAMILIES = new Set([
  'redis:string',
  'redis:hash',
  'redis:list',
  'redis:set',
  'redis:zset',
  'redis:stream',
]);

/**
 * Resolve which IBI family hooks should fire for a command based on its categories.
 * Returns a deduplicated, ordered list of hook names (excluding `redis:command`).
 */
export function resolveIbiHooks(
  categories: ReadonlySet<string>
): IbiHookName[] {
  const result = new Set<IbiHookName>();

  for (const [category, family] of CATEGORY_TO_FAMILY) {
    if (!categories.has(category)) continue;

    if (READ_WRITE_FAMILIES.has(family)) {
      if (categories.has('@write')) {
        result.add(`${family}:write` as IbiHookName);
      } else {
        result.add(`${family}:read` as IbiHookName);
      }
    } else {
      result.add(family as IbiHookName);
    }
  }

  // Commands with @admin or @dangerous that didn't match any family → redis:server
  if (result.size === 0) {
    result.add('redis:server');
  }

  return [...result];
}

/**
 * IBI Hook Manager — holds all inbound command hooks.
 *
 * Usage:
 * 1. `manager.hook('redis:command').tap(fn)` — register a generic hook
 * 2. `manager.hook('redis:string:write').tap(fn)` — register a family hook
 * 3. `manager.execute(ctx, familyHooks, baseFn)` — execute through hook chain
 */
export class IbiHookManager {
  private readonly hooks = new Map<
    IbiHookName,
    AsyncHook<CommandHookCtx, Reply>
  >();

  constructor() {
    for (const name of ALL_HOOK_NAMES) {
      this.hooks.set(name, new AsyncHook());
    }
  }

  /** Get a specific hook by name for tapping/untapping. */
  hook(name: IbiHookName): AsyncHook<CommandHookCtx, Reply> {
    const h = this.hooks.get(name);
    if (!h) {
      throw new Error(`Unknown IBI hook: ${name}`);
    }
    return h;
  }

  /** Check if any hooks are registered (useful to skip async overhead). */
  get hasHooks(): boolean {
    for (const h of this.hooks.values()) {
      if (h.size > 0) {
        return true;
      }
    }
    return false;
  }

  /**
   * Execute a command through the IBI hook chain.
   *
   * Chain order: redis:command → family hooks (in order) → baseFn
   *
   * @param ctx - command hook context
   * @param familyHooks - resolved family hook names (from resolveIbiHooks)
   * @param baseFn - the actual command execution function
   */
  execute(
    ctx: CommandHookCtx,
    familyHooks: IbiHookName[],
    baseFn: () => Reply
  ): Promise<Reply> {
    // Build the chain from inside out:
    // innermost = baseFn
    // then each family hook wraps it
    // outermost = redis:command
    let current: () => Promise<Reply> = () => Promise.resolve(baseFn());

    // Wrap with family hooks (last in list = innermost)
    for (let i = familyHooks.length - 1; i >= 0; i--) {
      const hookName = familyHooks[i];
      const hook = hookName ? this.hooks.get(hookName) : undefined;
      if (hook) {
        const next = current;
        current = () => hook.execute(ctx, next);
      }
    }

    // Wrap with redis:command (outermost)
    const commandHook = this.hooks.get('redis:command');
    if (!commandHook) return current();
    const innerChain = current;
    return commandHook.execute(ctx, innerChain);
  }
}
