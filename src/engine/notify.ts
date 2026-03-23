/**
 * Convenience wrapper for emitting keyspace notifications from command handlers.
 *
 * Extracts config, pubsub, and dbid from CommandContext and delegates to
 * the core notifyKeyspaceEvent function.
 */

import type { CommandContext } from './types.ts';
import { notifyKeyspaceEvent } from './keyspace-events.ts';

export { EVENT_FLAGS } from './keyspace-events.ts';

/**
 * Emit a keyspace notification from a command handler.
 * No-op if config or pubsub are unavailable on the context.
 */
export function notify(
  ctx: CommandContext,
  type: number,
  event: string,
  key: string
): void {
  const config = ctx.config;
  const pubsub = ctx.pubsub ?? ctx.engine.pubsub;
  if (!config) return;
  const dbid = ctx.client?.dbIndex ?? 0;
  notifyKeyspaceEvent(config, pubsub, type, event, key, dbid);
}
