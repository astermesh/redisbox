/**
 * LATENCY command implementation.
 *
 * Subcommands: LATEST, HISTORY, RESET, GRAPH, DOCTOR, HELP
 */

import type { Reply, CommandContext } from '../types.ts';
import {
  arrayReply,
  bulkReply,
  integerReply,
  unknownSubcommandError,
  EMPTY_ARRAY,
} from '../types.ts';
import type { CommandSpec } from '../command-table.ts';
import type { LatencySample } from '../latency.ts';

// ---------------------------------------------------------------------------
// Subcommand handlers
// ---------------------------------------------------------------------------

export function latencyLatest(ctx: CommandContext): Reply {
  const entries = ctx.engine.latency.latest();
  if (entries.length === 0) return EMPTY_ARRAY;

  const result: Reply[] = entries.map((e) =>
    arrayReply([
      bulkReply(e.event),
      integerReply(e.timestamp),
      integerReply(e.latest),
      integerReply(e.max),
    ])
  );

  return arrayReply(result);
}

export function latencyHistory(ctx: CommandContext, args: string[]): Reply {
  if (args.length === 0) {
    return unknownSubcommandError('latency', 'HISTORY');
  }
  const event = args[0] ?? '';
  const samples = ctx.engine.latency.history(event);
  if (samples.length === 0) return EMPTY_ARRAY;

  const result: Reply[] = samples.map((s) =>
    arrayReply([integerReply(s.timestamp), integerReply(s.latency)])
  );

  return arrayReply(result);
}

export function latencyReset(ctx: CommandContext, args: string[]): Reply {
  const events = args.length > 0 ? args : undefined;
  const count = ctx.engine.latency.reset(events);
  return integerReply(count);
}

export function latencyGraph(ctx: CommandContext, args: string[]): Reply {
  if (args.length === 0) {
    return unknownSubcommandError('latency', 'GRAPH');
  }
  const event = args[0] ?? '';
  const samples = ctx.engine.latency.history(event);

  if (samples.length === 0) {
    return bulkReply(`${event} - `);
  }

  return bulkReply(buildGraph(event, samples));
}

export function latencyDoctor(ctx: CommandContext): Reply {
  const entries = ctx.engine.latency.latest();

  if (entries.length === 0) {
    return bulkReply(
      'I have no latency reports to analyze. ' +
        "Be sure to enable latency tracking if you haven't already. " +
        'You can enable it with: "CONFIG SET latency-monitor-threshold <milliseconds>".'
    );
  }

  const lines: string[] = [];
  for (const e of entries) {
    lines.push(
      `${e.event} - latest: ${e.latest} ms, all-time max: ${e.max} ms.`
    );
  }

  return bulkReply(lines.join('\n'));
}

export function latencyHelp(): Reply {
  const lines = [
    'LATENCY <subcommand> [<arg> [value] [opt] ...]. Subcommands are:',
    'DOCTOR',
    '    Return a human readable latency analysis report.',
    'GRAPH <event>',
    '    Return a latency graph for the <event> class.',
    'HISTORY <event>',
    '    Return time-latency samples for the <event> class.',
    'LATEST',
    '    Return the latest latency samples for all events.',
    'RESET [<event> [event ...]]',
    '    Reset latency data for one or more events.',
    '    (default: reset all events)',
    'HELP',
    '    Return this help.',
  ];
  return arrayReply(lines.map((l) => bulkReply(l)));
}

// ---------------------------------------------------------------------------
// Graph builder
// ---------------------------------------------------------------------------

function buildGraph(event: string, samples: LatencySample[]): string {
  const maxLatency = Math.max(...samples.map((s) => s.latency));
  const minLatency = Math.min(...samples.map((s) => s.latency));
  const graphHeight = 16;
  const lines: string[] = [];

  lines.push(
    `${event} - high ${maxLatency} ms, low ${minLatency} ms (all time high ${maxLatency} ms)`
  );

  // Build columns of the graph
  const cols: string[] = [];
  for (const sample of samples) {
    const height =
      maxLatency === minLatency
        ? graphHeight
        : Math.max(
            1,
            Math.round(
              ((sample.latency - minLatency) / (maxLatency - minLatency)) *
                (graphHeight - 1)
            ) + 1
          );
    cols.push('#'.repeat(height).padStart(graphHeight, ' '));
  }

  // Render rows top-to-bottom
  for (let row = 0; row < graphHeight; row++) {
    let line = '';
    for (const col of cols) {
      line += col[row] ?? ' ';
    }
    lines.push(line.trimEnd());
  }

  // Time axis labels
  if (samples.length > 0) {
    const first = samples[0];
    const last = samples.at(-1);
    if (first && last) {
      const elapsed = last.timestamp - first.timestamp;
      lines.push(`${elapsed}s`);
    }
  }

  return lines.join('\n');
}

// ---------------------------------------------------------------------------
// Main dispatcher
// ---------------------------------------------------------------------------

function latency(ctx: CommandContext, args: string[]): Reply {
  if (args.length === 0) {
    return unknownSubcommandError('latency', '');
  }

  const sub = (args[0] ?? '').toUpperCase();

  switch (sub) {
    case 'LATEST':
      return latencyLatest(ctx);
    case 'HISTORY':
      return latencyHistory(ctx, args.slice(1));
    case 'RESET':
      return latencyReset(ctx, args.slice(1));
    case 'GRAPH':
      return latencyGraph(ctx, args.slice(1));
    case 'DOCTOR':
      return latencyDoctor(ctx);
    case 'HELP':
      return latencyHelp();
    default:
      return unknownSubcommandError('latency', args[0] ?? '');
  }
}

// ---------------------------------------------------------------------------
// Command spec
// ---------------------------------------------------------------------------

export const specs: CommandSpec[] = [
  {
    name: 'LATENCY',
    handler: latency,
    arity: -2,
    flags: ['admin', 'loading', 'stale'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@admin', '@slow', '@dangerous'],
    subcommands: [
      {
        name: 'LATEST',
        handler: latency,
        arity: 2,
        flags: ['admin', 'loading', 'stale', 'fast'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@admin', '@slow', '@dangerous'],
      },
      {
        name: 'HISTORY',
        handler: latency,
        arity: 3,
        flags: ['admin', 'loading', 'stale'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@admin', '@slow', '@dangerous'],
      },
      {
        name: 'RESET',
        handler: latency,
        arity: -2,
        flags: ['admin', 'loading', 'stale'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@admin', '@slow', '@dangerous'],
      },
      {
        name: 'GRAPH',
        handler: latency,
        arity: 3,
        flags: ['admin', 'loading', 'stale'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@admin', '@slow', '@dangerous'],
      },
      {
        name: 'DOCTOR',
        handler: latency,
        arity: 2,
        flags: ['admin', 'loading', 'stale'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@admin', '@slow', '@dangerous'],
      },
      {
        name: 'HELP',
        handler: latency,
        arity: 2,
        flags: ['admin', 'loading', 'stale'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@admin', '@slow', '@dangerous'],
      },
    ],
  },
];
