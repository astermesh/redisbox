/**
 * LATENCY command implementation.
 *
 * Subcommands: LATEST, HISTORY, RESET, GRAPH, DOCTOR, HELP
 */

import type { Reply, CommandContext } from '../types.ts';
import {
  arrayReply,
  bulkReply,
  errorReply,
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
  const event = args[0] ?? '';
  const samples = ctx.engine.latency.history(event);

  if (samples.length === 0) {
    return errorReply('ERR', `No samples available for event '${event}'`);
  }

  const allTimeMax = ctx.engine.latency.allTimeMax(event);
  return bulkReply(buildGraph(event, samples, allTimeMax));
}

export function latencyDoctor(ctx: CommandContext): Reply {
  const entries = ctx.engine.latency.latest();

  if (entries.length === 0) {
    return bulkReply(
      "I'm sorry, Dave, I can't do that. Latency monitoring is disabled in this Redis instance. " +
        'You may use "CONFIG SET latency-monitor-threshold <milliseconds>." in order to enable it. ' +
        "If we weren't in a deep space mission I'd suggest to take a look at https://redis.io/topics/latency-monitor."
    );
  }

  const lines: string[] = [
    "Dave, I have observed latency spikes in this Redis instance. You don't mind talking about it, do you Dave?\n",
  ];

  for (let i = 0; i < entries.length; i++) {
    const e = entries[i];
    if (!e) continue;
    const samples = ctx.engine.latency.history(e.event);
    const count = samples.length;
    const avg =
      count > 0
        ? Math.round(samples.reduce((s, x) => s + x.latency, 0) / count)
        : 0;
    const mad =
      count > 0
        ? Math.round(
            samples.reduce((s, x) => s + Math.abs(x.latency - avg), 0) / count
          )
        : 0;
    const first = samples[0];
    const last = samples.at(-1);
    const period =
      first && last && count > 1
        ? Math.round((last.timestamp - first.timestamp) / (count - 1))
        : 0;

    lines.push(
      `${i + 1}. ${e.event}: ${count} latency spike${count !== 1 ? 's' : ''} (average ${avg}ms, mean deviation ${mad}ms, period ${period} sec). Worst all time event ${e.max}ms.`
    );
  }

  lines.push(
    "\nWhile there are latency events logged, I'm not able to suggest any easy fix. Please use the Redis community to get some help, providing this report in your help request.\n"
  );

  return bulkReply(lines.join('\n'));
}

export function latencyHelp(): Reply {
  const lines = [
    'LATENCY <subcommand> [<arg> [value] [opt] ...]. Subcommands are:',
    'DOCTOR',
    '    Return a human readable latency analysis report.',
    'GRAPH <event>',
    '    Return an ASCII latency graph for the <event> class.',
    'HISTORY <event>',
    '    Return time-latency samples for the <event> class.',
    'LATEST',
    '    Return the latest latency samples for all events.',
    'RESET [<event> ...]',
    '    Reset latency data of one or more <event> classes.',
    '    (default: reset all data for all event classes)',
    'HELP',
    '    Print this help.',
  ];
  return arrayReply(lines.map((l) => bulkReply(l)));
}

// ---------------------------------------------------------------------------
// Graph builder — matches Redis LATENCY GRAPH output
// ---------------------------------------------------------------------------

const GRAPH_COLS = 80;
const GRAPH_ROWS = 4;
const GRAPH_CHARSET = '_o#';

function buildGraph(
  event: string,
  samples: LatencySample[],
  allTimeMax: number
): string {
  const maxLatency = Math.max(...samples.map((s) => s.latency));
  const minLatency = Math.min(...samples.map((s) => s.latency));
  const lines: string[] = [];

  lines.push(
    `${event} - high ${maxLatency} ms, low ${minLatency} ms (all time high ${allTimeMax} ms)`
  );
  lines.push('-'.repeat(GRAPH_COLS));

  // Build sparkline columns — resample to GRAPH_COLS if needed
  const resampled = resample(samples, GRAPH_COLS);

  // Build column heights and characters
  const cols: string[] = [];
  for (const latency of resampled) {
    const normalized =
      maxLatency === minLatency
        ? 1
        : (latency - minLatency) / (maxLatency - minLatency);
    const height = Math.max(1, Math.round(normalized * (GRAPH_ROWS - 1)) + 1);
    const charIdx = Math.min(
      Math.floor(normalized * GRAPH_CHARSET.length),
      GRAPH_CHARSET.length - 1
    );
    const ch = GRAPH_CHARSET[charIdx] ?? '#';
    cols.push(ch.repeat(height).padStart(GRAPH_ROWS, ' '));
  }

  // Render rows top-to-bottom
  for (let row = 0; row < GRAPH_ROWS; row++) {
    let line = '';
    for (const col of cols) {
      line += col[row] ?? ' ';
    }
    lines.push(line.trimEnd());
  }

  // Time axis
  if (samples.length > 0) {
    const first = samples[0];
    const last = samples.at(-1);
    if (first && last) {
      const elapsed = last.timestamp - first.timestamp;
      lines.push(formatElapsed(elapsed));
    }
  }

  return lines.join('\n');
}

/** Resample an array of samples to exactly targetLen latency values */
function resample(samples: LatencySample[], targetLen: number): number[] {
  if (samples.length === 0) return [];
  if (samples.length <= targetLen) {
    return samples.map((s) => s.latency);
  }

  const result: number[] = [];
  for (let i = 0; i < targetLen; i++) {
    const pos = (i / (targetLen - 1)) * (samples.length - 1);
    const lo = Math.floor(pos);
    const hi = Math.min(lo + 1, samples.length - 1);
    const loSample = samples[lo];
    const hiSample = samples[hi];
    if (!loSample || !hiSample) continue;
    // Take the max in the bucket (Redis uses max for merged samples)
    result.push(Math.max(loSample.latency, hiSample.latency));
  }
  return result;
}

/** Format elapsed seconds with appropriate unit */
function formatElapsed(seconds: number): string {
  if (seconds < 60) return `${seconds}s`;
  if (seconds < 3600) return `${Math.round(seconds / 60)}m`;
  if (seconds < 86400) return `${Math.round(seconds / 3600)}h`;
  return `${Math.round(seconds / 86400)}d`;
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
        flags: ['admin', 'loading', 'stale'],
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
        flags: ['loading', 'stale'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@slow'],
      },
    ],
  },
];
