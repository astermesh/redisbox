/**
 * Inline command parser.
 *
 * Parses plain text commands (non-RESP) for redis-cli and telnet
 * compatibility, matching Redis sdssplitargs behavior.
 */

const MAX_INLINE_LEN = 65536; // 64 KB (Redis 7.2+)

export interface InlineParseResult {
  args: Buffer[];
  bytesConsumed: number;
}

/**
 * Parse an inline command line from the buffer.
 * Returns undefined if the buffer doesn't contain a complete line yet.
 */
export function parseInlineCommand(
  buffer: Buffer,
  offset = 0
): InlineParseResult | undefined {
  // Find \r\n or \n to get the line
  let lineEnd = -1;
  let crlfLen = 1;
  for (let i = offset; i < buffer.length; i++) {
    if (buffer[i] === 0x0a) {
      // \n
      lineEnd = i;
      if (i > offset && buffer[i - 1] === 0x0d) {
        // \r\n
        lineEnd = i - 1;
        crlfLen = 2;
      }
      break;
    }
  }

  if (lineEnd === -1) {
    // Check max inline length
    if (buffer.length - offset > MAX_INLINE_LEN) {
      throw new Error('Protocol error: too big inline request');
    }
    return undefined;
  }

  const lineLen = lineEnd - offset;
  if (lineLen > MAX_INLINE_LEN) {
    throw new Error('Protocol error: too big inline request');
  }

  const line = buffer.toString('utf8', offset, lineEnd);
  const args = splitArgs(line);
  const bytesConsumed = lineEnd - offset + crlfLen;

  return { args, bytesConsumed };
}

/**
 * Check if the first byte indicates an inline command (not RESP multibulk).
 */
export function isInlineCommand(firstByte: number): boolean {
  return firstByte !== 0x2a; // not '*'
}

/**
 * Split a line into arguments following Redis sdssplitargs rules.
 */
function splitArgs(line: string): Buffer[] {
  const args: Buffer[] = [];
  let i = 0;

  while (i < line.length) {
    // skip whitespace
    while (i < line.length && (line[i] === ' ' || line[i] === '\t')) {
      i++;
    }
    if (i >= line.length) break;

    const ch = line[i];

    if (ch === '"') {
      // double-quoted string
      i++; // skip opening quote
      const bytes: number[] = [];
      while (i < line.length && line[i] !== '"') {
        if (line[i] === '\\' && i + 1 < line.length) {
          i++;
          switch (line[i]) {
            case 'n':
              bytes.push(0x0a);
              break;
            case 'r':
              bytes.push(0x0d);
              break;
            case 't':
              bytes.push(0x09);
              break;
            case 'a':
              bytes.push(0x07);
              break;
            case 'b':
              bytes.push(0x08);
              break;
            case '\\':
              bytes.push(0x5c);
              break;
            case '"':
              bytes.push(0x22);
              break;
            case 'x':
              if (i + 2 < line.length) {
                const hex = line.substring(i + 1, i + 3);
                if (/^[0-9a-fA-F]{2}$/.test(hex)) {
                  bytes.push(parseInt(hex, 16));
                  i += 2;
                } else {
                  bytes.push(0x5c); // backslash
                  bytes.push(0x78); // x
                }
              } else {
                bytes.push(0x5c);
                bytes.push(0x78);
              }
              break;
            default:
              bytes.push(0x5c);
              bytes.push(line.charCodeAt(i));
              break;
          }
        } else {
          bytes.push(line.charCodeAt(i));
        }
        i++;
      }
      if (i < line.length && line[i] === '"') {
        i++; // skip closing quote
      } else {
        throw new Error('Protocol error: unbalanced quotes in inline request');
      }
      args.push(Buffer.from(bytes));
    } else if (ch === "'") {
      // single-quoted string
      i++; // skip opening quote
      const bytes: number[] = [];
      while (i < line.length && line[i] !== "'") {
        if (line[i] === '\\' && i + 1 < line.length) {
          const next = line[i + 1];
          if (next === '\\' || next === "'") {
            bytes.push(next.charCodeAt(0));
            i += 2;
            continue;
          }
        }
        bytes.push(line.charCodeAt(i));
        i++;
      }
      if (i < line.length && line[i] === "'") {
        i++; // skip closing quote
      } else {
        throw new Error('Protocol error: unbalanced quotes in inline request');
      }
      args.push(Buffer.from(bytes));
    } else {
      // unquoted argument
      const start = i;
      while (i < line.length && line[i] !== ' ' && line[i] !== '\t') {
        i++;
      }
      args.push(Buffer.from(line.substring(start, i), 'utf8'));
    }
  }

  return args;
}
