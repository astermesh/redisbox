/**
 * C-struct packing/unpacking for the Redis struct library bridge.
 *
 * Supports format specifiers: b/B (int8/uint8), h/H (int16/uint16),
 * i/I/l/L (int32/uint32), f (float32), d (float64), s (zero-terminated string).
 * Byte order: > (big-endian), < (little-endian), = (native/little-endian).
 *
 * Data is transferred as hex strings to avoid null byte issues in
 * wasmoon string transfer.
 */

interface FmtSpec {
  type: string;
  size: number;
}

function parseStructFormat(fmt: string): {
  bigEndian: boolean;
  specs: FmtSpec[];
} {
  let bigEndian = true;
  const specs: FmtSpec[] = [];
  let i = 0;
  if (fmt[0] === '>') {
    bigEndian = true;
    i = 1;
  } else if (fmt[0] === '<') {
    bigEndian = false;
    i = 1;
  } else if (fmt[0] === '=') {
    bigEndian = false;
    i = 1;
  }

  while (i < fmt.length) {
    const ch = fmt[i];
    i++;
    switch (ch) {
      case 'b':
        specs.push({ type: 'int8', size: 1 });
        break;
      case 'B':
        specs.push({ type: 'uint8', size: 1 });
        break;
      case 'h':
        specs.push({ type: 'int16', size: 2 });
        break;
      case 'H':
        specs.push({ type: 'uint16', size: 2 });
        break;
      case 'i':
      case 'l':
        specs.push({ type: 'int32', size: 4 });
        break;
      case 'I':
      case 'L':
        specs.push({ type: 'uint32', size: 4 });
        break;
      case 'f':
        specs.push({ type: 'float32', size: 4 });
        break;
      case 'd':
        specs.push({ type: 'float64', size: 8 });
        break;
      case 's':
        specs.push({ type: 'string', size: 0 });
        break;
      case ' ':
        break;
      default:
        break;
    }
  }
  return { bigEndian, specs };
}

export function structSize(fmt: string): number {
  const { specs } = parseStructFormat(fmt);
  let size = 0;
  for (const spec of specs) {
    if (spec.type === 'string') {
      // Zero-terminated string has variable size, not computable statically.
      // Redis struct library throws for variable-length formats in size().
      // Return 0 as placeholder — struct.size with 's' is rarely used.
      return 0;
    }
    size += spec.size;
  }
  return size;
}

export function structPackHex(fmt: string, ...values: unknown[]): string {
  const { bigEndian, specs } = parseStructFormat(fmt);
  const parts: number[] = [];
  let vi = 0;

  for (const spec of specs) {
    const val = values[vi++];
    if (spec.type === 'string') {
      const str = String(val ?? '');
      const encoded = new TextEncoder().encode(str);
      for (const b of encoded) parts.push(b);
      parts.push(0); // null terminator
      continue;
    }
    const n = Number(val ?? 0);
    const buf = new ArrayBuffer(spec.size);
    const view = new DataView(buf);
    switch (spec.type) {
      case 'int8':
        view.setInt8(0, n);
        break;
      case 'uint8':
        view.setUint8(0, n);
        break;
      case 'int16':
        view.setInt16(0, n, !bigEndian);
        break;
      case 'uint16':
        view.setUint16(0, n, !bigEndian);
        break;
      case 'int32':
        view.setInt32(0, n, !bigEndian);
        break;
      case 'uint32':
        view.setUint32(0, n, !bigEndian);
        break;
      case 'float32':
        view.setFloat32(0, n, !bigEndian);
        break;
      case 'float64':
        view.setFloat64(0, n, !bigEndian);
        break;
    }
    for (let j = 0; j < spec.size; j++) {
      parts.push(view.getUint8(j));
    }
  }
  // Return as hex string to avoid null byte issues in wasmoon string transfer
  return parts.map((b) => b.toString(16).padStart(2, '0')).join('');
}

export function structUnpackHex(
  fmt: string,
  hexData: string,
  startPos?: number
): string {
  // Convert hex string back to byte values
  const bytes: number[] = [];
  for (let i = 0; i < hexData.length; i += 2) {
    bytes.push(parseInt(hexData.substring(i, i + 2), 16));
  }

  const { bigEndian, specs } = parseStructFormat(fmt);
  const results: string[] = [];
  let offset = (startPos ?? 1) - 1;

  for (const spec of specs) {
    if (spec.type === 'string') {
      // Zero-terminated: scan for null byte
      let end = offset;
      while (end < bytes.length && (bytes[end] ?? 0) !== 0) {
        end++;
      }
      const len = end - offset;
      const strBytes = new Uint8Array(len);
      for (let i = 0; i < len; i++) {
        strBytes[i] = bytes[offset + i] ?? 0;
      }
      const str = new TextDecoder().decode(strBytes);
      // Escape for Lua string literal
      results.push(
        '"' +
          str
            .replace(/\\/g, '\\\\')
            .replace(/"/g, '\\"')
            .replace(/\n/g, '\\n')
            .replace(/\r/g, '\\r') +
          '"'
      );
      offset = end + 1; // skip past null terminator
      continue;
    }
    const buf = new ArrayBuffer(spec.size);
    const view = new DataView(buf);
    for (let j = 0; j < spec.size; j++) {
      view.setUint8(j, bytes[offset + j] ?? 0);
    }
    offset += spec.size;
    let val: number;
    switch (spec.type) {
      case 'int8':
        val = view.getInt8(0);
        break;
      case 'uint8':
        val = view.getUint8(0);
        break;
      case 'int16':
        val = view.getInt16(0, !bigEndian);
        break;
      case 'uint16':
        val = view.getUint16(0, !bigEndian);
        break;
      case 'int32':
        val = view.getInt32(0, !bigEndian);
        break;
      case 'uint32':
        val = view.getUint32(0, !bigEndian);
        break;
      case 'float32':
        val = view.getFloat32(0, !bigEndian);
        break;
      case 'float64':
        val = view.getFloat64(0, !bigEndian);
        break;
      default:
        val = 0;
    }
    results.push(String(val));
  }
  // Append final position (1-based for Lua)
  results.push(String(offset + 1));
  // Return as comma-separated Lua expression: "return val1,val2,...,pos"
  return 'return ' + results.join(',');
}
