/**
 * MessagePack encode/decode for the cmsgpack library bridge.
 *
 * JS-side binary encoding and decoding — the Lua side communicates
 * via JSON (for pack) and tagged encoding (for unpack) to avoid
 * wasmoon table transfer issues.
 */

// ---- encode ----

export function msgpackEncode(value: unknown): number[] {
  const buf: number[] = [];
  writeValue(buf, value);
  return buf;
}

function writeValue(buf: number[], value: unknown): void {
  if (value === null || value === undefined) {
    buf.push(0xc0);
    return;
  }
  if (typeof value === 'boolean') {
    buf.push(value ? 0xc3 : 0xc2);
    return;
  }
  if (typeof value === 'number') {
    if (Number.isInteger(value)) {
      writeInteger(buf, value);
    } else {
      writeFloat64(buf, value);
    }
    return;
  }
  if (typeof value === 'string') {
    writeString(buf, value);
    return;
  }
  if (Array.isArray(value)) {
    writeArray(buf, value);
    return;
  }
  if (typeof value === 'object') {
    writeMap(buf, value as Record<string, unknown>);
    return;
  }
  buf.push(0xc0);
}

function writeInteger(buf: number[], n: number): void {
  if (n >= 0) {
    if (n <= 0x7f) {
      buf.push(n);
    } else if (n <= 0xff) {
      buf.push(0xcc, n);
    } else if (n <= 0xffff) {
      buf.push(0xcd, (n >> 8) & 0xff, n & 0xff);
    } else if (n <= 0xffffffff) {
      buf.push(
        0xce,
        (n >>> 24) & 0xff,
        (n >>> 16) & 0xff,
        (n >>> 8) & 0xff,
        n & 0xff
      );
    } else {
      writeFloat64(buf, n);
    }
  } else {
    if (n >= -32) {
      buf.push(n & 0xff);
    } else if (n >= -128) {
      buf.push(0xd0, n & 0xff);
    } else if (n >= -32768) {
      buf.push(0xd1, (n >> 8) & 0xff, n & 0xff);
    } else if (n >= -2147483648) {
      buf.push(
        0xd2,
        (n >> 24) & 0xff,
        (n >> 16) & 0xff,
        (n >> 8) & 0xff,
        n & 0xff
      );
    } else {
      writeFloat64(buf, n);
    }
  }
}

function writeFloat64(buf: number[], n: number): void {
  buf.push(0xcb);
  const view = new DataView(new ArrayBuffer(8));
  view.setFloat64(0, n);
  for (let i = 0; i < 8; i++) {
    buf.push(view.getUint8(i));
  }
}

function writeString(buf: number[], s: string): void {
  const encoded = new TextEncoder().encode(s);
  const len = encoded.length;
  if (len <= 31) {
    buf.push(0xa0 | len);
  } else if (len <= 0xff) {
    buf.push(0xd9, len);
  } else if (len <= 0xffff) {
    buf.push(0xda, (len >> 8) & 0xff, len & 0xff);
  } else {
    buf.push(
      0xdb,
      (len >>> 24) & 0xff,
      (len >>> 16) & 0xff,
      (len >>> 8) & 0xff,
      len & 0xff
    );
  }
  for (const b of encoded) {
    buf.push(b);
  }
}

function writeArray(buf: number[], arr: unknown[]): void {
  const len = arr.length;
  if (len <= 15) {
    buf.push(0x90 | len);
  } else if (len <= 0xffff) {
    buf.push(0xdc, (len >> 8) & 0xff, len & 0xff);
  } else {
    buf.push(
      0xdd,
      (len >>> 24) & 0xff,
      (len >>> 16) & 0xff,
      (len >>> 8) & 0xff,
      len & 0xff
    );
  }
  for (const item of arr) {
    writeValue(buf, item);
  }
}

function writeMap(buf: number[], obj: Record<string, unknown>): void {
  const keys = Object.keys(obj);
  const len = keys.length;
  if (len <= 15) {
    buf.push(0x80 | len);
  } else if (len <= 0xffff) {
    buf.push(0xde, (len >> 8) & 0xff, len & 0xff);
  } else {
    buf.push(
      0xdf,
      (len >>> 24) & 0xff,
      (len >>> 16) & 0xff,
      (len >>> 8) & 0xff,
      len & 0xff
    );
  }
  for (const key of keys) {
    writeValue(buf, key);
    writeValue(buf, obj[key]);
  }
}

// ---- decode ----

interface DecodeResult {
  value: unknown;
  offset: number;
}

function b(bytes: Uint8Array, i: number): number {
  return bytes[i] ?? 0;
}

export function msgpackDecode(bytes: Uint8Array, offset: number): DecodeResult {
  const byte = b(bytes, offset);
  if (byte <= 0x7f) return { value: byte, offset: offset + 1 };
  if ((byte & 0xf0) === 0x80) return readMsgMap(bytes, offset + 1, byte & 0x0f);
  if ((byte & 0xf0) === 0x90) return readMsgArr(bytes, offset + 1, byte & 0x0f);
  if ((byte & 0xe0) === 0xa0) return readMsgStr(bytes, offset + 1, byte & 0x1f);
  if (byte >= 0xe0) return { value: byte - 256, offset: offset + 1 };

  switch (byte) {
    case 0xc0:
      return { value: null, offset: offset + 1 };
    case 0xc2:
      return { value: false, offset: offset + 1 };
    case 0xc3:
      return { value: true, offset: offset + 1 };
    case 0xcc:
      return { value: b(bytes, offset + 1), offset: offset + 2 };
    case 0xcd:
      return {
        value: (b(bytes, offset + 1) << 8) | b(bytes, offset + 2),
        offset: offset + 3,
      };
    case 0xce:
      return {
        value:
          ((b(bytes, offset + 1) << 24) |
            (b(bytes, offset + 2) << 16) |
            (b(bytes, offset + 3) << 8) |
            b(bytes, offset + 4)) >>>
          0,
        offset: offset + 5,
      };
    case 0xd0: {
      let v = b(bytes, offset + 1);
      if (v >= 128) v -= 256;
      return { value: v, offset: offset + 2 };
    }
    case 0xd1: {
      let v = (b(bytes, offset + 1) << 8) | b(bytes, offset + 2);
      if (v >= 32768) v -= 65536;
      return { value: v, offset: offset + 3 };
    }
    case 0xd2: {
      const v =
        (b(bytes, offset + 1) << 24) |
        (b(bytes, offset + 2) << 16) |
        (b(bytes, offset + 3) << 8) |
        b(bytes, offset + 4);
      return { value: v | 0, offset: offset + 5 };
    }
    case 0xca: {
      const view = new DataView(bytes.buffer, bytes.byteOffset + offset + 1, 4);
      return { value: view.getFloat32(0), offset: offset + 5 };
    }
    case 0xcb: {
      const view = new DataView(bytes.buffer, bytes.byteOffset + offset + 1, 8);
      return { value: view.getFloat64(0), offset: offset + 9 };
    }
    case 0xd9:
      return readMsgStr(bytes, offset + 2, b(bytes, offset + 1));
    case 0xda:
      return readMsgStr(
        bytes,
        offset + 3,
        (b(bytes, offset + 1) << 8) | b(bytes, offset + 2)
      );
    case 0xdb:
      return readMsgStr(
        bytes,
        offset + 5,
        ((b(bytes, offset + 1) << 24) |
          (b(bytes, offset + 2) << 16) |
          (b(bytes, offset + 3) << 8) |
          b(bytes, offset + 4)) >>>
          0
      );
    case 0xdc:
      return readMsgArr(
        bytes,
        offset + 3,
        (b(bytes, offset + 1) << 8) | b(bytes, offset + 2)
      );
    case 0xdd:
      return readMsgArr(
        bytes,
        offset + 5,
        ((b(bytes, offset + 1) << 24) |
          (b(bytes, offset + 2) << 16) |
          (b(bytes, offset + 3) << 8) |
          b(bytes, offset + 4)) >>>
          0
      );
    case 0xde:
      return readMsgMap(
        bytes,
        offset + 3,
        (b(bytes, offset + 1) << 8) | b(bytes, offset + 2)
      );
    case 0xdf:
      return readMsgMap(
        bytes,
        offset + 5,
        ((b(bytes, offset + 1) << 24) |
          (b(bytes, offset + 2) << 16) |
          (b(bytes, offset + 3) << 8) |
          b(bytes, offset + 4)) >>>
          0
      );
    default:
      return { value: null, offset: offset + 1 };
  }
}

function readMsgStr(
  buf: Uint8Array,
  offset: number,
  len: number
): DecodeResult {
  const value = new TextDecoder().decode(buf.slice(offset, offset + len));
  return { value, offset: offset + len };
}

function readMsgArr(
  buf: Uint8Array,
  offset: number,
  len: number
): DecodeResult {
  const arr: unknown[] = [];
  let pos = offset;
  for (let i = 0; i < len; i++) {
    const r = msgpackDecode(buf, pos);
    arr.push(r.value);
    pos = r.offset;
  }
  return { value: arr, offset: pos };
}

function readMsgMap(
  buf: Uint8Array,
  offset: number,
  len: number
): DecodeResult {
  const map: Record<string, unknown> = {};
  let pos = offset;
  for (let i = 0; i < len; i++) {
    const kr = msgpackDecode(buf, pos);
    pos = kr.offset;
    const vr = msgpackDecode(buf, pos);
    pos = vr.offset;
    map[String(kr.value)] = vr.value;
  }
  return { value: map, offset: pos };
}
