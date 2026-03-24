/**
 * Tagged encoding for JS→Lua table transfer.
 *
 * Uses a flat object format with type tags to avoid wasmoon's
 * 0-indexed array proxy issues. Shared by cjson.decode,
 * cmsgpack.unpack, and struct.unpack bridges.
 */

const TAG_ARRAY = 1;
const TAG_MAP = 2;
const TAG_NULL = 3;

export function jsToTagged(value: unknown): unknown {
  if (value === null || value === undefined) {
    return { t: TAG_NULL };
  }
  if (
    typeof value === 'boolean' ||
    typeof value === 'number' ||
    typeof value === 'string'
  ) {
    return value;
  }
  if (Array.isArray(value)) {
    const obj: Record<string, unknown> = { t: TAG_ARRAY, n: value.length };
    for (let i = 0; i < value.length; i++) {
      obj[String(i)] = jsToTagged(value[i]);
    }
    return obj;
  }
  if (typeof value === 'object') {
    const entries = Object.entries(value as Record<string, unknown>);
    const obj: Record<string, unknown> = { t: TAG_MAP, n: entries.length };
    for (let i = 0; i < entries.length; i++) {
      const entry = entries[i];
      if (entry) {
        obj['k' + i] = entry[0];
        obj['v' + i] = jsToTagged(entry[1]);
      }
    }
    return obj;
  }
  return { t: TAG_NULL };
}
