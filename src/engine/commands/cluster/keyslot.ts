// --- CRC16-CCITT lookup table ---

const CRC16_TABLE = new Uint16Array(256);

(function buildTable() {
  for (let i = 0; i < 256; i++) {
    let crc = i << 8;
    for (let j = 0; j < 8; j++) {
      if (crc & 0x8000) {
        crc = ((crc << 1) ^ 0x1021) & 0xffff;
      } else {
        crc = (crc << 1) & 0xffff;
      }
    }
    CRC16_TABLE[i] = crc;
  }
})();

/**
 * Compute CRC16-CCITT for a string (same algorithm as Redis).
 */
function crc16(data: string): number {
  let crc = 0;
  for (let i = 0; i < data.length; i++) {
    const idx = ((crc >> 8) ^ data.charCodeAt(i)) & 0xff;
    crc = ((crc << 8) ^ (CRC16_TABLE[idx] ?? 0)) & 0xffff;
  }
  return crc;
}

/**
 * Extract the hash tag from a key (content between first { and next }).
 * If no valid hash tag exists, the entire key is used.
 */
function extractHashTag(key: string): string {
  const start = key.indexOf('{');
  if (start === -1) return key;
  const end = key.indexOf('}', start + 1);
  if (end === -1 || end === start + 1) return key;
  return key.substring(start + 1, end);
}

/**
 * Compute the hash slot for a key (0-16383).
 */
export function keySlot(key: string): number {
  return crc16(extractHashTag(key)) & 16383;
}
