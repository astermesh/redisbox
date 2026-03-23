/**
 * Pure-JS SHA-1 — no Node/browser-specific dependencies.
 *
 * Used for Lua script caching in EVAL/EVALSHA (Redis uses SHA-1 hex digests
 * to identify cached scripts).
 */

function rotr(x: number, n: number): number {
  return (x >>> n) | (x << (32 - n));
}

function utf8Encode(s: string): Uint8Array {
  const encoder = new TextEncoder();
  return encoder.encode(s);
}

export function sha1(message: string): string {
  const msgBytes = utf8Encode(message);
  const bitLen = msgBytes.length * 8;

  // Pre-processing: pad to 512-bit blocks
  const padLen =
    msgBytes.length + 1 + 8 + ((64 - ((msgBytes.length + 1 + 8) % 64)) % 64);
  const padded = new Uint8Array(padLen);
  padded.set(msgBytes);
  padded[msgBytes.length] = 0x80;

  // Append length as 64-bit big-endian
  const view = new DataView(padded.buffer);
  view.setUint32(padLen - 4, bitLen, false);

  // Initial hash values
  let h0 = 0x67452301;
  let h1 = 0xefcdab89;
  let h2 = 0x98badcfe;
  let h3 = 0x10325476;
  let h4 = 0xc3d2e1f0;

  const w = new Uint32Array(80);

  for (let offset = 0; offset < padLen; offset += 64) {
    for (let i = 0; i < 16; i++) {
      w[i] = view.getUint32(offset + i * 4, false);
    }
    for (let i = 16; i < 80; i++) {
      // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
      w[i] = rotr((w[i - 3]! ^ w[i - 8]! ^ w[i - 14]! ^ w[i - 16]!) >>> 0, 31);
    }

    let a = h0,
      b = h1,
      c = h2,
      d = h3,
      e = h4;

    for (let i = 0; i < 80; i++) {
      let f: number, k: number;
      if (i < 20) {
        f = (b & c) | (~b & d);
        k = 0x5a827999;
      } else if (i < 40) {
        f = b ^ c ^ d;
        k = 0x6ed9eba1;
      } else if (i < 60) {
        f = (b & c) | (b & d) | (c & d);
        k = 0x8f1bbcdc;
      } else {
        f = b ^ c ^ d;
        k = 0xca62c1d6;
      }

      // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
      const temp = (rotr(a, 27) + f + e + k + w[i]!) | 0;
      e = d;
      d = c;
      c = rotr(b, 2);
      b = a;
      a = temp;
    }

    h0 = (h0 + a) | 0;
    h1 = (h1 + b) | 0;
    h2 = (h2 + c) | 0;
    h3 = (h3 + d) | 0;
    h4 = (h4 + e) | 0;
  }

  return [h0, h1, h2, h3, h4]
    .map((v) => (v >>> 0).toString(16).padStart(8, '0'))
    .join('');
}
