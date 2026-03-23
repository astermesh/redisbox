import { describe, it, expect } from 'vitest';
import { sha1 } from './sha1.ts';

describe('sha1', () => {
  it('returns correct hash for empty string', () => {
    expect(sha1('')).toBe('da39a3ee5e6b4b0d3255bfef95601890afd80709');
  });

  it('returns correct hash for "abc"', () => {
    expect(sha1('abc')).toBe('a9993e364706816aba3e25717850c26c9cd0d89d');
  });

  it('returns correct hash for "message digest"', () => {
    expect(sha1('message digest')).toBe(
      'c12252ceda8be8994d5fa0290a47231c1d16aae3'
    );
  });

  it('returns correct hash for longer input', () => {
    expect(
      sha1('abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq')
    ).toBe('84983e441c3bd26ebaae4aa1f95129e5e54670f1');
  });

  it('returns 40-character lowercase hex string', () => {
    const hash = sha1('test');
    expect(hash).toMatch(/^[0-9a-f]{40}$/);
  });

  it('produces different hashes for different inputs', () => {
    expect(sha1('return 1')).not.toBe(sha1('return 2'));
  });

  it('produces same hash for same input', () => {
    expect(sha1('return 1')).toBe(sha1('return 1'));
  });
});
