import { describe, it, expect } from 'vitest';
import { sha256 } from './sha256.ts';

describe('sha256', () => {
  it('hashes empty string correctly', () => {
    expect(sha256('')).toBe(
      'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'
    );
  });

  it('hashes "hello" correctly', () => {
    expect(sha256('hello')).toBe(
      '2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824'
    );
  });

  it('hashes "password" correctly', () => {
    expect(sha256('password')).toBe(
      '5e884898da28047151d0e56f8dc6292773603d0d6aabbdd62a11ef721d1542d8'
    );
  });

  it('returns 64-char hex string', () => {
    const result = sha256('test');
    expect(result).toHaveLength(64);
    expect(result).toMatch(/^[0-9a-f]+$/);
  });
});
