import { describe, it, expect } from 'vitest';
import { determineStringEncoding } from './encoding.ts';

describe('determineStringEncoding', () => {
  it('returns int for zero', () => {
    expect(determineStringEncoding('0')).toBe('int');
  });

  it('returns int for positive integers', () => {
    expect(determineStringEncoding('42')).toBe('int');
    expect(determineStringEncoding('123456789')).toBe('int');
  });

  it('returns int for negative integers', () => {
    expect(determineStringEncoding('-1')).toBe('int');
    expect(determineStringEncoding('-999')).toBe('int');
  });

  it('returns int for max 64-bit signed integer', () => {
    expect(determineStringEncoding('9223372036854775807')).toBe('int');
  });

  it('returns int for min 64-bit signed integer', () => {
    expect(determineStringEncoding('-9223372036854775808')).toBe('int');
  });

  it('returns embstr for values exceeding 64-bit signed integer range', () => {
    expect(determineStringEncoding('9223372036854775808')).toBe('embstr');
    expect(determineStringEncoding('-9223372036854775809')).toBe('embstr');
  });

  it('returns embstr for short non-numeric strings', () => {
    expect(determineStringEncoding('hello')).toBe('embstr');
    expect(determineStringEncoding('')).toBe('embstr');
  });

  it('returns embstr for strings up to 44 bytes', () => {
    const s44 = 'a'.repeat(44);
    expect(determineStringEncoding(s44)).toBe('embstr');
  });

  it('returns raw for strings over 44 bytes', () => {
    const s45 = 'a'.repeat(45);
    expect(determineStringEncoding(s45)).toBe('raw');
  });

  it('returns embstr for float strings (not int)', () => {
    expect(determineStringEncoding('3.14')).toBe('embstr');
    expect(determineStringEncoding('-0.5')).toBe('embstr');
  });

  it('returns embstr for numeric strings with leading zeros', () => {
    expect(determineStringEncoding('007')).toBe('embstr');
    expect(determineStringEncoding('00')).toBe('embstr');
  });

  it('returns embstr for numeric strings with leading/trailing spaces', () => {
    expect(determineStringEncoding(' 42')).toBe('embstr');
    expect(determineStringEncoding('42 ')).toBe('embstr');
  });

  it('returns int for long numeric string within int range', () => {
    expect(determineStringEncoding('1000000000000000000')).toBe('int');
  });
});
