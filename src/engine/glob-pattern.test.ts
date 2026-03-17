import { describe, it, expect } from 'vitest';
import { matchGlob } from './glob-pattern.ts';

describe('matchGlob', () => {
  it('matches exact string', () => {
    expect(matchGlob('hello', 'hello')).toBe(true);
    expect(matchGlob('hello', 'world')).toBe(false);
  });

  it('* matches any sequence', () => {
    expect(matchGlob('h*o', 'hello')).toBe(true);
    expect(matchGlob('h*o', 'ho')).toBe(true);
    expect(matchGlob('h*o', 'hx')).toBe(false);
    expect(matchGlob('*', 'anything')).toBe(true);
    expect(matchGlob('*', '')).toBe(true);
  });

  it('? matches single character', () => {
    expect(matchGlob('h?llo', 'hello')).toBe(true);
    expect(matchGlob('h?llo', 'hallo')).toBe(true);
    expect(matchGlob('h?llo', 'hllo')).toBe(false);
  });

  it('[abc] matches character class', () => {
    expect(matchGlob('h[ae]llo', 'hello')).toBe(true);
    expect(matchGlob('h[ae]llo', 'hallo')).toBe(true);
    expect(matchGlob('h[ae]llo', 'hillo')).toBe(false);
  });

  it('[^abc] matches negated character class', () => {
    expect(matchGlob('h[^ae]llo', 'hillo')).toBe(true);
    expect(matchGlob('h[^ae]llo', 'hello')).toBe(false);
  });

  it('[a-z] matches range', () => {
    expect(matchGlob('[a-z]', 'm')).toBe(true);
    expect(matchGlob('[a-z]', 'A')).toBe(false);
    expect(matchGlob('[0-9]', '5')).toBe(true);
  });

  it('backslash escapes special chars', () => {
    expect(matchGlob('h\\*llo', 'h*llo')).toBe(true);
    expect(matchGlob('h\\*llo', 'hello')).toBe(false);
    expect(matchGlob('h\\?llo', 'h?llo')).toBe(true);
  });

  it('complex patterns', () => {
    expect(matchGlob('user:*', 'user:123')).toBe(true);
    expect(matchGlob('user:*', 'admin:123')).toBe(false);
    expect(matchGlob('*:*', 'user:123')).toBe(true);
    expect(matchGlob('key[0-9]', 'key5')).toBe(true);
    expect(matchGlob('key[0-9]', 'keya')).toBe(false);
  });

  it('empty pattern matches empty string', () => {
    expect(matchGlob('', '')).toBe(true);
    expect(matchGlob('', 'a')).toBe(false);
  });
});
