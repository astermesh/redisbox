export function matchGlob(pattern: string, str: string): boolean {
  return matchGlobRecursive(pattern, 0, str, 0);
}

function matchGlobRecursive(
  pattern: string,
  pi: number,
  str: string,
  si: number
): boolean {
  while (pi < pattern.length) {
    const pc = pattern.charAt(pi);

    if (pc === '*') {
      while (pi < pattern.length && pattern.charAt(pi) === '*') pi++;
      if (pi === pattern.length) return true;
      for (let i = si; i <= str.length; i++) {
        if (matchGlobRecursive(pattern, pi, str, i)) return true;
      }
      return false;
    }

    if (si >= str.length) return false;

    if (pc === '?') {
      pi++;
      si++;
      continue;
    }

    if (pc === '\\') {
      pi++;
      if (pi >= pattern.length) return false;
      if (pattern.charAt(pi) !== str.charAt(si)) return false;
      pi++;
      si++;
      continue;
    }

    if (pc === '[') {
      pi++;
      let negate = false;
      if (
        pi < pattern.length &&
        (pattern.charAt(pi) === '^' || pattern.charAt(pi) === '!')
      ) {
        negate = true;
        pi++;
      }

      let matched = false;
      let first = true;
      while (pi < pattern.length && (first || pattern.charAt(pi) !== ']')) {
        first = false;
        const rangeStart = pattern.charAt(pi);
        pi++;
        if (
          pi < pattern.length &&
          pattern.charAt(pi) === '-' &&
          pi + 1 < pattern.length &&
          pattern.charAt(pi + 1) !== ']'
        ) {
          pi++; // skip '-'
          const rangeEnd = pattern.charAt(pi);
          pi++;
          if (
            str.charCodeAt(si) >= rangeStart.charCodeAt(0) &&
            str.charCodeAt(si) <= rangeEnd.charCodeAt(0)
          ) {
            matched = true;
          }
        } else {
          if (str.charAt(si) === rangeStart) matched = true;
        }
      }
      if (pi < pattern.length && pattern.charAt(pi) === ']') pi++;

      if (negate ? matched : !matched) return false;
      si++;
      continue;
    }

    if (pc !== str.charAt(si)) return false;
    pi++;
    si++;
  }

  return si === str.length;
}
