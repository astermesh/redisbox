/**
 * FunctionRegistry — JS-side storage for Redis Function libraries and metadata.
 *
 * Stores library definitions (name, engine, code) and their registered functions
 * (name, flags, description). Lua callbacks live in the Lua VM's __rb_functions
 * table; this registry tracks metadata for FUNCTION LIST/STATS/DELETE.
 */

export interface FunctionFlags {
  noWrites: boolean;
  allowOom: boolean;
  allowStale: boolean;
  noCluster: boolean;
}

export interface FunctionDef {
  name: string;
  flags: FunctionFlags;
  description: string;
}

export interface Library {
  name: string;
  engine: string;
  code: string;
  functions: Map<string, FunctionDef>;
}

export class FunctionRegistry {
  private readonly libraries = new Map<string, Library>();
  /** Reverse index: function name → library name */
  private readonly functionToLib = new Map<string, string>();

  addLibrary(lib: Library): void {
    this.libraries.set(lib.name, lib);
    for (const [funcName] of lib.functions) {
      this.functionToLib.set(funcName, lib.name);
    }
  }

  deleteLibrary(name: string): boolean {
    const lib = this.libraries.get(name);
    if (!lib) return false;
    for (const [funcName] of lib.functions) {
      this.functionToLib.delete(funcName);
    }
    this.libraries.delete(name);
    return true;
  }

  getLibrary(name: string): Library | undefined {
    return this.libraries.get(name);
  }

  hasLibrary(name: string): boolean {
    return this.libraries.has(name);
  }

  /**
   * Look up a function by name, returning its definition and parent library.
   */
  getFunction(name: string): { lib: Library; func: FunctionDef } | undefined {
    const libName = this.functionToLib.get(name);
    if (!libName) return undefined;
    const lib = this.libraries.get(libName);
    if (!lib) return undefined;
    const func = lib.functions.get(name);
    if (!func) return undefined;
    return { lib, func };
  }

  hasFunction(name: string): boolean {
    return this.functionToLib.has(name);
  }

  /**
   * List all libraries, optionally filtered by a glob pattern on library name.
   */
  listLibraries(pattern?: string): Library[] {
    const libs = [...this.libraries.values()];
    if (!pattern) return libs;
    const regex = globToRegex(pattern);
    return libs.filter((lib) => regex.test(lib.name));
  }

  flush(): void {
    this.libraries.clear();
    this.functionToLib.clear();
  }

  get libraryCount(): number {
    return this.libraries.size;
  }

  get functionCount(): number {
    return this.functionToLib.size;
  }
}

/**
 * Convert a simple glob pattern (only * and ? wildcards) to a RegExp.
 */
function globToRegex(pattern: string): RegExp {
  let regex = '^';
  for (const ch of pattern) {
    if (ch === '*') regex += '.*';
    else if (ch === '?') regex += '.';
    else if ('.+^${}()|[]\\'.includes(ch)) regex += '\\' + ch;
    else regex += ch;
  }
  regex += '$';
  return new RegExp(regex);
}
