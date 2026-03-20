/**
 * LuaEngine — abstraction layer for Lua VM implementations.
 *
 * Allows swapping between wasmoon-lua5.1 (primary) and fengari (fallback)
 * without changing the rest of the codebase.
 */

/** Result of executing a Lua script */
export interface LuaExecResult {
  /** Return values from the script (may be empty) */
  values: unknown[];
}

/** Error thrown when Lua script execution fails */
export class LuaScriptError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'LuaScriptError';
  }
}

/**
 * Abstract Lua VM engine interface.
 *
 * Lifecycle: create → execute scripts → close.
 * Implementations must be initialized via an async factory (WASM loading).
 */
export interface LuaEngine {
  /**
   * Execute a Lua script string and return its results.
   * Throws LuaScriptError on Lua runtime or syntax errors.
   */
  execute(script: string): Promise<LuaExecResult>;

  /**
   * Set a global variable in the Lua VM.
   */
  setGlobal(name: string, value: unknown): void;

  /**
   * Get a global variable from the Lua VM.
   */
  getGlobal(name: string): unknown;

  /**
   * Shut down the VM and release all resources.
   * After close(), the engine must not be used.
   */
  close(): void;

  /** Whether the engine has been closed */
  readonly closed: boolean;
}
