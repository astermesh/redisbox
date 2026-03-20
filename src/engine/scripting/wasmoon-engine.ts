/**
 * WasmoonEngine — LuaEngine implementation using wasmoon-lua5.1.
 *
 * Provides exact Lua 5.1 behavioral parity via WebAssembly.
 */

import { Lua } from 'wasmoon-lua5.1';
import type { LuaEngine, LuaExecResult } from './lua-engine.ts';
import { LuaScriptError } from './lua-engine.ts';

export class WasmoonEngine implements LuaEngine {
  private vm: Lua;
  private _closed = false;

  private constructor(vm: Lua) {
    this.vm = vm;
  }

  /**
   * Create and initialize a WasmoonEngine.
   * Loads the WASM module and opens standard Lua 5.1 libraries
   * (openStandardLibs defaults to true in wasmoon).
   */
  static async create(): Promise<WasmoonEngine> {
    const vm = await Lua.create();
    return new WasmoonEngine(vm);
  }

  get closed(): boolean {
    return this._closed;
  }

  async execute(script: string): Promise<LuaExecResult> {
    this.assertOpen();
    try {
      const result = await this.vm.doString(script);
      // doString returns the last expression value or undefined
      const values = result === undefined ? [] : [result];
      return { values };
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : String(err);
      throw new LuaScriptError(message);
    }
  }

  setGlobal(name: string, value: unknown): void {
    this.assertOpen();
    this.vm.global.set(name, value);
  }

  getGlobal(name: string): unknown {
    this.assertOpen();
    return this.vm.global.get(name);
  }

  close(): void {
    if (!this._closed) {
      this._closed = true;
      this.vm.global.close();
    }
  }

  private assertOpen(): void {
    if (this._closed) {
      throw new LuaScriptError('Lua engine is closed');
    }
  }
}
