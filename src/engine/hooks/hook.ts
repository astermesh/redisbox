/**
 * Hook primitives for SimBox integration.
 *
 * AsyncHook<Ctx, T> — middleware-style async hook chain (IBI and async OBI).
 * SyncHook<T> — middleware-style sync hook chain (WASM-boundary OBI: time, random).
 *
 * Pre-phase decisions: continue, delay, fail, short_circuit, execute_with.
 * Post-phase decisions: pass, transform, fail.
 */

/** Async hook function: receives context and next(), returns promised value. */
export type AsyncHookFn<Ctx, T> = (
  ctx: Ctx,
  next: () => Promise<T>
) => Promise<T>;

/** Sync hook function: receives next(), returns value. */
export type SyncHookFn<T> = (next: () => T) => T;

/**
 * Error thrown by hook pre/post-phase fail decisions.
 */
export class HookFailError extends Error {
  override readonly name = 'HookFailError';
}

/**
 * Pre-phase decision helpers.
 *
 * These encode the decision vocabulary for hooks before calling next():
 * - continue: just call next() (no helper needed)
 * - delay: await PreDecision.delay(ms) then call next()
 * - fail: throw PreDecision.fail(message) — skips execution
 * - short_circuit: return value directly — skips next()
 * - execute_with: mutate ctx, then call next()
 */
export const PreDecision = {
  /** Add latency before execution. */
  delay(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  },

  /** Create an error to throw — prevents execution of next(). */
  fail(message: string): HookFailError {
    return new HookFailError(message);
  },
} as const;

/**
 * Post-phase decision helpers.
 *
 * These encode the decision vocabulary for hooks after calling next():
 * - pass: return result as-is (no helper needed)
 * - transform: return PostDecision.transform(result, fn)
 * - fail: throw PostDecision.fail(message)
 */
export const PostDecision = {
  /** Apply a transformation to the result. */
  transform<T>(value: T, fn: (v: T) => T): T {
    return fn(value);
  },

  /** Create an error to throw — replaces result with error. */
  fail(message: string): HookFailError {
    return new HookFailError(message);
  },
} as const;

/**
 * Async hook chain with middleware-style composition.
 *
 * Hooks are called in registration order (first registered = outermost).
 * Each hook receives the context and a next() function to continue the chain.
 */
export class AsyncHook<Ctx, T> {
  private readonly handlers: AsyncHookFn<Ctx, T>[] = [];

  /** Number of registered handlers. */
  get size(): number {
    return this.handlers.length;
  }

  /** Register a hook handler. */
  tap(fn: AsyncHookFn<Ctx, T>): void {
    this.handlers.push(fn);
  }

  /** Remove a previously registered hook handler. */
  untap(fn: AsyncHookFn<Ctx, T>): void {
    const idx = this.handlers.indexOf(fn);
    if (idx !== -1) {
      this.handlers.splice(idx, 1);
    }
  }

  /**
   * Execute the hook chain with the given context and base function.
   *
   * Composes all registered handlers into a middleware chain,
   * with the base function at the innermost position.
   */
  execute(ctx: Ctx, baseFn: () => Promise<T>): Promise<T> {
    let current = baseFn;
    for (let i = this.handlers.length - 1; i >= 0; i--) {
      const handler = this.handlers[i];
      if (!handler) continue;
      const next = current;
      current = () => handler(ctx, next);
    }
    return current();
  }
}

/**
 * Sync hook chain with middleware-style composition.
 *
 * Designed for WASM-boundary OBI hooks (time, random) where
 * async overhead is unacceptable.
 *
 * Hooks are called in registration order (first registered = outermost).
 */
export class SyncHook<T> {
  private readonly handlers: SyncHookFn<T>[] = [];

  /** Number of registered handlers. */
  get size(): number {
    return this.handlers.length;
  }

  /** Register a hook handler. */
  tap(fn: SyncHookFn<T>): void {
    this.handlers.push(fn);
  }

  /** Remove a previously registered hook handler. */
  untap(fn: SyncHookFn<T>): void {
    const idx = this.handlers.indexOf(fn);
    if (idx !== -1) {
      this.handlers.splice(idx, 1);
    }
  }

  /**
   * Execute the hook chain with the given base function.
   *
   * Composes all registered handlers into a middleware chain,
   * with the base function at the innermost position.
   */
  execute(baseFn: () => T): T {
    let current = baseFn;
    for (let i = this.handlers.length - 1; i >= 0; i--) {
      const handler = this.handlers[i];
      if (!handler) continue;
      const next = current;
      current = () => handler(next);
    }
    return current();
  }
}
