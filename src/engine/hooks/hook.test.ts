import { describe, it, expect, vi } from 'vitest';
import {
  AsyncHook,
  SyncHook,
  type AsyncHookFn,
  type SyncHookFn,
  PreDecision,
  PostDecision,
} from './hook.ts';

describe('AsyncHook', () => {
  describe('basic execution', () => {
    it('executes base function when no hooks are registered', async () => {
      const hook = new AsyncHook<{ key: string }, string>();
      const result = await hook.execute({ key: 'a' }, async () => 'value');
      expect(result).toBe('value');
    });

    it('passes context through to hooks', async () => {
      const hook = new AsyncHook<{ key: string }, string>();
      const seen: { key: string }[] = [];
      hook.tap(async (ctx, next) => {
        seen.push(ctx);
        return next();
      });
      await hook.execute({ key: 'hello' }, async () => 'v');
      expect(seen).toEqual([{ key: 'hello' }]);
    });

    it('calls hooks in registration order (first registered = outermost)', async () => {
      const hook = new AsyncHook<unknown, string>();
      const order: number[] = [];
      hook.tap(async (_ctx, next) => {
        order.push(1);
        const r = await next();
        order.push(4);
        return r;
      });
      hook.tap(async (_ctx, next) => {
        order.push(2);
        const r = await next();
        order.push(3);
        return r;
      });
      await hook.execute({}, async () => 'base');
      expect(order).toEqual([1, 2, 3, 4]);
    });
  });

  describe('tap and untap', () => {
    it('untap removes a hook', async () => {
      const hook = new AsyncHook<unknown, string>();
      const fn: AsyncHookFn<unknown, string> = async (_ctx, _next) => {
        return 'intercepted';
      };
      hook.tap(fn);
      expect(await hook.execute({}, async () => 'base')).toBe('intercepted');
      hook.untap(fn);
      expect(await hook.execute({}, async () => 'base')).toBe('base');
    });

    it('untap with unknown function is a no-op', () => {
      const hook = new AsyncHook<unknown, string>();
      expect(() => hook.untap(async (_ctx, next) => next())).not.toThrow();
    });

    it('supports multiple hooks', async () => {
      const hook = new AsyncHook<unknown, number>();
      hook.tap(async (_ctx, next) => (await next()) + 1);
      hook.tap(async (_ctx, next) => (await next()) * 2);
      // chain: hook1(hook2(base))
      // base = 5, hook2 = 5*2 = 10, hook1 = 10+1 = 11
      expect(await hook.execute({}, async () => 5)).toBe(11);
    });
  });

  describe('pre-phase decisions', () => {
    it('continue: executes normally', async () => {
      const hook = new AsyncHook<unknown, string>();
      hook.tap(async (_ctx, next) => {
        // continue decision: just call next
        return next();
      });
      expect(await hook.execute({}, async () => 'ok')).toBe('ok');
    });

    it('delay: adds latency before execution', async () => {
      const hook = new AsyncHook<unknown, string>();
      hook.tap(async (ctx, next) => {
        await PreDecision.delay(10);
        return next();
      });
      const start = Date.now();
      const result = await hook.execute({}, async () => 'delayed');
      expect(result).toBe('delayed');
      expect(Date.now() - start).toBeGreaterThanOrEqual(5);
    });

    it('fail: returns error without executing base', async () => {
      const hook = new AsyncHook<unknown, string>();
      const baseFn = vi.fn(async () => 'should not run');
      hook.tap(async (_ctx, _next) => {
        throw PreDecision.fail('simulated error');
      });
      await expect(hook.execute({}, baseFn)).rejects.toThrow('simulated error');
      expect(baseFn).not.toHaveBeenCalled();
    });

    it('short_circuit: returns value without executing base', async () => {
      const hook = new AsyncHook<unknown, string>();
      const baseFn = vi.fn(async () => 'should not run');
      hook.tap(async (_ctx, _next) => {
        return 'short-circuited';
      });
      expect(await hook.execute({}, baseFn)).toBe('short-circuited');
      expect(baseFn).not.toHaveBeenCalled();
    });

    it('execute_with: modifies context before passing to next hook', async () => {
      const hook = new AsyncHook<{ value: number }, number>();
      hook.tap(async (ctx, next) => {
        ctx.value = ctx.value * 10;
        return next();
      });
      hook.tap(async (ctx, next) => {
        // second hook sees the mutated context
        return ctx.value + (await next());
      });
      // base=1, hook2 sees value=30, returns 30+1=31, hook1 returns 31
      const result = await hook.execute({ value: 3 }, async () => 1);
      expect(result).toBe(31);
    });
  });

  describe('post-phase decisions', () => {
    it('pass: returns result as-is', async () => {
      const hook = new AsyncHook<unknown, string>();
      hook.tap(async (_ctx, next) => {
        const result = await next();
        // pass decision: return as-is
        return result;
      });
      expect(await hook.execute({}, async () => 'original')).toBe('original');
    });

    it('transform: modifies result before returning', async () => {
      const hook = new AsyncHook<unknown, string>();
      hook.tap(async (_ctx, next) => {
        const result = await next();
        return PostDecision.transform(result, (v) => v.toUpperCase());
      });
      expect(await hook.execute({}, async () => 'hello')).toBe('HELLO');
    });

    it('fail: replaces result with error', async () => {
      const hook = new AsyncHook<unknown, string>();
      hook.tap(async (_ctx, next) => {
        await next();
        throw PostDecision.fail('post-phase failure');
      });
      await expect(hook.execute({}, async () => 'ok')).rejects.toThrow(
        'post-phase failure'
      );
    });
  });

  describe('chain composition', () => {
    it('multiple hooks compose as middleware stack', async () => {
      const hook = new AsyncHook<{ log: string[] }, string>();
      hook.tap(async (ctx, next) => {
        ctx.log.push('A-pre');
        const r = await next();
        ctx.log.push('A-post');
        return r;
      });
      hook.tap(async (ctx, next) => {
        ctx.log.push('B-pre');
        const r = await next();
        ctx.log.push('B-post');
        return r;
      });
      const log: string[] = [];
      const ctx = { log };
      await hook.execute(ctx, async () => {
        log.push('base');
        return 'done';
      });
      expect(log).toEqual(['A-pre', 'B-pre', 'base', 'B-post', 'A-post']);
    });

    it('early hook can short-circuit entire chain', async () => {
      const hook = new AsyncHook<unknown, string>();
      const secondHook = vi.fn(
        async (_ctx: unknown, next: () => Promise<string>) => next()
      );
      const baseFn = vi.fn(async () => 'base');
      hook.tap(async (_ctx, _next) => 'early-exit');
      hook.tap(secondHook);
      expect(await hook.execute({}, baseFn)).toBe('early-exit');
      expect(secondHook).not.toHaveBeenCalled();
      expect(baseFn).not.toHaveBeenCalled();
    });

    it('later hook can transform what earlier hook sees', async () => {
      const hook = new AsyncHook<unknown, number>();
      hook.tap(async (_ctx, next) => {
        const v = await next();
        return v + 100;
      });
      hook.tap(async (_ctx, next) => {
        const v = await next();
        return v * 2;
      });
      // base=5, hook2: 5*2=10, hook1: 10+100=110
      expect(await hook.execute({}, async () => 5)).toBe(110);
    });
  });

  describe('error handling', () => {
    it('propagates base function errors through hooks', async () => {
      const hook = new AsyncHook<unknown, string>();
      hook.tap(async (_ctx, next) => next());
      await expect(
        hook.execute({}, async () => {
          throw new Error('base error');
        })
      ).rejects.toThrow('base error');
    });

    it('hook can catch and handle base errors', async () => {
      const hook = new AsyncHook<unknown, string>();
      hook.tap(async (_ctx, next) => {
        try {
          return await next();
        } catch {
          return 'recovered';
        }
      });
      expect(
        await hook.execute({}, async () => {
          throw new Error('oops');
        })
      ).toBe('recovered');
    });
  });
});

describe('SyncHook', () => {
  describe('basic execution', () => {
    it('executes base function when no hooks are registered', () => {
      const hook = new SyncHook<number>();
      expect(hook.execute(() => 42)).toBe(42);
    });

    it('calls hooks in registration order', () => {
      const hook = new SyncHook<string>();
      const order: number[] = [];
      hook.tap((next) => {
        order.push(1);
        const r = next();
        order.push(4);
        return r;
      });
      hook.tap((next) => {
        order.push(2);
        const r = next();
        order.push(3);
        return r;
      });
      hook.execute(() => 'base');
      expect(order).toEqual([1, 2, 3, 4]);
    });
  });

  describe('tap and untap', () => {
    it('untap removes a hook', () => {
      const hook = new SyncHook<number>();
      const fn: SyncHookFn<number> = (_next) => 99;
      hook.tap(fn);
      expect(hook.execute(() => 1)).toBe(99);
      hook.untap(fn);
      expect(hook.execute(() => 1)).toBe(1);
    });

    it('untap with unknown function is a no-op', () => {
      const hook = new SyncHook<number>();
      expect(() => hook.untap((_next) => 0)).not.toThrow();
    });
  });

  describe('composition', () => {
    it('multiple hooks compose as middleware stack', () => {
      const hook = new SyncHook<number>();
      hook.tap((next) => next() + 1);
      hook.tap((next) => next() * 2);
      // base=5, hook2: 5*2=10, hook1: 10+1=11
      expect(hook.execute(() => 5)).toBe(11);
    });

    it('hook can short-circuit without calling next', () => {
      const hook = new SyncHook<number>();
      const baseFn = vi.fn(() => 42);
      hook.tap((_next) => 0);
      expect(hook.execute(baseFn)).toBe(0);
      expect(baseFn).not.toHaveBeenCalled();
    });

    it('hook can transform result', () => {
      const hook = new SyncHook<string>();
      hook.tap((next) => next().toUpperCase());
      expect(hook.execute(() => 'hello')).toBe('HELLO');
    });
  });

  describe('error handling', () => {
    it('propagates base function errors', () => {
      const hook = new SyncHook<number>();
      hook.tap((next) => next());
      expect(() =>
        hook.execute(() => {
          throw new Error('sync error');
        })
      ).toThrow('sync error');
    });

    it('hook can catch and handle errors', () => {
      const hook = new SyncHook<number>();
      hook.tap((next) => {
        try {
          return next();
        } catch {
          return -1;
        }
      });
      expect(
        hook.execute(() => {
          throw new Error('oops');
        })
      ).toBe(-1);
    });
  });
});

describe('PreDecision', () => {
  it('delay returns a promise that resolves after given ms', async () => {
    const start = Date.now();
    await PreDecision.delay(15);
    expect(Date.now() - start).toBeGreaterThanOrEqual(10);
  });

  it('fail creates an error with HookFailError type', () => {
    const err = PreDecision.fail('test error');
    expect(err).toBeInstanceOf(Error);
    expect(err.message).toBe('test error');
    expect(err.name).toBe('HookFailError');
  });
});

describe('PostDecision', () => {
  it('transform applies function to value', () => {
    expect(PostDecision.transform(5, (v) => v * 2)).toBe(10);
    expect(PostDecision.transform('hi', (v) => v + '!')).toBe('hi!');
  });

  it('fail creates an error with HookFailError type', () => {
    const err = PostDecision.fail('post error');
    expect(err).toBeInstanceOf(Error);
    expect(err.message).toBe('post error');
    expect(err.name).toBe('HookFailError');
  });
});
