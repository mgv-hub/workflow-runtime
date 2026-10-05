import type { TimerHub } from '../clock.js';

export type SettledResult<T> = { ok: true; value: T } | { ok: false; error: unknown };

export function settle<T>(p: PromiseLike<T>): Promise<SettledResult<T>> {
  return Promise.resolve(p).then(
    (value) => ({ ok: true, value }),
    (error) => ({ ok: false, error }),
  );
}

export interface Middleware<Ctx> {
  (ctx: Ctx, next: () => Promise<unknown>): Promise<unknown> | unknown;
}

export function composeMiddleware<Ctx>(
  list: Middleware<Ctx>[],
): <T>(ctx: Ctx, core: () => Promise<T>) => Promise<T> {
  if (list.length === 0) {
    return <T>(_ctx: Ctx, core: () => Promise<T>) => core();
  }
  return <T>(ctx: Ctx, core: () => Promise<T>) => {
    const dispatch =
      (i: number): (() => Promise<T>) =>
      () => {
        if (i === list.length) return core();
        return Promise.resolve(list[i](ctx, dispatch(i + 1)) as unknown as Promise<T>);
      };
    return Promise.resolve(list[0](ctx, dispatch(1)) as unknown as Promise<T>);
  };
}

export interface TimeoutResult<T> {
  settled: SettledResult<T>;
  timedOut: boolean;
}

export async function raceTimeout<T>(
  p: Promise<SettledResult<T>>,
  timeoutMs: number | undefined,
  hub: TimerHub,
  onTimeout: () => Error,
): Promise<TimeoutResult<T>> {
  if (timeoutMs == null || !Number.isFinite(timeoutMs) || timeoutMs <= 0) {
    return { settled: await p, timedOut: false };
  }
  let cancelTimer: () => void = () => {};
  let timedOut = false;
  const timeoutPromise = new Promise<SettledResult<T>>((resolve) => {
    const handle = hub.after(timeoutMs, () => {
      timedOut = true;
      resolve({ ok: false, error: onTimeout() });
    });
    cancelTimer = () => handle.cancel();
  });
  const winner = await Promise.race([p, timeoutPromise]);
  cancelTimer();
  return {
    settled: winner,
    timedOut,
  };
}

export function delay(ms: number, hub: TimerHub): Promise<void> {
  return new Promise((resolve) => {
    hub.after(ms, () => resolve());
  });
}
