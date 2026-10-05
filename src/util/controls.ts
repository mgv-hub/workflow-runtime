import { SystemClock, TimerHub, type Clock, type TimerHandle } from '../clock.js';
import { parseDuration } from './time.js';

interface DefaultHub {
  hub: TimerHub;
  clock: Clock;
}

let shared: DefaultHub | null = null;

function defaultHub(): DefaultHub {
  if (!shared) {
    const clock = new SystemClock();
    shared = { clock, hub: new TimerHub({ clock, driver: 'system' }) };
  }
  return shared;
}

function resolveClock(hub?: TimerHub, clock?: Clock): DefaultHub {
  if (hub && clock) return { hub, clock };
  return defaultHub();
}

export interface Debounced<F extends (...args: never[]) => unknown> {
  (...args: Parameters<F>): void;
  cancel(): void;
  flush(): void;
  pending(): boolean;
}

export function debounce<F extends (...args: never[]) => unknown>(
  fn: F,
  wait: number | string,
  opts: { leading?: boolean; hub?: TimerHub; clock?: Clock } = {},
): Debounced<F> {
  const waitMs = parseDuration(wait);
  const { hub } = resolveClock(opts.hub, opts.clock);
  let timer: TimerHandle | null = null;
  let lastArgs: Parameters<F> | null = null;
  let leading = opts.leading ?? false;
  function invoke(): void {
    timer?.cancel();
    timer = null;
    const args = lastArgs;
    lastArgs = null;
    if (args) fn(...args);
  }
  const debounced = ((...args: Parameters<F>) => {
    lastArgs = args;
    timer?.cancel();
    timer = hub.after(waitMs, invoke);
  }) as Debounced<F>;
  debounced.cancel = () => {
    timer?.cancel();
    timer = null;
    lastArgs = null;
  };
  debounced.flush = () => {
    if (timer) invoke();
  };
  debounced.pending = () => timer != null;
  void leading;
  leading = false;
  return debounced;
}

export interface Throttled<F extends (...args: never[]) => unknown> {
  (...args: Parameters<F>): void;
  cancel(): void;
  pending(): boolean;
}

export function throttle<F extends (...args: never[]) => unknown>(
  fn: F,
  interval: number | string,
  opts: { leading?: boolean; trailing?: boolean; hub?: TimerHub; clock?: Clock } = {},
): Throttled<F> {
  const intervalMs = parseDuration(interval);
  const { hub, clock } = resolveClock(opts.hub, opts.clock);
  const leading = opts.leading ?? true;
  const trailing = opts.trailing ?? true;
  let lastRun = -Infinity;
  let timer: TimerHandle | null = null;
  let queued: Parameters<F> | null = null;
  function fire(args: Parameters<F>): void {
    lastRun = clock.now();
    fn(...args);
  }
  const throttled = ((...args: Parameters<F>) => {
    const now = clock.now();
    const since = now - lastRun;
    if (since >= intervalMs) {
      if (leading) fire(args);
      else {
        queued = args;
        timer ??= hub.after(intervalMs - since, () => {
          timer = null;
          if (queued) {
            fire(queued);
            queued = null;
          }
        });
      }
      return;
    }
    if (trailing) {
      queued = args;
      timer ??= hub.after(intervalMs - since, () => {
        timer = null;
        if (queued) {
          fire(queued);
          queued = null;
        }
      });
    }
  }) as Throttled<F>;
  throttled.cancel = () => {
    timer?.cancel();
    timer = null;
    queued = null;
  };
  throttled.pending = () => timer != null || queued != null;
  return throttled;
}

export interface RateLimiter {
  tryAcquire(): boolean;
  available(): number;
  reset(): void;
}

export function createRateLimiter(
  config: { max: number; windowMs: number },
  clock?: Clock,
): RateLimiter {
  const c = clock ?? defaultHub().clock;
  const stamps: number[] = [];
  return {
    tryAcquire() {
      const now = c.now();
      while (stamps.length > 0 && now - stamps[0] >= config.windowMs) stamps.shift();
      if (stamps.length >= config.max) return false;
      stamps.push(now);
      return true;
    },
    available() {
      const now = c.now();
      while (stamps.length > 0 && now - stamps[0] >= config.windowMs) stamps.shift();
      return config.max - stamps.length;
    },
    reset() {
      stamps.length = 0;
    },
  };
}

export interface Cooldown {
  trigger(): boolean;
  ready(): boolean;
  remaining(): number;
  reset(): void;
}

export function createCooldown(duration: number | string, clock?: Clock): Cooldown {
  const ms = parseDuration(duration);
  const c = clock ?? defaultHub().clock;
  let until = 0;
  return {
    trigger() {
      const now = c.now();
      if (now < until) return false;
      until = now + ms;
      return true;
    },
    ready() {
      return c.now() >= until;
    },
    remaining() {
      const now = c.now();
      return now >= until ? 0 : until - now;
    },
    reset() {
      until = 0;
    },
  };
}

export type CircuitState = 'closed' | 'open' | 'half-open';

export interface CircuitBreaker {
  state(): CircuitState;
  tryAcquire(): boolean;
  recordSuccess(): void;
  recordFailure(): void;
  execute<T>(fn: () => Promise<T>): Promise<T>;
  reset(): void;
}

export function createCircuitBreaker(
  config: { failureThreshold?: number; resetAfterMs?: number | string; halfOpenMax?: number },
  clock?: Clock,
): CircuitBreaker {
  const threshold = config.failureThreshold ?? 5;
  const resetAfter = parseDuration(config.resetAfterMs ?? 30000);
  const halfOpenMax = config.halfOpenMax ?? 1;
  const c = clock ?? defaultHub().clock;
  let state: CircuitState = 'closed';
  let failures = 0;
  let openedAt = 0;
  let halfOpenUsed = 0;
  function canPass(): boolean {
    const now = c.now();
    if (state === 'open' && now - openedAt >= resetAfter) {
      state = 'half-open';
      halfOpenUsed = 0;
    }
    if (state === 'half-open') {
      if (halfOpenUsed >= halfOpenMax) return false;
      halfOpenUsed += 1;
      return true;
    }
    return state === 'closed';
  }
  const breaker: CircuitBreaker = {
    state: () => (state === 'open' && c.now() - openedAt >= resetAfter ? 'half-open' : state),
    tryAcquire: canPass,
    recordSuccess() {
      failures = 0;
      state = 'closed';
      halfOpenUsed = 0;
    },
    recordFailure() {
      failures += 1;
      if (state === 'half-open' || failures >= threshold) {
        state = 'open';
        openedAt = c.now();
      }
    },
    async execute(fn) {
      if (!canPass()) {
        return Promise.reject(new Error('circuit breaker open'));
      }
      try {
        const value = await fn();
        breaker.recordSuccess();
        return value;
      } catch (err) {
        breaker.recordFailure();
        throw err;
      }
    },
    reset() {
      failures = 0;
      state = 'closed';
      halfOpenUsed = 0;
    },
  };
  return breaker;
}
