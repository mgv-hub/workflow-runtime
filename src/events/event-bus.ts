import { EventWaitTimeoutError } from '../errors.js';
import { newId } from '../ids.js';
import type { Clock, TimerHub } from '../clock.js';
import { SystemClock, TimerHub as Hub } from '../clock.js';

export interface EventRecord {
  id: string;
  name: string;
  payload?: unknown;
  time: number;
  meta?: Record<string, unknown>;
}

export type EventHandler = (event: EventRecord) => void | Promise<void>;

export interface SubscribeOptions {
  filter?: (event: EventRecord) => boolean;
}

export interface WaitForOptions extends SubscribeOptions {
  timeoutMs?: number;
}

interface Subscription {
  handler: EventHandler;
  filter?: (event: EventRecord) => boolean;
  once: boolean;
  off(): void;
}

export interface EventBus {
  on(name: string, handler: EventHandler, opts?: SubscribeOptions): () => void;
  once(name: string, handler: EventHandler, opts?: SubscribeOptions): () => void;
  off(name: string, handler: EventHandler): boolean;
  emit(name: string, payload?: unknown, meta?: Record<string, unknown>): void;
  waitFor(name: string, opts?: WaitForOptions): Promise<EventRecord>;
  recent(limit?: number): EventRecord[];
  listenerCount(name?: string): number;
  clear(): void;
  dispose(): void;
}

export interface EventBusOptions {
  hub?: TimerHub;
  clock?: Clock;
  recentLimit?: number;
  onError?: (error: unknown, event: EventRecord) => void;
}

export function createEventBus(opts: EventBusOptions = {}): EventBus {
  const clock = opts.clock ?? new SystemClock();
  let hub = opts.hub;
  let ownsHub = false;
  if (!hub) {
    hub = new Hub({ clock, driver: 'system' });
    ownsHub = true;
  }
  const recentLimit = opts.recentLimit ?? 0;
  const recentBuffer: EventRecord[] = [];
  const exact = new Map<string, Subscription[]>();
  const patterns: Array<{ prefix: string; subs: Subscription[] }> = [];
  const defaultOnError = (error: unknown) =>
    console.error('[workflow-runtime] event handler failed', error);

  function subscribe(
    pattern: string,
    handler: EventHandler,
    once: boolean,
    filter?: (e: EventRecord) => boolean,
  ): () => void {
    const sub: Subscription = {
      handler,
      filter,
      once,
      off: () => {
        if (pattern === '*' || pattern.endsWith('.*')) {
          for (const p of patterns) {
            if (p.prefix === (pattern === '*' ? '' : pattern.slice(0, -2))) {
              const i = p.subs.indexOf(sub);
              if (i >= 0) p.subs.splice(i, 1);
            }
          }
        } else {
          const list = exact.get(pattern);

          if (list) {
            const i = list.indexOf(sub);
            if (i >= 0) list.splice(i, 1);
            if (list.length === 0) exact.delete(pattern);
          }
        }
      },
    };

    if (pattern === '*') {
      addToPatterns('', sub);
    } else if (pattern.endsWith('.*')) {
      addToPatterns(pattern.slice(0, -2), sub);
    } else {
      let list = exact.get(pattern);
      if (!list) exact.set(pattern, (list = []));
      list.push(sub);
    }
    return () => sub.off();
  }

  function addToPatterns(prefix: string, sub: Subscription): void {
    let p = patterns.find((x) => x.prefix === prefix);
    if (!p) patterns.push((p = { prefix, subs: [] }));
    p.subs.push(sub);
  }

  function matching(name: string): Subscription[] {
    const out: Subscription[] = [];
    const list = exact.get(name);
    if (list) out.push(...list);

    for (const p of patterns) {
      if (p.prefix === '' || name.startsWith(p.prefix + '.') || name === p.prefix) {
        if (p.prefix === '' || name.startsWith(p.prefix + '.')) out.push(...p.subs);
      }
    }
    return out;
  }

  function removeSub(target: Subscription): void {
    target.off();
  }

  const bus: EventBus = {
    on(name, handler, o) {
      return subscribe(name, handler, false, o?.filter);
    },

    once(name, handler, o) {
      return subscribe(name, handler, true, o?.filter);
    },

    off(name, handler) {
      const list = exact.get(name);
      if (!list) return false;
      const i = list.findIndex((s) => s.handler === handler);
      if (i < 0) return false;
      list.splice(i, 1);
      if (list.length === 0) exact.delete(name);
      return true;
    },

    emit(name, payload, meta) {
      const event: EventRecord = { id: newId(), name, payload, time: clock.now(), meta };

      if (recentLimit > 0) {
        recentBuffer.push(event);
        if (recentBuffer.length > recentLimit)
          recentBuffer.splice(0, recentBuffer.length - recentLimit);
      }

      const subs = matching(name).slice();
      for (const sub of subs) {
        if (sub.filter && !sub.filter(event)) continue;
        if (sub.once) removeSub(sub);
        try {
          const r = sub.handler(event);
          if (r && typeof (r as Promise<void>).catch === 'function') {
            (r as Promise<void>).catch((err) => (opts.onError ?? defaultOnError)(err, event));
          }
        } catch (err) {
          (opts.onError ?? defaultOnError)(err, event);
        }
      }
    },

    waitFor(name, o) {
      return new Promise<EventRecord>((resolve, reject) => {
        let timer: { cancel(): void } | undefined;
        let done = false;
        const off = subscribe(
          name,
          (event) => {
            if (done) return;
            done = true;
            timer?.cancel();
            off();
            resolve(event);
          },
          true,
          o?.filter,
        );

        if (o?.timeoutMs != null && Number.isFinite(o.timeoutMs) && o.timeoutMs > 0) {
          timer = hub!.after(o.timeoutMs, () => {
            if (done) return;
            done = true;
            off();
            reject(new EventWaitTimeoutError(name, o.timeoutMs!));
          });
        }
      });
    },

    recent(limit) {
      return limit == null ? recentBuffer.slice() : recentBuffer.slice(-limit);
    },

    listenerCount(name) {
      if (name == null) {
        let n = 0;
        for (const l of exact.values()) n += l.length;
        for (const p of patterns) n += p.subs.length;
        return n;
      }

      if (name === '*' || name.endsWith('.*')) {
        const prefix = name === '*' ? '' : name.slice(0, -1);
        const p = patterns.find((x) => x.prefix === prefix);
        return p ? p.subs.length : 0;
      }
      return exact.get(name)?.length ?? 0;
    },

    clear() {
      exact.clear();
      patterns.length = 0;
      recentBuffer.length = 0;
    },

    dispose() {
      bus.clear();
      if (ownsHub) hub!.dispose();
    },
  };
  return bus;
}
