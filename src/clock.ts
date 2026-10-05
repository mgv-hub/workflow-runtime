import { ConfigurationError } from './errors.js';
import { nextSeq } from './ids.js';
import { byKey, MinHeap } from './util/heap.js';

export interface Clock {
  now(): number;
}

export class SystemClock implements Clock {
  now(): number {
    return Date.now();
  }
}

export class VirtualClock implements Clock {
  private ms: number;
  private hooks = new Set<(target: number) => void>();
  public advanceTarget = 0;

  constructor(startAt = 0) {
    this.ms = startAt;
  }

  now(): number {
    return this.ms;
  }

  _setVirtualTime(ms: number): void {
    if (ms > this.ms) this.ms = ms;
  }

  onAdvance(hook: (target: number) => void): () => void {
    this.hooks.add(hook);
    return () => this.hooks.delete(hook);
  }

  advance(ms: number): void {
    this.advanceTo(this.ms + Math.max(0, ms));
  }

  advanceTo(target: number): void {
    const t = Math.max(target, this.ms);
    this.advanceTarget = t;
    for (const hook of this.hooks) hook(t);
    if (t > this.ms) this.ms = t;
  }
}

export interface TimerHandle {
  cancel(): void;
}

interface TimerEntry {
  seq: number;
  dueAt: number;
  run: () => void;
  canceled?: boolean;
}

export type TimerErrorHandler = (error: unknown, context: string) => void;

const MAX_FIRES_PER_BATCH = 10000;
const inertHandle: TimerHandle = { cancel() {} };

export interface TimerHubOptions {
  clock: Clock;
  driver: 'system' | 'virtual';
  onError?: TimerErrorHandler;
}

export class TimerHub {
  private heap = new MinHeap<TimerEntry>(byKey((e) => e.dueAt));
  private clock: Clock;
  private virtual: VirtualClock | null = null;
  private onError?: TimerErrorHandler;
  private sysTimer?: ReturnType<typeof setTimeout>;
  private armedFor: number | null = null;
  private disposed = false;

  constructor(opts: TimerHubOptions) {
    this.clock = opts.clock;
    this.onError = opts.onError;
    if (opts.driver === 'virtual') {
      if (!(opts.clock instanceof VirtualClock)) {
        throw new ConfigurationError('virtual driver requires a VirtualClock');
      }
      this.virtual = opts.clock;
      this.virtual.onAdvance((target: number) => this.processDue(target));
    }
  }

  now(): number {
    return this.clock.now();
  }

  get size(): number {
    return this.heap.size;
  }

  after(ms: number, run: () => void): TimerHandle {
    if (this.disposed) return inertHandle;
    const entry: TimerEntry = { seq: nextSeq(), dueAt: this.clock.now() + Math.max(0, ms), run };
    this.heap.push(entry);
    if (!this.virtual) this.sync();
    return {
      cancel: () => {
        if (entry.canceled) return;
        entry.canceled = true;
        if (!this.virtual) this.sync();
      },
    };
  }

  nextDueAt(): number | undefined {
    for (;;) {
      const head = this.heap.peek();
      if (!head) return undefined;
      if (head.canceled) {
        this.heap.pop();
        continue;
      }
      return head.dueAt;
    }
  }

  processDue(target: number): void {
    let fired = 0;
    for (;;) {
      const head = this.heap.peek();
      if (!head) break;
      if (head.canceled) {
        this.heap.pop();
        continue;
      }
      if (head.dueAt > target) break;
      this.heap.pop();
      this.virtual?._setVirtualTime(head.dueAt);
      fired += 1;
      if (fired > MAX_FIRES_PER_BATCH) {
        this.onError?.(
          new Error('workflow-runtime: timer batch limit exceeded (possible zero-delay loop)'),
          'timer',
        );
        break;
      }
      try {
        head.run();
      } catch (err) {
        this.onError?.(err, 'timer');
      }
    }
  }

  private sync(): void {
    if (this.disposed) return;
    const next = this.nextDueAt();
    if (this.armedFor === next) return;
    if (this.sysTimer) {
      clearTimeout(this.sysTimer);
      this.sysTimer = undefined;
      this.armedFor = null;
    }
    if (next == null) return;
    const delay = Math.min(Math.max(next - this.clock.now(), 1), 2147483647);
    this.sysTimer = setTimeout(() => {
      this.armedFor = null;
      this.sysTimer = undefined;
      this.processDue(this.clock.now());
      this.sync();
    }, delay);
    this.sysTimer.unref?.();
    this.armedFor = next;
  }

  dispose(): void {
    this.disposed = true;
    this.heap.clear();
    if (this.sysTimer) {
      clearTimeout(this.sysTimer);
      this.sysTimer = undefined;
    }
    this.armedFor = null;
  }
}
