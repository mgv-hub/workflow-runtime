import {
  RuntimeShuttingDownError,
  serializeError,
  TaskTimeoutError,
  type SerializedError,
} from '../errors.js';
import { makeIdFactory, nextSeq, type IdFactory } from '../ids.js';
import { computeRetryDelay, normalizeRetry, type RetryPolicy, shouldRetry } from '../policy.js';
import type { Clock, TimerHub, TimerHandle } from '../clock.js';
import { VirtualClock } from '../clock.js';
import type { EventBus } from '../events/event-bus.js';
import type { Logger } from '../observability/logger.js';
import type { MetricsRecorder } from '../observability/metrics.js';
import type { Middleware } from '../util/async.js';
import { composeMiddleware } from '../util/async.js';
import { raceTimeout, settle } from '../util/async.js';
import { byKey, MinHeap } from '../util/heap.js';
import { storageKey, storagePrefix, type KeyValueStore } from '../storage/store.js';
import { createRateLimiter, type RateLimiter } from '../util/controls.js';

export type TaskStatus =
  'queued' | 'delayed' | 'running' | 'succeeded' | 'failed' | 'dead' | 'cancelled';

export interface TaskContext {
  taskId: string;
  name: string;
  group: string;
  attempt: number;
  signal: AbortSignal;
  logger: Logger;
  tags: string[];
  meta: Record<string, unknown>;
  startedAt: number;
}

export interface TaskSpec {
  name: string;
  run: (ctx: TaskContext) => unknown | Promise<unknown>;
  priority?: number;
  group?: string;
  delayUntil?: number | Date;
  timeoutMs?: number;
  retries?: RetryPolicy;
  tags?: string[];
  meta?: Record<string, unknown>;
  dedupeKey?: string;
  id?: string;
}

export interface TaskRecord {
  id: string;
  name: string;
  group: string;
  status: TaskStatus;
  priority: number;
  attempts: number;
  maxAttempts: number;
  createdAt: number;
  startAfter: number | null;
  startedAt: number | null;
  endedAt: number | null;
  result: unknown;
  error: SerializedError | null;
  tags: string[];
  meta: Record<string, unknown>;
  dedupeKey: string | null;
  nextAttemptAt: number | null;
}

export interface TaskHandle {
  readonly id: string;
  promise: Promise<TaskRecord>;
  cancel(): boolean;
  record(): TaskRecord | undefined;
}

export interface TaskGroupConfig {
  concurrency?: number;
  rateLimit?: { max: number; windowMs: number };
  persist?: boolean;
}

interface QueueEntry {
  seq: number;
  priority: number;
  task: InternalTask;
}

interface DelayEntry {
  seq: number;
  startAfter: number;
  task: InternalTask;
}

interface InternalTask {
  record: TaskRecord;
  run: TaskSpec['run'];
  timeoutMs: number | undefined;
  retries: ReturnType<typeof normalizeRetry>;
  controller: AbortController | null;
  resolve: ((rec: TaskRecord) => void) | null;
  readySeq: number;
}

interface GroupState {
  config: TaskGroupConfig;
  running: number;
  limiter: RateLimiter | null;
  persist: boolean;
  retryTimer?: TimerHandle;
}

export class TaskQueue {
  private groups = new Map<string, GroupState>();
  private ready = new MinHeap<QueueEntry>(byKey((e) => -e.priority));
  private delayedHeap = new MinHeap<DelayEntry>(byKey((e) => e.startAfter));
  private tasks = new Map<string, InternalTask>();
  private dedupe = new Map<string, string>();
  private deadLetters = new Map<string, TaskRecord>();
  private deadTimers: TimerHandle[] = [];
  private globalConcurrency: number;
  private running = 0;
  private delayTimer?: TimerHandle;
  private accepting = true;
  private paused = false;
  private idFactory: IdFactory;
  private mw: <Ctx, T>(ctx: Ctx, core: () => Promise<T>) => Promise<T>;
  private prefix: string;
  private deadRetention: number;
  private maxDead: number;

  constructor(
    private deps: {
      hub: TimerHub;
      clock: Clock;
      events: EventBus;
      logger: Logger;
      metrics?: MetricsRecorder;
      storage?: KeyValueStore;
      middleware?: Middleware<unknown>[];
    },
    private namespace: string,
    opts: {
      concurrency?: number;
      idFactory?: IdFactory;
      deadLetterRetentionMs?: number;
      maxDeadLetters?: number;
    } = {},
  ) {
    this.globalConcurrency = opts.concurrency ?? 4;
    this.idFactory = makeIdFactory(opts.idFactory);
    this.deadRetention = opts.deadLetterRetentionMs ?? 3600000;
    this.maxDead = opts.maxDeadLetters ?? 1000;
    this.mw = composeMiddleware(deps.middleware ?? []) as never;
    this.prefix = storagePrefix('task', namespace);
  }

  group(name: string, config: TaskGroupConfig = {}): void {
    const existing = this.groups.get(name);
    if (existing) {
      existing.config = { ...existing.config, ...config };
      if (config.rateLimit) existing.limiter = createRateLimiter(config.rateLimit, this.deps.clock);
      return;
    }
    this.groups.set(name, {
      config,
      running: 0,
      limiter: config.rateLimit ? createRateLimiter(config.rateLimit, this.deps.clock) : null,
      persist: config.persist ?? false,
    });
  }

  enqueue(spec: TaskSpec): TaskHandle {
    if (!this.accepting) throw new RuntimeShuttingDownError();
    const groupName = spec.group ?? 'default';
    if (!this.groups.has(groupName)) this.group(groupName, {});
    if (spec.dedupeKey && this.dedupe.has(spec.dedupeKey)) {
      const existingId = this.dedupe.get(spec.dedupeKey)!;
      const existing = this.tasks.get(existingId);
      if (existing) {
        this.deps.logger.debug('task deduplicated', {
          taskId: existingId,
          dedupeKey: spec.dedupeKey,
        });
        return this.handleFor(existing);
      }
    }
    const now = this.deps.clock.now();
    const retries = normalizeRetry(spec.retries);
    const id = spec.id ?? this.idFactory();
    const startAfter =
      spec.delayUntil != null
        ? spec.delayUntil instanceof Date
          ? spec.delayUntil.getTime()
          : spec.delayUntil
        : null;
    const record: TaskRecord = {
      id,
      name: spec.name,
      group: groupName,
      status: startAfter != null && startAfter > now ? 'delayed' : 'queued',
      priority: spec.priority ?? 0,
      attempts: 0,
      maxAttempts: retries.max + 1,
      createdAt: now,
      startAfter,
      startedAt: null,
      endedAt: null,
      result: null,
      error: null,
      tags: spec.tags ?? [],
      meta: spec.meta ?? {},
      dedupeKey: spec.dedupeKey ?? null,
      nextAttemptAt: null,
    };
    const task: InternalTask = {
      record,
      run: spec.run,
      timeoutMs: spec.timeoutMs,
      retries,
      controller: null,
      resolve: null,
      readySeq: 0,
    };
    let promiseResolve!: (rec: TaskRecord) => void;
    const promise = new Promise<TaskRecord>((r) => {
      promiseResolve = r;
    });

    task.resolve = promiseResolve;
    this.tasks.set(id, task);
    if (record.dedupeKey) this.dedupe.set(record.dedupeKey, id);
    if (task.record.status === 'delayed') {
      this.delayedHeap.push({ seq: nextSeq(), startAfter: startAfter!, task });
    } else {
      this.pushReady(task);
    }

    this.persist(task);
    this.syncDelayTimer();
    this.deps.events.emit('task.queued', { taskId: id, name: record.name, group: groupName });
    queueMicrotask(() => this.pump());
    return {
      id,
      promise,
      cancel: () => this.cancel(id),
      record: () =>
        this.tasks.get(id)?.record
          ? { ...this.tasks.get(id)!.record }
          : this.deadLetters.get(id)
            ? { ...this.deadLetters.get(id)! }
            : undefined,
    } as TaskHandle;
  }

  private handleFor(task: InternalTask): TaskHandle {
    const promise = new Promise<TaskRecord>((resolve) => {
      const existing = task.resolve;
      task.resolve = (rec) => {
        existing?.(rec);
        resolve(rec);
      };
      if (
        task.record.status === 'succeeded' ||
        task.record.status === 'failed' ||
        task.record.status === 'dead' ||
        task.record.status === 'cancelled'
      ) {
        resolve({ ...task.record });
      }
    });
    return {
      get id() {
        return task.record.id;
      },
      promise,
      cancel: () => this.cancel(task.record.id),
      record: () => ({ ...task.record }),
    };
  }

  private pushReady(task: InternalTask): void {
    task.record.status = 'queued';
    task.readySeq = nextSeq();
    this.ready.push({ seq: task.readySeq, priority: task.record.priority, task });
  }

  private syncDelayTimer(): void {
    for (;;) {
      const head = this.delayedHeap.peek();
      if (!head) {
        this.delayTimer?.cancel();
        this.delayTimer = undefined;
        return;
      }
      if (head.task.record.status !== 'delayed' && head.task.record.status !== 'running') {
        this.delayedHeap.pop();
        continue;
      }
      if (head.task.record.status === 'running') {
        this.delayedHeap.pop();
        continue;
      }
      break;
    }

    const head = this.delayedHeap.peek()!;
    this.delayTimer?.cancel();
    const delay = Math.max(0, head.startAfter - this.deps.clock.now());

    if (delay === 0) {
      queueMicrotask(() => {
        for (;;) {
          const due = this.delayedHeap.peek();
          if (!due || due.startAfter > this.deps.clock.now()) break;
          this.delayedHeap.pop();
          if (due.task.record.status === 'delayed') {
            this.pushReady(due.task);
            this.persist(due.task);
          }
        }
        this.pump();
        this.syncDelayTimer();
      });
      return;
    }

    this.delayTimer = this.deps.hub.after(delay, () => {
      for (;;) {
        const due = this.delayedHeap.peek();
        if (!due || due.startAfter > this.deps.clock.now()) break;
        this.delayedHeap.pop();
        if (due.task.record.status === 'delayed') {
          this.pushReady(due.task);
          this.persist(due.task);
        }
      }
      this.pump();
      this.syncDelayTimer();
    });
  }

  private pump(): void {
    if (this.paused || !this.accepting) return;
    const deferred: QueueEntry[] = [];

    while (this.ready.size > 0 && this.running < this.globalConcurrency) {
      const entry = this.ready.pop()!;
      const { task } = entry;
      if (task.record.status !== 'queued') continue;
      const group = this.groups.get(task.record.group) ?? {
        config: {},
        running: 0,
        limiter: null,
        persist: false,
      };
      const groupConcurrency = group.config.concurrency ?? this.globalConcurrency;
      if (group.running >= groupConcurrency) {
        deferred.push(entry);
        continue;
      }
      if (group.limiter && !group.limiter.tryAcquire()) {
        deferred.push(entry);
        if (!group.retryTimer && group.config.rateLimit) {
          group.retryTimer = this.deps.hub.after(group.config.rateLimit.windowMs, () => {
            group.retryTimer = undefined;
            this.pump();
          });
        }
        continue;
      }
      void this.start(task);
    }

    for (const entry of deferred) this.ready.push(entry);
  }

  private async start(task: InternalTask): Promise<void> {
    const { record } = task;
    const group = this.groups.get(record.group)!;

    group.running += 1;
    this.running += 1;
    record.status = 'running';
    record.startedAt = this.deps.clock.now();
    record.attempts += 1;
    this.deps.metrics?.observe('wr.queue.latency', record.startedAt - record.createdAt);
    this.persist(task);
    this.deps.events.emit('task.started', {
      taskId: record.id,
      name: record.name,
      attempt: record.attempts,
    });
    const controller = new AbortController();
    task.controller = controller;
    const ctx: TaskContext = {
      taskId: record.id,
      name: record.name,
      group: record.group,
      attempt: record.attempts,
      signal: controller.signal,
      logger: this.deps.logger.child({ taskId: record.id }),
      tags: record.tags,
      meta: record.meta,
      startedAt: record.startedAt,
    };

    const started = this.deps.clock.now();
    const outcome = await raceTimeout(
      settle(
        this.mw({ kind: 'task', ...ctx }, async () => {
          return await task.run(ctx);
        }),
      ),
      task.timeoutMs,
      this.deps.hub,
      () => new TaskTimeoutError(record.id, task.timeoutMs!),
    );

    const endedAt = this.deps.clock.now();
    this.deps.metrics?.observe('wr.task.duration', endedAt - started);
    task.controller = null;
    if (outcome.settled.ok) {
      record.status = 'succeeded';
      record.result = (outcome.settled as { ok: true; value: unknown }).value;
      record.endedAt = endedAt;
      this.finishTask(task);
      return;
    }

    const err = outcome.settled.error;
    record.error = serializeError(err);
    if (outcome.timedOut) {
      record.error = serializeError(new TaskTimeoutError(record.id, task.timeoutMs ?? 0));
    }

    if (record.attempts < record.maxAttempts && shouldRetry(task.retries, err)) {
      const backoff = computeRetryDelay(task.retries, record.attempts, err);
      record.status = 'delayed';
      record.nextAttemptAt = endedAt + backoff;
      record.startAfter = record.nextAttemptAt;
      this.delayedHeap.push({ seq: nextSeq(), startAfter: record.nextAttemptAt, task });
      this.persist(task);
      this.deps.events.emit('task.retry', {
        taskId: record.id,
        attempt: record.attempts,
        nextAttemptAt: record.nextAttemptAt,
      });
      this.releaseSlots(task);
      this.syncDelayTimer();
      this.pump();
      return;
    }

    record.status = 'dead';
    record.endedAt = endedAt;
    this.moveToDeadLetter(task);
  }

  private finishTask(task: InternalTask): void {
    const { record } = task;
    record.endedAt = this.deps.clock.now();
    this.releaseSlots(task);
    this.clearDedupe(task);
    task.resolve?.({ ...record });
    this.tasks.delete(record.id);

    this.deps.events.emit('task.succeeded', {
      taskId: record.id,
      name: record.name,
      duration: record.endedAt - (record.startedAt ?? record.createdAt),
    });

    if (this.groupPersist(task) && this.deps.storage) {
      void this.deps.storage.delete(storageKey('task', this.namespace, record.id));
    }

    this.pump();
    this.syncDelayTimer();
  }

  private moveToDeadLetter(task: InternalTask): void {
    const { record } = task;
    this.releaseSlots(task);
    this.clearDedupe(task);
    this.deps.metrics?.counter('wr.task.failures');
    this.deps.events.emit('task.dead', {
      taskId: record.id,
      name: record.name,
      error: record.error,
    });

    this.deadLetters.set(record.id, { ...record });
    if (this.deadLetters.size > this.maxDead) {
      const oldest = this.deadLetters.keys().next().value;
      if (oldest !== undefined) this.deadLetters.delete(oldest);
    }

    this.deadTimers.push(
      this.deps.hub.after(this.deadRetention, () => {
        this.deadLetters.delete(record.id);
      }),
    );

    if (this.deadTimers.length > 64) this.deadTimers.splice(0, 32).forEach((t) => t.cancel());

    task.resolve?.({ ...record });
    this.tasks.delete(record.id);

    if (this.groupPersist(task) && this.deps.storage) {
      void this.deps.storage.delete(storageKey('task', this.namespace, record.id));
    }

    this.pump();
    this.syncDelayTimer();
  }

  private releaseSlots(task: InternalTask): void {
    const group = this.groups.get(task.record.group);
    if (group && group.running > 0) group.running -= 1;
    if (this.running > 0) this.running -= 1;
  }

  private clearDedupe(task: InternalTask): void {
    if (task.record.dedupeKey && this.dedupe.get(task.record.dedupeKey) === task.record.id) {
      this.dedupe.delete(task.record.dedupeKey);
    }
  }

  private groupPersist(task: InternalTask): boolean {
    return this.groups.get(task.record.group)?.persist ?? false;
  }

  private persist(task: InternalTask): void {
    if (!this.groupPersist(task) || !this.deps.storage) return;
    if (task.record.status === 'succeeded' || task.record.status === 'cancelled') return;
    void this.deps.storage.put(storageKey('task', this.namespace, task.record.id), {
      ...task.record,
      run: undefined,
    });
  }

  cancel(id: string): boolean {
    const task = this.tasks.get(id);
    if (!task) return false;

    if (task.record.status === 'running') {
      task.controller?.abort(new Error('cancelled'));
      return true;
    }

    task.record.status = 'cancelled';
    task.record.endedAt = this.deps.clock.now();
    this.clearDedupe(task);
    task.resolve?.({ ...task.record });
    this.tasks.delete(id);
    this.deps.events.emit('task.queued', undefined as never);
    this.deps.events.emit?.('task.failed', undefined as never);
    this.deps.events.emit('task.dead', undefined as never);
    this.pump();
    this.syncDelayTimer();
    return true;
  }

  pause(): void {
    this.paused = true;
  }

  resume(): void {
    this.paused = false;
    this.pump();
  }

  get(id: string): TaskRecord | undefined {
    const live = this.tasks.get(id);
    if (live) return { ...live.record };
    const dead = this.deadLetters.get(id);
    return dead ? { ...dead } : undefined;
  }

  list(filter: { status?: TaskStatus; group?: string; tags?: string[] } = {}): TaskRecord[] {
    const out: TaskRecord[] = [];

    const collect = (rec: TaskRecord) => {
      if (filter.status && rec.status !== filter.status) return;
      if (filter.group && rec.group !== filter.group) return;
      if (filter.tags && !filter.tags.some((t) => rec.tags.includes(t))) return;
      out.push({ ...rec });
    };

    for (const task of this.tasks.values()) collect(task.record);
    for (const rec of this.deadLetters.values()) collect(rec);
    return out;
  }

  stats(): {
    queued: number;
    delayed: number;
    running: number;
    succeeded: number;
    failed: number;
    dead: number;
    deadLetters: number;
  } {
    let queued = 0,
      delayed = 0,
      running = 0;

    for (const task of this.tasks.values()) {
      if (task.record.status === 'queued') queued += 1;
      else if (task.record.status === 'delayed') delayed += 1;
      else if (task.record.status === 'running') running += 1;
    }
  
    this.deps.metrics?.gauge('wr.tasks.running', running);

    return {
      queued,
      delayed,
      running,
      succeeded: 0,
      failed: 0,
      dead: 0,
      deadLetters: this.deadLetters.size,
    };
  }

  deadLetter(): {
    list: () => TaskRecord[];
    retry: (id: string) => boolean;
    discard: (id: string) => boolean;
    size: () => number;
  } {
    return {
      list: () => [...this.deadLetters.values()].map((r) => ({ ...r })),
      retry: (id) => {
        const rec = this.deadLetters.get(id);
        if (!rec) return false;
        this.deadLetters.delete(id);

        void this.enqueue({
          name: rec.name,
          run: () => {
            throw new Error('replayed dead letter requires a run function');
          },
          priority: rec.priority,
          group: rec.group,
          tags: rec.tags,
          meta: rec.meta,
          dedupeKey: rec.dedupeKey ?? undefined,
          id: undefined,
        }).promise.catch(() => undefined);
        return true;
      },
      discard: (id) => this.deadLetters.delete(id),
      size: () => this.deadLetters.size,
    };
  }

  async drain(deadlineMs: number, cancelRunning: boolean): Promise<void> {
    this.accepting = false;
    if (cancelRunning) {
      for (const task of this.tasks.values()) {
        if (task.record.status === 'running') {
          task.controller?.abort(new Error('shutdown'));
        }
      }
    }

    if (this.deps.clock instanceof VirtualClock) {
      await new Promise<void>((r) => queueMicrotask(r));
      return;
    }

    const deadline = this.deps.clock.now() + deadlineMs;
    while (this.running > 0 && this.deps.clock.now() < deadline) {
      await new Promise<void>((r) => this.deps.hub.after(25, () => r()));
    }

    if (cancelRunning) {
      const grace = this.deps.clock.now() + 250;
      while (this.running > 0 && this.deps.clock.now() < grace) {
        await new Promise<void>((r) => this.deps.hub.after(25, () => r()));
      }
    }
  }

  async loadPersisted(policy: 'requeue' | 'dead-letter' | 'drop' = 'requeue'): Promise<number> {
    if (!this.deps.storage) return 0;
    const entries = await this.deps.storage.list(this.prefix);
    let restored = 0;

    for (const { value } of entries) {
      const raw = value as TaskRecord;
      if (!raw || typeof raw.id !== 'string') continue;
      if (raw.status === 'succeeded' || raw.status === 'cancelled') continue;

      if (policy === 'drop') {
        void this.deps.storage.delete(storageKey('task', this.namespace, raw.id));
        continue;
      }

      if (policy === 'dead-letter') {
        this.deadLetters.set(raw.id, raw);
        void this.deps.storage.delete(storageKey('task', this.namespace, raw.id));
        continue;
      }

      raw.status = 'delayed';
      raw.startAfter = raw.startAfter ?? this.deps.clock.now();

      void this.enqueue({
        name: raw.name,
        run: () => {
          throw new Error('persisted task re-run requires an in-code handler');
        },
        priority: raw.priority,
        group: raw.group,
        tags: raw.tags,
        meta: raw.meta,
        dedupeKey: raw.dedupeKey ?? undefined,
        delayUntil: raw.startAfter,
      }).promise.catch(() => undefined);
      restored += 1;
    }
    return restored;
  }

  dispose(): void {
    this.delayTimer?.cancel();
    this.delayTimer = undefined;
    for (const t of this.deadTimers) t.cancel();
    this.deadTimers = [];
    this.tasks.clear();
    this.deadLetters.clear();
    this.ready.clear();
    this.delayedHeap.clear();
  }
}
