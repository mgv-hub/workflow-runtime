import { serializeError, errorMeta, ConfigurationError, type SerializedError } from '../errors.js';
import { makeIdFactory, type IdFactory } from '../ids.js';
import { computeRetryDelay, normalizeRetry } from '../policy.js';
import type { Clock, TimerHub, TimerHandle } from '../clock.js';
import { VirtualClock } from '../clock.js';
import type { EventBus } from '../events/event-bus.js';
import type { Logger } from '../observability/logger.js';
import type { MetricsRecorder } from '../observability/metrics.js';
import type { Middleware } from '../util/async.js';
import { composeMiddleware } from '../util/async.js';
import { parseDuration, toTime } from '../util/time.js';
import { storageKey, storagePrefix, type KeyValueStore } from '../storage/store.js';
import { nextCronRun } from './cron.js';
import { nextCalendarRun, normalizeCalendar, type CalendarSpec } from './calendar.js';

export type JobHandler = (ctx: JobContext) => void | Promise<void>;
export type JobAction =
  JobHandler | { workflow: string; input?: unknown } | { task: string; input?: unknown };

export interface JobContext {
  job: JobRecord;
  fireAt: number;
  logger: Logger;
}

export type JobKind = 'at' | 'delay' | 'every' | 'cron' | 'calendar';
export type JobSpec =
  | { kind: 'at'; at: number }
  | { kind: 'delay'; delay: number }
  | { kind: 'every'; every: number; startsAt?: number; endsAt?: number | null }
  | { kind: 'cron'; expr: string; tz?: string; endsAt?: number | null }
  | ({ kind: 'calendar'; endsAt?: number | null } & Omit<CalendarSpec, 'kind'>);

export type JobStatus = 'scheduled' | 'paused' | 'done' | 'failed' | 'cancelled';

export interface JobRecord {
  id: string;
  name?: string;
  kind: JobKind;
  spec: JobSpec;
  nextRunAt: number | null;
  lastRunAt: number | null;
  runCount: number;
  status: JobStatus;
  tags: string[];
  meta: Record<string, unknown>;
  misfireGraceMs: number;
  maxRuns: number | null;
  endsAt: number | null;
  createdAt: number;
  updatedAt: number;
  lastError?: SerializedError;
}

export interface JobHandle {
  readonly id: string;
  info(): JobRecord;
  cancel(): boolean;
  pause(): boolean;
  resume(): boolean;
  nextRuns(count: number, from?: number): number[];
}

export interface SchedulerOptions {
  misfireGraceMs?: number;
  idFactory?: IdFactory;
}

export interface SchedulerDeps {
  hub: TimerHub;
  clock: Clock;
  events: EventBus;
  logger: Logger;
  storage?: KeyValueStore;
  metrics?: MetricsRecorder;
  middleware?: Middleware<unknown>[];
  runWorkflow?: (id: string, input: unknown, meta: Record<string, unknown>) => unknown;
  enqueueTask?: (name: string, input: unknown, meta: Record<string, unknown>) => unknown;
}

function computeNext(spec: JobSpec, fromMs: number): number | null {
  switch (spec.kind) {
    case 'at':
      return spec.at > fromMs ? spec.at : null;
    case 'delay':
      return spec.delay > fromMs ? spec.delay : null;
    case 'every':
      return spec.startsAt != null && spec.startsAt > fromMs ? spec.startsAt : fromMs + spec.every;
    case 'cron':
      return nextCronRun(spec.expr, fromMs, spec.tz);
    case 'calendar':
      return nextCalendarRun(spec as unknown as CalendarSpec, fromMs);
  }
}

export class Scheduler {
  private jobs = new Map<string, { record: JobRecord; timer?: TimerHandle; action?: JobAction }>();
  private idFactory: IdFactory;
  private defaultGrace: number;
  private mw: <Ctx, T>(ctx: Ctx, core: () => Promise<T>) => Promise<T>;
  private prefix: string;

  constructor(
    private deps: SchedulerDeps,
    private namespace: string,
    opts: SchedulerOptions = {},
  ) {
    this.idFactory = makeIdFactory(opts.idFactory);
    this.defaultGrace = opts.misfireGraceMs ?? 60000;
    this.mw = composeMiddleware(deps.middleware ?? []) as never;
    this.prefix = storagePrefix('job', namespace);
  }

  at(when: number | Date, action: JobAction, opts: JobCreateOptions = {}): JobHandle {
    const at = toTime(when);
    return this.register({ kind: 'at', at }, action, opts);
  }

  delay(ms: number | string, action: JobAction, opts: JobCreateOptions = {}): JobHandle {
    return this.register(
      { kind: 'delay', delay: this.deps.clock.now() + parseDuration(ms) },
      action,
      opts,
    );
  }

  every(interval: number | string, action: JobAction, opts: JobCreateOptions = {}): JobHandle {
    const every = parseDuration(interval);
    if (every <= 0) throw new ConfigurationError('every() requires a positive interval');
    return this.register(
      {
        kind: 'every',
        every,
        startsAt: opts.startsAt != null ? toTime(opts.startsAt) : undefined,
        endsAt: opts.endsAt != null ? toTime(opts.endsAt) : null,
      },
      action,
      opts,
    );
  }

  cron(expr: string, action: JobAction, opts: CronOptions = {}): JobHandle {
    return this.register(
      { kind: 'cron', expr, tz: opts.tz, endsAt: opts.endsAt != null ? toTime(opts.endsAt) : null },
      action,
      opts,
    );
  }

  daily(time: string, action: JobAction, opts: CalendarOptions = {}): JobHandle {
    return this.register({ kind: 'calendar', kind2: 'daily' } as never, action, opts, {
      kind: 'daily',
      time,
      tz: opts.tz,
    });
  }

  weekly(days: number[], time: string, action: JobAction, opts: CalendarOptions = {}): JobHandle {
    return this.register({ kind: 'calendar' } as never, action, opts, {
      kind: 'weekly',
      days,
      time,
      tz: opts.tz,
    });
  }

  monthly(day: number, time: string, action: JobAction, opts: CalendarOptions = {}): JobHandle {
    return this.register({ kind: 'calendar' } as never, action, opts, {
      kind: 'monthly',
      day,
      time,
      tz: opts.tz,
    });
  }

  private register(
    bare: JobSpec | Record<string, unknown>,
    action: JobAction,
    opts: JobCreateOptions,
    calendar?: CalendarSpec & { endsAt?: number | null },
  ): JobHandle {
    const now = this.deps.clock.now();
    let spec: JobSpec;
    if (calendar) {
      const normalized = normalizeCalendar({ ...calendar, tz: calendar.tz });
      spec = {
        ...normalized,
        kind: 'calendar' as const,
        endsAt: opts.endsAt != null ? toTime(opts.endsAt) : null,
      } as JobSpec;
    } else {
      spec = bare as JobSpec;
    }

    const id = opts.id ?? this.idFactory();
    const existing = this.jobs.get(id);
    if (existing) {
      existing.action = action;
      return this.handleFor(existing);
    }

    const record: JobRecord = {
      id,
      name: opts.name,
      kind: spec.kind,
      spec,
      nextRunAt: null,
      lastRunAt: null,
      runCount: 0,
      status: 'scheduled',
      tags: opts.tags ?? [],
      meta: opts.meta ?? {},
      misfireGraceMs: opts.misfireGraceMs ?? this.defaultGrace,
      maxRuns: opts.maxRuns ?? null,
      endsAt: (spec as { endsAt?: number | null }).endsAt ?? null,
      createdAt: now,
      updatedAt: now,
    };

    const entry = { record, action };
    this.jobs.set(id, entry);
    const first = computeNext(spec, now);
    if (first == null || (record.endsAt != null && first > record.endsAt)) {
      this.finish(entry, 'done');
    } else {
      record.nextRunAt = first;
      this.arm(entry);
      this.persist(entry);
    }
    this.deps.events.emit('schedule.registered', {
      jobId: id,
      kind: record.kind,
      nextRunAt: record.nextRunAt,
    });
    return this.handleFor(entry);
  }

  private handleFor(entry: {
    record: JobRecord;
    timer?: TimerHandle;
    action?: JobAction;
  }): JobHandle {
    return {
      get id() {
        return entry.record.id;
      },
      info: () => ({ ...entry.record }),
      cancel: () => this.cancel(entry.record.id),
      pause: () => this.pause(entry.record.id),
      resume: () => this.resume(entry.record.id),
      nextRuns: (count: number, from?: number) => this.preview(entry.record.id, count, from),
    };
  }

  private arm(entry: { record: JobRecord; timer?: TimerHandle }): void {
    entry.timer?.cancel();
    entry.timer = undefined;
    if (entry.record.nextRunAt == null || entry.record.status !== 'scheduled') return;
    const dueAt = entry.record.nextRunAt;
    const delay = Math.max(0, dueAt - this.deps.clock.now());
    if (delay === 0) {
      queueMicrotask(() => {
        if (entry.record.status === 'scheduled') {
          void this.fire(entry.record.id);
        }
      });
      return;
    }

    entry.timer = this.deps.hub.after(delay, () => {
      void this.fire(entry.record.id);
    });
  }

  private async fire(id: string): Promise<void> {
    const entry = this.jobs.get(id);
    if (!entry || entry.record.status !== 'scheduled') return;
    const { record } = entry;
    const due = record.nextRunAt;
    if (due == null) return;

    let now = this.deps.clock.now();
    if (this.deps.clock instanceof VirtualClock) {
      const vc = this.deps.clock as VirtualClock;
      if (vc.advanceTarget > now) {
        now = vc.advanceTarget;
      }
    }

    if (now - due > record.misfireGraceMs) {
      this.deps.events.emit('schedule.skipped', { jobId: id, dueAt: due });
      record.lastRunAt = due;
      this.advanceToNext(entry, due);
      return;
    }
    record.lastRunAt = due;
    record.runCount += 1;
    record.nextRunAt = null;
    record.updatedAt = now;
    this.persist(entry);
    this.deps.events.emit('schedule.fired', { jobId: id, fireAt: due, runCount: record.runCount });
    const ctx: JobContext = {
      job: { ...record },
      fireAt: due,
      logger: this.deps.logger.child({ jobId: id }),
    };
    const started = this.deps.clock.now();
    try {
      await this.invoke(entry.action, ctx);
      this.deps.metrics?.observe('wr.job.duration', this.deps.clock.now() - started);
    } catch (err) {
      record.lastError = serializeError(err);
      this.deps.logger.warn('job failed', { jobId: id, ...errorMeta(err) });
      this.deps.metrics?.counter('wr.job.failures');
      this.deps.events.emit('schedule.failed', { jobId: id, error: record.lastError });

      if (this.isRecurring(record)) {
        this.advanceToNext(entry, due);
        return;
      }

      this.finish(entry, 'failed');
      return;
    }
    if (this.isRecurring(record)) {
      this.advanceToNext(entry, due);
    } else {
      this.finish(entry, 'done');
    }
  }

  private isRecurring(record: JobRecord): boolean {
    return record.kind === 'every' || record.kind === 'cron' || record.kind === 'calendar';
  }

  private advanceToNext(entry: { record: JobRecord; timer?: TimerHandle }, fromDue: number): void {
    const { record } = entry;
    let next: number | null;
    try {
      next = computeNext(record.spec, fromDue);
    } catch {
      next = null;
    }
    if (
      next == null ||
      (record.endsAt != null && next > record.endsAt) ||
      (record.maxRuns != null && record.runCount >= record.maxRuns)
    ) {
      this.finish(entry, 'done');
      return;
    }
    record.nextRunAt = next;
    record.updatedAt = this.deps.clock.now();
    this.arm(entry);
    this.persist(entry);
  }

  private async invoke(action: JobAction | undefined, ctx: JobContext): Promise<void> {
    if (!action) {
      throw new ConfigurationError(`No handler registered for persisted job "${ctx.job.id}"`);
    }
    if (typeof action === 'function') {
      await this.mw({ kind: 'job', ...ctx }, () => Promise.resolve(action(ctx)));
      return;
    }
    if ('workflow' in action) {
      this.deps.runWorkflow?.(action.workflow, action.input ?? null, { jobId: ctx.job.id });
      return;
    }
    this.deps.enqueueTask?.(action.task, action.input ?? null, { jobId: ctx.job.id });
  }

  private finish(
    entry: { record: JobRecord; timer?: TimerHandle },
    status: 'done' | 'failed' | 'cancelled',
  ): void {
    entry.timer?.cancel();
    entry.timer = undefined;
    entry.record.status = status;
    entry.record.nextRunAt = null;
    entry.record.updatedAt = this.deps.clock.now();
    const isRecurring = this.isRecurring(entry.record);
    if (status !== 'done' || isRecurring) {
      this.jobs.delete(entry.record.id);
      this.deps.storage?.delete(storageKey('job', this.namespace, entry.record.id));
    }
    this.deps.events.emit(
      status === 'done'
        ? 'schedule.completed'
        : status === 'failed'
          ? 'schedule.failed'
          : 'schedule.cancelled',
      { jobId: entry.record.id },
    );
  }

  cancel(id: string): boolean {
    const entry = this.jobs.get(id);
    if (!entry) return false;
    this.finish(entry, 'cancelled');
    return true;
  }

  pause(id: string): boolean {
    const entry = this.jobs.get(id);
    if (!entry || entry.record.status !== 'scheduled') return false;
    entry.timer?.cancel();
    entry.timer = undefined;
    entry.record.status = 'paused';
    entry.record.updatedAt = this.deps.clock.now();
    this.persist(entry);
    this.deps.events.emit('schedule.paused', { jobId: id });
    return true;
  }

  resume(id: string): boolean {
    const entry = this.jobs.get(id);
    if (!entry || entry.record.status !== 'paused') return false;
    entry.record.status = 'scheduled';
    entry.record.nextRunAt = computeNext(entry.record.spec, this.deps.clock.now());
    entry.record.updatedAt = this.deps.clock.now();
    if (entry.record.nextRunAt == null) {
      this.finish(entry, 'done');
      return true;
    }
    this.arm(entry);
    this.persist(entry);
    this.deps.events.emit('schedule.resumed', { jobId: id });
    return true;
  }

  preview(id: string, count: number, from?: number): number[] {
    const entry = this.jobs.get(id);
    if (!entry) return [];
    const out: number[] = [];
    let cursor = from ?? this.deps.clock.now();
    for (let i = 0; i < count; i++) {
      let next: number | null;
      try {
        next = computeNext(entry.record.spec, cursor + 1);
      } catch {
        next = null;
      }
      if (entry.record.kind === 'every' && entry.record.spec.kind === 'every') {
        next = cursor + entry.record.spec.every;
      }
      if (next == null) break;
      out.push(next);
      cursor = next;
    }
    return out;
  }

  get(id: string): JobRecord | undefined {
    const entry = this.jobs.get(id);
    return entry ? { ...entry.record } : undefined;
  }

  list(filter: { status?: JobStatus; tags?: string[] } = {}): JobRecord[] {
    const out: JobRecord[] = [];
    for (const entry of this.jobs.values()) {
      if (filter.status && entry.record.status !== filter.status) continue;
      if (filter.tags && !filter.tags.some((t) => entry.record.tags.includes(t))) continue;
      out.push({ ...entry.record });
    }
    return out;
  }

  private persist(entry: { record: JobRecord }): void {
    if (!this.deps.storage) return;
    void this.deps.storage.put(storageKey('job', this.namespace, entry.record.id), entry.record);
  }

  async loadPersisted(): Promise<number> {
    if (!this.deps.storage) return 0;
    const entries = await this.deps.storage.list(this.prefix);
    let loaded = 0;
    for (const { value } of entries) {
      const record = value as JobRecord;
      if (!record || typeof record.id !== 'string' || !record.spec) continue;
      if (this.jobs.has(record.id)) continue;
      const entry = {
        record,
        timer: undefined as TimerHandle | undefined,
        action: undefined as JobAction | undefined,
      };
      this.jobs.set(record.id, entry);
      if (record.status === 'scheduled' && record.nextRunAt != null) {
        this.arm(entry);
      }
      loaded += 1;
    }
    return loaded;
  }

  dispose(): void {
    for (const entry of this.jobs.values()) {
      entry.timer?.cancel();
      entry.timer = undefined;
    }
    this.jobs.clear();
  }
}

export interface JobCreateOptions {
  id?: string;
  name?: string;
  tags?: string[];
  meta?: Record<string, unknown>;
  misfireGraceMs?: number;
  maxRuns?: number;
  endsAt?: number | Date;
  startsAt?: number | Date;
}

export interface CronOptions extends JobCreateOptions {
  tz?: string;
}

export interface CalendarOptions extends JobCreateOptions {
  tz?: string;
}

export const schedulerRetryHelpers = { computeRetryDelay, normalizeRetry };
