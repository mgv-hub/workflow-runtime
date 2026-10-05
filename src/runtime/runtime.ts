import { VirtualClock, SystemClock, TimerHub, type Clock } from '../clock.js';
import { createEventBus, type EventBus } from '../events/event-bus.js';
import { createConsoleLogger, type Logger } from '../observability/logger.js';
import {
  createMetrics,
  type MetricsRecorder,
  type MetricsSnapshot,
} from '../observability/metrics.js';
import { HistoryManager } from '../observability/history.js';
import { InMemoryStore, type KeyValueStore } from '../storage/store.js';
import { InProcessLockProvider, type LockProvider } from '../storage/locks.js';
import { LockManager } from '../storage/locks.js';
import { StateManager } from '../state/state-manager.js';
import { Scheduler } from '../scheduler/scheduler.js';
import { EntityManager, type EntityRecord } from '../entities/entity-manager.js';
import { TaskQueue, type TaskRecord } from '../queue/task-queue.js';
import { WorkflowEngine, type EngineFilter } from '../workflow/engine.js';
import { WorkflowRegistry } from '../workflow/registry.js';
import type { ExecutionRecord } from '../workflow/types.js';
import { newId } from '../ids.js';
import type { Middleware } from '../util/async.js';
import type { JobRecord } from '../scheduler/scheduler.js';
import { errorMeta, type SerializedError } from '../errors.js';

export type RuntimeState = 'created' | 'started' | 'draining' | 'stopped';

export interface TracingHooks {
  onSpan?(span: {
    name: string;
    kind: 'workflow' | 'task' | 'job';
    id: string;
    correlationId?: string;
    startedAt: number;
    endedAt?: number;
    status: 'ok' | 'error' | 'cancelled';
    error?: SerializedError;
  }): void;
}

export interface RuntimeOptions {
  namespace?: string;
  clock?: Clock | 'virtual';
  storage?: KeyValueStore;
  locks?: LockProvider;
  logger?: Logger;
  metrics?: MetricsRecorder;
  tracing?: TracingHooks;
  middleware?: Middleware<unknown>[];
  concurrency?: number;
  executionRetentionMs?: number;
  history?: { maxStreams?: number; maxEntriesPerStream?: number };
  recovery?: {
    executions?: 'resume' | 'fail' | 'drop';
    tasks?: 'requeue' | 'dead-letter' | 'drop';
  };
  entities?: { sweepIntervalMs?: number };
  deadLetter?: { retentionMs?: number; max?: number };
  events?: { recentLimit?: number };
  handleSignals?: boolean;
  idFactory?: () => string;
}

export interface HealthReport {
  state: RuntimeState;
  uptimeMs: number;
  activeExecutions: number;
  queuedTasks: number;
  runningTasks: number;
  deadLetters: number;
  scheduledJobs: number;
  entities: Record<string, number>;
  timers: number;
}

export interface InspectionReport {
  namespace: string;
  state: RuntimeState;
  clock: { virtual: boolean; now: number };
  counts: {
    workflows: number;
    executions: number;
    entities: number;
    jobs: number;
    tasks: number;
  };
  config: {
    concurrency: number;
    executionRetentionMs: number;
    recovery: Required<NonNullable<RuntimeOptions['recovery']>>;
  };
}

export interface RuntimeStartReport {
  recoveredExecutions: { recovered: number; failed: number; dropped: number };
  requeuedTasks: number;
  loadedJobs: number;
  loadedEntities: number;
}

export interface Runtime {
  readonly namespace: string;
  readonly state: RuntimeState;
  readonly clock: Clock;
  readonly events: EventBus;
  readonly storage: KeyValueStore;
  readonly logger: Logger;
  readonly metrics: MetricsRecorder & { snapshot(): MetricsSnapshot };
  readonly history: HistoryManager;
  readonly tracing: TracingHooks;
  readonly stateManager: StateManager;
  readonly locks: LockManager;
  readonly scheduler: Scheduler;
  readonly entities: EntityManager;
  readonly tasks: TaskQueue;
  readonly workflows: WorkflowEngine;
  queries: {
    executions(f?: {
      status?: string;
      workflowId?: string;
      tags?: string[];
      limit?: number;
    }): ExecutionRecord[];
    entities(f?: {
      type?: string;
      status?: string;
      tags?: string[];
      limit?: number;
    }): EntityRecord[];
    tasks(f?: { status?: string; group?: string; tags?: string[] }): TaskRecord[];
    jobs(f?: { status?: string; tags?: string[] }): JobRecord[];
  };
  start(): Promise<RuntimeStartReport>;
  shutdown(opts?: { drainMs?: number; cancelRunning?: boolean }): Promise<void>;
  health(): HealthReport;
  inspect(): InspectionReport;
}

export function createRuntime(options: RuntimeOptions = {}): Runtime {
  const namespace = options.namespace ?? 'default';
  const clock: Clock =
    options.clock === 'virtual' ? new VirtualClock() : (options.clock ?? new SystemClock());

  const virtual = clock instanceof VirtualClock;
  const logger = options.logger ?? createConsoleLogger();
  const nsLogger = logger.child({ ns: namespace });
  const hub = new TimerHub({
    clock,
    driver: virtual ? 'virtual' : 'system',
    onError: (err, context) => nsLogger.error('timer error', { context, ...errorMeta(err) }),
  });

  const events = createEventBus({
    hub,
    clock,
    recentLimit: options.events?.recentLimit ?? 100,
    onError: (err) => nsLogger.warn('event handler failed', errorMeta(err)),
  });

  const storage = options.storage ?? new InMemoryStore();
  const lockProvider = options.locks ?? new InProcessLockProvider();

  const metrics = (options.metrics ?? createMetrics()) as MetricsRecorder & {
    snapshot(): MetricsSnapshot;
  };

  const tracing: TracingHooks = options.tracing ?? {};
  const history = new HistoryManager(clock, options.history ?? {});
  const middleware = options.middleware ?? [];
  const stateManager = new StateManager({ storage, namespace, events, logger: nsLogger });
  const registry = new WorkflowRegistry();

  const recoveryConfig = {
    executions: options.recovery?.executions ?? 'resume',
    tasks: options.recovery?.tasks ?? 'requeue',
  };

  let startedAt = 0;
  let state: RuntimeState = 'created';
  let signalHandlers: Array<[NodeJS.Signals, () => void]> | null = null;

  const runtime: Runtime = {
    namespace,
    get state() {
      return state;
    },
    clock,
    events,
    storage,
    logger: nsLogger,
    metrics,
    history,
    tracing,
    get stateManager() {
      return stateManager;
    },
    get locks() {
      return new LockManager(lockProvider, { hub, clock, logger: nsLogger });
    },
    scheduler: new Scheduler(
      {
        hub,
        clock,
        events,
        logger: nsLogger,
        storage,
        metrics,
        middleware,
        runWorkflow: (id, input) => runtime.workflows.start(id, { input }),
        enqueueTask: undefined,
      },
      namespace,
      { idFactory: options.idFactory },
    ) as never,
    entities: new EntityManager(
      { hub, clock, events, logger: nsLogger, history, storage },
      namespace,
      { idFactory: options.idFactory, sweepIntervalMs: options.entities?.sweepIntervalMs },
    ) as never,
    tasks: new TaskQueue(
      { hub, clock, events, logger: nsLogger, metrics, storage, middleware },
      namespace,
      {
        concurrency: options.concurrency ?? 4,
        idFactory: options.idFactory,
        deadLetterRetentionMs: options.deadLetter?.retentionMs,
        maxDeadLetters: options.deadLetter?.max,
      },
    ) as never,
    workflows: new WorkflowEngine({
      hub,
      clock,
      events,
      logger: nsLogger,
      history,
      metrics,
      storage,
      registry,
      namespace,
      runtime: null as never,
      middleware,
      idFactory: options.idFactory,
      retentionMs: options.executionRetentionMs ?? 1800000,
    }) as never,
    queries: {
      executions: (f?: { status?: string; workflowId?: string; tags?: string[]; limit?: number }) =>
        (runtime.workflows as unknown as { list: (f?: EngineFilter) => ExecutionRecord[] }).list(
          f as EngineFilter,
        ),

      entities: (f?: { type?: string; status?: string; tags?: string[]; limit?: number }) =>
        (runtime.entities as unknown as { list: (f?: unknown) => EntityRecord[] }).list(f),

      tasks: (f?: { status?: string; group?: string; tags?: string[] }) =>
        (runtime.tasks as unknown as { list: (f?: unknown) => TaskRecord[] }).list(f),

      jobs: (f?: { status?: string; tags?: string[] }) =>
        (runtime.scheduler as unknown as { list: (f?: unknown) => JobRecord[] }).list(f),
    },

    async start() {
      if (state !== 'created') throw new Error('runtime already started');
      state = 'started';
      startedAt = clock.now();
      const loadedEntities = await (
        runtime.entities as unknown as { loadPersisted: () => Promise<number> }
      ).loadPersisted();
      const loadedJobs = await (
        runtime.scheduler as unknown as { loadPersisted: () => Promise<number> }
      ).loadPersisted();
      const recoveredExecutions = await (
        runtime.workflows as unknown as { loadPersisted: (p: unknown) => Promise<unknown> }
      ).loadPersisted(recoveryConfig.executions);
      const requeuedTasks = await (
        runtime.tasks as unknown as { loadPersisted: (p: unknown) => Promise<number> }
      ).loadPersisted(recoveryConfig.tasks);
      events.emit('runtime.started', { namespace });
      return {
        recoveredExecutions: recoveredExecutions as {
          recovered: number;
          failed: number;
          dropped: number;
        },
        requeuedTasks,
        loadedJobs,
        loadedEntities,
      };
    },

    async shutdown(opts: { drainMs?: number; cancelRunning?: boolean } = {}) {
      if (state === 'stopped') return;

      state = 'draining';
      events.emit('runtime.draining', { namespace });
      const drainMs = opts.drainMs ?? 5000;
      const cancelRunning = opts.cancelRunning ?? true;
      await (
        runtime.tasks as unknown as { drain: (ms: number, cr: boolean) => Promise<void> }
      ).drain(drainMs, cancelRunning);
      await (runtime.workflows as unknown as { drain: (cr: boolean) => Promise<void> }).drain(
        cancelRunning,
      );
      if (signalHandlers) {
        for (const [sig, handler] of signalHandlers) process.removeListener(sig, handler);
        signalHandlers = null;
      }

      events.emit('runtime.stopped', { namespace });
      (runtime.scheduler as unknown as { dispose: () => void }).dispose();
      (runtime.entities as unknown as { dispose: () => void }).dispose();
      (runtime.workflows as unknown as { dispose: () => void }).dispose();
      (runtime.tasks as unknown as { dispose: () => void }).dispose();

      events.clear();
      hub.dispose();
      await (storage as { close?: () => void | Promise<void> }).close?.();
      lockProvider.dispose?.();
      state = 'stopped';
    },

    health() {
      const taskStats = (runtime.tasks as unknown as { stats: () => unknown }).stats() as {
        queued: number;
        delayed: number;
        running: number;
        deadLetters: number;
      };

      const entityStats = (
        runtime.entities as unknown as { stats: () => unknown }
      ).stats() as Record<string, number>;
      const execs = (runtime.workflows as unknown as { list: (f: unknown) => ExecutionRecord[] })
        .list({})
        .filter((e) => e.status === 'running' || e.status === 'pending');
      const jobs = (runtime.scheduler as unknown as { list: (f: unknown) => JobRecord[] }).list({
        status: 'scheduled',
      });

      metrics.gauge('wr.executions.active', execs.length);

      return {
        state,
        uptimeMs: startedAt ? clock.now() - startedAt : 0,
        activeExecutions: execs.length,
        queuedTasks: taskStats.queued + taskStats.delayed,
        runningTasks: taskStats.running,
        deadLetters: taskStats.deadLetters,
        scheduledJobs: jobs.length,
        entities: entityStats,
        timers: hub.size,
      };
    },

    inspect() {
      return {
        namespace,
        state,
        clock: { virtual, now: clock.now() },
        counts: {
          workflows: registry.list().length,
          executions: (
            runtime.workflows as unknown as { list: (f: unknown) => ExecutionRecord[] }
          ).list({}).length,
          entities: (
            (runtime.entities as unknown as { stats: () => unknown }).stats() as { total: number }
          ).total,
          jobs: (runtime.scheduler as unknown as { list: (f: unknown) => JobRecord[] }).list({})
            .length,
          tasks: (runtime.tasks as unknown as { list: (f: unknown) => TaskRecord[] }).list({})
            .length,
        },
        config: {
          concurrency: options.concurrency ?? 4,
          executionRetentionMs: options.executionRetentionMs ?? 1800000,
          recovery: recoveryConfig,
        },
      };
    },
  };

  (runtime.workflows as unknown as { deps: { runtime: unknown } }).deps.runtime = runtime;
  (runtime.scheduler as unknown as { deps: { enqueueTask: unknown } }).deps.enqueueTask = (
    name: string,
    input: unknown,
    meta: Record<string, unknown>,
  ) =>
    (runtime.tasks as unknown as { enqueue: (s: unknown) => unknown }).enqueue({
      name,
      run: () => undefined,
      meta: { ...meta, jobId: newId() },
    });

  if (options.handleSignals) {
    const handler = () => {
      void runtime.shutdown({ drainMs: 5000 }).then(() => process.exit(0));
    };
    signalHandlers = [
      ['SIGINT', handler],
      ['SIGTERM', handler],
    ] as Array<[NodeJS.Signals, () => void]>;
    for (const [sig, h] of signalHandlers) process.on(sig, h);
  }

  return runtime;
}
