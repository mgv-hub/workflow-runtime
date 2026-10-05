import {
  errorMeta,
  RecoveryError,
  serializeError,
  StepTimeoutError,
  WorkflowDefinitionError,
  WorkflowTimeoutError,
  type SerializedError,
} from '../errors.js';
import { makeIdFactory, type IdFactory } from '../ids.js';
import { computeRetryDelay, normalizeRetry, shouldRetry } from '../policy.js';
import type { Clock, TimerHub, TimerHandle } from '../clock.js';
import type { EventBus } from '../events/event-bus.js';
import type { Logger } from '../observability/logger.js';
import type { MetricsRecorder } from '../observability/metrics.js';
import type { HistoryManager } from '../observability/history.js';
import type { KeyValueStore } from '../storage/store.js';
import { storageKey, storagePrefix } from '../storage/store.js';
import { composeMiddleware, raceTimeout, settle, type Middleware } from '../util/async.js';
import { validateWorkflow, WorkflowRegistry } from './registry.js';
import type {
  CompensateContext,
  ExecutionHandle,
  ExecutionRecord,
  ExecutionStatus,
  StartOptions,
  StepContext,
  StepDef,
  StepState,
  WorkflowDef,
} from './types.js';

interface LiveExec {
  record: ExecutionRecord;
  def: WorkflowDef;
  order: string[];
  stepById: Map<string, StepDef>;
  dependents: Map<string, string[]>;
  pendingDeps: Map<string, Set<string>>;
  unsettled: number;
  parked: string[];
  gate: { promise: Promise<void>; resolve: () => void } | null;
  compensators: Array<{
    stepId: string;
    order: number;
    fn: (ctx: CompensateContext) => void | Promise<void>;
  }>;
  signals: Map<string, AbortController>;
  wfTimer?: TimerHandle;
  startOff?: () => void;
  startTimer?: TimerHandle;
  retentionTimer?: TimerHandle;
  resolve: (rec: ExecutionRecord) => void;
  stopping: boolean;
  compOrder: number;
}

export interface EngineDeps {
  hub: TimerHub;
  clock: Clock;
  events: EventBus;
  logger: Logger;
  history?: HistoryManager;
  metrics?: MetricsRecorder;
  storage?: KeyValueStore;
  registry: WorkflowRegistry;
  namespace: string;
  runtime: unknown;
  middleware?: Middleware<unknown>[];
  idFactory?: IdFactory;
  retentionMs?: number;
  capLiveCompleted?: number;
}

export interface EngineFilter {
  status?: ExecutionStatus;
  workflowId?: string;
  tags?: string[];
  limit?: number;
}

export class WorkflowEngine {
  private live = new Map<string, LiveExec>();
  private idFactory: IdFactory;
  private retentionMs: number;
  private mw: <Ctx, T>(ctx: Ctx, core: () => Promise<T>) => Promise<T>;
  private prefix: string;

  constructor(private deps: EngineDeps) {
    this.idFactory = makeIdFactory(deps.idFactory);
    this.retentionMs = deps.retentionMs ?? 1800000;
    this.mw = composeMiddleware(deps.middleware ?? []) as never;
    this.prefix = storagePrefix('exec', deps.namespace);
  }

  register(def: WorkflowDef): WorkflowDef {
    return this.deps.registry.register(def);
  }

  start(target: string | WorkflowDef, opts: StartOptions = {}): ExecutionHandle {
    const def =
      typeof target === 'string'
        ? this.deps.registry.get(target, opts.version)
        : (validateWorkflow(target), target);
    if (!def)
      throw new WorkflowDefinitionError(`Unknown workflow "${String(target)}"`, String(target));

    const id = opts.id ?? this.idFactory();
    if (this.live.has(id)) throw new WorkflowDefinitionError(`Execution id "${id}" already exists`);
    const now = this.deps.clock.now();

    const record: ExecutionRecord = {
      id,
      workflowId: def.id,
      version: String(def.version ?? 1),
      status: 'pending',
      paused: false,
      input: opts.input ?? null,
      state: def.initialState ? def.initialState((opts.input ?? null) as never) : ({} as unknown),
      steps: {},
      results: {},
      error: null,
      tags: [...(def.tags ?? []), ...(opts.tags ?? [])],
      meta: { ...(def.meta ?? {}), ...(opts.meta ?? {}) },
      createdAt: now,
      startedAt: null,
      endedAt: null,
      updatedAt: now,
      partial: false,
      dryRun: opts.dryRun ?? false,
      replayOf: null,
      startEvent: null,
      compensations: [],
    };

    let resolvePromise!: (rec: ExecutionRecord) => void;
    new Promise<ExecutionRecord>((r) => {
      resolvePromise = r;
    });

    const exec: LiveExec = {
      record,
      def,
      order: [],
      stepById: new Map(),
      dependents: new Map(),
      pendingDeps: new Map(),
      unsettled: 0,
      parked: [],
      gate: null,
      compensators: [],
      signals: new Map(),
      resolve: (rec) => resolvePromise(rec),
      stopping: false,
      compOrder: 0,
    };

    this.live.set(id, exec);
    this.deps.events.emit('execution.created', {
      executionId: id,
      workflowId: def.id,
      version: record.version,
    });

    this.addHistory(exec, 'created');
    const startWhen = opts.waitFor ?? def.startWhen;

    if (startWhen?.event) {
      record.startEvent = { event: startWhen.event, timeoutMs: startWhen.timeoutMs };
      this.subscribeStart(exec, startWhen.event, startWhen.filter, startWhen.timeoutMs);
      this.persist(exec);
      return this.handleById(id)!;
    }

    this.begin(exec, new Map<string, StepState>());
    return this.handleById(id)!;
  }

  private subscribeStart(
    exec: LiveExec,
    event: string,
    filter?: (payload: unknown) => boolean,
    timeoutMs?: number,
  ): void {
    const { record } = exec;
    const busFilter = (e: { payload?: unknown }) => (filter ? filter(e.payload) : true);

    exec.startOff = this.deps.events.on(
      event,
      (e) => {
        if (record.status !== 'pending') return;
        exec.startTimer?.cancel();
        exec.startTimer = undefined;
        exec.startOff?.();
        exec.startOff = undefined;

        record.input = e.payload ?? null;
        record.startEvent = null;
        record.state = exec.def.initialState
          ? exec.def.initialState(record.input as never)
          : record.state;

        this.deps.events.emit('execution.started', {
          executionId: record.id,
          workflowId: record.workflowId,
          reason: 'event',
        });

        this.begin(exec, new Map<string, StepState>());
      },
      { filter: busFilter as never },
    );

    if (timeoutMs != null && Number.isFinite(timeoutMs) && timeoutMs > 0) {
      exec.startTimer = this.deps.hub.after(timeoutMs, () => {
        if (record.status !== 'pending') return;
        exec.startOff?.();
        exec.startOff = undefined;
        record.status = 'cancelled';
        record.error = serializeError(new EventWaitTimeoutErrorLite(event, timeoutMs));
        record.endedAt = this.deps.clock.now();
        this.finalizeRecord(exec, 'cancelled');
      });
    }
  }

  private begin(exec: LiveExec, preSettled: Map<string, StepState>): void {
    const { record, def } = exec;
    validateWorkflow(def);
    const order = validateWorkflow(def);
    exec.order = order;

    for (const step of def.steps) {
      exec.stepById.set(step.id, step);
      for (const dep of step.dependsOn ?? []) {
        const list = exec.dependents.get(dep) ?? [];
        list.push(step.id);
        exec.dependents.set(dep, list);
      }
    }

    for (const step of def.steps) {
      const prior = preSettled.get(step.id);
      record.steps[step.id] = prior ?? { status: 'pending', attempts: 0 };
    }

    for (const step of def.steps) {
      const deps = (step.dependsOn ?? []).filter((d) => !preSettled.has(d));
      const pending = new Set<string>();

      for (const dep of deps) {
        if (!preSettled.has(dep) && record.steps[dep].status === 'pending') pending.add(dep);
        else if (record.steps[dep]?.status === 'running') pending.add(dep);
      }

      exec.pendingDeps.set(step.id, pending);
      if (!preSettled.has(step.id) && record.steps[step.id].status === 'pending')
        exec.unsettled += 1;
    }

    for (const [stepId, state] of preSettled) {
      const step = exec.stepById.get(stepId);
      if (state.status === 'succeeded' && step?.compensate && !record.dryRun) {
        exec.compensators.push({ stepId, order: ++exec.compOrder, fn: step.compensate });
      }
      if (state.status === 'succeeded') record.results[stepId] = state.result;
    }

    if (record.status === 'pending') {
      record.status = 'running';
      record.startedAt = record.startedAt ?? this.deps.clock.now();
      this.deps.events.emit('execution.started', {
        executionId: record.id,
        workflowId: record.workflowId,
        reason: record.startedAt === this.deps.clock.now() ? 'start' : 'recovery',
      });
      this.addHistory(exec, 'started');
    }

    if (def.timeoutMs != null) {
      exec.wfTimer = this.deps.hub.after(def.timeoutMs, () => {
        this.failWorkflow(
          exec,
          new WorkflowTimeoutError(def.timeoutMs!, record.id, record.workflowId),
        );
      });
    }

    const ready = order.filter(
      (id) => (exec.pendingDeps.get(id)?.size ?? 0) === 0 && record.steps[id].status === 'pending',
    );
    this.dispatch(exec, ready);
  }

  private dispatch(exec: LiveExec, stepIds: string[]): void {
    const { record } = exec;
    const sorted = [...stepIds].sort((a, b) => exec.order.indexOf(a) - exec.order.indexOf(b));

    for (const stepId of sorted) {
      if (record.status !== 'running' || exec.stopping) {
        this.settleStep(exec, stepId, {
          status: record.status === 'cancelled' ? 'cancelled' : 'skipped',
          reason: record.status === 'cancelled' ? 'workflow-cancelled' : 'workflow-failed',
        });
        continue;
      }

      if (exec.gate) {
        exec.parked.push(stepId);
        continue;
      }

      void this.runStep(exec, stepId);
    }
  }

  private async gate(exec: LiveExec): Promise<boolean> {
    while (exec.record.paused && exec.record.status === 'running' && !exec.stopping) {
      if (!exec.gate) {
        let resolveGate!: () => void;
        void new Promise<void>((r) => {
          resolveGate = r;
        });
        exec.gate = {
          promise: new Promise<void>((r) => {
            resolveGate = r;
          }),
          resolve: resolveGate,
        };
      }

      await exec.gate.promise;
    }
    return exec.record.status === 'running' && !exec.stopping;
  }

  private async runStep(exec: LiveExec, stepId: string): Promise<void> {
    const { record } = exec;
    const step = exec.stepById.get(stepId)!;
    const state: StepState = record.steps[stepId];
    if (state.status !== 'pending') return;

    if (!(await this.gate(exec))) {
      this.settleStep(exec, stepId, { status: 'cancelled', reason: 'workflow-stopped' });
      return;
    }

    if (record.dryRun && step.sideEffects) {
      this.settleStep(exec, stepId, { status: 'skipped', reason: 'dry-run' });
      return;
    }

    const whenCtx = {
      state: record.state,
      input: record.input,
      results: record.results,
      steps: record.steps,
      logger: this.deps.logger.child({ executionId: record.id, stepId }),
      runtime: this.deps.runtime,
    };
    let conditionOk = true;

    if (step.when) {
      try {
        conditionOk = await step.when(whenCtx);
      } catch (err) {
        this.settleStep(exec, stepId, {
          status: 'failed',
          error: serializeError(err),
          reason: 'condition-error',
        });
        return;
      }
    }

    if (!conditionOk) {
      this.settleStep(exec, stepId, { status: 'skipped', reason: 'condition' });
      return;
    }

    state.status = 'running';
    state.startedAt = this.deps.clock.now();
    record.updatedAt = state.startedAt;
    this.deps.events.emit('execution.step.started', { executionId: record.id, stepId });
    const retries = normalizeRetry(step.retries);
    const logger = this.deps.logger.child({ executionId: record.id, stepId });

    for (let attempt = 1; ; attempt += 1) {
      if (!(await this.gate(exec))) {
        if (record.steps[stepId].status === 'running') {
          this.settleStep(exec, stepId, { status: 'cancelled', reason: 'workflow-stopped' });
        }
        return;
      }

      const controller = new AbortController();
      exec.signals.set(stepId, controller);
      const ctx: StepContext = {
        stepId,
        attempt,
        state: record.state,
        input: record.input,
        results: record.results,
        steps: record.steps,
        signal: controller.signal,
        logger,
        meta: { ...(step.meta ?? {}) },
        tags: step.tags ?? [],
        dryRun: record.dryRun,
        wait: (ms: number) =>
          new Promise<void>((resolve) => {
            this.deps.hub.after(ms, () => resolve());
          }),
        compensate: (fn) => {
          if (record.dryRun) return;
          exec.compensators.push({ stepId, order: ++exec.compOrder, fn });
        },
        runtime: this.deps.runtime,
      };

      const outcome = await raceTimeout(
        settle(
          this.mw({ kind: 'step', ...ctx }, async () => {
            return await step.run(ctx);
          }),
        ),
        step.timeoutMs,
        this.deps.hub,
        () => new StepTimeoutError(stepId, step.timeoutMs!, record.id, record.workflowId),
      );

      exec.signals.delete(stepId);
      state.attempts = attempt;
      if (outcome.settled.ok) {
        const value = (outcome.settled as { ok: true; value: unknown }).value;
        this.settleStep(exec, stepId, { status: 'succeeded', result: value, attempts: attempt });
        return;
      }

      const err = outcome.settled.error;
      state.error = serializeError(err);
      const retryable = shouldRetry(retries, err);
      if (retryable && attempt <= retries.max) {
        if (attempt > 1 || retries.max > 0) {
          this.deps.metrics?.counter('wr.step.retries');
        }
        const backoff = computeRetryDelay(retries, attempt, err);
        await new Promise<void>((r) => this.deps.hub.after(backoff, () => r()));
        continue;
      }
      if (step.failure === 'fallback' && step.fallback && !outcome.timedOut) {
        try {
          const fb = await step.fallback(ctx);
          this.settleStep(exec, stepId, {
            status: 'succeeded',
            result: fb,
            usedFallback: true,
            attempts: attempt,
          });
          return;
        } catch (fbErr) {
          state.error = serializeError(fbErr);
        }
      }

      if (step.failure === 'ignore' || step.failure === 'continue') {
        this.settleStep(exec, stepId, {
          status: 'failed',
          error: state.error,
          reason: `policy-${step.failure}`,
        });
        return;
      }

      state.status = 'failed';
      state.error = serializeError(err);
      state.attempts = attempt;
      state.endedAt = this.deps.clock.now();
      record.updatedAt = state.endedAt;
      this.deps.metrics?.counter('wr.step.failures');
      this.deps.events.emit('execution.step.failed', {
        executionId: record.id,
        stepId,
        error: state.error,
      });

      this.addHistory(exec, `step:failed:${stepId}`);
      this.persist(exec);
      if (exec.unsettled > 0) exec.unsettled -= 1;
      this.failWorkflow(exec, err instanceof Error ? err : new Error(String(err)));
      return;
    }
  }

  private settleStep(
    exec: LiveExec,
    stepId: string,
    outcome: {
      status: StepState['status'];
      result?: unknown;
      error?: SerializedError;
      reason?: string;
      attempts?: number;
      usedFallback?: boolean;
      replayed?: boolean;
    },
  ): void {
    const { record } = exec;
    const state = record.steps[stepId];
    if (
      !state ||
      state.status === 'succeeded' ||
      state.status === 'failed' ||
      state.status === 'skipped' ||
      state.status === 'cancelled'
    ) {
      return;
    }
    state.status = outcome.status;
    state.result = outcome.result;
    state.error = outcome.error;
    state.reason = outcome.reason;
    state.attempts = outcome.attempts ?? state.attempts ?? 0;
    state.usedFallback = outcome.usedFallback ?? false;
    if (outcome.replayed !== undefined) {
      state.replayed = outcome.replayed;
    }
    state.endedAt = this.deps.clock.now();
    record.updatedAt = state.endedAt;

    if (outcome.status === 'succeeded') {
      record.results[stepId] = outcome.result;
      const step = exec.stepById.get(stepId);
      if (step?.compensate && !record.dryRun) {
        exec.compensators.push({ stepId, order: ++exec.compOrder, fn: step.compensate });
      }
      this.deps.events.emit('execution.step.succeeded', {
        executionId: record.id,
        stepId,
        attempt: state.attempts,
      });
    } else if (outcome.status === 'failed') {
      this.deps.metrics?.counter('wr.step.failures');
      this.deps.events.emit('execution.step.failed', {
        executionId: record.id,
        stepId,
        error: state.error,
      });
    } else if (outcome.status === 'skipped') {
      this.deps.events.emit('execution.step.skipped', {
        executionId: record.id,
        stepId,
        reason: state.reason,
      });
    } else if (outcome.status === 'cancelled') {
      this.deps.events.emit('execution.step.cancelled', {
        executionId: record.id,
        stepId,
        reason: state.reason,
      });
    }

    this.addHistory(exec, `step:${outcome.status}:${stepId}`);
    this.persist(exec);
    if (exec.unsettled > 0) exec.unsettled -= 1;
    if (exec.unsettled === 0) {
      this.finalize(exec);
      return;
    }
    if (exec.stopping || record.status !== 'running') return;
    const candidates: string[] = [];
    for (const nextId of exec.dependents.get(stepId) ?? []) {
      const pending = exec.pendingDeps.get(nextId);
      if (!pending || !pending.delete(stepId)) continue;
      if (pending.size === 0) candidates.push(nextId);
    }

    for (const nextId of candidates) {
      const nextStep = exec.stepById.get(nextId)!;
      const policy = nextStep.depsPolicy ?? 'all-succeeded';
      if (policy === 'all-succeeded') {
        const deps = nextStep.dependsOn ?? [];
        const blocker = deps.find((d) => {
          const step = record.steps[d];
          if (step.status === 'succeeded') return false;
          if (step.status === 'skipped' && step.reason === 'dry-run') return false;
          return true;
        });
        if (blocker) {
          this.settleStep(exec, nextId, {
            status: 'skipped',
            reason: `upstream-${record.steps[blocker].status}`,
          });
          continue;
        }
      }
    }
    const runnable = candidates.filter((id) => record.steps[id].status === 'pending');
    this.dispatch(exec, runnable);
  }

  private finalize(exec: LiveExec): void {
    const { record } = exec;
    if (record.status === 'running') {
      record.status = 'completed';
      record.partial = Object.values(record.steps).some((s) => s.status === 'failed');
      record.endedAt = this.deps.clock.now();
      this.deps.metrics?.observe(
        'wr.execution.duration',
        (record.endedAt ?? 0) - (record.startedAt ?? record.createdAt),
      );
      this.deps.events.emit('execution.completed', {
        executionId: record.id,
        workflowId: record.workflowId,
        partial: record.partial,
      });
      this.addHistory(exec, 'completed');
      this.finishRecord(exec);
    }
  }

  private failWorkflow(exec: LiveExec, error: Error): void {
    const { record } = exec;
    if (record.status !== 'running') return;
    record.status = 'failed';
    record.error = serializeError(error);
    record.endedAt = this.deps.clock.now();
    exec.stopping = true;
    exec.wfTimer?.cancel();
    exec.wfTimer = undefined;
    for (const [, controller] of exec.signals) {
      controller.abort(error);
    }
    for (const stepId of exec.order) {
      if (record.steps[stepId].status === 'pending') {
        record.steps[stepId].status = 'skipped';
        record.steps[stepId].reason = 'workflow-failed';
        record.steps[stepId].endedAt = record.endedAt;
        if (exec.unsettled > 0) exec.unsettled -= 1;
      }
    }
    this.runCompensations(exec);
    this.deps.metrics?.counter('wr.execution.failures');
    this.deps.metrics?.observe(
      'wr.execution.duration',
      record.endedAt - (record.startedAt ?? record.createdAt),
    );
    this.deps.events.emit('execution.failed', {
      executionId: record.id,
      workflowId: record.workflowId,
      error: record.error,
    });
    this.addHistory(exec, 'failed');
    this.finishRecord(exec);
  }

  private runCompensations(exec: LiveExec): void {
    const { record } = exec;
    const list = [...exec.compensators].sort((a, b) => b.order - a.order);
    for (const comp of list) {
      if (record.dryRun) {
        record.compensations.push({ stepId: comp.stepId, status: 'skipped' });
        continue;
      }
      try {
        void comp.fn({
          state: record.state,
          results: record.results,
          logger: this.deps.logger.child({ executionId: record.id, stepId: comp.stepId }),
          runtime: this.deps.runtime,
        });
        record.compensations.push({ stepId: comp.stepId, status: 'succeeded' });
      } catch (err) {
        record.compensations.push({
          stepId: comp.stepId,
          status: 'failed',
          error: serializeError(err),
        });
        this.deps.logger.warn('compensation failed', {
          executionId: record.id,
          stepId: comp.stepId,
          ...errorMeta(err),
        });
      }
    }
  }

  private finalizeRecord(exec: LiveExec, status: ExecutionStatus): void {
    void status;
    this.finishRecord(exec);
  }

  private finishRecord(exec: LiveExec): void {
    const { record } = exec;
    record.updatedAt = this.deps.clock.now();
    this.persist(exec);
    this.scheduleRetention(exec);
    exec.resolve({ ...record });
    this.live.delete(record.id);
  }

  private scheduleRetention(exec: LiveExec): void {
    const { record } = exec;
    if (this.deps.storage) {
      exec.retentionTimer = this.deps.hub.after(this.retentionMs, () => {
        void this.deps.storage?.delete(storageKey('exec', this.deps.namespace, record.id));
      });
      return;
    }
    exec.retentionTimer = this.deps.hub.after(this.retentionMs, () => undefined);
  }

  pause(id: string): boolean {
    const exec = this.live.get(id);
    if (!exec || exec.record.status !== 'running' || exec.record.paused) return false;
    exec.record.paused = true;
    exec.record.updatedAt = this.deps.clock.now();
    this.deps.events.emit('execution.paused', { executionId: id });
    this.addHistory(exec, 'paused');
    this.persist(exec);
    return true;
  }

  resume(id: string): boolean {
    const exec = this.live.get(id);
    if (!exec || !exec.record.paused || exec.record.status !== 'running') return false;
    exec.record.paused = false;
    exec.record.updatedAt = this.deps.clock.now();
    const gate = exec.gate;
    exec.gate = null;
    gate?.resolve();
    this.deps.events.emit('execution.resumed', { executionId: id });
    this.addHistory(exec, 'resumed');
    this.persist(exec);
    const parked = exec.parked.splice(0);
    this.dispatch(exec, parked);
    return true;
  }

  cancel(id: string, reason = 'manual'): boolean {
    const exec = this.live.get(id);
    if (!exec) return false;
    const { record } = exec;
    if (record.status !== 'running' && record.status !== 'pending') return false;
    record.status = 'cancelled';
    record.paused = false;
    record.error = { name: 'Cancelled', message: reason };
    record.endedAt = this.deps.clock.now();
    exec.stopping = true;
    exec.wfTimer?.cancel();
    exec.startTimer?.cancel();
    exec.startOff?.();
    for (const [, controller] of exec.signals) controller.abort(new Error(reason));
    for (const stepId of exec.order) {
      if (record.steps[stepId] && record.steps[stepId].status === 'pending') {
        record.steps[stepId].status = 'cancelled';
        record.steps[stepId].reason = reason;
        record.steps[stepId].endedAt = record.endedAt;
        if (exec.unsettled > 0) exec.unsettled -= 1;
      }
    }

    const gate = exec.gate;
    exec.gate = null;
    gate?.resolve();
    this.deps.events.emit('execution.cancelled', { executionId: id, reason });
    this.addHistory(exec, 'cancelled');
    this.finishRecord(exec);
    return true;
  }

  get(id: string): ExecutionRecord | undefined {
    const exec = this.live.get(id);
    return exec ? { ...exec.record } : undefined;
  }

  list(filter: EngineFilter = {}): ExecutionRecord[] {
    const out: ExecutionRecord[] = [];
    for (const exec of this.live.values()) {
      if (filter.status && exec.record.status !== filter.status) continue;
      if (filter.workflowId && exec.record.workflowId !== filter.workflowId) continue;
      if (filter.tags && !filter.tags.some((t) => exec.record.tags.includes(t))) continue;
      out.push({ ...exec.record });
      if (filter.limit && out.length >= filter.limit) break;
    }
    return out;
  }

  async replay(id: string, opts: { dryRun?: boolean } = {}): Promise<ExecutionHandle | null> {
    const prior =
      this.live.get(id)?.record ??
      (this.deps.storage
        ? await this.deps.storage.get<ExecutionRecord>(storageKey('exec', this.deps.namespace, id))
        : undefined);
    if (
      !prior ||
      (prior.status !== 'completed' && prior.status !== 'failed' && prior.status !== 'cancelled')
    )
      return null;
    const preSettled = new Map<string, StepState>();
    for (const [stepId, state] of Object.entries(prior.steps)) {
      if (state.status === 'skipped') {
        preSettled.set(stepId, { ...state, replayed: true });
      }
    }

    const reuse = new Map<string, StepState>();
    for (const [stepId, state] of Object.entries(prior.steps)) {
      if (state.status !== 'succeeded') continue;
      const defStep = this.lookupStep(prior, stepId);
      if (defStep?.replaySafe !== true) {
        reuse.set(stepId, { ...state, replayed: true });
      }
    }

    for (const [k, v] of reuse) preSettled.set(k, v);
    const def = this.deps.registry.get(prior.workflowId, prior.version);
    if (!def)
      throw new RecoveryError(
        `Cannot replay: workflow "${prior.workflowId}" v${prior.version} is not registered`,
        id,
      );

    const handle = this.start(def, {
      id: undefined,
      input: prior.input,
      tags: prior.tags,
      meta: { ...prior.meta, replayOf: id },
      dryRun: opts.dryRun ?? false,
    });

    const exec = this.live.get(handle.id);
    if (exec) {
      exec.record.replayOf = id;
      exec.record.state = prior.state;
      for (const [stepId, state] of preSettled) {
        exec.record.steps[stepId] = { ...state };
        if (state.status === 'succeeded') {
          exec.record.results[stepId] = state.result;
          exec.unsettled = Math.max(0, exec.unsettled - 1);
        }
      }
    }
    return this.handleById(handle.id);
  }

  private lookupStep(record: ExecutionRecord, stepId: string): StepDef | undefined {
    const def = this.deps.registry.get(record.workflowId, record.version);
    return def?.steps.find((s) => s.id === stepId);
  }

  handleById(id: string): ExecutionHandle | null {
    const exec = this.live.get(id);
    if (!exec) return null;
    const promise = new Promise<ExecutionRecord>((resolve) => {
      const prev = exec.resolve;
      exec.resolve = (rec) => {
        prev(rec);
        resolve(rec);
      };
      if (['completed', 'failed', 'cancelled'].includes(exec.record.status)) {
        resolve({ ...exec.record });
      }
    });
    return {
      id,
      workflowId: exec.record.workflowId,
      promise,
      record: () => ({ ...exec.record }),
      status: () => exec.record.status,
      isPaused: () => exec.record.paused,
      pause: () => this.pause(id),
      resume: () => this.resume(id),
      cancel: (reason?: string) => this.cancel(id, reason),
    };
  }

  private persist(exec: LiveExec): void {
    const defPersist = exec.def.persist ?? false;
    if (!defPersist || !this.deps.storage) return;
    void this.deps.storage.put(storageKey('exec', this.deps.namespace, exec.record.id), {
      ...exec.record,
    });
  }

  private addHistory(exec: LiveExec, type: string): void {
    if (!(exec.def.history ?? false) || !this.deps.history) return;
    this.deps.history.add(`exec:${exec.record.id}`, { type, status: exec.record.status });
  }

  async loadPersisted(
    policy: 'resume' | 'fail' | 'drop' = 'resume',
  ): Promise<{ recovered: number; failed: number; dropped: number }> {
    if (!this.deps.storage) return { recovered: 0, failed: 0, dropped: 0 };
    const entries = await this.deps.storage.list(this.prefix);
    let recovered = 0;
    let failed = 0;
    let dropped = 0;

    for (const { value } of entries) {
      const record = value as ExecutionRecord;
      if (!record || typeof record.id !== 'string') continue;
      if (policy === 'drop') {
        dropped += 1;
        void this.deps.storage.delete(storageKey('exec', this.deps.namespace, record.id));
        continue;
      }

      let def = this.deps.registry.get(record.workflowId, record.version);
      if (!def) {
        def = this.deps.registry.get(record.workflowId);
      }

      if (!def) {
        failed += 1;
        record.status = 'failed';
        record.error = serializeError(
          new RecoveryError(
            `Workflow "${record.workflowId}" v${record.version} not registered`,
            record.id,
          ),
        );
        record.endedAt = this.deps.clock.now();
        void this.deps.storage.put(storageKey('exec', this.deps.namespace, record.id), record);
        continue;
      }

      if (String(def.version ?? 1) !== record.version && def.migrate) {
        const migrated = def.migrate(record, record.version);
        Object.assign(record, migrated);
        record.version = String(def.version ?? 1);
      }

      if (policy === 'fail') {
        failed += 1;
        record.status = 'failed';
        record.error = serializeError(
          new RecoveryError('Marked failed by recovery policy', record.id),
        );
        record.endedAt = this.deps.clock.now();
        this.rebuildCompensators(def, record);
        void this.deps.storage.put(storageKey('exec', this.deps.namespace, record.id), record);
        continue;
      }

      recovered += 1;
      this.resumeRecord(def, record);
    }
    return { recovered, failed, dropped };
  }

  private rebuildCompensators(def: WorkflowDef, record: ExecutionRecord): void {
    record.compensations = record.compensations ?? [];
    for (const step of def.steps) {
      if (step.compensate && record.steps[step.id]?.status === 'succeeded') {
        try {
          void step.compensate({
            state: record.state,
            results: record.results,
            logger: this.deps.logger.child({ executionId: record.id, stepId: step.id }),
            runtime: this.deps.runtime,
          });

          record.compensations.push({ stepId: step.id, status: 'succeeded' });
        } catch (err) {
          record.compensations.push({
            stepId: step.id,
            status: 'failed',
            error: serializeError(err),
          });
        }
      }
    }
  }

  private resumeRecord(def: WorkflowDef, record: ExecutionRecord): void {
    let resolvePromise!: (rec: ExecutionRecord) => void;
    new Promise<ExecutionRecord>((r) => {
      resolvePromise = r;
    });
    const exec: LiveExec = {
      record,
      def,
      order: [],
      stepById: new Map(),
      dependents: new Map(),
      pendingDeps: new Map(),
      unsettled: 0,
      parked: [],
      gate: null,
      compensators: [],
      signals: new Map(),
      resolve: (rec) => resolvePromise(rec),
      stopping: false,
      compOrder: 0,
    };
    this.live.set(record.id, exec);

    if (
      record.status === 'completed' ||
      record.status === 'failed' ||
      record.status === 'cancelled'
    ) {
      resolvePromise({ ...record });
      return;
    }

    const preSettled = new Map<string, StepState>();
    for (const [stepId, state] of Object.entries(record.steps)) {
      if (state.status !== 'pending') preSettled.set(stepId, state);
    }

    for (const step of def.steps) {
      const state = record.steps[step.id];
      if (!state || state.status === 'running' || state.status === 'pending') {
        record.steps[step.id] = { status: 'pending', attempts: 0 };
      }
    }
    record.status = 'running';
    this.deps.events.emit('execution.recovered', {
      executionId: record.id,
      workflowId: record.workflowId,
    });
    this.begin(exec, preSettled);
    if (record.startEvent?.event) {
      this.subscribeStart(exec, record.startEvent.event, undefined, record.startEvent.timeoutMs);
    }
  }

  async drain(cancelRunning: boolean): Promise<void> {
    const ids = [...this.live.keys()];
    for (const id of ids) {
      const exec = this.live.get(id);
      if (!exec) continue;
      if (exec.record.status === 'running' || exec.record.status === 'pending') {
        if (cancelRunning) this.cancel(id, 'shutdown');
      }
    }
  }

  dispose(): void {
    for (const exec of this.live.values()) {
      exec.wfTimer?.cancel();
      exec.startTimer?.cancel();
      exec.retentionTimer?.cancel();
      exec.startOff?.();
    }
    this.live.clear();
  }
}

class EventWaitTimeoutErrorLite extends Error {
  constructor(event: string, timeoutMs: number) {
    super(`Timed out after ${timeoutMs}ms waiting for start event "${event}"`);
    this.name = 'EventWaitTimeout';
  }
}

export function waitForEventStep(
  event: string,
  opts: {
    filter?: (payload: unknown) => boolean;
    timeoutMs?: number;
    id?: string;
    timeoutAsFailure?: boolean;
  } = {},
): StepDef {
  return {
    id: opts.id ?? `wait-${event}`,
    async run(ctx) {
      const runtime = ctx.runtime as {
        events: {
          waitFor(
            name: string,
            o: { filter?: (e: { payload?: unknown }) => boolean; timeoutMs?: number },
          ): Promise<{ payload?: unknown }>;
        };
      };
      const arrived = await runtime.events.waitFor(event, {
        filter: (e) => (opts.filter ? opts.filter(e.payload) : true),
        timeoutMs: opts.timeoutMs,
      });
      return arrived.payload;
    },
    timeoutMs: opts.timeoutMs,
  };
}
