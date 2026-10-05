import type { Middleware } from '../util/async.js';
import type { RetryPolicy } from '../policy.js';
import type { SerializedError } from '../errors.js';
import type { Logger } from '../observability/logger.js';

export type StepStatus = 'pending' | 'running' | 'succeeded' | 'failed' | 'skipped' | 'cancelled';
export type ExecutionStatus = 'pending' | 'running' | 'completed' | 'failed' | 'cancelled';

export type StepFailurePolicy = 'fail' | 'continue' | 'ignore' | 'fallback';

export interface StepContext<I = unknown, S = unknown> {
  stepId: string;
  attempt: number;
  state: S;
  input: I;
  results: Record<string, unknown>;
  steps: Record<string, StepState>;
  signal: AbortSignal;
  logger: Logger;
  meta: Record<string, unknown>;
  tags: string[];
  dryRun: boolean;
  wait(ms: number): Promise<void>;
  compensate(fn: (ctx: CompensateContext) => void | Promise<void>): void;
  runtime: unknown;
}

export interface CompensateContext {
  state: unknown;
  results: Record<string, unknown>;
  logger: Logger;
  runtime: unknown;
}

export interface StepState {
  status: StepStatus;
  result?: unknown;
  error?: SerializedError;
  attempts: number;
  startedAt?: number;
  endedAt?: number;
  reason?: string;
  replayed?: boolean;
  usedFallback?: boolean;
}

export interface StepDef<I = unknown, S = unknown> {
  id: string;
  title?: string;
  run: (ctx: StepContext<I, S>) => unknown | Promise<unknown>;
  when?: (ctx: ConditionContext<I, S>) => boolean | Promise<boolean>;
  dependsOn?: string[];
  depsPolicy?: 'all-succeeded' | 'settled';
  timeoutMs?: number;
  retries?: RetryPolicy;
  failure?: StepFailurePolicy;
  fallback?: (ctx: StepContext<I, S>) => unknown | Promise<unknown>;
  compensate?: (ctx: CompensateContext) => void | Promise<void>;
  sideEffects?: boolean;
  replaySafe?: boolean;
  tags?: string[];
  meta?: Record<string, unknown>;
}

export interface ConditionContext<I = unknown, S = unknown> {
  state: S;
  input: I;
  results: Record<string, unknown>;
  steps: Record<string, StepState>;
  logger: Logger;
  runtime: unknown;
}

export interface WorkflowDef<I = unknown, S = unknown> {
  id: string;
  version?: number | string;
  description?: string;
  steps: StepDef<I, S>[];
  initialState?: (input: I) => S;
  timeoutMs?: number;
  failurePolicy?: 'fail' | 'continue';
  persist?: boolean;
  history?: boolean;
  retention?: number;
  startWhen?: { event: string; filter?: (payload: unknown) => boolean; timeoutMs?: number };
  middleware?: Middleware<unknown>[];
  tags?: string[];
  meta?: Record<string, unknown>;
  migrate?: (record: ExecutionRecord, fromVersion: string) => ExecutionRecord;
}

export interface StartOptions {
  input?: unknown;
  id?: string;
  version?: number | string;
  tags?: string[];
  meta?: Record<string, unknown>;
  dryRun?: boolean;
  waitFor?: { event: string; filter?: (payload: unknown) => boolean; timeoutMs?: number };
}

export interface ExecutionRecord {
  id: string;
  workflowId: string;
  version: string;
  status: ExecutionStatus;
  paused: boolean;
  input: unknown;
  state: unknown;
  steps: Record<string, StepState>;
  results: Record<string, unknown>;
  error: SerializedError | null;
  tags: string[];
  meta: Record<string, unknown>;
  createdAt: number;
  startedAt: number | null;
  endedAt: number | null;
  updatedAt: number;
  partial: boolean;
  dryRun: boolean;
  replayOf: string | null;
  startEvent: { event: string; filterKey?: string; timeoutMs?: number } | null;
  compensations: Array<{
    stepId: string;
    status: 'succeeded' | 'failed' | 'skipped';
    error?: SerializedError;
  }>;
}

export interface ExecutionHandle {
  readonly id: string;
  readonly workflowId: string;
  promise: Promise<ExecutionRecord>;
  record(): ExecutionRecord | undefined;
  status(): ExecutionStatus;
  isPaused(): boolean;
  pause(): boolean;
  resume(): boolean;
  cancel(reason?: string): boolean;
}
