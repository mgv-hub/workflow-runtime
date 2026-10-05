export interface SerializedError {
  code?: string;
  name: string;
  message: string;
}

export class WorkflowRuntimeError extends Error {
  readonly code: string;
  readonly cause?: unknown;
  readonly workflowId?: string;
  readonly executionId?: string;
  readonly taskId?: string;
  readonly jobId?: string;

  constructor(
    code: string,
    message: string,
    details: {
      cause?: unknown;
      workflowId?: string;
      executionId?: string;
      taskId?: string;
      jobId?: string;
    } = {},
  ) {
    super(message);
    this.name = new.target.name;
    this.code = code;
    this.cause = details.cause;
    this.workflowId = details.workflowId;
    this.executionId = details.executionId;
    this.taskId = details.taskId;
    this.jobId = details.jobId;
  }
}

export class ConfigurationError extends WorkflowRuntimeError {
  constructor(message: string, cause?: unknown) {
    super('WR_CONFIGURATION', message, { cause });
  }
}

export class WorkflowDefinitionError extends WorkflowRuntimeError {
  constructor(message: string, workflowId?: string) {
    super('WR_WORKFLOW_DEFINITION', message, { workflowId });
  }
}

export class StepTimeoutError extends WorkflowRuntimeError {
  constructor(stepId: string, timeoutMs: number, executionId?: string, workflowId?: string) {
    super('WR_STEP_TIMEOUT', `Step "${stepId}" exceeded timeout of ${timeoutMs}ms`, {
      executionId,
      workflowId,
    });
  }
}

export class WorkflowTimeoutError extends WorkflowRuntimeError {
  constructor(timeoutMs: number, executionId?: string, workflowId?: string) {
    super('WR_WORKFLOW_TIMEOUT', `Workflow exceeded timeout of ${timeoutMs}ms`, {
      executionId,
      workflowId,
    });
  }
}

export class TaskTimeoutError extends WorkflowRuntimeError {
  constructor(taskId: string, timeoutMs: number) {
    super('WR_TASK_TIMEOUT', `Task "${taskId}" exceeded timeout of ${timeoutMs}ms`, { taskId });
  }
}

export class RetriesExhaustedError extends WorkflowRuntimeError {
  constructor(attempts: number, lastError: unknown, executionId?: string, taskId?: string) {
    super('WR_RETRIES_EXHAUSTED', `Failed after ${attempts} attempt(s)`, {
      cause: lastError,
      executionId,
      taskId,
    });
  }
}

export class LockUnavailableError extends WorkflowRuntimeError {
  constructor(key: string, waitedMs?: number) {
    super(
      'WR_LOCK_UNAVAILABLE',
      waitedMs == null
        ? `Lock "${key}" is held by another owner`
        : `Lock "${key}" could not be acquired within ${waitedMs}ms`,
    );
  }
}

export class CronParseError extends WorkflowRuntimeError {
  constructor(expression: string, reason: string) {
    super('WR_CRON_PARSE', `Invalid cron expression "${expression}": ${reason}`);
  }
}

export class RecoveryError extends WorkflowRuntimeError {
  constructor(message: string, executionId?: string) {
    super('WR_RECOVERY', message, { executionId });
  }
}

export class RuntimeShuttingDownError extends WorkflowRuntimeError {
  constructor() {
    super('WR_SHUTTING_DOWN', 'Runtime is shutting down and no longer accepts new work');
  }
}

export class EventWaitTimeoutError extends WorkflowRuntimeError {
  constructor(event: string, timeoutMs: number) {
    super('WR_EVENT_WAIT_TIMEOUT', `Timed out after ${timeoutMs}ms waiting for event "${event}"`);
  }
}

export function serializeError(err: unknown): SerializedError {
  if (err instanceof WorkflowRuntimeError) {
    return { code: err.code, name: err.name, message: err.message };
  }
  if (err instanceof Error) {
    return { name: err.name, message: err.message };
  }
  return { name: 'Error', message: typeof err === 'string' ? err : JSON.stringify(err) };
}

export function errorMeta(err: unknown): Record<string, unknown> {
  if (err instanceof WorkflowRuntimeError) {
    const meta: Record<string, unknown> = {
      code: err.code,
      name: err.name,
      message: err.message,
    };
    if (err.executionId) meta.executionId = err.executionId;
    if (err.workflowId) meta.workflowId = err.workflowId;
    if (err.taskId) meta.taskId = err.taskId;
    if (err.jobId) meta.jobId = err.jobId;
    if (err.cause !== undefined) meta.cause = serializeError(err.cause);
    return meta;
  }
  return { name: (err as Error)?.name ?? 'Error', message: (err as Error)?.message ?? String(err) };
}
