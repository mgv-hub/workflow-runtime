export interface RetryPolicy {
  max?: number;
  type?: 'fixed' | 'exponential' | 'custom';
  delay?: number;
  factor?: number;
  maxDelay?: number;
  custom?: (attempt: number, error: unknown) => number;
  retryIf?: (error: unknown) => boolean;
}

export function normalizeRetry(policy: RetryPolicy | undefined): Required<
  Pick<RetryPolicy, 'max' | 'delay' | 'factor' | 'maxDelay'>
> & { type: 'fixed' | 'exponential' | 'custom' } & {
  custom?: (attempt: number, error: unknown) => number;
  retryIf?: (error: unknown) => boolean;
} {
  const p = policy ?? {};
  return {
    max: p.max ?? 0,
    delay: p.delay ?? 1000,
    factor: p.factor ?? 2,
    maxDelay: p.maxDelay ?? 60000,
    type: p.type ?? 'fixed',
    custom: p.custom,
    retryIf: p.retryIf,
  };
}

export function computeRetryDelay(policy: RetryPolicy, attempt: number, error: unknown): number {
  if (policy.type === 'custom') {
    if (!policy.custom) return policy.delay ?? 1000;
    return Math.max(0, policy.custom(attempt, error));
  }
  if (policy.type === 'exponential') {
    const factor = policy.factor ?? 2;
    const base = policy.delay ?? 1000;
    const raw = base * Math.pow(factor, Math.max(0, attempt - 1));
    return Math.min(raw, policy.maxDelay ?? 60000);
  }
  return policy.delay ?? 1000;
}

export function shouldRetry(policy: RetryPolicy, error: unknown): boolean {
  return policy.retryIf ? policy.retryIf(error) : true;
}
