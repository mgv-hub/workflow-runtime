import { LockUnavailableError } from '../errors.js';
import { newId } from '../ids.js';
import type { Clock, TimerHub } from '../clock.js';
import type { Logger } from '../observability/logger.js';

export interface LockLease {
  key: string;
  ownerId: string;
  expiresAt: number;
}

export interface LockProvider {
  acquire(key: string, ttlMs: number, ownerId?: string): Promise<LockLease | null>;
  release(key: string, ownerId: string): Promise<boolean>;
  renew?(key: string, ownerId: string, ttlMs: number): Promise<boolean>;
  inspect?(): Array<{ key: string; ownerId: string; expiresAt: number }>;
  dispose?(): void;
}

export class InProcessLockProvider implements LockProvider {
  private held = new Map<string, LockLease>();

  async acquire(key: string, ttlMs: number, ownerId?: string): Promise<LockLease | null> {
    const now = Date.now();
    const existing = this.held.get(key);
    if (existing && existing.expiresAt > now) return null;
    const lease: LockLease = { key, ownerId: ownerId ?? newId(), expiresAt: now + ttlMs };
    this.held.set(key, lease);
    return lease;
  }

  async release(key: string, ownerId: string): Promise<boolean> {
    const lease = this.held.get(key);
    if (!lease || lease.ownerId !== ownerId) return false;
    this.held.delete(key);
    return true;
  }

  async renew(key: string, ownerId: string, ttlMs: number): Promise<boolean> {
    const lease = this.held.get(key);
    if (!lease || lease.ownerId !== ownerId) return false;
    lease.expiresAt = Date.now() + ttlMs;
    return true;
  }

  inspect(): Array<{ key: string; ownerId: string; expiresAt: number }> {
    const now = Date.now();
    for (const [key, lease] of this.held) {
      if (lease.expiresAt <= now) this.held.delete(key);
    }
    return [...this.held.values()].map((l) => ({ ...l }));
  }

  dispose(): void {
    this.held.clear();
  }
}

export interface LockRunOptions {
  ttlMs: number;
  waitMs?: number;
}

export class LockManager {
  constructor(
    private provider: LockProvider,
    private deps: { hub: TimerHub; clock: Clock; logger?: Logger },
  ) {}

  async run<T>(
    key: string,
    opts: LockRunOptions,
    fn: (lease: LockLease) => Promise<T> | T,
  ): Promise<T> {
    const deadline = opts.waitMs != null ? this.deps.clock.now() + opts.waitMs : null;
    let lease: LockLease | null = null;
    for (;;) {
      lease = await this.provider.acquire(key, opts.ttlMs);
      if (lease) break;
      if (deadline == null) throw new LockUnavailableError(key);
      if (this.deps.clock.now() >= deadline) throw new LockUnavailableError(key, opts.waitMs);
      await new Promise<void>((r) => this.deps.hub.after(25, () => r()));
    }
    try {
      return await fn(lease);
    } finally {
      this.provider.release(key, lease.ownerId).catch((err) => {
        this.deps.logger?.warn('lock release failed', { key, error: String(err) });
      });
    }
  }

  active(): Array<{ key: string; ownerId: string; expiresAt: number }> {
    return this.provider.inspect?.() ?? [];
  }

  dispose(): void {
    this.provider.dispose?.();
  }
}
