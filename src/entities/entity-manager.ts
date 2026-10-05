import { serializeError, ConfigurationError } from '../errors.js';
import { makeIdFactory, type IdFactory } from '../ids.js';
import type { Clock, TimerHub, TimerHandle } from '../clock.js';
import type { EventBus } from '../events/event-bus.js';
import type { Logger } from '../observability/logger.js';
import type { HistoryManager } from '../observability/history.js';
import { parseDuration } from '../util/time.js';
import { byKey, MinHeap } from '../util/heap.js';
import { storageKey, storagePrefix, type KeyValueStore } from '../storage/store.js';

export type EntityStatus = 'active' | 'inactive' | 'expired' | 'removed';

export interface EntityRecord {
  id: string;
  type: string;
  data: unknown;
  status: EntityStatus;
  createdAt: number;
  updatedAt: number;
  lastActivityAt: number | null;
  inactiveSince: number | null;
  expiresAt: number | null;
  expiredAt: number | null;
  ttlMode: 'sliding' | 'absolute';
  ttlMs: number | null;
  inactiveAfterMs: number | null;
  onExpirePolicy: 'remove' | 'keep';
  retentionMs: number;
  persist: boolean;
  historyEnabled: boolean;
  tags: string[];
  meta: Record<string, unknown>;
}

export interface EntityHooks {
  onCreate?: (entity: EntityRecord) => void | Promise<void>;
  onUpdate?: (entity: EntityRecord) => void | Promise<void>;
  onTouch?: (entity: EntityRecord) => void | Promise<void>;
  onExpire?: (entity: EntityRecord) => void | Promise<void>;
  onInactive?: (entity: EntityRecord) => void | Promise<void>;
  onRemove?: (entity: EntityRecord) => void | Promise<void>;
}

export interface EntityConfig {
  ttl?: number | string;
  ttlMode?: 'sliding' | 'absolute';
  inactiveAfter?: number | string;
  onExpire?: 'remove' | 'keep';
  retention?: number | string;
  persist?: boolean;
  history?: boolean;
  tags?: string[];
  hooks?: EntityHooks;
}

interface NormalizedConfig {
  ttlMs: number | null;
  ttlMode: 'sliding' | 'absolute';
  inactiveAfterMs: number | null;
  onExpirePolicy: 'remove' | 'keep';
  retentionMs: number;
  persist: boolean;
  historyEnabled: boolean;
  tags: string[];
  hooks: EntityHooks;
}

interface HeapEntry {
  seq: number;
  dueAt: number;
  entityId: string;
  arm: number;
  canceled?: boolean;
}

export interface EntityCreateOptions {
  data?: unknown;
  expiresAt?: number | Date;
  tags?: string[];
  meta?: Record<string, unknown>;
  lastActivityAt?: number | Date;
}

export class EntityManager {
  private types = new Map<string, NormalizedConfig>();
  private entities = new Map<string, EntityRecord & { _arm: number }>();
  private deferred: Array<EntityRecord & { _arm: number }> = [];
  private heap = new MinHeap<HeapEntry>(byKey((e) => e.dueAt));
  private sweepTimer?: TimerHandle;
  private armedFor: number | null = null;
  private idFactory: IdFactory;
  private prefix: string;
  private armCounter = 0;

  constructor(
    private deps: {
      hub: TimerHub;
      clock: Clock;
      events: EventBus;
      logger: Logger;
      history?: HistoryManager;
      storage?: KeyValueStore;
    },
    private namespace: string,
    opts: { idFactory?: IdFactory; sweepIntervalMs?: number } = {},
  ) {
    this.idFactory = makeIdFactory(opts.idFactory);
    this.prefix = storagePrefix('entity', namespace);
    const sweep = opts.sweepIntervalMs ?? 60000;

    if (sweep > 0) {
      this.sweepTimer = deps.hub.after(sweep, () => this.sweepTick(sweep));
    }
  }

  define(type: string, config: EntityConfig = {}): void {
    if (this.types.has(type)) {
      throw new ConfigurationError(`Entity type "${type}" is already defined`);
    }
    const cfg: NormalizedConfig = {
      ttlMs: config.ttl != null ? parseDuration(config.ttl) : null,
      ttlMode: config.ttlMode ?? 'absolute',
      inactiveAfterMs: config.inactiveAfter != null ? parseDuration(config.inactiveAfter) : null,
      onExpirePolicy: config.onExpire ?? 'remove',
      retentionMs: config.retention != null ? parseDuration(config.retention) : 0,
      persist: config.persist ?? false,
      historyEnabled: config.history ?? false,
      tags: config.tags ?? [],
      hooks: config.hooks ?? {},
    };

    if (cfg.ttlMs != null && cfg.ttlMs <= 0)
      throw new ConfigurationError(`Entity type "${type}": ttl must be positive`);
    if (cfg.inactiveAfterMs != null && cfg.inactiveAfterMs <= 0) {
      throw new ConfigurationError(`Entity type "${type}": inactiveAfter must be positive`);
    }
    this.types.set(type, cfg);
    // Records loaded before this type was declared can be armed now.
    const stillDeferred: typeof this.deferred = [];
    for (const rec of this.deferred) {
      if (rec.type === type) this.arm(rec);
      else stillDeferred.push(rec);
    }
    this.deferred = stillDeferred;
  }

  create(type: string, id?: string, opts: EntityCreateOptions = {}): EntityRecord {
    const cfg = this.types.get(type);
    if (!cfg) throw new ConfigurationError(`Unknown entity type "${type}"`);

    const now = this.deps.clock.now();
    const entityId = id ?? `${type}:${this.idFactory()}`;

    if (this.entities.has(entityId)) {
      throw new ConfigurationError(`Entity "${entityId}" already exists`);
    }

    const explicitExpires =
      opts.expiresAt != null
        ? opts.expiresAt instanceof Date
          ? opts.expiresAt.getTime()
          : opts.expiresAt
        : null;

    const expiresAt = explicitExpires ?? (cfg.ttlMs != null ? now + cfg.ttlMs : null);
    const lastActivityAt =
      opts.lastActivityAt != null
        ? opts.lastActivityAt instanceof Date
          ? opts.lastActivityAt.getTime()
          : opts.lastActivityAt
        : now;

    const rec: EntityRecord & { _arm: number } = {
      id: entityId,
      type,
      data: opts.data ?? null,
      status: 'active',
      createdAt: now,
      updatedAt: now,
      lastActivityAt,
      inactiveSince: null,
      expiresAt,
      expiredAt: null,
      ttlMode: cfg.ttlMode,
      ttlMs: cfg.ttlMs,
      inactiveAfterMs: cfg.inactiveAfterMs,
      onExpirePolicy: cfg.onExpirePolicy,
      retentionMs: cfg.retentionMs,
      persist: cfg.persist,
      historyEnabled: cfg.historyEnabled,
      tags: [...cfg.tags, ...(opts.tags ?? [])],
      meta: opts.meta ?? {},
      _arm: 0,
    };

    this.entities.set(entityId, rec);
    this.recordHistory(rec, 'created');
    this.persist(rec);
    void this.safeHook(cfg.hooks.onCreate, rec);
    this.deps.events.emit('entity.created', { entityId, type });
    this.arm(rec);
    return this.project(rec);
  }

  get(id: string): EntityRecord | undefined {
    const rec = this.entities.get(id);
    return rec ? this.project(rec) : undefined;
  }

  touch(id: string): boolean {
    const rec = this.entities.get(id);
    if (!rec) return false;
    const cfg = this.types.get(rec.type);
    if (!cfg || (rec.status !== 'active' && rec.status !== 'inactive')) return false;
    const now = this.deps.clock.now();
    rec.lastActivityAt = now;
    rec.updatedAt = now;

    if (rec.status === 'inactive') {
      rec.status = 'active';
      rec.inactiveSince = null;
    }

    if (rec.ttlMode === 'sliding' && rec.ttlMs != null) {
      rec.expiresAt = now + rec.ttlMs;
    }

    this.persist(rec);
    void this.safeHook(cfg.hooks.onTouch, rec);
    this.deps.events.emit('entity.touched', { entityId: id, type: rec.type });
    this.arm(rec);
    return true;
  }

  update(
    id: string,
    patch: Partial<EntityCreateOptions> | ((data: unknown) => unknown),
  ): EntityRecord | undefined {
    const rec = this.entities.get(id);
    if (!rec) return undefined;
    const cfg = this.types.get(rec.type);
    if (!cfg) return undefined;

    rec.data =
      typeof patch === 'function'
        ? patch(rec.data)
        : ((patch as { data?: unknown }).data ?? rec.data);

    if (typeof patch === 'object' && patch !== null && !Array.isArray(patch) && 'meta' in patch) {
      const p = patch as { meta?: Record<string, unknown> };
      if (p.meta) rec.meta = { ...rec.meta, ...p.meta };
    }

    rec.updatedAt = this.deps.clock.now();
    this.persist(rec);
    void this.safeHook(cfg.hooks.onUpdate, rec);
    this.deps.events.emit('entity.updated', { entityId: id, type: rec.type });
    return this.project(rec);
  }

  extend(id: string, by: number | string | Date): boolean {
    const rec = this.entities.get(id);
    if (!rec || (rec.status !== 'active' && rec.status !== 'inactive')) return false;
    if (by instanceof Date) {
      rec.expiresAt = by.getTime();
    } else if (typeof by === 'string') {
      if (rec.expiresAt == null) return false;
      rec.expiresAt = rec.expiresAt + parseDuration(by);
    } else {
      if (rec.expiresAt == null) return false;
      rec.expiresAt = rec.expiresAt + by;
    }
    rec.updatedAt = this.deps.clock.now();
    this.persist(rec);
    this.arm(rec);
    return true;
  }

  remove(id: string, reason = 'manual'): boolean {
    const rec = this.entities.get(id);
    if (!rec) return false;
    const cfg = this.types.get(rec.type);
    this.removeNow(rec, reason, cfg);
    return true;
  }

  list(
    filter: { type?: string; status?: EntityStatus; tags?: string[]; limit?: number } = {},
  ): EntityRecord[] {
    const out: EntityRecord[] = [];
    for (const rec of this.entities.values()) {
      if (filter.type && rec.type !== filter.type) continue;
      if (filter.status && rec.status !== filter.status) continue;
      if (filter.tags && !filter.tags.some((t) => rec.tags.includes(t))) continue;
      out.push(this.project(rec));
      if (filter.limit && out.length >= filter.limit) break;
    }
    return out;
  }

  stats(): Record<EntityStatus, number> & { total: number } {
    const out = { active: 0, inactive: 0, expired: 0, removed: 0, total: 0 } as Record<
      EntityStatus,
      number
    > & { total: number };

    for (const rec of this.entities.values()) {
      out[rec.status] += 1;
      out.total += 1;
    }
    return out;
  }

  // Per-entity "next event" model: one heap entry per entity pointing at the
  // earliest of expiry / inactivity / retention, re-armed on every mutation.
  private arm(rec: EntityRecord & { _arm: number }): void {
    rec._arm += 1;
    const candidates: number[] = [];

    if ((rec.status === 'active' || rec.status === 'inactive') && rec.expiresAt != null)
      candidates.push(rec.expiresAt);

    if (rec.status === 'active' && rec.inactiveAfterMs != null) {
      candidates.push((rec.lastActivityAt ?? rec.createdAt) + rec.inactiveAfterMs);
    }

    if (rec.status === 'expired' && rec.onExpirePolicy === 'keep' && rec.expiredAt != null) {
      candidates.push(rec.expiredAt + rec.retentionMs);
    }

    const dueAt = candidates.length ? Math.min(...candidates) : null;
    if (dueAt == null) return;
    this.heap.push({ seq: ++this.armCounter, dueAt, entityId: rec.id, arm: rec._arm });
    this.syncSweepTimer();
  }

  private syncSweepTimer(): void {
    for (;;) {
      const head = this.heap.peek();
      if (!head) {
        this.sweepTimer?.cancel();
        this.sweepTimer = undefined;
        this.armedFor = null;
        return;
      }

      if (head.canceled) {
        this.heap.pop();
        continue;
      }

      if (this.armedFor === head.dueAt && this.sweepTimer) return;
      this.sweepTimer?.cancel();
      this.sweepTimer = this.deps.hub.after(Math.max(0, head.dueAt - this.deps.clock.now()), () =>
        this.processDue(),
      );

      this.armedFor = head.dueAt;
      return;
    }
  }

  private sweepTick(interval: number): void {
    this.processDue();
    this.sweepTimer = this.deps.hub.after(interval, () => this.sweepTick(interval));
  }

  private processDue(): void {
    const now = this.deps.clock.now();
    let guard = 0;
    for (;;) {
      const head = this.heap.peek();
      if (!head) break;

      if (head.canceled) {
        this.heap.pop();
        continue;
      }

      if (head.dueAt > now) break;

      this.heap.pop();
      const rec = this.entities.get(head.entityId);
      if (!rec || head.arm !== rec._arm) continue;
      if (++guard > 10000) break;

      this.applyTransitions(rec, now);
    }
    this.syncSweepTimer();
  }

  private applyTransitions(rec: EntityRecord & { _arm: number }, now: number): void {
    const cfg = this.types.get(rec.type);
    if (!cfg) return;
    for (;;) {
      if (rec.status === 'active' || rec.status === 'inactive') {
        if (rec.expiresAt != null && rec.expiresAt <= now) {
          rec.status = 'expired';
          rec.expiredAt = rec.expiresAt;
          rec.updatedAt = now;
          this.recordHistory(rec, 'expired');
          this.persist(rec);
          this.deps.events.emit('entity.expired', { entityId: rec.id, type: rec.type });
          void this.safeHook(cfg.hooks.onExpire, rec);

          if (rec.onExpirePolicy === 'remove') {
            this.removeNow(rec, 'expired', cfg);
            return;
          }
          continue;
        }

        if (rec.status === 'active' && rec.inactiveAfterMs != null) {
          const since = (rec.lastActivityAt ?? rec.createdAt) + rec.inactiveAfterMs;
          if (since <= now) {
            rec.status = 'inactive';
            rec.inactiveSince = since;
            rec.updatedAt = now;
            this.recordHistory(rec, 'inactive');
            this.persist(rec);
            this.deps.events.emit('entity.inactive', { entityId: rec.id, type: rec.type });
            void this.safeHook(cfg.hooks.onInactive, rec);
            continue;
          }
        }
      }
      if (
        rec.status === 'expired' &&
        rec.onExpirePolicy === 'keep' &&
        rec.expiredAt != null &&
        rec.retentionMs > 0
      ) {
        if (rec.expiredAt + rec.retentionMs <= now) {
          this.removeNow(rec, 'retention', cfg);
          return;
        }
      }
      break;
    }
    this.arm(rec);
  }

  private removeNow(
    rec: EntityRecord & { _arm: number },
    reason: string,
    cfg?: NormalizedConfig,
  ): void {
    rec._arm += 1;
    this.entities.delete(rec.id);
    this.recordHistory(rec, 'removed');

    if (rec.persist && this.deps.storage) {
      void this.deps.storage.delete(storageKey('entity', this.namespace, rec.id));
    }

    this.deps.events.emit('entity.removed', { entityId: rec.id, type: rec.type, reason });
    void this.safeHook(cfg?.hooks.onRemove, rec);
  }

  private async safeHook(
    hook: ((e: EntityRecord) => void | Promise<void>) | undefined,
    rec: EntityRecord,
  ): Promise<void> {
    if (!hook) return;
    try {
      await hook(this.project(rec));
    } catch (err) {
      this.deps.logger.warn('entity hook failed', { entityId: rec.id, error: serializeError(err) });
    }
  }

  private recordHistory(rec: EntityRecord, type: string): void {
    if (!rec.historyEnabled || !this.deps.history) return;
    this.deps.history.add(`entity:${rec.id}`, { type, status: rec.status });
  }

  private persist(rec: EntityRecord): void {
    if (!rec.persist || !this.deps.storage) return;
    void this.deps.storage.put(storageKey('entity', this.namespace, rec.id), this.project(rec));
  }

  private project(rec: EntityRecord): EntityRecord {
    const { _arm, ...rest } = rec as EntityRecord & { _arm: number };
    void _arm;
    return { ...rest };
  }

  async loadPersisted(): Promise<number> {
    if (!this.deps.storage) return 0;
    const entries = await this.deps.storage.list(this.prefix);
    let loaded = 0;

    for (const { value } of entries) {
      const raw = value as EntityRecord;
      if (!raw || typeof raw.id !== 'string' || !this.types.has(raw.type ?? '')) {
        if (raw && typeof raw.id === 'string') {
          this.deferred.push({ ...raw, _arm: 0 });
        }
        continue;
      }
      const rec: EntityRecord & { _arm: number } = { ...raw, _arm: 0 };
      if (this.entities.has(rec.id)) continue;
      this.entities.set(rec.id, rec);
      this.arm(rec);
      loaded += 1;
    }

    // Anything already past its deadline (crash downtime) processes now.
    this.processDue();
    return loaded;
  }

  dispose(): void {
    this.sweepTimer?.cancel();
    this.sweepTimer = undefined;
    this.heap.clear();
    this.entities.clear();
  }
}
