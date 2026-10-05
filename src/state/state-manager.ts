import type { EventBus } from '../events/event-bus.js';
import type { Logger } from '../observability/logger.js';
import { storageKey, storagePrefix, type KeyValueStore } from '../storage/store.js';

export interface StateManagerDeps {
  storage: KeyValueStore;
  namespace: string;
  events?: EventBus;
  logger?: Logger;
}

export class StateManager {
  private prefix: string;

  constructor(private deps: StateManagerDeps) {
    this.prefix = storagePrefix('state', deps.namespace);
  }

  async get<T = unknown>(key: string): Promise<T | undefined> {
    return this.deps.storage.get<T>(storageKey('state', this.deps.namespace, key));
  }

  async set(key: string, value: unknown): Promise<void> {
    await this.deps.storage.put(storageKey('state', this.deps.namespace, key), value);
    this.deps.events?.emit('state.updated', { key, value });
  }

  async update<T = unknown>(key: string, fn: (current: T | undefined) => T): Promise<T> {
    const current = await this.get<T>(key);
    const next = fn(current);
    await this.set(key, next);
    return next;
  }

  async inc(key: string, by = 1): Promise<number> {
    return this.update<number>(key, (cur) => (cur ?? 0) + by);
  }

  async delete(key: string): Promise<boolean> {
    const ok = await this.deps.storage.delete(storageKey('state', this.deps.namespace, key));
    if (ok) this.deps.events?.emit('state.deleted', { key });
    return ok;
  }

  async has(key: string): Promise<boolean> {
    return (await this.get(key)) !== undefined;
  }

  async keys(prefix = ''): Promise<string[]> {
    const entries = await this.deps.storage.list(this.prefix + prefix);
    return entries.map((e) => e.key.slice(this.prefix.length));
  }
}
