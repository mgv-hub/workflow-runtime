# Custom adapters

## KeyValueStore

Implement four methods:

    export interface KeyValueStore {
        get<T = unknown>(key: string): Promise<T | undefined>;
        put(key: string, value: unknown): Promise<void>;
        delete(key: string): Promise<boolean>;
        list(prefix: string): Promise<StorageEntry[]>;
        close?(): Promise<void> | void;
    }

Values are always JSON-safe records. Example Redis sketch:

    import type { KeyValueStore, StorageEntry } from 'workflow-runtime';

    export function createRedisStore(client: { get(k: string): Promise<string | null>; set(k: string, v: string): Promise<unknown>; del(k: string): Promise<unknown>; keys(p: string): Promise<string[]> }): KeyValueStore {
        return {
            async get(key) {
                const raw = await client.get(key);
                return raw ? (JSON.parse(raw) as never) : undefined;
            },
            async put(key, value) {
                await client.set(key, JSON.stringify(value));
            },
            async delete(key) {
                return (await client.del(key)) > 0;
            },
            async list(prefix) {
                const keys = await client.keys(prefix + '*');
                const out: StorageEntry[] = [];
                for (const key of keys) {
                    const value = await this.get(key);
                    if (value !== undefined) out.push({ key, value });
                }
                return out;
            },
        };
    }

## LockProvider

    export interface LockProvider {
        acquire(key: string, ttlMs: number, ownerId?: string): Promise<LockLease | null>;
        release(key: string, ownerId: string): Promise<boolean>;
        renew?(key: string, ownerId: string, ttlMs: number): Promise<boolean>;
        inspect?(): Array<{ key: string; ownerId: string; expiresAt: number }>;
        dispose?(): void;
    }

For Redis, back `acquire` with `SET key ownerId PX ttlMs NX` and `release` with a
compare-and-delete Lua script. TTL expiry gives crash safety for free.

## Logger

Anything with `trace/debug/info/warn/error` and `child(bindings)` works.
Pino, for example, satisfies the interface directly.

## Tracing (OpenTelemetry)

    createRuntime({
        tracing: {
            onSpan(span) {
                tracer.startSpan(span.name, { startTime: span.startedAt }).end(span.endedAt);
            },
        },
    });

Spans are plain objects with `name`, `kind`, `id`, `startedAt`, `endedAt`,
`status`, and `error` - bridge them however your backend requires.
