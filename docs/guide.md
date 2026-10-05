# workflow-runtime - Guide

## 1. Introduction
A programmable runtime for time, state, events, tasks, lifecycles, expiration,
scheduling, and automated execution. Node 18+, zero runtime dependencies.

## 2. Installation
`npm install workflow-runtime`. CommonJS, ESM, and TypeScript are all supported
through the package `exports` map.

## 3. Quick start
See README. `createRuntime()` then `await rt.start()`. Every subsystem is usable
independently; the runtime wires them together.

## 4. Core concepts
Clock (system or virtual), one TimerHub per runtime, records (executions,
entities, tasks, jobs), storage adapters, and the event bus.

## 5. Runtime
`createRuntime({ namespace, clock, storage, locks, logger, metrics, tracing,
middleware, concurrency, recovery, ... })`. Access: `.events`, `.stateManager`,
`.scheduler`, `.entities`, `.tasks`, `.workflows`, `.locks`, `.queries`,
`.health()`, `.inspect()`, `.start()`, `.shutdown()`.

## 6. Workflows
Register a definition, then start executions. Executions capture the definition
snapshot, so later re-registrations never mutate a running execution.

## 7. Tasks
`rt.tasks.group(name, { concurrency, rateLimit })`, `rt.tasks.enqueue(spec)`.
Tasks have priority, delay, timeout, retries, dedupe keys, and a dead-letter
queue with inspection and retry.

## 8. Scheduling
`at`, `delay`, `every`, `cron`, `daily`, `weekly`, `monthly`. All accept a
timezone; misfire grace controls late-fire behavior. `nextRuns()` previews
future runs without executing anything.

## 9. Expiration
Entities with TTL. Absolute TTL expires at a fixed offset; sliding TTL moves
forward on every `touch()`.

## 10. Inactivity
`inactiveAfter` on an entity type. The runtime tracks `lastActivityAt` and
computes `inactiveSince`. Activity on an inactive entity reactivates it.

## 11. Events
`on/once/off/emit/waitFor`. Wildcards: `task.*` and `*`. Filters are predicates
on the event record.

## 12. State
`rt.stateManager.get/set/update/inc/delete/keys`. Changes emit `state.updated`
and `state.deleted`.

## 13. Persistence
Pass a `KeyValueStore`. `persist: true` on workflow defs, entity types, and task
groups opts those records into write-through persistence. The filesystem adapter
lives at `workflow-runtime/adapters/fs`.

## 14. Retries
`retries: { max, type: 'fixed'|'exponential'|'custom', delay, factor, maxDelay,
custom, retryIf }`. Shared by workflow steps and tasks.

## 15. Timeouts
Steps, workflows, tasks, and event waits all support timeouts that produce
structured errors (`WR_STEP_TIMEOUT`, `WR_WORKFLOW_TIMEOUT`, `WR_TASK_TIMEOUT`,
`WR_EVENT_WAIT_TIMEOUT`).

## 16. Cancellation
Execution handles and task handles expose `cancel()`. AbortSignals propagate to
step and task contexts. Cancellation is a terminal state, not an exception.

## 17. Concurrency
Global `concurrency` option for tasks; per-group `concurrency` overrides.
Workflow parallelism comes from the DAG: independent steps run concurrently.

## 18. Locks
`rt.locks.run(key, { ttlMs, waitMs }, fn)` - acquire, run, release. Swap the
`LockProvider` for Redis or another backend in multi-process deployments.

## 19. Recovery
`await rt.start()` loads persisted entities, jobs, tasks, and executions.
Unfinished executions resume: terminal steps are kept, the rest re-run - steps
must be idempotent. `recovery.executions: 'fail'` instead marks them failed and
runs compensations; `migrate` hooks upgrade old persisted state.

## 20. Observability
Structured JSON logs with child bindings; metrics counters/gauges/observations;
tracing spans bridgeable to OpenTelemetry; capped per-stream history.

## 21. Middleware
`createRuntime({ middleware: [fn] })`. Middleware wrap step, task, and job
executions: `(ctx, next) => ...`. Use for logging, auth, metrics, validation.

## 22. Testing
Create runtimes with `new VirtualClock()`. Everything scheduled advances
deterministically. No real waits.

## 23. Virtual time
`clock.advance(ms)` / `clock.advanceTo(t)` fire every due timer in order.
Time travel is how tests verify retries, TTLs, inactivity, and drains.

## 24. Advanced usage
Replay: `rt.workflows.replay(id)` reuses results of steps not marked
`replaySafe: true` and re-executes the rest. Dry runs skip `sideEffects: true`
steps and all compensations.

## 25. Architecture
See [architecture.md](architecture.md).

## 26. Custom adapters
See [adapters.md](adapters.md).

## 27. API reference
See the generated `dist/types/index.d.ts`; every public symbol is exported from
the package root.

## 28. Examples
`examples/` contains fifteen runnable scenarios across unrelated domains.
