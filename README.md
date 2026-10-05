# workflow-runtime

One runtime for time, state, events, tasks, workflows, expiration, and automation.

`workflow-runtime` is a general-purpose automation engine for Node.js. It manages
anything with a lifecycle: workflows that branch and retry, scheduled jobs, stateful
entities that expire or go inactive, background task queues, and event-driven
behavior - all observable, cancellable, testable, and optionally persistent.

It is not a workflow library with extras bolted on. Workflows, schedules, entities,
and tasks are different projections of the same core: a clock, a timer hub, plain
records, and a storage abstraction.

## Install

    npm install workflow-runtime

Zero runtime dependencies. Works out of the box with in-memory state; persistence
is opt-in via storage adapters.

## Quick start

    import { createRuntime } from 'workflow-runtime';

    const rt = createRuntime({ namespace: 'my-app' });
    await rt.start();

    // 1. A workflow with branching, retries, and compensation
    rt.workflows.register({
        id: 'order',
        steps: [
            { id: 'validate', run: (ctx) => checkOrder(ctx.input) },
            { id: 'charge', sideEffects: true, dependsOn: ['validate'],
              run: (ctx) => charge(ctx.input),
              compensate: () => refund(ctx.input),
              retries: { max: 3, type: 'exponential', delay: 500 } },
            { id: 'notify', dependsOn: ['charge'], run: (ctx) => email(ctx.input) },
        ],
    });
    const handle = rt.workflows.start('order', { input: order });
    const record = await handle.promise;

    // 2. Scheduled work with a timezone
    rt.scheduler.daily('09:00', () => sendReport(), { tz: 'Europe/Berlin' });
    rt.scheduler.cron('*/15 * * * *', () => pollApi());
    rt.scheduler.at(new Date('2025-06-01T10:00:00Z'), () => openRegistrations());

    // 3. Entities with TTL and inactivity
    rt.entities.define('session', { ttl: '30m', ttlMode: 'sliding', onExpire: 'remove' });
    rt.entities.create('session', 's:user-1', { data: { userId: 'user-1' } });
    rt.entities.touch('s:user-1'); // sliding expiry extends

    // 4. Background tasks with concurrency and a dead-letter queue
    rt.tasks.group('emails', { concurrency: 5, rateLimit: { max: 10, windowMs: 1000 } });
    rt.tasks.enqueue({ name: 'welcome', group: 'emails', run: (ctx) => send(ctx) });

## Feature overview

- Workflow engine: DAG steps, conditions, branching, parallel fan-out and joins,
  per-step timeouts, retries with fixed/exponential/custom backoff, failure
  policies (fail / continue / ignore / fallback), application-level compensation,
  checkpoints, pause/resume/cancel, replay with replay-safe guards, dry runs.
- Scheduling: one-shot, interval, cron (5/6 field, Vixie dom/dow semantics),
  calendar (daily / weekly / monthly), IANA timezones, misfire grace, pause/resume,
  next-run previews.
- Entities: createdAt / updatedAt / lastActivityAt / expiresAt / inactiveSince,
  absolute and sliding TTL, inactivity detection, retention, per-type hooks.
- Tasks: priority queue, global and group concurrency, rate limits, dedupe,
  delayed execution, retries, dead-letter inspection and retry.
- Events: exact + wildcard names, filters, once, waitFor with timeout.
- State: namespaced observable key-value storage with counters.
- Locks: TTL-based logical locks with a pluggable distributed-lock provider.
- Observability: structured logging, metrics hooks, tracing spans, capped history.
- Recovery: persisted executions, jobs, tasks, and entities resume after restart,
  with versioned workflow migration.
- Testing: VirtualClock and time travel - no real waiting anywhere.
- Controls: debounce, throttle, rate limiter, cooldown, circuit breaker.

## Documentation

- [Architecture](docs/architecture.md)
- [Guide (all 28 topics)](docs/guide.md)
- [Custom adapters](docs/adapters.md)

## Examples

See `examples/` - Discord bot temp state, web sessions, inactive users, scheduled
reports, payment workflows, email sequences, background processing, API polling,
deployment pipelines, game server timers, cache expiration, notifications,
approval processes, and data pipelines.

## Building

    npm install
    npm run build   # ESM + CJS + type declarations
    npm test

## License

MIT
