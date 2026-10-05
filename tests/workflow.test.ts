import { describe, expect, it } from 'vitest';
import {
  createRuntime,
  InMemoryStore,
  VirtualClock,
  waitForEventStep,
  type WorkflowDef,
} from '../src/index.js';

function setup(opts: { storage?: InMemoryStore; recovery?: 'resume' | 'fail' | 'drop' } = {}) {
  const clock = new VirtualClock(0);
  const rt = createRuntime({
    clock,
    namespace: 'test',
    storage: opts.storage ?? new InMemoryStore(),
    recovery: { executions: opts.recovery ?? 'resume' },
  });
  return { clock, rt };
}

describe('WorkflowEngine', () => {
  it('runs linear workflows and collects results', async () => {
    const { rt } = setup();
    rt.workflows.register({
      id: 'linear',
      steps: [
        { id: 'a', run: () => 1 },
        { id: 'b', dependsOn: ['a'], run: (ctx) => (ctx.results.a as number) + 1 },
      ],
    } satisfies WorkflowDef);
    const handle = rt.workflows.start('linear');
    const record = await handle.promise;
    expect(record.status).toBe('completed');
    expect(record.results).toEqual({ a: 1, b: 2 });
  });

  it('skips steps on false conditions and cascades to dependents', async () => {
    const { rt } = setup();
    rt.workflows.register({
      id: 'branch',
      steps: [
        { id: 'check', run: () => 'yes' },
        { id: 'pathA', dependsOn: ['check'], when: () => false, run: () => 'A' },
        { id: 'afterA', dependsOn: ['pathA'], run: () => 'after' },
        { id: 'always', dependsOn: ['check'], run: () => 'always' },
      ],
    } satisfies WorkflowDef);
    const record = await rt.workflows.start('branch').promise;
    expect(record.steps.pathA.status).toBe('skipped');
    expect(record.steps.afterA.status).toBe('skipped');
    expect(record.steps.afterA.reason).toBe('upstream-skipped');
    expect(record.steps.always.status).toBe('succeeded');
    expect(record.status).toBe('completed');
  });

  it('joins after conditional branches with depsPolicy settled', async () => {
    const { rt } = setup();
    rt.workflows.register({
      id: 'join',
      steps: [
        { id: 'taken', run: () => 1 },
        { id: 'notTaken', when: () => false, run: () => 2 },
        {
          id: 'joinStep',
          dependsOn: ['taken', 'notTaken'],
          depsPolicy: 'settled',
          run: () => 'joined',
        },
      ],
    } satisfies WorkflowDef);
    const record = await rt.workflows.start('join').promise;
    expect(record.steps.joinStep.status).toBe('succeeded');
    expect(record.results.joinStep).toBe('joined');
  });

  it('runs parallel branches concurrently', async () => {
    const { clock, rt } = setup();
    const started: number[] = [];
    rt.workflows.register({
      id: 'parallel',
      steps: [
        {
          id: 'left',
          run: () => {
            started.push(clock.now());
          },
        },
        {
          id: 'right',
          run: () => {
            started.push(clock.now());
          },
        },
        { id: 'join', dependsOn: ['left', 'right'], run: () => 'done' },
      ],
    } satisfies WorkflowDef);
    const record = await rt.workflows.start('parallel').promise;
    expect(record.status).toBe('completed');
    expect(record.steps.join.status).toBe('succeeded');
    expect(started.length).toBe(2);
  });

  it('retries with backoff and succeeds', async () => {
    const { clock, rt } = setup();
    let attempts = 0;
    rt.workflows.register({
      id: 'retry',
      steps: [
        {
          id: 'flaky',
          retries: { max: 2, type: 'exponential', delay: 100, factor: 2 },
          run: () => {
            attempts += 1;
            if (attempts < 3) throw new Error('flaky');
            return 'ok';
          },
        },
      ],
    } satisfies WorkflowDef);
    const handle = rt.workflows.start('retry');
    await new Promise((r) => setTimeout(r, 0));
    clock.advance(100);
    await new Promise((r) => setTimeout(r, 0));
    clock.advance(200);
    await new Promise((r) => setTimeout(r, 0));
    const record = await handle.promise;
    expect(record.status).toBe('completed');
    expect(record.steps.flaky.attempts).toBe(3);
    expect(record.results.flaky).toBe('ok');
  });

  it('respects retryIf and fails fast on non-retryable errors', async () => {
    const { rt } = setup();
    let attempts = 0;
    rt.workflows.register({
      id: 'retryIf',
      steps: [
        {
          id: 'boom',
          retries: { max: 3, retryIf: (e) => (e as Error).message === 'retryable' },
          run: () => {
            attempts += 1;
            throw new Error('fatal');
          },
        },
      ],
    } satisfies WorkflowDef);
    const record = await rt.workflows.start('retryIf').promise;
    expect(record.status).toBe('failed');
    expect(attempts).toBe(1);
  });

  it('times out steps with a structured error', async () => {
    const { clock, rt } = setup();
    rt.workflows.register({
      id: 'stepTimeout',
      steps: [
        {
          id: 'slow',
          timeoutMs: 500,
          run: () => new Promise(() => {}),
        },
      ],
    } satisfies WorkflowDef);
    const handle = rt.workflows.start('stepTimeout');
    await new Promise((r) => setTimeout(r, 0));
    clock.advance(501);
    await new Promise((r) => setTimeout(r, 0));
    const record = await handle.promise;
    expect(record.status).toBe('failed');
    expect(record.steps.slow.error?.code).toBe('WR_STEP_TIMEOUT');
  });

  it('times out whole workflows', async () => {
    const { clock, rt } = setup();
    rt.workflows.register({
      id: 'wfTimeout',
      timeoutMs: 1000,
      steps: [
        { id: 'one', run: () => 1 },
        { id: 'two', dependsOn: ['one'], run: () => new Promise(() => {}) },
      ],
    } satisfies WorkflowDef);
    const handle = rt.workflows.start('wfTimeout');
    clock.advance(1001);
    await new Promise((r) => setTimeout(r, 0));
    const record = await handle.promise;
    expect(record.status).toBe('failed');
    expect(record.error?.code).toBe('WR_WORKFLOW_TIMEOUT');
  });

  it('runs compensations in reverse order on failure', async () => {
    const { rt } = setup();
    const order: string[] = [];
    rt.workflows.register({
      id: 'compensate',
      steps: [
        {
          id: 's1',
          run: () => 1,
          compensate: () => {
            order.push('undo-s1');
          },
        },
        {
          id: 's2',
          dependsOn: ['s1'],
          run: () => 2,
          compensate: () => {
            order.push('undo-s2');
          },
        },
        {
          id: 's3',
          dependsOn: ['s2'],
          run: () => {
            throw new Error('fatal');
          },
        },
      ],
    } satisfies WorkflowDef);
    const record = await rt.workflows.start('compensate').promise;
    expect(record.status).toBe('failed');
    expect(order).toEqual(['undo-s2', 'undo-s1']);
    expect(record.compensations.map((c) => c.status)).toEqual(['succeeded', 'succeeded']);
  });

  it('uses fallback actions when the primary fails', async () => {
    const { rt } = setup();
    rt.workflows.register({
      id: 'fallback',
      steps: [
        {
          id: 'primary',
          failure: 'fallback',
          run: () => {
            throw new Error('primary down');
          },
          fallback: () => 'fallback-value',
        },
      ],
    } satisfies WorkflowDef);
    const record = await rt.workflows.start('fallback').promise;
    expect(record.status).toBe('completed');
    expect(record.steps.primary.usedFallback).toBe(true);
    expect(record.results.primary).toBe('fallback-value');
  });

  it('continue policy completes with a partial flag', async () => {
    const { rt } = setup();
    rt.workflows.register({
      id: 'partial',
      steps: [
        {
          id: 'bad',
          failure: 'continue',
          run: () => {
            throw new Error('ignored');
          },
        },
        { id: 'good', run: () => 'fine' },
      ],
    } satisfies WorkflowDef);
    const record = await rt.workflows.start('partial').promise;
    expect(record.status).toBe('completed');
    expect(record.partial).toBe(true);
  });

  it('cancels mid-run and settles in-flight steps', async () => {
    const { rt } = setup();
    let settleSlow: (v: unknown) => void = () => {};
    rt.workflows.register({
      id: 'cancel',
      steps: [
        { id: 'first', run: () => 'one' },
        {
          id: 'second',
          dependsOn: ['first'],
          run: () => new Promise((resolve) => (settleSlow = resolve)),
        },
      ],
    } satisfies WorkflowDef);
    const handle = rt.workflows.start('cancel');
    handle.cancel('user asked');
    expect(handle.status()).toBe('cancelled');
    settleSlow('late');
    await Promise.resolve();
    const record = await handle.promise;
    expect(record.status).toBe('cancelled');
    expect(record.steps.second.status).toBe('cancelled');
  });

  it('pauses and resumes across retry delays', async () => {
    const { clock, rt } = setup();
    let attempts = 0;
    rt.workflows.register({
      id: 'pause',
      steps: [
        {
          id: 'p1',
          retries: { max: 1, delay: 200 },
          run: () => {
            attempts += 1;
            if (attempts === 1) throw new Error('again');
            return 'recovered';
          },
        },
      ],
    } satisfies WorkflowDef);
    const handle = rt.workflows.start('pause');
    await new Promise((r) => setTimeout(r, 0));
    handle.pause();
    clock.advance(5000);
    expect(handle.isPaused()).toBe(true);
    expect(handle.record()?.steps.p1.status).toBe('running');
    handle.resume();
    const record = await handle.promise;
    expect(record.status).toBe('completed');
    expect(record.results.p1).toBe('recovered');
    expect(attempts).toBe(2);
  });

  it('dry run skips side-effecting steps and compensations', async () => {
    const { rt } = setup();
    const sideEffects: string[] = [];
    rt.workflows.register({
      id: 'dry',
      steps: [
        { id: 'calc', run: () => 42 },
        {
          id: 'charge',
          sideEffects: true,
          run: () => {
            sideEffects.push('charged');
          },
          compensate: () => {
            sideEffects.push('refund');
          },
        },
        {
          id: 'last',
          dependsOn: ['charge'],
          run: () => {
            throw new Error('boom');
          },
        },
      ],
    } satisfies WorkflowDef);
    const record = await rt.workflows.start('dry', { dryRun: true }).promise;
    expect(record.status).toBe('failed');
    expect(record.steps.charge.status).toBe('skipped');
    expect(record.steps.charge.reason).toBe('dry-run');
    expect(sideEffects).toEqual([]);
  });

  it('waits for a start event with timeout', async () => {
    const { clock, rt } = setup();
    rt.workflows.register({
      id: 'eventStart',
      steps: [{ id: 'go', run: (ctx) => (ctx.input as { user: string }).user }],
    } satisfies WorkflowDef);
    const handle = rt.workflows.start('eventStart', {
      waitFor: { event: 'signup', timeoutMs: 1000 },
    });
    expect(handle.status()).toBe('pending');
    rt.events.emit('signup', { user: 'mia' });
    const record = await handle.promise;
    expect(record.status).toBe('completed');
    expect(record.results.go).toBe('mia');
    const handle2 = rt.workflows.start('eventStart', {
      waitFor: { event: 'signup', timeoutMs: 500 },
    });
    clock.advance(600);
    const record2 = await handle2.promise;
    expect(record2.status).toBe('cancelled');
  });

  it('waitForEventStep resolves and supports cancellation', async () => {
    const { clock, rt } = setup();
    rt.workflows.register({
      id: 'waitStep',
      steps: [
        waitForEventStep('approval', { id: 'await-approval', timeoutMs: 2000 }),
        { id: 'after', dependsOn: ['await-approval'], run: () => 'shipped' },
      ],
    } satisfies WorkflowDef);
    const handle = rt.workflows.start('waitStep');
    await new Promise((r) => setTimeout(r, 0));
    rt.events.emit('approval', { by: 'boss' });
    clock.advance(1);
    await new Promise((r) => setTimeout(r, 0));
    const record = await handle.promise;
    expect(record.status).toBe('completed');
    expect(record.results['await-approval']).toEqual({ by: 'boss' });
    expect(record.results.after).toBe('shipped');
  });

  it('recovers persisted executions on a second runtime', async () => {
    const storage = new InMemoryStore();
    const runLog: string[] = [];
    const def: WorkflowDef = {
      id: 'recover',
      version: 1,
      persist: true,
      steps: [
        {
          id: 's1',
          run: () => {
            runLog.push('s1');
          },
        },
        {
          id: 's2',
          dependsOn: ['s1'],
          run: () => {
            runLog.push('s2');
          },
        },
        {
          id: 's3',
          dependsOn: ['s2'],
          run: () => {
            runLog.push('s3');
          },
        },
      ],
    };
    const first = setup({ storage });
    first.rt.workflows.register(def);
    const h1 = first.rt.workflows.start('recover');
    first.clock.advance(1);
    await new Promise((r) => setTimeout(r, 0));
    const clock2 = new VirtualClock(1);
    const rt2 = createRuntime({ clock: clock2, storage, namespace: 'test' });
    rt2.workflows.register(def);
    const report = await rt2.start();
    expect(report.recoveredExecutions.recovered).toBe(1);
    clock2.advance(5);
    await new Promise((r) => setTimeout(r, 0));
    const live = rt2.workflows.list({});
    expect(live[0]?.status).toBe('completed');
    expect(runLog.filter((x) => x === 's1')).toHaveLength(1);
    expect(runLog.filter((x) => x === 's2')).toHaveLength(1);
    expect(runLog.filter((x) => x === 's3')).toHaveLength(1);
    void h1;
  });

  it('migrates persisted state when the definition version changes', async () => {
    const storage = new InMemoryStore();
    const v1: WorkflowDef = {
      id: 'migrate',
      version: 1,
      persist: true,
      steps: [{ id: 'a', run: () => 'v1-done' }],
    };
    const first = setup({ storage });
    first.rt.workflows.register(v1);
    const handle = first.rt.workflows.start('migrate', { input: { count: 1 } });
    first.clock.advance(1);
    await handle.promise;
    const clock2 = new VirtualClock(1);
    const rt2 = createRuntime({ clock: clock2, storage, namespace: 'test' });
    rt2.workflows.register({
      ...v1,
      version: 2,
      migrate: (record) => ({
        ...record,
        input: { count: (record.input as { count: number }).count + 100 },
      }),
      steps: [{ id: 'a', run: () => 'v2-done' }],
    });
    const report = await rt2.start();
    expect(report.recoveredExecutions.recovered).toBe(1);
    const live = rt2.workflows.list({});
    expect(live[0]?.version).toBe('2');
  });

  it('replays completed executions reusing non-replaySafe results', async () => {
    const { rt } = setup();
    let riskyRuns = 0;
    rt.workflows.register({
      id: 'replay',
      persist: true,
      steps: [
        { id: 'pure', run: () => 10 },
        {
          id: 'risky',
          replaySafe: true,
          run: () => {
            riskyRuns += 1;
          },
        },
      ],
    } satisfies WorkflowDef);
    const first = await rt.workflows.start('replay').promise;
    expect(first.status).toBe('completed');
    const second = await rt.workflows.replay(first.id);
    expect(second).not.toBeNull();
    const record = await second!.promise;
    expect(record.replayOf).toBe(first.id);
    expect(record.steps.pure.replayed).toBe(true);
    expect(record.steps.risky.replayed).toBeUndefined();
    expect(riskyRuns).toBe(2);
  });
});
