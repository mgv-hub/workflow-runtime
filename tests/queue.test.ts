import { describe, expect, it } from 'vitest';
import { createRuntime, VirtualClock } from '../src/index.js';

function setup() {
  const clock = new VirtualClock(0);
  const rt = createRuntime({ clock, namespace: 'test' });
  return { clock, rt };
}

describe('TaskQueue', () => {
  it('executes in priority order with FIFO ties', async () => {
    const { rt } = setup();
    const order: string[] = [];
    rt.tasks.group('ordered', { concurrency: 1 });
    const specs = [
      { name: 'low', priority: 0, tag: 'low' },
      { name: 'high', priority: 10, tag: 'high' },
      { name: 'high2', priority: 10, tag: 'high2' },
    ];
    for (const s of specs) {
      rt.tasks.enqueue({
        name: s.name,
        group: 'ordered',
        priority: s.priority,
        run: () => {
          order.push(s.name);
        },
      });
    }
    await new Promise((r) => setTimeout(r, 0));
    expect(order).toEqual(['high', 'high2', 'low']);
  });

  it('enforces global and group concurrency', async () => {
    const { rt } = setup();
    rt.tasks.group('serial', { concurrency: 1 });
    let inFlight = 0;
    let maxInFlight = 0;
    const gate: Array<() => void> = [];
    const makeTask = (group: string, name: string) =>
      rt.tasks.enqueue({
        name,
        group,
        run: async () => {
          inFlight += 1;
          maxInFlight = Math.max(maxInFlight, inFlight);
          await new Promise<void>((resolve) => gate.push(resolve));
          inFlight -= 1;
        },
      });
    makeTask('serial', 'a');
    makeTask('serial', 'b');
    makeTask('default', 'c');
    makeTask('default', 'd');
    makeTask('default', 'e');
    while (gate.length < 3) await Promise.resolve();
    expect(maxInFlight).toBeLessThanOrEqual(4);
    for (const g of gate) g();
    await new Promise((r) => setTimeout(r, 0));
    expect(maxInFlight).toBeLessThanOrEqual(4);
  });

  it('applies group rate limits', async () => {
    const { clock, rt } = setup();
    rt.tasks.group('limited', { concurrency: 10, rateLimit: { max: 2, windowMs: 1000 } });
    let ran = 0;
    for (let i = 0; i < 5; i++) {
      rt.tasks.enqueue({
        name: 'hit',
        group: 'limited',
        run: () => {
          ran += 1;
        },
      });
    }
    await new Promise((r) => setTimeout(r, 0));
    expect(ran).toBe(2);
    clock.advance(2000);
    await new Promise((r) => setTimeout(r, 0));
    expect(ran).toBe(5);
  });

  it('deduplicates by dedupeKey', async () => {
    const { rt } = setup();
    let ran = 0;
    const a = rt.tasks.enqueue({
      name: 'email',
      dedupeKey: 'user-1',
      run: () => {
        ran += 1;
      },
    });
    const b = rt.tasks.enqueue({
      name: 'email',
      dedupeKey: 'user-1',
      run: () => {
        ran += 1;
      },
    });
    expect(b.id).toBe(a.id);
    await a.promise;
    expect(ran).toBe(1);
  });

  it('retries then dead-letters, and the DLQ can retry', async () => {
    const { clock, rt } = setup();
    rt.tasks.group('flaky', {});
    let attempts = 0;
    const task = rt.tasks.enqueue({
      name: 'always-fails',
      group: 'flaky',
      retries: { max: 2, delay: 50 },
      run: () => {
        attempts += 1;
        throw new Error('nope');
      },
    });
    await new Promise((r) => setTimeout(r, 0));
    clock.advance(50);
    await new Promise((r) => setTimeout(r, 0));
    clock.advance(50);
    await new Promise((r) => setTimeout(r, 0));
    const record = await task.promise;
    expect(record.status).toBe('dead');
    expect(attempts).toBe(3);
    expect(rt.tasks.deadLetter().size()).toBe(1);
    expect(rt.tasks.deadLetter().list()[0].error?.message).toBe('nope');
  });

  it('times out tasks', async () => {
    const { clock, rt } = setup();
    const task = rt.tasks.enqueue({
      name: 'slow',
      timeoutMs: 300,
      run: () => new Promise(() => {}),
    });
    await new Promise((r) => setTimeout(r, 0));
    clock.advance(301);
    await new Promise((r) => setTimeout(r, 0));
    const record = await task.promise;
    expect(record.status).toBe('dead');
    expect(record.error?.code).toBe('WR_TASK_TIMEOUT');
  });

  it('cancels queued tasks', async () => {
    const { rt } = setup();
    const task = rt.tasks.enqueue({ name: 'queued', delayUntil: 10000, run: () => 'x' });
    expect(task.cancel()).toBe(true);
    await new Promise((r) => setTimeout(r, 0));
    const record = await task.promise;
    expect(record.status).toBe('cancelled');
  });

  it('drains on shutdown and aborts running tasks', async () => {
    const { clock, rt } = setup();
    let aborted = false;
    rt.tasks.enqueue({
      name: 'long',
      run: (ctx) =>
        new Promise<void>((resolve) => {
          ctx.signal.addEventListener('abort', () => {
            aborted = true;
            resolve();
          });
        }),
    });
    await new Promise((r) => setTimeout(r, 0));
    await rt.shutdown({ drainMs: 100, cancelRunning: true });
    expect(aborted).toBe(true);
    void clock;
  });
});
