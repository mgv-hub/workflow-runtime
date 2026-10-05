import { describe, expect, it } from 'vitest';
import { createRuntime, InMemoryStore, nextCalendarRun, VirtualClock } from '../src/index.js';

function runtimeOver(storage?: InMemoryStore) {
  const clock = new VirtualClock(Date.UTC(2024, 0, 1));
  const rt = createRuntime({ clock, storage: storage ?? new InMemoryStore(), namespace: 'test' });
  return { clock, rt };
}

describe('Scheduler', () => {
  it('fires at-time jobs once and completes them', async () => {
    const { clock, rt } = runtimeOver();
    const fired: number[] = [];
    rt.scheduler.at(
      clock.now() + 1000,
      (ctx) => {
        fired.push(ctx.fireAt);
      },
      { id: 'once' },
    );
    clock.advance(999);
    expect(fired).toHaveLength(0);
    clock.advance(2);
    await new Promise((r) => setTimeout(r, 0));
    expect(fired).toEqual([clock.now() - 1]);
    expect(rt.scheduler.get('once')?.status).toBe('done');
  });

  it('does not overlap recurring runs and keeps cadence', async () => {
    const { clock, rt } = runtimeOver();
    let busy = 0;
    let maxBusy = 0;
    let completed = 0;
    let release: () => void = () => {};
    rt.scheduler.every(
      1000,
      async () => {
        busy += 1;
        maxBusy = Math.max(maxBusy, busy);
        await new Promise<void>((r) => (release = r));
        busy -= 1;
        completed += 1;
      },
      { id: 'tick' },
    );
    clock.advance(1000);
    await new Promise((r) => setTimeout(r, 0));
    expect(maxBusy).toBe(1);
    expect(rt.scheduler.get('tick')?.nextRunAt).toBeNull();
    release();
    await new Promise((r) => setTimeout(r, 0));
    clock.advance(0);
    expect(rt.scheduler.get('tick')?.nextRunAt).toBe(clock.now() + 1000);
    clock.advance(1000);
    release();
    await new Promise((r) => setTimeout(r, 0));
    clock.advance(1000);
    release();
    await new Promise((r) => setTimeout(r, 0));
    expect(completed).toBe(3);
    expect(maxBusy).toBe(1);
  });

  it('skips runs beyond the misfire grace', async () => {
    const { clock, rt } = runtimeOver();
    const fired: number[] = [];
    const skipped: unknown[] = [];
    rt.events.on('schedule.skipped', (e) => {
      skipped.push(e.payload);
    });
    rt.scheduler.every(
      1000,
      (ctx) => {
        fired.push(ctx.fireAt);
      },
      { id: 'mis', misfireGraceMs: 500 },
    );

    clock.advance(6000);
    await new Promise((r) => setTimeout(r, 0));

    expect(skipped.length).toBe(5);
    expect(fired).toEqual([1704067206000]);

    clock.advance(1000);
    await new Promise((r) => setTimeout(r, 0));
    expect(fired).toEqual([1704067206000, 1704067207000]);
  });

  it('supports pause, resume, and preview', async () => {
    const { clock, rt } = runtimeOver();
    let fired = 0;
    const job = rt.scheduler.every(
      500,
      () => {
        fired += 1;
      },
      { id: 'p' },
    );
    clock.advance(500);
    await new Promise((r) => setTimeout(r, 0));
    expect(fired).toBe(1);
    job.pause();
    clock.advance(5000);
    expect(fired).toBe(1);
    job.resume();
    const preview = job.nextRuns(3);
    expect(preview).toHaveLength(3);
    expect(preview[0]).toBeGreaterThan(clock.now() - 1);
    clock.advance(preview[0] - clock.now());
    await new Promise((r) => setTimeout(r, 0));
    expect(fired).toBe(2);
  });

  it('maxRuns stops recurring jobs', async () => {
    const { clock, rt } = runtimeOver();
    let fired = 0;
    rt.scheduler.every(
      100,
      () => {
        fired += 1;
      },
      { id: 'cap', maxRuns: 3 },
    );
    clock.advance(1000);
    await new Promise((r) => setTimeout(r, 0));
    expect(fired).toBe(3);
    expect(rt.scheduler.get('cap')).toBeUndefined();
  });

  it('restores persisted jobs across restarts with the original schedule', async () => {
    const storage = new InMemoryStore();
    const first = runtimeOver(storage);
    const fireAt = first.clock.now() + 5000;
    first.rt.scheduler.at(fireAt, () => {}, { id: 'nightly' });
    const clock2 = new VirtualClock(Date.UTC(2024, 0, 1));
    const rt2 = createRuntime({ clock: clock2, storage, namespace: 'test' });
    let fired = 0;
    rt2.scheduler.at(
      fireAt,
      () => {
        fired += 1;
      },
      { id: 'nightly' },
    );
    await rt2.start();
    clock2.advance(5000);
    await new Promise((r) => setTimeout(r, 0));
    expect(fired).toBe(1);
  });
});

describe('calendar schedules', () => {
  it('computes the next daily, weekly, and monthly runs', () => {
    const from = Date.UTC(2024, 1, 10, 12, 0, 0);
    expect(nextCalendarRun({ kind: 'daily', time: '09:00' }, from)).toBe(
      Date.UTC(2024, 1, 11, 9, 0, 0),
    );
    expect(nextCalendarRun({ kind: 'weekly', days: [1, 3, 5], time: '08:30' }, from)).toBe(
      Date.UTC(2024, 1, 12, 8, 30, 0),
    );
    expect(nextCalendarRun({ kind: 'monthly', day: 31, time: '00:00' }, from)).toBe(
      Date.UTC(2024, 2, 31, 0, 0, 0),
    );
  });
});
