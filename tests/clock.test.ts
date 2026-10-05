import { describe, expect, it, vi, afterEach } from 'vitest';
import { SystemClock, TimerHub, VirtualClock } from '../src/index.js';

afterEach(() => {
  vi.useRealTimers();
});

describe('VirtualClock + TimerHub', () => {
  it('fires entries in (dueAt, insertion) order', () => {
    const clock = new VirtualClock(1000);
    const hub = new TimerHub({ clock, driver: 'virtual' });
    const calls: string[] = [];
    hub.after(50, () => calls.push('a'));
    hub.after(50, () => calls.push('b'));
    hub.after(10, () => calls.push('c'));
    clock.advance(100);
    expect(calls).toEqual(['c', 'a', 'b']);
    expect(clock.now()).toBe(1100);
  });

  it('cancel prevents firing', () => {
    const clock = new VirtualClock(0);
    const hub = new TimerHub({ clock, driver: 'virtual' });
    let fired = 0;
    const h = hub.after(10, () => (fired += 1));
    h.cancel();
    clock.advance(20);
    expect(fired).toBe(0);
    expect(hub.size).toBeGreaterThanOrEqual(0);
  });

  it('callbacks scheduled during processing fire in the same advance', () => {
    const clock = new VirtualClock(0);
    const hub = new TimerHub({ clock, driver: 'virtual' });
    const seen: number[] = [];
    hub.after(10, () => {
      seen.push(clock.now());
      hub.after(10, () => seen.push(clock.now()));
    });
    clock.advance(100);
    expect(seen).toEqual([10, 20]);
  });

  it('routes callback errors to onError without stopping others', () => {
    const clock = new VirtualClock(0);
    const errors: unknown[] = [];
    const hub = new TimerHub({ clock, driver: 'virtual', onError: (e) => errors.push(e) });
    let after = 0;
    hub.after(5, () => {
      throw new Error('boom');
    });
    hub.after(10, () => (after += 1));
    clock.advance(20);
    expect(errors).toHaveLength(1);
    expect(after).toBe(1);
  });

  it('advanceTo is monotonic', () => {
    const clock = new VirtualClock(500);
    clock.advanceTo(300);
    expect(clock.now()).toBe(500);
    clock.advanceTo(900);
    expect(clock.now()).toBe(900);
  });
});

describe('SystemClock driver', () => {
  it('fires through a single setTimeout and re-arms', async () => {
    vi.useFakeTimers();
    const clock = new SystemClock();
    const hub = new TimerHub({ clock, driver: 'system' });
    let fired = 0;
    hub.after(50, () => (fired += 1));
    hub.after(120, () => (fired += 10));
    await vi.advanceTimersByTimeAsync(49);
    expect(fired).toBe(0);
    await vi.advanceTimersByTimeAsync(1);
    expect(fired).toBe(1);
    await vi.advanceTimersByTimeAsync(70);
    expect(fired).toBe(11);
    hub.dispose();
    expect(hub.size).toBe(0);
  });

  it('handles delays beyond the setTimeout ceiling', async () => {
    vi.useFakeTimers();
    const clock = new SystemClock();
    const hub = new TimerHub({ clock, driver: 'system' });
    let fired = 0;
    hub.after(2147483647 + 5000, () => (fired += 1));
    await vi.advanceTimersByTimeAsync(2147483647);
    expect(fired).toBe(0);
    await vi.advanceTimersByTimeAsync(6000);
    expect(fired).toBe(1);
    hub.dispose();
  });
});
