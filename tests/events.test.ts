import { describe, expect, it } from 'vitest';
import { createEventBus, EventWaitTimeoutError, TimerHub, VirtualClock } from '../src/index.js';

function setup() {
  const clock = new VirtualClock(0);
  const hub = new TimerHub({ clock, driver: 'virtual' });
  const bus = createEventBus({ hub, clock, recentLimit: 3 });
  return { clock, hub, bus };
}

describe('EventBus', () => {
  it('delivers to exact and wildcard listeners with filters', () => {
    const { bus } = setup();
    const got: string[] = [];
    bus.on('task.succeeded', (e) => {
      got.push(`exact:${String(e.payload)}`);
    });
    bus.on('task.*', (e) => {
      got.push(`wild:${String(e.payload)}`);
    });
    bus.on('task.*', () => undefined, { filter: (e) => e.payload === 2 });
    bus.on(
      'task.*',
      (e) => {
        if (e.payload === 2) got.push('filtered');
      },
      { filter: (e) => e.payload === 2 },
    );
    bus.emit('task.succeeded', 1);
    bus.emit('task.succeeded', 2);
    bus.emit('task.queued', 3);
    expect(got).toContain('exact:1');
    expect(got).toContain('wild:1');
    expect(got).toContain('filtered');
    expect(got.filter((g) => g.startsWith('exact:3'))).toHaveLength(0);
  });

  it('once listeners fire a single time and off removes', () => {
    const { bus } = setup();
    let n = 0;
    const off = bus.once('go', () => {
      n += 1;
    });
    bus.emit('go');
    bus.emit('go');
    expect(n).toBe(1);
    off();
    let m = 0;
    const handler = () => {
      m += 1;
    };
    bus.on('go2', handler);
    expect(bus.off('go2', handler)).toBe(true);
    bus.emit('go2');
    expect(m).toBe(0);
  });

  it('listener errors are isolated', () => {
    const { bus } = setup();
    const errors: unknown[] = [];
    const bus2 = createEventBus({ onError: (e) => errors.push(e), recentLimit: 0 });
    bus2.on('x', () => {
      throw new Error('nope');
    });
    let ok = 0;
    bus2.on('x', () => {
      ok += 1;
    });
    bus2.emit('x');
    expect(ok).toBe(1);
    expect(errors).toHaveLength(1);
    void bus;
  });

  it('waitFor resolves on matching event and times out via the hub', async () => {
    const { clock, bus } = setup();
    const waiting = bus.waitFor('payment.completed', { timeoutMs: 500 });
    bus.emit('payment.completed', { id: 'p1' });
    const event = await waiting;
    expect((event.payload as { id: string }).id).toBe('p1');
    const waiting2 = bus.waitFor('payment.completed', { timeoutMs: 500 });
    const rejected = waiting2.catch((e) => e);
    clock.advance(600);
    await expect(rejected).resolves.toBeInstanceOf(EventWaitTimeoutError);
  });

  it('caps the recent buffer', () => {
    const { bus } = setup();
    bus.emit('a', 1);
    bus.emit('a', 2);
    bus.emit('a', 3);
    bus.emit('a', 4);
    expect(bus.recent()).toHaveLength(3);
    expect(bus.recent().map((e) => e.payload)).toEqual([2, 3, 4]);
  });
});
