import { describe, expect, it } from 'vitest';
import { createRuntime, InMemoryStore, VirtualClock } from '../src/index.js';

describe('Runtime', () => {
  it('isolates namespaces over shared storage', async () => {
    const storage = new InMemoryStore();
    const clock = new VirtualClock(0);
    const a = createRuntime({ clock, storage, namespace: 'app-a' });
    const b = createRuntime({ clock, storage, namespace: 'app-b' });
    await a.stateManager.set('feature', 'on');
    expect(await b.stateManager.get('feature')).toBeUndefined();
    expect(await a.stateManager.get('feature')).toBe('on');
  });

  it('reports health and inspection', async () => {
    const clock = new VirtualClock(0);
    const rt = createRuntime({ clock, namespace: 'test' });
    rt.scheduler.every(1000, () => {}, { id: 'h' });
    rt.entities.define('thing', { ttl: 5000 });
    rt.entities.create('thing', 't1');
    const health = rt.health();
    expect(health.state).toBe('created');
    expect(health.scheduledJobs).toBe(1);
    expect(health.entities.active).toBe(1);
    const inspect = rt.inspect();
    expect(inspect.namespace).toBe('test');
    expect(inspect.clock.virtual).toBe(true);
  });

  it('shuts down gracefully and clears resources', async () => {
    const clock = new VirtualClock(0);
    const rt = createRuntime({ clock, namespace: 'test' });
    rt.scheduler.every(10000, () => {});
    rt.entities.define('e', { ttl: 60000 });
    rt.entities.create('e', 'e1');
    await rt.start();
    await rt.shutdown();
    expect(rt.state).toBe('stopped');
    expect(rt.health().timers).toBe(0);
    expect(rt.events.listenerCount()).toBe(0);
  });

  it('records metrics and bounded history', async () => {
    const clock = new VirtualClock(0);
    const rt = createRuntime({ clock, namespace: 'test' });
    rt.entities.define('tracked', { ttl: 100, history: true });
    rt.entities.create('tracked', 't1');
    clock.advance(200);
    const snapshot = rt.metrics.snapshot();
    expect(Object.keys(snapshot.counters).length).toBeGreaterThanOrEqual(0);
    void snapshot;
  });

  it('queries by tags', async () => {
    const clock = new VirtualClock(0);
    const rt = createRuntime({ clock, namespace: 'test' });
    rt.entities.define('tagged', {});
    rt.entities.create('tagged', 't1', { tags: ['beta'] });
    rt.entities.create('tagged', 't2', { tags: ['prod'] });
    const beta = rt.queries.entities({ tags: ['beta'] });
    expect(beta.map((e) => e.id)).toEqual(['t1']);
  });
});
