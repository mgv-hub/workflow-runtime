import { describe, expect, it } from 'vitest';
import { createRuntime, InMemoryStore, VirtualClock } from '../src/index.js';

function setup() {
  const clock = new VirtualClock(0);
  const rt = createRuntime({ clock, namespace: 'test' });
  return { clock, rt };
}

describe('EntityManager', () => {
  it('expires absolute TTL entities and runs hooks', async () => {
    const { clock, rt } = setup();
    const expired: string[] = [];
    rt.entities.define('session', {
      ttl: '30m',
      onExpire: 'remove',
      hooks: { onExpire: (e) => expired.push(e.id) },
    });
    rt.entities.create('session', 's1', { data: { user: 'u1' } });
    clock.advance(29 * 60000);
    expect(rt.entities.get('s1')?.status).toBe('active');
    clock.advance(60000);
    expect(rt.entities.get('s1')).toBeUndefined();
    expect(expired).toEqual(['s1']);
    const events: string[] = [];
    rt.events.recent().forEach(() => {});
    expect(rt.entities.stats().total).toBe(0);
    void events;
  });

  it('sliding TTL extends on touch', async () => {
    const { clock, rt } = setup();
    rt.entities.define('web-session', { ttl: 1000, ttlMode: 'sliding' });
    rt.entities.create('web-session', 'w1');
    clock.advance(800);
    expect(rt.entities.touch('w1')).toBe(true);
    clock.advance(800); // 1600 total, but last activity at 800
    expect(rt.entities.get('w1')?.status).toBe('active');
    clock.advance(1001);
    expect(rt.entities.get('w1')).toBeUndefined();
  });

  it('detects inactivity and resurrects on activity', async () => {
    const { clock, rt } = setup();
    const inactiveAt: number[] = [];
    rt.entities.define('user', {
      inactiveAfter: 300,
      hooks: {
        onInactive: (e) => inactiveAt.push((e as { inactiveSince: number }).inactiveSince ?? -1),
      },
    });
    rt.entities.create('user', 'u1');
    clock.advance(100);
    rt.entities.touch('u1');
    clock.advance(200); // 300 since activity? lastActivity=100, +300 = 400
    expect(rt.entities.get('u1')?.status).toBe('active');
    clock.advance(101); // now 401
    const rec = rt.entities.get('u1');
    expect(rec?.status).toBe('inactive');
    expect(rec?.inactiveSince).toBe(400);
    // Activity brings it back.
    rt.entities.touch('u1');
    expect(rt.entities.get('u1')?.status).toBe('active');
    expect(inactiveAt).toEqual([400]);
  });

  it('keeps expired entities for retention before removal', async () => {
    const { clock, rt } = setup();
    rt.entities.define('audit', { ttl: 500, onExpire: 'keep', retention: 1000 });
    rt.entities.create('audit', 'a1');
    clock.advance(500);
    expect(rt.entities.get('a1')?.status).toBe('expired');
    clock.advance(999);
    expect(rt.entities.get('a1')).toBeDefined();
    clock.advance(2);
    expect(rt.entities.get('a1')).toBeUndefined();
  });

  it('persists and reloads entities with a second runtime', async () => {
    const storage = new InMemoryStore();
    const clock1 = new VirtualClock(0);
    const rt1 = createRuntime({ clock: clock1, storage, namespace: 'test' });
    rt1.entities.define('cache', { ttl: 10000, persist: true, history: true });
    rt1.entities.create('cache', 'c1', { data: { k: 'v' } });
    clock1.advance(1000);
    const clock2 = new VirtualClock(1000);
    const rt2 = createRuntime({ clock: clock2, storage, namespace: 'test' });
    rt2.entities.define('cache', { ttl: 10000, persist: true });
    const report = await rt2.start();
    expect(report.loadedEntities).toBe(1);
    expect(rt2.entities.get('c1')?.status).toBe('active');
    clock2.advance(9001);
    expect(rt2.entities.get('c1')).toBeUndefined();
  });

  it('update mutates data and emits events', async () => {
    const { rt } = setup();
    rt.entities.define('profile', {});
    rt.entities.create('profile', 'p1', { data: { name: 'old' } });
    const seen: string[] = [];
    rt.events.on('entity.updated', (e) => seen.push((e.payload as { entityId: string }).entityId));
    rt.entities.update('p1', (d) => ({ ...(d as { name: string }), name: 'new' }));
    expect(rt.entities.get('p1')?.data).toEqual({ name: 'new' });
    expect(seen).toEqual(['p1']);
  });
});
