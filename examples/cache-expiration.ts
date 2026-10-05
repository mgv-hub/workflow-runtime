import { createRuntime, VirtualClock } from '../src/index.js';

// Infrastructure example: a TTL cache that evicts on expiry and counts hits.
const clock = new VirtualClock(0);
const rt = createRuntime({ clock, namespace: 'cache' });
await rt.start();

rt.entities.define('cache-entry', {
  ttl: 5000,
  onExpire: 'remove',
  hooks: {
    onExpire: (e) => {
      rt.metrics.counter('wr.cache.evictions');
      console.log(`[cache] evicted ${e.id}`);
    },
  },
});

rt.entities.create('cache-entry', 'cache:user-profile-1', { data: { theme: 'dark' } });
rt.entities.create('cache-entry', 'cache:user-profile-2', { data: { theme: 'light' } });

rt.entities.touch('cache-entry:missing' as string); // no-op for unknown ids
clock.advance(4999);
console.log('[cache] still warm:', rt.entities.stats().active);
clock.advance(2);
console.log('[cache] after ttl:', rt.entities.stats());
console.log('[cache] evictions:', rt.metrics.snapshot().counters['wr.cache.evictions|'] ?? 0);
await rt.shutdown();
