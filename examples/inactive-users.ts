import { createRuntime, VirtualClock } from '../src/index.js';

// SaaS example: mark users inactive after 30 days without activity,
// then reach out. Virtual clock compresses a month into a millisecond.
const clock = new VirtualClock(Date.UTC(2025, 0, 1));
const rt = createRuntime({ clock, namespace: 'saas' });
await rt.start();

rt.entities.define('user', {
  inactiveAfter: '30d',
  hooks: {
    onInactive: (e) => {
      console.log(
        `[saas] ${e.id} inactive since ${new Date((e as { inactiveSince: number }).inactiveSince!).toISOString()}; sending win-back email`,
      );
      rt.entities.update(e.id, (d) => ({ ...(d as object), winBackSent: true }));
    },
  },
});

rt.entities.create('user', 'user:mia', { data: { plan: 'pro' } });
rt.entities.create('user', 'user:kai', { data: { plan: 'free' } });

rt.entities.touch('user:kai'); // Kai was active later than Mia
clock.advance(20 * 86400000);
rt.entities.touch('user:kai');
clock.advance(11 * 86400000); // 31 days total
console.log(
  'mia:',
  rt.entities.get('user:mia')?.status,
  'kai:',
  rt.entities.get('user:kai')?.status,
);
await rt.shutdown();
