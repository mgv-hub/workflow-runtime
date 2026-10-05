import { createRuntime, VirtualClock } from '../src/index.js';

// Product example: delayed one-shot notifications plus a weekly digest.
const clock = new VirtualClock(Date.UTC(2025, 0, 6)); // Monday
const rt = createRuntime({ clock, namespace: 'notify' });
await rt.start();

rt.scheduler.delay('30s', () => console.log('[notify] trial ending in 3 days'), {
  id: 'trial-warn',
});

rt.scheduler.weekly(
  [1],
  '09:30',
  (ctx) => {
    console.log(`[notify] weekly digest at ${new Date(ctx.fireAt).toISOString()}`);
  },
  { name: 'digest' },
);

clock.advance(31000);
console.log('[notify] preview digest:', rt.scheduler.get('digest') ? 'scheduled' : 'missing');
clock.advance(6 * 86400000);
await rt.shutdown();
