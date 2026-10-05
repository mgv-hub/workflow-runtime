import { createRuntime, VirtualClock, waitForEventStep } from '../src/index.js';

// Lifecycle marketing example: a 3-day email sequence that cancels when the
// user unsubscribes, using an event-wait step and virtual time.
const clock = new VirtualClock(0);
const rt = createRuntime({ clock, namespace: 'lifecycle' });
await rt.start();

rt.workflows.register({
  id: 'onboarding-sequence',
  startWhen: { event: 'user.created' },
  steps: [
    {
      id: 'day1',
      run: (ctx) => console.log(`[email] day1 -> ${(ctx.input as { email: string }).email}`),
    },
    waitForEventStep('user.unsubscribed', { id: 'watch-unsub', timeoutMs: 3 * 86400000 }),
    { id: 'day3', dependsOn: ['watch-unsub'], run: () => console.log('[email] day3 offer') },
  ],
});

const handle = rt.workflows.start('onboarding-sequence');
rt.events.emit('user.created', { email: 'mia@example.com' });
clock.advance(86400000);
console.log('[email] unsubscribed?', handle.record()?.status);
handle.cancel('user unsubscribed out of band');
clock.advance(4 * 86400000);
console.log('[email] final:', handle.status());
await rt.shutdown();
