import { createRuntime, VirtualClock, waitForEventStep } from '../src/index.js';

// Process example: an approval request that escalates when nobody acts,
// combining an event wait with an entity inactivity timer.
const clock = new VirtualClock(0);
const rt = createRuntime({ clock, namespace: 'approvals' });
await rt.start();

rt.entities.define('request', {
  inactiveAfter: '7d',
  hooks: { onInactive: (e) => console.log(`[approval] ${e.id} escalated to management`) },
});
rt.entities.create('request', 'req:expense-17', { data: { amount: 1200 } });

rt.workflows.register({
  id: 'expense-approval',
  steps: [
    waitForEventStep('approval.granted', { id: 'wait-grant', timeoutMs: 14 * 86400000 }),
    { id: 'payout', dependsOn: ['wait-grant'], run: () => 'paid' },
  ],
});

const handle = rt.workflows.start('expense-approval');
clock.advance(7 * 86400000); // inactivity first
console.log('[approval] request:', rt.entities.get('req:expense-17')?.status);
clock.advance(8 * 86400000); // then the wait times out
console.log('[approval] workflow:', handle.status());
await rt.shutdown();
