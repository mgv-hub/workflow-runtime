import { createRuntime, VirtualClock } from '../src/index.js';

// Commerce example: charge with compensation (refund) on later failure.
const clock = new VirtualClock(0);
const rt = createRuntime({ clock, namespace: 'shop' });
await rt.start();

let charged = false;
rt.workflows.register({
  id: 'checkout',
  steps: [
    { id: 'validate', run: (ctx) => ({ ok: (ctx.input as { amount: number }).amount > 0 }) },
    {
      id: 'charge',
      dependsOn: ['validate'],
      sideEffects: true,
      retries: { max: 2, delay: 100 },
      run: () => {
        charged = true;
        return 'ch_123';
      },
      compensate: () => {
        charged = false;
        console.log('[shop] refunded charge');
      },
    },
    {
      id: 'fulfill',
      dependsOn: ['charge'],
      run: () => {
        throw new Error('warehouse offline');
      },
    },
  ],
});

const record = await rt.workflows.start('checkout', { input: { amount: 4200 } }).promise;
console.log(
  '[shop] status:',
  record.status,
  'charged:',
  charged,
  'compensations:',
  record.compensations,
);
await rt.shutdown();
