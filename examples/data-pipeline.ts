import { createRuntime, VirtualClock } from '../src/index.js';

// Data example: parallel extraction, a join, and a failure-tolerant policy.
const clock = new VirtualClock(0);
const rt = createRuntime({ clock, namespace: 'etl' });
await rt.start();

rt.workflows.register({
  id: 'nightly-etl',
  steps: [
    { id: 'extract-crm', run: () => ['crm rows'] },
    { id: 'extract-billing', run: () => ['billing rows'] },
    {
      id: 'extract-logs',
      failure: 'continue',
      run: () => {
        throw new Error('log source down');
      },
    },
    {
      id: 'transform',
      dependsOn: ['extract-crm', 'extract-billing', 'extract-logs'],
      depsPolicy: 'settled',
      run: (ctx) => [
        ...(ctx.results['extract-crm'] as string[]),
        ...(ctx.results['extract-billing'] as string[]),
      ],
    },
    { id: 'load', dependsOn: ['transform'], run: () => 'warehouse updated' },
  ],
});

const record = await rt.workflows.start('nightly-etl').promise;
console.log(
  '[etl] status:',
  record.status,
  'partial:',
  record.partial,
  'load:',
  record.results.load,
);
console.log('[etl] health:', rt.health().activeExecutions);
await rt.shutdown();
