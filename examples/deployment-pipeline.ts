import { createRuntime, VirtualClock, waitForEventStep } from '../src/index.js';

// Delivery example: parallel test suites, an approval gate that times out,
// and a rollback compensating action.
const clock = new VirtualClock(0);
const rt = createRuntime({ clock, namespace: 'delivery' });
await rt.start();

let deployed = false;
rt.workflows.register({
  id: 'deploy',
  timeoutMs: 30 * 60000,
  steps: [
    { id: 'build', run: () => 'artifact-7' },
    { id: 'unit', dependsOn: ['build'], run: () => 'unit-ok' },
    { id: 'integration', dependsOn: ['build'], run: () => 'integ-ok' },
    waitForEventStep('deploy.approved', { id: 'approval', timeoutMs: 60000 }),
    {
      id: 'release',
      dependsOn: ['unit', 'integration', 'approval'],
      sideEffects: true,
      run: () => {
        deployed = true;
        return 'live';
      },
      compensate: () => {
        deployed = false;
        console.log('[delivery] rolled back');
      },
    },
  ],
});

const handle = rt.workflows.start('deploy', { input: { version: '1.2.3' } });
clock.advance(1000);
rt.events.emit('deploy.approved', { by: 'release-manager' });
clock.advance(1000);
const record = await handle.promise;
console.log('[delivery] status:', record.status, 'deployed:', deployed, 'results:', record.results);
await rt.shutdown();
