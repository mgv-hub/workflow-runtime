import { createRuntime } from '../src/index.js';

// Maintenance example: recurring cleanup through a serial task group,
// with retries and dead-letter inspection.
const rt = createRuntime({ namespace: 'ops' });
await rt.start();

rt.tasks.group('cleanup', { concurrency: 1, persist: false });

rt.scheduler.every('5m', () => {
  rt.tasks.enqueue({
    name: 'purge-temp-files',
    group: 'cleanup',
    retries: { max: 2, delay: 5000 },
    tags: ['maintenance'],
    run: async (ctx) => {
      console.log(`[cleanup] attempt ${ctx.attempt} running`);
      // imagine fs.rm calls here
      return 'purged';
    },
  });
});

setTimeout(async () => {
  console.log('[cleanup] dead letters:', rt.tasks.deadLetter().size());
  await rt.shutdown();
}, 1000);
