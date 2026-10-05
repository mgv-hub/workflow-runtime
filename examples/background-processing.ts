import { createRuntime } from '../src/index.js';

// Worker example: prioritized background processing with dedupe and a DLQ.
const rt = createRuntime({ namespace: 'worker' });
await rt.start();

rt.tasks.group('media', { concurrency: 3, rateLimit: { max: 5, windowMs: 1000 } });

for (let i = 0; i < 6; i++) {
  rt.tasks.enqueue({
    name: 'transcode',
    group: 'media',
    priority: i === 0 ? 10 : 0,
    dedupeKey: `job-${i % 3}`, // 6 requests collapse to 3 distinct jobs
    retries: { max: 1, delay: 200 },
    run: async (ctx) => {
      console.log(`[worker] ${ctx.name} #${ctx.taskId.slice(0, 8)} attempt ${ctx.attempt}`);
      if (ctx.attempt === 1 && ctx.meta.failOnce) throw new Error('transient');
      return 'done';
    },
    meta: { failOnce: i === 2 },
  });
}

setTimeout(async () => {
  console.log('[worker] stats:', rt.tasks.stats());
  await rt.shutdown();
}, 500);
