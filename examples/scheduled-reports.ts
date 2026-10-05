import { createRuntime } from '../src/index.js';

// Operations example: a daily report at 09:00 Berlin time, plus a preview
// of the next three runs without executing anything.
const rt = createRuntime({ namespace: 'ops' });
await rt.start();

const daily = rt.scheduler.daily(
  '09:00',
  (ctx) => console.log(`[ops] report fired at ${new Date(ctx.fireAt).toISOString()}`),
  { tz: 'Europe/Berlin', name: 'daily-report', tags: ['reporting'] },
);

console.log(
  '[ops] next 3 runs:',
  daily.nextRuns(3).map((t) => new Date(t).toISOString()),
);

rt.scheduler.cron('0 6 * * 1', () => console.log('[ops] Monday digest'), {
  tz: 'Europe/Berlin',
  name: 'weekly-digest',
});
await rt.shutdown();
