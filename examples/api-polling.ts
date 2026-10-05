import { createCooldown, createCircuitBreaker, createRuntime, VirtualClock } from '../src/index.js';

const clock = new VirtualClock(0);
const rt = createRuntime({ clock, namespace: 'poller' });
await rt.start();

const cooldown = createCooldown(1000, clock);
const breaker = createCircuitBreaker({ failureThreshold: 3, resetAfterMs: 5000 }, clock);

let calls = 0;
let failing = true;

rt.scheduler.every(200, () => {
  if (!cooldown.ready()) return;
  if (!breaker.tryAcquire()) {
    console.log('[poller] circuit open, skipping');
    return;
  }
  cooldown.trigger();
  calls += 1;
  try {
    if (failing) throw new Error('upstream 503');
    breaker.recordSuccess();
    console.log('[poller] ok');
  } catch {
    breaker.recordFailure();
    console.log('[poller] failed, breaker:', breaker.state());
  }
});

clock.advance(3000);
failing = false;
clock.advance(6000);

console.log('[poller] total calls:', calls, 'breaker:', breaker.state());
await rt.shutdown();
