import { createRuntime } from '../src/index.js';

// Web app example: sessions with sliding 30-minute expiry.
const rt = createRuntime({ namespace: 'web' });
await rt.start();

rt.entities.define('session', {
  ttl: '30m',
  ttlMode: 'sliding',
  onExpire: 'remove',
  hooks: { onExpire: (e) => console.log(`[web] session ${e.id} expired; terminating`) },
});

rt.entities.create('session', 'sess:abc', { data: { userId: 'u1', cart: [] } });

setInterval(() => {
  rt.entities.touch('sess:abc'); // every request keeps the session alive
}, 1000).unref();

setTimeout(() => {
  console.log('session status:', rt.entities.get('sess:abc')?.status);
  clearInterval(1000 as unknown as NodeJS.Timeout);
  void rt.shutdown();
}, 3000);
