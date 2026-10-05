import { createRuntime, VirtualClock } from '../src/index.js';

// Discord bot example: temporary voice-channel state with sliding TTL.
// Any message in the channel extends the lifetime; silence cleans it up.
const clock = new VirtualClock(Date.now());
const rt = createRuntime({ clock, namespace: 'discord-bot' });
await rt.start();

rt.entities.define('voice-temp', {
  ttl: '5m',
  ttlMode: 'sliding',
  onExpire: 'remove',
  hooks: {
    onExpire: (e) => console.log(`[discord] cleaning temp state for ${e.id}`),
  },
});

rt.entities.create('voice-temp', 'voice:1234', { data: { channelId: '1234', topic: 'Among us' } });

function onMessage(): void {
  rt.entities.touch('voice:1234');
}

onMessage();
onMessage();
console.log('expiresAt:', rt.entities.get('voice:1234')?.expiresAt);
clock.advance(4 * 60000);
onMessage();
clock.advance(4 * 60000);
console.log('still active after 8m with mid activity:', rt.entities.get('voice:1234')?.status);
clock.advance(5 * 60000);
console.log('after silence:', rt.entities.get('voice:1234')); // undefined: removed
await rt.shutdown();
