import { createRuntime, VirtualClock } from '../src/index.js';

// Game server example: match entities with TTL, a respawn timer, and a
// pausable countdown workflow for round starts.
const clock = new VirtualClock(0);
const rt = createRuntime({ clock, namespace: 'game' });
await rt.start();

rt.entities.define('match', {
  ttl: '45m',
  onExpire: 'remove',
  hooks: { onExpire: (e) => console.log(`[game] match ${e.id} dissolved`) },
});
rt.entities.create('match', 'match:99', { data: { players: 12, map: 'dunes' } });

rt.workflows.register({
  id: 'round-start',
  steps: [
    {
      id: 'countdown',
      run: async (ctx) => {
        await ctx.wait(10000);
      },
    },
    { id: 'open-doors', dependsOn: ['countdown'], run: () => console.log('[game] doors open') },
  ],
});

const round = rt.workflows.start('round-start');
clock.advance(3000);
round.pause();
clock.advance(20000); // paused: doors must not open
console.log('[game] paused round status:', round.status(), round.isPaused());
round.resume();
clock.advance(7000);
const record = await round.promise;
console.log('[game] round:', record.status);
clock.advance(46 * 60000);
console.log('[game] match after ttl:', rt.entities.get('match:99'));
await rt.shutdown();
