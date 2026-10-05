import { mkdirSync, writeFileSync } from 'node:fs';

mkdirSync('dist/cjs', { recursive: true });
writeFileSync('dist/cjs/package.json', JSON.stringify({ type: 'commonjs' }, null, 2) + '\n');
