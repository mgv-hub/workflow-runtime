import { mkdir, readdir, readFile, unlink, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { ConfigurationError } from '../errors.js';
import type { KeyValueStore, StorageEntry } from './store.js';

function encode(key: string): string {
  return Buffer.from(key, 'utf8').toString('base64url');
}

function decode(file: string): string | null {
  try {
    return Buffer.from(file.replace(/\.json$/, ''), 'base64url').toString('utf8');
  } catch {
    return null;
  }
}

// Filesystem adapter: one JSON file per key under baseDir. Suitable for single
// process persistence; not a multi-process database.
export class FileSystemStore implements KeyValueStore {
  private dir: string;

  constructor(baseDir: string) {
    if (!baseDir) throw new ConfigurationError('FileSystemStore requires a base directory');
    this.dir = baseDir;
  }

  private path(key: string): string {
    return join(this.dir, `${encode(key)}.json`);
  }

  async get<T = unknown>(key: string): Promise<T | undefined> {
    try {
      const raw = await readFile(this.path(key), 'utf8');
      return JSON.parse(raw) as T;
    } catch {
      return undefined;
    }
  }

  async put(key: string, value: unknown): Promise<void> {
    await mkdir(this.dir, { recursive: true });
    await writeFile(this.path(key), JSON.stringify(value), 'utf8');
  }

  async delete(key: string): Promise<boolean> {
    try {
      await unlink(this.path(key));
      return true;
    } catch {
      return false;
    }
  }

  async list(prefix: string): Promise<StorageEntry[]> {
    let files: string[];
    try {
      files = await readdir(this.dir);
    } catch {
      return [];
    }

    // Base64 is not prefix-monotonic, so decode every file and filter in memory.
    const keys: string[] = [];
    for (const f of files) {
      const key = decode(f);
      if (key && key.startsWith(prefix)) keys.push(key);
    }

    const out: StorageEntry[] = [];
    for (const key of keys) {
      const value = await this.get(key);
      if (value !== undefined) out.push({ key, value });
    }
    return out;
  }
}
