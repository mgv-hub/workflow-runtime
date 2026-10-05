export interface StorageEntry {
  key: string;
  value: unknown;
}

export interface KeyValueStore {
  get<T = unknown>(key: string): Promise<T | undefined>;
  put(key: string, value: unknown): Promise<void>;
  delete(key: string): Promise<boolean>;
  list(prefix: string): Promise<StorageEntry[]>;
  close?(): Promise<void> | void;
}

export class InMemoryStore implements KeyValueStore {
  private map = new Map<string, unknown>();

  async get<T = unknown>(key: string): Promise<T | undefined> {
    return this.map.get(key) as T | undefined;
  }

  async put(key: string, value: unknown): Promise<void> {
    this.map.set(key, value);
  }

  async delete(key: string): Promise<boolean> {
    return this.map.delete(key);
  }

  async list(prefix: string): Promise<StorageEntry[]> {
    const out: StorageEntry[] = [];
    for (const [key, value] of this.map) {
      if (key.startsWith(prefix)) out.push({ key, value });
    }
    return out;
  }
}

export function storageKey(section: string, namespace: string, rest: string): string {
  return `${section}:${namespace}:${rest}`;
}

export function storagePrefix(section: string, namespace: string): string {
  return `${section}:${namespace}:`;
}
