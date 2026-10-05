import type { Clock } from '../clock.js';

export interface HistoryEntry {
  t: number;
  type: string;
  [key: string]: unknown;
}

export interface HistoryConfig {
  maxStreams?: number;
  maxEntriesPerStream?: number;
}

// Bounded per-stream ring buffers with LRU stream eviction. History never grows
// without limit; the caps are enforced on every add.
export class HistoryManager {
  private streams = new Map<string, HistoryEntry[]>();
  private maxStreams: number;
  private maxEntries: number;

  constructor(
    private clock: Clock,
    config: HistoryConfig = {},
  ) {
    this.maxStreams = config.maxStreams ?? 2000;
    this.maxEntries = config.maxEntriesPerStream ?? 200;
  }

  add(streamId: string, entry: Omit<HistoryEntry, 't'>): void {
    let entries = this.streams.get(streamId);
    if (!entries) {
      entries = [];
      if (this.streams.size >= this.maxStreams) {
        const oldest = this.streams.keys().next().value;
        if (oldest !== undefined) this.streams.delete(oldest);
      }
      this.streams.set(streamId, entries);
    } else {
      this.streams.delete(streamId);
      this.streams.set(streamId, entries);
    }
    entries.push({ t: this.clock.now(), ...entry } as HistoryEntry);
    if (entries.length > this.maxEntries) {
      entries.splice(0, entries.length - this.maxEntries);
    }
  }

  for(streamId: string): HistoryEntry[] {
    return (this.streams.get(streamId) ?? []).slice();
  }

  streamIds(): string[] {
    return [...this.streams.keys()];
  }

  clear(): void {
    this.streams.clear();
  }
}
