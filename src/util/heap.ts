export interface Sequenced {
  seq: number;
}

export class MinHeap<T extends Sequenced> {
  private items: T[] = [];

  constructor(private compare: (a: T, b: T) => number) {}

  get size(): number {
    return this.items.length;
  }

  peek(): T | undefined {
    return this.items[0];
  }

  push(item: T): void {
    this.items.push(item);
    let i = this.items.length - 1;
    while (i > 0) {
      const p = (i - 1) >> 1;
      if (this.compare(this.items[i], this.items[p]) < 0) {
        [this.items[i], this.items[p]] = [this.items[p], this.items[i]];
        i = p;
      } else {
        break;
      }
    }
  }

  pop(): T | undefined {
    const n = this.items.length;
    if (n === 0) return undefined;
    const top = this.items[0];
    if (n === 1) {
      this.items.pop();
      return top;
    }
    this.items[0] = this.items[n - 1];
    this.items.pop();
    let i = 0;
    for (;;) {
      const l = 2 * i + 1;
      const r = l + 1;
      let s = i;
      if (l < this.items.length && this.compare(this.items[l], this.items[s]) < 0) s = l;
      if (r < this.items.length && this.compare(this.items[r], this.items[s]) < 0) s = r;
      if (s === i) break;
      [this.items[i], this.items[s]] = [this.items[s], this.items[i]];
      i = s;
    }
    return top;
  }

  clear(): void {
    this.items.length = 0;
  }
}

// Orders by key ascending, breaking ties by insertion sequence for FIFO determinism.
export function byKey<T extends Sequenced>(key: (t: T) => number): (a: T, b: T) => number {
  return (a, b) => key(a) - key(b) || a.seq - b.seq;
}
