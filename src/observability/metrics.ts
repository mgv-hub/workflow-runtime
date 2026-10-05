export type MetricTags = Record<string, string | number>;

export interface MetricsRecorder {
  counter(name: string, by?: number, tags?: MetricTags): void;
  gauge(name: string, value: number, tags?: MetricTags): void;
  observe(name: string, value: number, tags?: MetricTags): void;
}

export interface ObservationSummary {
  count: number;
  sum: number;
  min: number;
  max: number;
}

export interface MetricsSnapshot {
  counters: Record<string, number>;
  gauges: Record<string, number>;
  observations: Record<string, ObservationSummary>;
}

function tagKey(tags?: MetricTags): string {
  if (!tags) return '';
  const parts = Object.keys(tags)
    .sort()
    .map((k) => `${k}=${String(tags[k])}`);
  return parts.length ? `|${parts.join(',')}` : '';
}

export function createMetrics(): MetricsRecorder & { snapshot(): MetricsSnapshot } {
  const counters = new Map<string, number>();
  const gauges = new Map<string, number>();
  const observations = new Map<string, ObservationSummary>();

  const recorder: MetricsRecorder & { snapshot(): MetricsSnapshot } = {
    counter(name, by = 1, tags) {
      const k = name + tagKey(tags);
      counters.set(k, (counters.get(k) ?? 0) + by);
    },
    gauge(name, value, tags) {
      gauges.set(name + tagKey(tags), value);
    },
    observe(name, value, tags) {
      const k = name + tagKey(tags);
      const cur = observations.get(k);
      if (!cur) {
        observations.set(k, { count: 1, sum: value, min: value, max: value });
      } else {
        cur.count += 1;
        cur.sum += value;
        cur.min = Math.min(cur.min, value);
        cur.max = Math.max(cur.max, value);
      }
    },
    snapshot() {
      return {
        counters: Object.fromEntries(counters),
        gauges: Object.fromEntries(gauges),
        observations: Object.fromEntries(observations),
      };
    },
  };
  return recorder;
}
