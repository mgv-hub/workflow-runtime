import { ConfigurationError } from '../errors.js';

const DURATION_RE = /(\d+(?:\.\d+)?)(ms|s|m|h|d)/g;
const UNIT_MS: Record<string, number> = { ms: 1, s: 1000, m: 60000, h: 3600000, d: 86400000 };

export function parseDuration(input: number | string): number {
  if (typeof input === 'number') {
    if (!Number.isFinite(input) || input < 0) {
      throw new ConfigurationError(`Invalid duration: ${input}`);
    }
    return input;
  }
  let total = 0;
  let matched = false;
  DURATION_RE.lastIndex = 0;
  let m: RegExpExecArray | null;
  while ((m = DURATION_RE.exec(input)) !== null) {
    matched = true;
    total += Number(m[1]) * UNIT_MS[m[2]];
  }
  if (!matched || DURATION_RE.test(input.replace(DURATION_RE, ''))) {
    throw new ConfigurationError(`Invalid duration string: "${input}"`);
  }
  return total;
}

export function toTime(value: number | Date | string): number {
  if (value instanceof Date) {
    if (Number.isNaN(value.getTime())) throw new ConfigurationError('Invalid Date');
    return value.getTime();
  }
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) throw new ConfigurationError(`Invalid timestamp: ${value}`);
    return value;
  }
  if (typeof value === 'string') {
    const parsed = Date.parse(value);
    if (Number.isNaN(parsed)) throw new ConfigurationError(`Invalid time string: "${value}"`);
    return parsed;
  }
  throw new ConfigurationError(`Unsupported time value: ${String(value)}`);
}

export function parseTimeOfDay(text: string): { hour: number; minute: number; second: number } {
  const m = /^(\d{1,2}):(\d{2})(?::(\d{2}))?$/.exec(text.trim());
  if (!m) throw new ConfigurationError(`Invalid time of day: "${text}" (expected HH:MM[:SS])`);
  const hour = Number(m[1]);
  const minute = Number(m[2]);
  const second = m[3] ? Number(m[3]) : 0;
  if (hour > 23 || minute > 59 || second > 59) {
    throw new ConfigurationError(`Invalid time of day: "${text}"`);
  }
  return { hour, minute, second };
}

export function isoNow(ms: number): string {
  return new Date(ms).toISOString();
}
