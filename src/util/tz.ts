import { ConfigurationError } from '../errors.js';

export interface ZonedParts {
  year: number;
  month: number;
  day: number;
  hour: number;
  minute: number;
  second: number;
  dow: number;
}

const formatters = new Map<string, Intl.DateTimeFormat>();

function formatterFor(tz: string): Intl.DateTimeFormat {
  let fmt = formatters.get(tz);
  if (!fmt) {
    fmt = new Intl.DateTimeFormat('en-US', {
      timeZone: tz,
      hourCycle: 'h23',
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
    });
    formatters.set(tz, fmt);
  }
  return fmt;
}

export function assertTimezone(tz: string): void {
  try {
    formatterFor(tz).format(0);
  } catch {
    throw new ConfigurationError(`Unknown IANA timezone: "${tz}"`);
  }
}

export function tzParts(ms: number, tz?: string): ZonedParts {
  if (!tz) {
    const d = new Date(ms);
    return {
      year: d.getUTCFullYear(),
      month: d.getUTCMonth() + 1,
      day: d.getUTCDate(),
      hour: d.getUTCHours(),
      minute: d.getUTCMinutes(),
      second: d.getUTCSeconds(),
      dow: d.getUTCDay(),
    };
  }
  const parts = formatterFor(tz).formatToParts(ms);
  const map: Record<string, number> = {};
  for (const p of parts) {
    if (p.type !== 'literal') map[p.type] = Number(p.value);
  }
  const year = map.year;
  const month = map.month;
  const day = map.day;
  return {
    year,
    month,
    day,
    hour: map.hour,
    minute: map.minute,
    second: map.second,
    // Calendar weekday is timezone-independent given the local date.
    dow: new Date(Date.UTC(year, month - 1, day)).getUTCDay(),
  };
}

export function tzOffsetMs(ms: number, tz: string): number {
  const secFloor = Math.floor(ms / 1000) * 1000;
  const p = tzParts(secFloor, tz);
  const asUtc = Date.UTC(p.year, p.month - 1, p.day, p.hour, p.minute, p.second);
  return asUtc - secFloor;
}

// Two-pass offset resolution; handles ordinary DST transitions. Ambiguous or
// nonexistent local times resolve to one valid instant (documented limitation
// of a dependency-free implementation).
export function zonedToUtc(
  year: number,
  month: number,
  day: number,
  hour: number,
  minute: number,
  second: number,
  tz?: string,
): number {
  const guess = Date.UTC(year, month - 1, day, hour, minute, second);
  if (!tz) return guess;
  const off1 = tzOffsetMs(guess, tz);
  const utc1 = guess - off1;
  const off2 = tzOffsetMs(utc1, tz);
  if (off2 === off1) return utc1;
  const utc2 = guess - off2;
  const off3 = tzOffsetMs(utc2, tz);
  return off3 === off2 ? utc2 : guess - off3;
}
