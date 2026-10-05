import { CronParseError } from '../errors.js';
import { tzParts, zonedToUtc, type ZonedParts } from '../util/tz.js';

interface CronField {
  any: boolean;
  values: number[];
}

export interface CronSchedule {
  seconds: CronField;
  minutes: CronField;
  hours: CronField;
  daysOfMonth: CronField;
  months: CronField;
  daysOfWeek: CronField;
  raw: string;
  secondsField: boolean;
}

const MONTH_NAMES = [
  'jan',
  'feb',
  'mar',
  'apr',
  'may',
  'jun',
  'jul',
  'aug',
  'sep',
  'oct',
  'nov',
  'dec',
];
const DOW_NAMES = ['sun', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat'];

const RANGES: Array<[number, number]> = [
  [0, 59], // second
  [0, 59], // minute
  [0, 23], // hour
  [1, 31], // day of month
  [1, 12], // month
  [0, 7], // day of week (7 == sunday)
];

function parseValue(token: string, index: number): number {
  const range = RANGES[index];
  let value: number;
  const lower = token.toLowerCase();

  if (index === 4) {
    const mi = MONTH_NAMES.findIndex((n) => n === lower.slice(0, 3));
    if (mi >= 0) return mi + 1;
  }

  if (index === 5) {
    const di = DOW_NAMES.findIndex((n) => n === lower.slice(0, 3));
    if (di >= 0) return di;
  }

  const n = Number(token);
  if (!Number.isInteger(n)) {
    throw new CronParseError('', `field ${index + 1}: "${token}" is not a valid value`);
  }

  value = n;
  if (index === 5 && value === 7) value = 0;
  if (value < range[0] || value > (index === 5 ? 7 : range[1])) {
    throw new CronParseError('', `field ${index + 1}: value ${token} out of range`);
  }

  return value;
}

function parseField(field: string, index: number): CronField {
  const range = RANGES[index];
  const allowed: number[] = [];
  const isAny = field === '*';
  const items = field.split(',');

  for (const item of items) {
    const stepMatch = /^(.+?)\/(\d+)$/.exec(item);
    const step = stepMatch ? Number(stepMatch[2]) : 1;
    if (step <= 0) throw new CronParseError('', `step must be positive in "${field}"`);
    let lo: number;
    let hi: number;
    const base = stepMatch ? stepMatch[1] : item;
    if (base === '*') {
      lo = range[0];
      hi = index === 5 ? 7 : range[1];
    } else {
      const rangeMatch = /^(\S+)-(\S+)$/.exec(base);
      if (rangeMatch) {
        lo = parseValue(rangeMatch[1], index);
        hi = parseValue(rangeMatch[2], index);
        if (lo > hi) throw new CronParseError('', `inverted range in "${field}"`);
      } else {
        lo = hi = parseValue(base, index);
      }
    }
    for (let v = lo; v <= hi; v += step) allowed.push(index === 5 && v === 7 ? 0 : v);
  }
  const unique = [...new Set(allowed)].sort((a, b) => a - b);
  return { any: isAny, values: unique };
}

export function parseCron(expression: string): CronSchedule {
  const fields = expression.trim().split(/\s+/);
  let secondsField = false;
  let secondField: string;
  let rest: string[];
  if (fields.length === 6) {
    secondsField = true;
    secondField = fields[0];
    rest = fields.slice(1);
  } else if (fields.length === 5) {
    secondField = '0';
    rest = fields;
  } else {
    throw new CronParseError(expression, 'expected 5 or 6 fields');
  }
  const schedule: CronSchedule = {
    raw: expression,
    secondsField,
    seconds: parseField(secondField, 0),
    minutes: parseField(rest[0], 1),
    hours: parseField(rest[1], 2),
    daysOfMonth: parseField(rest[2], 3),
    months: parseField(rest[3], 4),
    daysOfWeek: parseField(rest[4], 5),
  };
  return schedule;
}

function dayMatches(schedule: CronSchedule, p: ZonedParts): boolean {
  if (!schedule.months.values.includes(p.month)) return false;
  const domOk = schedule.daysOfMonth.any || schedule.daysOfMonth.values.includes(p.day);
  const dowOk = schedule.daysOfWeek.any || schedule.daysOfWeek.values.includes(p.dow);
  // Vixie-cron semantics: if both dom and dow are restricted, either may match.
  if (!schedule.daysOfMonth.any && !schedule.daysOfWeek.any) {
    return domOk || dowOk;
  }
  return domOk && dowOk;
}

function findTimeOfDay(
  schedule: CronSchedule,
  p: ZonedParts,
  base: { hour: number; minute: number; second: number },
  tz: string | undefined,
): number | null {
  for (const hour of schedule.hours.values) {
    if (hour < base.hour) continue;
    const minFloor = hour === base.hour ? base.minute : 0;

    for (const minute of schedule.minutes.values) {
      if (minute < minFloor) continue;
      const secFloor = hour === base.hour && minute === base.minute ? base.second : 0;

      for (const second of schedule.seconds.values) {
        if (second < secFloor) continue;
        return zonedToUtc(p.year, p.month, p.day, hour, minute, second, tz);
      }
    }
  }
  return null;
}

export function nextCronRun(
  scheduleOrExpr: CronSchedule | string,
  fromMs: number,
  tz?: string,
): number | null {
  const schedule = typeof scheduleOrExpr === 'string' ? parseCron(scheduleOrExpr) : scheduleOrExpr;
  const first = tzParts(fromMs, tz);
  // Noon anchors keep day advancement inside the target day across DST shifts.
  let anchor = zonedToUtc(first.year, first.month, first.day, 12, 0, 0, tz);

  for (let i = 0; i < 1830; i++) {
    const p = tzParts(anchor, tz);
    const sameDay = p.year === first.year && p.month === first.month && p.day === first.day;
    const base = sameDay
      ? { hour: first.hour, minute: first.minute, second: first.second }
      : { hour: 0, minute: 0, second: 0 };
    if (dayMatches(schedule, p)) {
      const hit = findTimeOfDay(schedule, p, base, tz);
      if (hit != null && hit > fromMs) return hit;
    }
    anchor += 86400000;
  }
  return null;
}
