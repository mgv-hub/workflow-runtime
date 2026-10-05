import { ConfigurationError } from '../errors.js';
import { parseTimeOfDay } from '../util/time.js';
import { tzParts, zonedToUtc } from '../util/tz.js';

export type CalendarSpec =
  | { kind: 'daily'; time: string; tz?: string }
  | { kind: 'weekly'; days: number[]; time: string; tz?: string }
  | { kind: 'monthly'; day: number; time: string; tz?: string };

export function normalizeCalendar(spec: CalendarSpec): CalendarSpec {
  const time = parseTimeOfDay(spec.time);
  void time;
  if (spec.kind === 'weekly') {
    const days = [...new Set(spec.days.map((d) => Math.abs(d) % 7))].sort((a, b) => a - b);
    if (days.length === 0) throw new ConfigurationError('weekly schedule needs at least one day');
    return { ...spec, days };
  }
  if (spec.kind === 'monthly') {
    if (spec.day < 1 || spec.day > 31) {
      throw new ConfigurationError('monthly day must be between 1 and 31');
    }
    return { ...spec };
  }
  return { ...spec };
}

export function nextCalendarRun(spec: CalendarSpec, fromMs: number): number | null {
  const tz = spec.tz;
  const tod = parseTimeOfDay(spec.time);
  const first = tzParts(fromMs, tz);
  let anchor = zonedToUtc(first.year, first.month, first.day, 12, 0, 0, tz);

  for (let i = 0; i < 400; i++) {
    const p = tzParts(anchor, tz);
    const sameDay = p.year === first.year && p.month === first.month && p.day === first.day;
    const baseHour = sameDay ? first.hour : -1;

    let dayOk = false;
    if (spec.kind === 'daily') dayOk = true;
    else if (spec.kind === 'weekly') dayOk = spec.days.includes(p.dow);
    else dayOk = p.day === spec.day;

    if (dayOk) {
      const candidateTime = { hour: tod.hour, minute: tod.minute, second: tod.second };
      const laterThanBase =
        candidateTime.hour > baseHour ||
        (candidateTime.hour === baseHour && candidateTime.minute > first.minute) ||
        (candidateTime.hour === baseHour &&
          candidateTime.minute === first.minute &&
          candidateTime.second > first.second) ||
        baseHour < 0 ||
        (candidateTime.hour === baseHour &&
          candidateTime.minute === first.minute &&
          candidateTime.second > first.second);
      const strictlyAfter = sameDay
        ? candidateTime.hour > first.hour ||
          (candidateTime.hour === first.hour && candidateTime.minute > first.minute) ||
          (candidateTime.hour === first.hour &&
            candidateTime.minute === first.minute &&
            candidateTime.second > first.second)
        : true;

      void laterThanBase;

      if (strictlyAfter) {
        const hit = zonedToUtc(p.year, p.month, p.day, tod.hour, tod.minute, tod.second, tz);
        if (hit > fromMs) return hit;
      }

      if (!sameDay) {
        const hit = zonedToUtc(p.year, p.month, p.day, tod.hour, tod.minute, tod.second, tz);
        if (hit > fromMs) return hit;
      }
    }
    anchor += 86400000;
  }
  return null;
}
