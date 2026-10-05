import { describe, expect, it } from 'vitest';
import { CronParseError, nextCronRun, parseCron } from '../src/index.js';

describe('cron parsing', () => {
  it('rejects malformed expressions', () => {
    expect(() => parseCron('61 5 * * *')).toThrow(CronParseError);
    expect(() => parseCron('* * *')).toThrow(CronParseError);
    expect(() => parseCron('0 25 * * *')).toThrow(CronParseError);
  });

  it('supports steps, lists, ranges, names, and 7 as sunday', () => {
    const s = parseCron('*/15 9-11 * jan-mar mon,7');
    expect(s.minutes.values).toEqual([0, 15, 30, 45]);
    expect(s.hours.values).toEqual([9, 10, 11]);
    expect(s.months.values).toEqual([1, 2, 3]);
    expect(s.daysOfWeek.values).toEqual([0, 1]);
    expect(s.daysOfWeek.any).toBe(false);
    const six = parseCron('30 * * * * *');
    expect(six.secondsField).toBe(true);
    expect(six.seconds.values).toEqual([30]);
  });
});

describe('nextCronRun', () => {
  it('finds the next occurrence in UTC', () => {
    const from = Date.UTC(2024, 2, 10, 9, 15, 0);
    expect(nextCronRun('30 9 * * *', from)).toBe(Date.UTC(2024, 2, 10, 9, 30, 0));
    const fromLater = Date.UTC(2024, 2, 10, 9, 45, 0);
    expect(nextCronRun('30 9 * * *', fromLater)).toBe(Date.UTC(2024, 2, 11, 9, 30, 0));
  });

  it('respects IANA timezones', () => {
    const from = Date.UTC(2024, 5, 1, 10, 0, 0);
    const next = nextCronRun('0 9 * * *', from, 'Europe/Berlin');
    expect(next).toBe(Date.UTC(2024, 5, 2, 7, 0, 0));
  });

  it('applies Vixie dom/dow OR semantics', () => {
    const from = Date.UTC(2024, 5, 1, 12, 0, 0);
    const next = nextCronRun('0 0 1 * 1', from);
    expect(next).toBe(Date.UTC(2024, 5, 3, 0, 0, 0));
  });

  it('handles a seconds field', () => {
    const from = Date.UTC(2024, 0, 1, 10, 0, 10);
    expect(nextCronRun('30 * * * * *', from)).toBe(Date.UTC(2024, 0, 1, 10, 0, 30));
  });

  it('returns null after the search horizon', () => {
    expect(nextCronRun('0 0 30 2 *', Date.UTC(2024, 0, 1))).toBeNull();
  });
});
