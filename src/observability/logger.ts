export type LogLevel = 'trace' | 'debug' | 'info' | 'warn' | 'error';

const LEVELS: Record<LogLevel, number> = { trace: 10, debug: 20, info: 30, warn: 40, error: 50 };

export type LogBindings = Record<string, unknown>;

export interface Logger {
  trace(msg: string, meta?: LogBindings): void;
  debug(msg: string, meta?: LogBindings): void;
  info(msg: string, meta?: LogBindings): void;
  warn(msg: string, meta?: LogBindings): void;
  error(msg: string, meta?: LogBindings): void;
  child(bindings: LogBindings): Logger;
}

export interface ConsoleLoggerOptions {
  level?: LogLevel;
  write?(line: string): void;
  bindings?: LogBindings;
}

export function createConsoleLogger(opts: ConsoleLoggerOptions = {}): Logger {
  const level = LEVELS[opts.level ?? 'info'];
  const write = opts.write ?? ((line: string) => process.stdout.write(line + '\n'));
  const base: LogBindings = { ...(opts.bindings ?? {}) };

  function emit(lvl: LogLevel, msg: string, meta?: LogBindings): void {
    if (LEVELS[lvl] < level) return;
    const line = JSON.stringify({
      time: new Date().toISOString(),
      level: lvl,
      msg,
      ...base,
      ...meta,
    });
    write(line);
  }

  const logger: Logger = {
    trace: (m, meta) => emit('trace', m, meta),
    debug: (m, meta) => emit('debug', m, meta),
    info: (m, meta) => emit('info', m, meta),
    warn: (m, meta) => emit('warn', m, meta),
    error: (m, meta) => emit('error', m, meta),
    child(bindings) {
      return createConsoleLogger({
        level: Object.entries(LEVELS).find(([, v]) => v === level)?.[0] as LogLevel,
        write,
        bindings: { ...base, ...bindings },
      });
    },
  };
  return logger;
}

export function createNoopLogger(): Logger {
  const noop = () => {};
  const logger: Logger = {
    trace: noop,
    debug: noop,
    info: noop,
    warn: noop,
    error: noop,
    child: () => logger,
  };
  return logger;
}
