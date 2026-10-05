import { randomUUID } from 'node:crypto';
import { ConfigurationError } from './errors.js';

let seq = 0;

export function nextSeq(): number {
  seq += 1;
  return seq;
}

export function newId(): string {
  return randomUUID();
}

export type IdFactory = () => string;

export function makeIdFactory(custom?: IdFactory): IdFactory {
  if (!custom) return newId;
  if (typeof custom !== 'function') {
    throw new ConfigurationError('idFactory must be a function returning a string id');
  }
  return custom;
}
