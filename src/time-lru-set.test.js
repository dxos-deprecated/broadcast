//
// Copyright 2019 DxOS.
//

import { TimeLRUSet } from './time-lru-set';

const delay = ms => new Promise(resolve => setTimeout(resolve, ms));

describe('time lru set', () => {
  const timeSet = new TimeLRUSet({
    maxAge: 2 * 1000,
    maxSize: 2
  });

  test('default timeSet', () => {
    const timeSet = new TimeLRUSet();
    expect(timeSet._maxAge).toBe(10 * 1000);
    expect(timeSet._maxSize).toBe(100);
  });

  test('time live', async () => {
    timeSet.add('foo');
    timeSet.add('bar');

    await delay(1.5 * 1000);

    timeSet.add('foo');

    await delay(1 * 1000);

    expect(timeSet.size).toBe(1);
    expect(Array.from(timeSet.values())).toEqual(['foo']);
    expect(timeSet.has('foo')).toBe(true);
    expect(timeSet.has('bar')).toBe(false);
  });

  test('delete and clear', () => {
    expect(timeSet.delete('foo')).toBe(true);
    expect(timeSet.delete('bar')).toBe(false);

    expect(timeSet.size).toBe(0);

    timeSet.add('foo');
    timeSet.add('bar');

    expect(timeSet.size).toBe(2);

    timeSet.clear();

    expect(timeSet.size).toBe(0);
  });

  test('cache size', () => {
    timeSet.add('foo');
    timeSet.add('bar');
    expect(timeSet.size).toBe(2);

    timeSet.add('baz');
    expect(timeSet.size).toBe(2);
    expect(timeSet.has('bar')).toBe(true);
    expect(timeSet.has('baz')).toBe(true);

    timeSet.clear();
  });

  test('check lru order', () => {
    timeSet.add('foo');
    timeSet.add('bar');

    // Update the LRU
    timeSet.has('foo');
    timeSet.add('baz');

    expect(timeSet.size).toBe(2);
    expect(timeSet.has('foo')).toBe(true);
    expect(timeSet.has('baz')).toBe(true);

    // Update the LRU
    timeSet.add('foo');
    timeSet.add('bar');

    expect(timeSet.size).toBe(2);
    expect(timeSet.has('foo')).toBe(true);
    expect(timeSet.has('bar')).toBe(true);

    timeSet.clear();
  });
});
