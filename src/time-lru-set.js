//
// Copyright 2019 DxOS.
//

class CachedItem {
  constructor ({ value, maxAge, onTimeout }) {
    this._value = value;
    this._maxAge = maxAge;
    this._onTimeout = onTimeout;
  }

  update () {
    this.clear();

    this._timer = setTimeout(() => {
      this._onTimeout(this._value);
    }, this._maxAge);
  }

  clear () {
    if (this._timer) {
      clearTimeout(this._timer);
      this._timer = null;
    }
  }
}

/**
 * Least Recently Used cache "set" structure with time live support.
 */
class TimeLRUSet {
  /**
   * @param {Object} options
   * @param {number} [options.maxAge=10000] Maximum live time for a value not updated recently.
   * @param {number} [options.maxSize=100] Max size of the cache.
   */
  constructor (options = {}) {
    const { maxAge = 10 * 1000, maxSize = 100 } = options;

    this._maxAge = maxAge;
    this._maxSize = maxSize;

    this._list = [];
    this._itemsByValue = new Map();
  }

  /**
   * @type {number}
   */
  get size () {
    return this._itemsByValue.size;
  }

  /**
   * Method returns a new Iterator object that contains the values
   * for each element in the Set object in insertion order.
   *
   * @returns {Iterator<*>}
   */
  values () {
    return this._itemsByValue.keys();
  }

  /**
   * Add a value.
   *
   * @param {*} value Value to store.
   * @returns {TimeLRUSet}
   */
  add (value) {
    if (this.has(value)) {
      return this;
    }

    if (this._list.length === this._maxSize) {
      this._deleteLRU();
    }

    const item = new CachedItem({
      value,
      maxAge: this._maxAge,
      onTimeout: value => this._onTimeout(value)
    });

    this._itemsByValue.set(value, item);

    item.update();
    this._updateLRU(value);
    return this;
  }

  /**
   * Delete a value.
   *
   * @param {*} value
   * @returns {boolean}
   */
  delete (value) {
    const item = this._itemsByValue.get(value);

    if (!item) {
      return false;
    }

    item.clear();
    this._itemsByValue.delete(value);

    const idx = this._list.indexOf(value);
    if (idx !== -1) {
      this._list.splice(idx, 1);
    }

    return true;
  }

  /**
   * Check the existence of a value in set.
   *
   * @param {*} value
   * @returns {boolean}
   */
  has (value) {
    const item = this._itemsByValue.get(value);

    if (item) {
      item.update();
      this._updateLRU(value);
    }

    return !!item;
  }

  /**
   * Clear all the values.
   *
   * @returns {undefined}
   */
  clear () {
    this._list = [];
    this._itemsByValue.forEach((value, key) => {
      value.clear();
      this._itemsByValue.delete(key);
    });
  }

  _updateLRU (value) {
    const idx = this._list.indexOf(value);
    if (idx !== -1) {
      this._list.splice(idx, 1);
    }
    this._list.push(value);
  }

  _deleteLRU () {
    this.delete(this._list.shift());
  }

  _onTimeout (value) {
    this.delete(value);
  }
}

export default TimeLRUSet;
