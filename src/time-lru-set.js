//
// Copyright 2019 DxOS.
//

class ItemCache {
  constructor ({ value, maxAge, onTimeout }) {
    this._value = value;
    this._maxAge = maxAge;
    this._onTimeout = value => onTimeout(value);
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

class TimeLRUSet {
  constructor (options = {}) {
    const { maxAge = 10 * 1000, maxSize = 100 } = options;

    this._maxAge = maxAge;
    this._maxSize = maxSize;

    this._list = [];
    this._map = new Map();
  }

  add (value) {
    if (this._list.length === this._maxSize) {
      this._deleteLRU();
    }

    if (this.has(value)) {
      return;
    }

    const item = new ItemCache({
      value,
      maxAge: this._maxAge,
      onTimeout: this._onTimeout.bind(this)
    });

    this._map.set(value, item);

    item.update();
    this._updateLRU(value);
  }

  delete (value) {
    const item = this._map.get(value);
    item.clear();

    this._map.delete(value);

    const idx = this._list.indexOf(value);
    if (idx !== -1) {
      this._list.splice(idx, 1);
    }
  }

  has (value) {
    const item = this._map.get(value);

    if (item) {
      item.update();
      this._updateLRU(value);
    }

    return !!item;
  }

  clear () {
    process.nextTick(() => {
      this._list = [];
      this._map.forEach((value, key) => {
        value.clear();
        this._map.delete(key);
      });
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
    const value = this._list.shift();
    if (value) {
      this.delete(value);
    }
  }

  _onTimeout (value) {
    this.delete(value);
  }
}

export default TimeLRUSet;
