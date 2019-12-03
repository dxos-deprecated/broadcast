//
// Copyright 2019 DxOS.
//

import assert from 'assert';
import { EventEmitter } from 'events';
import crypto from 'crypto';
import Codec from '@dxos/codec-protobuf';
import debug from 'debug';

// eslint-disable-next-line
import schema from './schema.json';
import TimeLRUSet from './time-lru-set';

debug.formatters.h = v => v.toString('hex').slice(0, 6);
const log = debug('broadcast');

const msgId = (seqno, from) => {
  assert(Buffer.isBuffer(seqno));
  assert(Buffer.isBuffer(from));
  return `${seqno.toString('hex')}:${from.toString('hex')}`;
};

/**
 * @typedef {Object} Middleware
 * @property {Function} lookup Async peer lookup.
 * @property {Function} send Defines how to send the packet builded by the broadcast.
 * @property {Function} subscribe Defines how to subscribe to incoming packets.
 */

/**
 * @typedef {Object} Packet
 * @property {Buffer} seqno Id message.
 * @property {Buffer} origin Represents the author's ID of the message. To identify a message (`msgId`) in the network you should check for the: `seqno + origin`.
 * @property {Buffer} from Represents the current sender's ID of the message.
 * @property {Buffer} data Represents an opaque blob of data, it can contain any data that the publisher wants to send.
 */

/**
 * Broadcast
 *
 * Abstract module to send broadcast messages.
 *
 * @extends {EventEmitter}
 */
export class Broadcast extends EventEmitter {
  /**
   * @constructor
   * @param {Middleware} middleware
   * @param {Object} options
   * @param {Buffer} options.id Defines an id for the current peer.
   * @param {number} [options.maxAge=10000] Defines the max live time for the cache messages.
   * @param {number} [options.maxSize=100] Defines the max size for the cache messages.
   */
  constructor (middleware, options = {}) {
    assert(middleware);
    assert(middleware.lookup);
    assert(middleware.send);
    assert(middleware.subscribe);

    super();

    const { id = crypto.randomBytes(32), maxAge = 10 * 1000, maxSize = 200 } = options;

    this._id = id;
    this._lookup = this._buildLookup(middleware.lookup);
    this._send = (...args) => middleware.send(...args);
    this._subscribe = onPacket => middleware.subscribe(onPacket);

    this._running = false;
    this._seenSeqs = new TimeLRUSet({ maxAge, maxSize });
    this._peers = [];
    this._codec = new Codec({ verify: true });
    this._codec.loadFromJSON(schema);
  }

  /**
   * Broadcast a flooding message to the peers neighboors.
   *
   * @param {Buffer} data
   * @param {Object} options
   * @param {Buffer} [options.seqno=crypto.randomBytes(32)]
   * @returns {Promise}
   */
  async publish (data, options = {}) {
    const { seqno = crypto.randomBytes(32) } = options;

    assert(Buffer.isBuffer(data));
    assert(Buffer.isBuffer(seqno));

    if (!this._running) {
      console.warn('Broadcast not running.');
      return;
    }

    const packet = { seqno, origin: this._id, data };
    await this._publish(packet);
  }

  /**
   * Initialize the cache and runs the defined subscription.
   *
   * @returns {undefined}
   */
  run () {
    if (this._running) return;
    this._running = true;

    this._subscription = this._subscribe(packetEncoded => this._onPacket(packetEncoded)) || (() => {});

    log('running %h', this._id);
  }

  /**
   * Clear the cache and unsubscribe from incoming messages.
   *
   * @returns {undefined}
   */
  stop () {
    if (!this._running) return;
    this._running = false;

    this._subscription();
    this._seenSeqs.clear();

    log('stop %h', this._id);
  }

  /**
   * Build a deferrer lookup.
   * If we call the lookup several times it would runs once a wait for it.
   *
   * @param {Function} lookup
   * @returns {Function}
   */
  _buildLookup (lookup) {
    let looking = null;
    return async () => {
      try {
        if (!looking) {
          looking = lookup();
        }
        this._peers = await looking;
        looking = null;
        log('lookup of %h', this._id, this._peers);
      } catch (err) {
        this.emit('lookup-error', err);
        looking = null;
      }
    };
  }

  /**
   * Publish and/or Forward a packet message to each peer neighboor.
   *
   * @param {Packet} packet
   * @returns {Promise}
   */
  async _publish (packet) {
    if (!this._running) return;

    try {
      // Seen it by me.
      this._seenSeqs.add(msgId(packet.seqno, this._id));

      await this._lookup();

      // I update the package to assign the from prop to me (the current sender).
      const message = Object.assign({}, packet, { from: this._id });

      const packetEncoded = this._codec.encode({
        type: 'broadcast.Packet',
        message
      });

      const waitFor = this._peers.map(async (peer) => {
        if (!this._running) return;

        // Don't send the message to neighbors that already seen the message.
        if (this._seenSeqs.has(msgId(message.seqno, peer.id))) return;

        log('publish %h -> %h', this._id, peer.id, message);

        try {
          this._seenSeqs.add(msgId(message.seqno, peer.id));
          await this._send(packetEncoded, peer);
        } catch (err) {
          this.emit('send-error', err);
        }
      });

      await Promise.all(waitFor);
    } catch (err) {
      this.emit('send-error', err);
    }
  }

  /**
   * Process incoming encoded packets.
   *
   * @param {Buffer} packetEncoded
   * @returns {boolean} Returns true if the decoding was successful.
   */
  _onPacket (packetEncoded) {
    if (!this._running) return;

    try {
      const { message: packet } = this._codec.decode(packetEncoded);

      // Cache the packet as "seen by the peer from".
      this._seenSeqs.add(msgId(packet.seqno, packet.from));

      // Check if I already see this packet.
      if (this._seenSeqs.has(msgId(packet.seqno, this._id))) return;

      const peer = this._peers.find(peer => peer.id.equals(packet.from));

      log('received %h -> %h', this._id, packet.from, packet);

      this.emit('packet', packet, peer);

      this._publish(packet).catch(() => {});

      return true;
    } catch (err) {
      this.emit('subscribe-error', err);
      return false;
    }
  }
}
