//
// Copyright 2019 DxOS.
//

import assert from 'assert';
import crypto from 'crypto';
import { Codec } from '@dxos/codec-protobuf';
import debug from 'debug';
import { NanoresourcePromise } from 'nanoresource-promise/emitter';
import LRU from 'lru';

// eslint-disable-next-line
import schema from './schema.json';

debug.formatters.h = v => v.toString('hex').slice(0, 6);
const log = debug('broadcast');

const packetIdGenerator = (seqno) => {
  seqno = seqno.toString('hex');
  return from => `${seqno}:${from.toString('hex')}`;
};

/**
 * @typedef {Object} Middleware
 * @property {Function} send Defines how to send the packet builded by the broadcast.
 * @property {Function} subscribe Defines how to subscribe to incoming packets and update the internal list of peers.
 */

/**
 * @typedef {Object} Packet
 * @property {Buffer} seqno Id message.
 * @property {Buffer} origin Represents the author's ID of the message. To identify a message in the network you should check for the: `seqno + origin`.
 * @property {Buffer} from Represents the current sender's ID of the message.
 * @property {Buffer} data Represents an opaque blob of data, it can contain any data that the publisher wants to send.
 */

/**
 * Abstract module to send broadcast messages.
 *
 * @extends {EventEmitter}
 */
export class Broadcast extends NanoresourcePromise {
  /**
   * @constructor
   * @param {Middleware} middleware
   * @param {Object} options
   * @param {Buffer} options.id Defines an id for the current peer.
   * @param {number} [options.maxAge=10000] Defines the max live time for the cache messages.
   * @param {number} [options.maxSize=1000] Defines the max size for the cache messages.
   */
  constructor (middleware, options = {}) {
    assert(middleware);
    assert(middleware.send);
    assert(middleware.subscribe);

    super();

    const { id = crypto.randomBytes(32), maxAge = 10 * 1000, maxSize = 1000 } = options;

    this._id = id;

    this._send = (...args) => middleware.send(...args);
    this._subscribe = (...args) => middleware.subscribe(...args);

    this._seenSeqs = new LRU({ maxAge, max: maxSize });
    this._peers = [];
    this._codec = new Codec('dxos.broadcast.Packet');
    this._codec
      .addJson(JSON.parse(schema))
      .build();

    /** @deprecated */
    if (middleware.lookup) {
      this._lookup = () => middleware.lookup();
    }
  }

  /**
   * Broadcast a flooding message to the peers neighboors.
   *
   * @param {Buffer} data
   * @param {Object} options
   * @param {Buffer} [options.seqno=crypto.randomBytes(32)]
   * @returns {Promise<Packet>}
   */
  publish (data, options = {}) {
    const { seqno = crypto.randomBytes(32) } = options;

    assert(Buffer.isBuffer(data));
    assert(Buffer.isBuffer(seqno));

    const getPacketId = packetIdGenerator(seqno);
    const packet = { seqno, origin: this._id, data };
    return this._publish(packet, getPacketId(this._id), getPacketId);
  }

  /**
   * Update internal list of peers.
   *
   * @param {Array<Peer>} peers
   */
  updatePeers (peers) {
    this._peers = peers;
  }

  /**
   * Update internal cache options
   *
   * @param {{ maxAge: number, maxSize: number }} opts
   */
  updateCache (opts = {}) {
    if (opts.maxAge) {
      this._seenSeqs.maxAge = opts.maxAge;
    }

    if (opts.maxSize) {
      this._seenSeqs.max = opts.maxSize;
    }
  }

  /**
   * Prune the internal cache items in timeout
   */
  pruneCache () {
    for (const key of this._seenSeqs.keys) {
      this._seenSeqs.peek(key);
    }
  }

  /**
   * @deprecated
   */
  run () {
    this.open().catch((err) => {
      process.nextTick(() => this.emit('error', err));
    });
  }

  /**
   * @deprecated
   */
  stop () {
    this.close().catch((err) => {
      process.nextTick(() => this.emit('error', err));
    });
  }

  _open () {
    this._unsubscribe = this._subscribe(this._onPacket.bind(this), this.updatePeers.bind(this)) || (() => {});

    log('running %h', this._id);
  }

  _close () {
    this._unsubscribe();
    this._seenSeqs.clear();

    log('stop %h', this._id);
  }

  /**
   * Publish and/or Forward a packet message to each peer neighboor.
   *
   * @param {Packet} packet
   * @param {String} ownerId
   * @param {function} getPacketId
   * @returns {Promise<Packet>}
   */
  async _publish (packet, ownerId, getPacketId) {
    await this._isOpen();

    // Seen it by me.
    this._seenSeqs.set(ownerId, true);

    /** @deprecated */
    this._lookup && this.updatePeers(await this._lookup());

    // Update the package to set the current sender..
    packet = Object.assign({}, packet, {
      from: this._id
    });

    const packetEncoded = this._codec.encode(packet);

    this._peers.forEach(peer => {
      // Don't send the message to the "origin" peer.
      if (packet.origin.equals(peer.id)) return;

      const packetId = getPacketId(peer.id);

      // Don't send the message to neighbors that have already seen the message.
      if (this._seenSeqs.get(packetId)) return;
      this._seenSeqs.set(packetId, true);

      log('publish %h -> %h', this._id, peer.id, packet);

      this._send(packetEncoded, peer).then(() => {
        this.emit('send', packetEncoded, peer);
      }).catch(err => {
        this.emit('send-error', err);
      });
    });

    return packet;
  }

  /**
   * Process incoming encoded packets.
   *
   * @param {Buffer} packetEncoded
   * @returns {(Packet|undefined)} Returns the packet if the decoding was successful.
   */
  _onPacket (packetEncoded) {
    if (!this.opened || this.closed || this.closing) return;

    try {
      const packet = this._codec.decode(packetEncoded);

      // Ignore packets produced by me and forwarded by others
      if (packet.origin.equals(this._id)) return;

      const getPacketId = packetIdGenerator(packet.seqno);

      // Cache the packet as "seen by the peer from".
      this._seenSeqs.set(getPacketId(packet.from), true);

      // Check if I already see this packet.
      const ownerId = getPacketId(this._id);
      if (this._seenSeqs.get(ownerId)) return;

      log('received %h -> %h', this._id, packet.from, packet);

      this.emit('packet', packet);

      this._publish(packet, ownerId, getPacketId).catch(() => {});

      return packet;
    } catch (err) {
      this.emit('subscribe-error', err);
    }
  }

  _isOpen () {
    if (this.opening) return this.open();
    if (!this.opened || this.closed || this.closing) throw new Error('broadcast closed');
  }
}
