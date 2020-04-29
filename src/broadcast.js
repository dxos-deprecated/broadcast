//
// Copyright 2019 DxOS.
//

import assert from 'assert';
import crypto from 'crypto';
import { Codec } from '@dxos/codec-protobuf';
import debug from 'debug';
import { NanoresourcePromise } from 'nanoresource-promise/emitter';

// eslint-disable-next-line
import schema from './schema.json';
import { TimeLRUSet } from './time-lru-set';

debug.formatters.h = v => v.toString('hex').slice(0, 6);
const log = debug('broadcast');

const msgId = (seqno, from) => {
  assert(Buffer.isBuffer(seqno));
  assert(Buffer.isBuffer(from));
  return `${seqno.toString('hex')}:${from.toString('hex')}`;
};

/**
 * @typedef {Object} Middleware
 * @property {Function} send Defines how to send the packet builded by the broadcast.
 * @property {Function} subscribe Defines how to subscribe to incoming packets and update the internal list of peers.
 */

/**
 * @typedef {Object} Packet
 * @property {Buffer} seqno Id message.
 * @property {Buffer} origin Represents the author's ID of the message. To identify a message (`msgId`) in the network you should check for the: `seqno + origin`.
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
   * @param {number} [options.maxSize=100] Defines the max size for the cache messages.
   */
  constructor (middleware, options = {}) {
    assert(middleware);
    assert(middleware.send);
    assert(middleware.subscribe);

    super();

    const { id = crypto.randomBytes(32), maxAge = 10 * 1000, maxSize = 100 } = options;

    this._id = id;
    this._send = (...args) => middleware.send(...args);
    this._subscribe = next => middleware.subscribe(next);

    this._seenSeqs = new TimeLRUSet({ maxAge, maxSize });
    this._peers = [];
    this._codec = new Codec('dxos.broadcast.Packet');
    this._codec
      .addJson(JSON.parse(schema))
      .build();
  }

  /**
   * Broadcast a flooding message to the peers neighboors.
   *
   * @param {Buffer} data
   * @param {Object} options
   * @param {Buffer} [options.seqno=crypto.randomBytes(32)]
   * @returns {Promise<Packet>}
   */
  async publish (data, options = {}) {
    const { seqno = crypto.randomBytes(32) } = options;

    assert(Buffer.isBuffer(data));
    assert(Buffer.isBuffer(seqno));

    const packet = { seqno, origin: this._id, data };
    return this._publish(packet, options);
  }

  /**
   * Update internal list of peers.
   *
   * @param {Array<Peer>} peers
   */
  updatePeers (peers) {
    this._peers = peers;
  }

  async _open () {
    const onData = this._onPacket.bind(this);
    const onPeers = this.updatePeers.bind(this);
    this._unsubscribe = this._subscribe({ onData, onPeers }) || (() => {});

    log('running %h', this._id);
  }

  async _close () {
    this._unsubscribe();
    this._seenSeqs.clear();

    log('stop %h', this._id);
  }

  /**
   * Publish and/or Forward a packet message to each peer neighboor.
   *
   * @param {Packet} packet
   * @param {Object} options
   * @returns {Promise<Packet>}
   */
  async _publish (packet, options = {}) {
    await this.open();

    try {
      const ownerId = msgId(packet.seqno, this._id);

      if (this._seenSeqs.has(ownerId)) {
        return;
      }

      // Seen it by me.
      this._seenSeqs.add(ownerId);

      // Update the package to set the current sender..
      packet = Object.assign({}, packet, { from: this._id });

      const packetEncoded = this._codec.encode(packet);

      const waitFor = this._peers.map((peer) => {
        if (!this.opened) return;

        // Don't send the message to the "origin" peer.
        if (packet.origin.equals(peer.id)) {
          return Promise.resolve();
        }

        // Don't send the message to neighbors that have already seen the message.
        if (this._seenSeqs.has(msgId(packet.seqno, peer.id))) {
          return Promise.resolve();
        }

        log('publish %h -> %h', this._id, peer.id, packet);

        this._seenSeqs.add(msgId(packet.seqno, peer.id));
        return this._send(packetEncoded, peer, options).then(() => {
          this.emit('send', packetEncoded, peer, options);
        }).catch(err => {
          this.emit('send-error', err);
        });
      });

      await Promise.all(waitFor);

      return packet;
    } catch (err) {
      this.emit('send-error', err);
      throw err;
    }
  }

  /**
   * Process incoming encoded packets.
   *
   * @param {Buffer} packetEncoded
   * @returns {(Packet|undefined)} Returns the packet if the decoding was successful.
   */
  _onPacket (packetEncoded) {
    if (!this.opened) return;

    try {
      const packet = this._codec.decode(packetEncoded);

      // Ignore packets produced by me and forwarded by others
      if (packet.origin.equals(this._id)) {
        return;
      }

      // Cache the packet as "seen by the peer from".
      this._seenSeqs.add(msgId(packet.seqno, packet.from));

      // Check if I already see this packet.
      if (this._seenSeqs.has(msgId(packet.seqno, this._id))) {
        return;
      }

      const peer = this._peers.find(peer => peer.id.equals(packet.from));

      if (!peer) {
        throw new Error('peer not found');
      }

      log('received %h -> %h', this._id, packet.from, packet);

      this.emit('packet', packet, peer);

      this._publish(packet).catch(() => {});

      return packet;
    } catch (err) {
      this.emit('subscribe-error', err);
    }
  }
}
