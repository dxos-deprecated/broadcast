//
// Copyright 2019 DxOS.
//

import { EventEmitter } from 'events';
import crypto from 'crypto';

import Broadcast from './broadcast';

class Peer extends EventEmitter {
  constructor () {
    super();
    this.id = crypto.randomBytes(32);
    this._peers = new Map();
    this._messages = [];
    this._broadcast = new Broadcast({
      id: this.id,
      middleware: {
        lookup: async () => Array.from(this._peers.values()),
        send: async (packet, node) => {
          node.send(packet);
        },
        subscribe: (onPacket) => {
          this.on('message', onPacket);
        }
      }
    });

    this._broadcast.on('packet', (packet) => {
      this.emit('packet', packet);
    });
    this._broadcast.run();
  }

  send (message) {
    this._messages.push(message);
    this.emit('message', message);
  }

  connect (peer) {
    this._peers.set(peer.id.toString('hex'), peer);
  }

  publish (message) {
    this._broadcast.publish(message);
  }

  stop () {
    this._broadcast.stop();
  }
}

test('broadcast: 10 peers connected lineal', async () => {
  const peers = [...Array(10).keys()].map(() => new Peer());
  peers.reduce((previous, current) => {
    if (previous) {
      previous.connect(current);
      current.connect(previous);
    }

    return current;
  }, null);

  const peerFirst = peers[0];
  const peerLast = peers[peers.length - 1];

  peerLast.once('packet', () => {
    peerLast.publish(Buffer.from('pong'));
  });

  peerFirst.publish(Buffer.from('ping'));

  const packet = await new Promise((resolve) => {
    peerFirst.once('packet', resolve);
  });

  expect(packet.data.toString('utf-8')).toBe('pong');

  peers.forEach(peer => peer.stop());
});
