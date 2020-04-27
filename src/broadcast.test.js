//
// Copyright 2019 DxOS.
//

import { EventEmitter } from 'events';
import waitForExpect from 'wait-for-expect';

import { NetworkGenerator } from '@dxos/network-generator';

import { Broadcast } from './broadcast';

const delay = ms => new Promise(resolve => setTimeout(resolve, ms));

const packetId = (packet) => packet.seqno.toString('hex') + packet.origin.toString('hex');

class Peer extends EventEmitter {
  constructor (id, opts = {}) {
    super();
    this.id = id;

    this._peers = new Map();
    this._messages = new Map();

    const middleware = {
      lookup: async () => Array.from(this._peers.values()),
      send: async (packet, node, options) => {
        node.send(packet);
      },
      subscribe: (onPacket) => {
        this.on('message', onPacket);
        return () => this.off('message', onPacket);
      }
    };

    this._broadcast = new Broadcast(middleware, {
      id: this.id,
      ...opts
    });

    this._broadcast.on('packet', (packet) => {
      this._messages.set(packetId(packet), packet.data.toString('utf8'));
      this.emit('packet', packet);
    });

    this._broadcast.run();
  }

  get messages () {
    return this._messages;
  }

  get seenMessagesSize () {
    return this._broadcast._seenSeqs.size;
  }

  send (message) {
    this.emit('message', message);
  }

  connect (peer) {
    this._peers.set(peer.id.toString('hex'), peer);
  }

  publish (message, options) {
    return this._broadcast.publish(message, options);
  }

  stop () {
    this._broadcast.stop();
  }
}

test('balancedBinTree: broadcast a message through 63 peers.', async () => {
  const generator = new NetworkGenerator({
    createPeer: (id) => new Peer(id),
    createConnection: (peerFrom, peerTo) => {
      peerFrom.connect(peerTo);
      peerTo.connect(peerFrom);
    }
  });

  const network = generator.balancedBinTree(5);
  const [peerOrigin, ...peers] = network.peers;

  let packet = await peerOrigin.publish(Buffer.from('message1'));
  await waitForExpect(() => {
    const finish = peers.reduce((prev, current) => {
      return prev && current.messages.has(packetId(packet));
    }, true);

    expect(finish).toBe(true);
  }, 10000, 1000);

  packet = await peerOrigin.publish(Buffer.from('message1'), { seqno: Buffer.from('custom-seqno') });
  expect(packet.seqno.toString()).toBe('custom-seqno');
  await waitForExpect(() => {
    const finish = peers.reduce((prev, current) => {
      return prev && current.messages.has(packetId(packet));
    }, true);

    expect(finish).toBe(true);
  }, 5000, 1000);

  network.peers.forEach(peer => peer.stop());
});

test('complete: broadcast a message through 50 peers.', async () => {
  const generator = new NetworkGenerator({
    createPeer: (id) => new Peer(id, { maxAge: 1000 }),
    createConnection: (peerFrom, peerTo) => {
      peerFrom.connect(peerTo);
      peerTo.connect(peerFrom);
    }
  });

  const network = generator.complete(50);
  const [peerOrigin, ...peers] = network.peers;

  const packet = await peerOrigin.publish(Buffer.from('message1'));
  await waitForExpect(() => {
    const finish = peers.reduce((prev, current) => {
      return prev && current.messages.has(packetId(packet));
    }, true);

    expect(finish).toBe(true);
  }, 10000, 1000);

  await delay(1000);

  expect(network.peers.reduce((prev, next) => (prev && next.seenMessagesSize === 0), true)).toBeTruthy();

  network.peers.forEach(peer => peer.stop());
});
