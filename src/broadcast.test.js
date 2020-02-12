//
// Copyright 2019 DxOS.
//

import { EventEmitter } from 'events';
import waitForExpect from 'wait-for-expect';

import { NetworkGenerator } from '@dxos/network-generator';

import { Broadcast } from './broadcast';

const packetId = (packet) => packet.seqno.toString('hex') + packet.origin.toString('hex');

class Peer extends EventEmitter {
  constructor (id) {
    super();
    this.id = id;

    this._peers = new Map();
    this._messages = new Map();

    const middleware = {
      lookup: async () => Array.from(this._peers.values()),
      send: async (packet, node) => {
        node.send(packet);
      },
      subscribe: (onPacket) => {
        this.on('message', onPacket);
        return () => this.off('message', onPacket);
      }
    };

    this._broadcast = new Broadcast(middleware, {
      id: this.id
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

test('broadcast a message through 63 peers connected in a balanced network.', async () => {
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

  peerOrigin.stop();
  peers.forEach(peer => peer.stop());
});
