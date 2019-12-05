//
// Copyright 2019 DxOS.
//

import { EventEmitter } from 'events';
import crypto from 'crypto';
import generator from 'ngraph.generators';
import waitForExpect from 'wait-for-expect';

import { Broadcast } from './broadcast';

class Peer extends EventEmitter {
  constructor () {
    super();
    this.id = crypto.randomBytes(32);
    this._peers = new Map();
    this._messages = new Map();

    const middleware = {
      lookup: async () => Array.from(this._peers.values()),
      send: async (packet, node) => {
        node.send(packet);
      },
      subscribe: (onPacket) => {
        this.on('message', onPacket);
      }
    };

    this._broadcast = new Broadcast(middleware, {
      id: this.id
    });

    this._broadcast.on('packet', (packet) => {
      const id = packet.seqno.toString('hex') + packet.origin.toString('hex');
      this._messages.set(id, packet.data.toString('utf8'));
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

  publish (message) {
    this._broadcast.publish(message);
  }

  stop () {
    this._broadcast.stop();
  }
}

function createPeers (graph) {
  const peers = new Map();

  graph.forEachNode(node => {
    peers.set(node.id, new Peer());
  });

  graph.forEachLink(link => {
    const fromPeer = peers.get(link.fromId);
    const toPeer = peers.get(link.toId);
    // Communication bidirectional.
    fromPeer.connect(toPeer);
    toPeer.connect(fromPeer);
  });

  return Array.from(peers.values());
}

test('broadcast a message through 63 peers connected in a balanced network.', async () => {
  const [peerOrigin, ...peers] = createPeers(generator.balancedBinTree(5));
  let packets = 62;

  peers.forEach(peer => {
    peer.once('packet', () => {
      packets--;
    });
  });

  peerOrigin.publish(Buffer.from('message1'));

  await waitForExpect(() => {
    expect(packets).toBe(0);
  }, 5000, 1000);

  peers.forEach(peer => {
    expect(Array.from(peer.messages.values())).toEqual(['message1']);
  });

  peers.forEach(peer => peer.stop());
});
