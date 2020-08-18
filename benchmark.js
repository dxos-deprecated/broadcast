//
// Copyright 2019 DXOS.org
//

const { EventEmitter } = require('events');
const crypto = require('crypto');
const { Suite } = require('@dxos/benchmark-suite');
const { NetworkGenerator } = require('@dxos/network-generator');

const { Broadcast } = require('.');

class Peer extends EventEmitter {
  constructor (id) {
    super();
    this.id = id;

    this._peers = new Map();
    this._messages = [];

    const middleware = {
      send: async (packet, node) => {
        node.send(packet);
      },
      subscribe: (onData, updatePeers) => {
        this.on('message', onData);

        const onPeerAdded = () => updatePeers(Array.from(this._peers.values()));
        this.on('peer-added', onPeerAdded);

        return () => {
          this.off('message', onData);
          this.off('peer-added', onPeerAdded);
        };
      }
    };

    this._broadcast = new Broadcast(middleware, {
      id: this.id
    });

    this._broadcast.on('packet', (packet) => {
      this._messages.push(packet.data);
      this.emit('packet', packet.data);
    });

    this._broadcast.open();
  }

  get codec () {
    return this._broadcast._codec;
  }

  get messages () {
    return this._messages;
  }

  send (message) {
    this.emit('message', message);
  }

  connect (peer) {
    this._peers.set(peer.id.toString('hex'), peer);
    this.emit('peer-added', peer);
  }

  publish (message, options) {
    return this._broadcast.publish(message, options);
  }

  close () {
    this._broadcast.close();
  }
}

(async () => {
  const suite = new Suite();

  const generator = new NetworkGenerator({
    createPeer: (id) => new Peer(id),
    createConnection: (peerFrom, peerTo) => {
      peerFrom.connect(peerTo);
      peerTo.connect(peerFrom);
    }
  });

  const network = generator.complete(2);

  suite.test('direct', async () => {
    const peer = network.peers[1];
    for (let i = 0; i < 10000; i++) {
      const done = new Promise(resolve => peer.once('message', msg => {
        peer.codec.decode(msg);
        resolve();
      }));
      await peer.send(peer.codec.encode({
        seqno: crypto.randomBytes(32),
        origin: peer.id,
        from: peer.id,
        data: Buffer.from('test')
      }));
      await done;
    }
  });

  suite.test('broadcast', async () => {
    for (let i = 0; i < 10000; i++) {
      const done = new Promise(resolve => network.peers[1].once('packet', resolve));
      await network.peers[0].publish(Buffer.from('test'));
      await done;
    }
  });

  suite.print(await suite.run());
})();
