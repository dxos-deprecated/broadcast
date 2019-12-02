# Broadcast
> Abstract module to send broadcast messages.

[![Build Status](https://travis-ci.com/dxos/broadcast.svg?branch=master)](https://travis-ci.com/dxos/broadcast)
[![Coverage Status](https://coveralls.io/repos/github/dxos/broadcast/badge.svg?branch=master)](https://coveralls.io/github/dxos/broadcast?branch=master)
![npm (scoped)](https://img.shields.io/npm/v/@dxos/broadcast)
[![Greenkeeper badge](https://badges.greenkeeper.io/dxos/broadcast.svg)](https://greenkeeper.io/)
[![js-semistandard-style](https://img.shields.io/badge/code%20style-semistandard-brightgreen.svg?style=flat-square)](https://github.com/standard/semistandard)
[![standard-readme compliant](https://img.shields.io/badge/readme%20style-standard-brightgreen.svg?style=flat-square)](https://github.com/RichardLitt/standard-readme)

Allows a node to originate a message that will be received at least once, within a
reasonably short time, on all nodes that are reachable from the origin node. Messages are
propagated via the `middleware` specified. Broadcast storms are
avoided by means of a [flooding](https://en.wikipedia.org/wiki/Flooding_(computer_networking)) routing scheme.

Broadcast messages follows the schema:

```proto
message Packet {
  bytes seqno = 1;
  bytes origin = 2;
  bytes from = 3
  bytes data = 4;
}
```

- `seqno`: By default is a random 32-bit but could be used to provide an alternative sorted sequence number.
- `origin`: Represents the author's ID of the message. To identify a message (`msgId`) in the network you should check for the: `seqno + origin`.
- `from`: Represents the current sender's ID of the message.
- `data`: field is an opaque blob of data, it can contain any data that the publisher wants
it to defined by higher layers (e.g. a presence information message).

Nodes send any message originating locally to all current peers. Upon receiving a message, a
node delivers it locally to any listeners, and forward the message on to its current
peers, excluding the peer from which it was received.

Nodes maintain a record of the messages they have received and originated
recently, by `msgId(seqno + from)`, and the set of peers to which each message has already
been sent. This is used to avoid sending the same message to the same peer
more than once. These records expire after some time to limit memory consumption by: `maxAge` and `maxSize`.

<p align="center">
  <img src="https://user-images.githubusercontent.com/819446/66934639-2bb67980-f011-11e9-9c27-739b5ee5fd5c.gif" alt="graph">
</p>

## Install

```
$ npm install @dxos/broadcast
```

## Usage

```javascript
import { Broadcast } from '@wirelineio/broadcast';

const middleware = {
  lookup: async () => {
    // Return the list of neighbors peers with the format:
    // [{ id: Buffer, ...extraArgs }, { id: Buffer, ...extraArgs }]
  },
  send: async (packet, node) => {
    // Define how to send your packets.
    // "packet" is the encoded message to send.
    // "node" is the peer object generate from the lookup.

    // e.g. If node is a stream
    node.write(packet);

    // e.g. If node is a websocket
    node.send(packet);
  },
  subscribe: (onPacket) => {
    // Defines how to process incoming packets.

    // e.g. Using websockets

    const onMessage = data => onPacket(data);
    socket.on('message', onMessage);

    // Return a dispose function.
    return () => {
      socket.off('message', onMessage);
    }
  }
};

const broadcast = new Broadcast(middleware, {
  id: crypto.randomBytes(32),
  maxAge: 10 * 1000, // Timeout for each message in the LRU cache.
  maxSize: 200 // Limit of messages in the LRU cache.
})

// We initialize the middleware and subscription inside the broadcast.
broadcast.run()

broadcast.publish(Buffer.from('Hello everyone'))

broadcast.stop()
```

You can check a real example in: [example](https://github.com/dxos/broadcast/tree/master/example)

## API

#### `const broadcast = new Broadcast(middleware, [options])`

- `middleware`: The middleware defines an interface to connect the broadcast to any request/response solution.
  - `lookup: () => Promise<Array<Peer>>`: Runs a lookup to get the ids of your peers neighboors.
    - `Peer: { id: Buffer, ...props }`
  - `send: (packet: Buffer, peer: Object) => Promise`: Defines how to send the packet builded by the broadcast.
  - `subscribe: (onPacket) => unsubscribeFunction`: Defines how to subscribe to incoming packets.
    - `onPacket: (data: Buffer) => boolean`: Callback to process incoming data. It returns true if the broadcast could decode the message or false if not.
    - `unsubscribeFunction: Function`: Defines a way to unsubscribe from listening messages if the broadcast stop working. Helpful if you are working with streams and event emitters.

- `options`
  - `id: Buffer`: Defines an id for the current peer. Default: `crypto.randomBytes(32)`.
  - `maxAge: number`: Defines the max live time for the cache messages. Default: `10 * 1000`.
  - `maxSize: number`: Defines the max size for the cache messages. Default: `100`.

#### `broadcast.run()`

Initialize the cache and runs the defined subscription.

#### `broadcast.stop()`

Clear the cache and unsubscribe from incoming messages.

#### `broadcast.publish(data, [options])`

Broadcast a flooding message to the peers neighboors.

- `data: Buffer`: Any data that you want to broadcast.
- `options`
  - `seqno: Buffer`: Defines a custom seqno for the message. Default: `crypto.randomBytes(32)`.

## Contributing

PRs accepted.

## License

GPL-3.0 © dxos
