# Key Value Servers
This directory includes a generic key-value server and a set of key-value
clients implemented using the lattices in [`lattices`](../lattices). Each
key-value client implements one of the weak isolation levels described in
[*Highly Available Transactions: Virtues and Limitations*][hat]. For example,
[`ruc_client.cc`](ruc_client.cc) implements supports READ UNCOMMITTED
transactions.

## Overview
| Isolation Level    | Client                           |
| ------------------ | -------------------------------- |
| READ UNCOMMITTED   | [`ruc_client.cc`](ruc_client.cc) |
| READ COMMITTED     | [`rc_client.cc`](rc_client.cc)   |
| Item Cut Isolation | [`ici_client.cc`](ici_client.cc) |

## Architecture
This directory contains a key-value server, a set of key-value clients, and a
message broker which passes messages between the two.

### Message Broker
A message broker (see [`msgqueue.cc`](msgqueue.cc)) is a simple program that
can connect any number of client sockets to any number of server sockets:

    clients                     servers
    =======                     =======

    +---+                       +---+
    | a |\                     /| 1 |
    +---+ \                   / +---+
           \                 /
    +---+   \ +----------+  /   +---+
    | b |---->| msgqueue |<-----| 2 |
    +---+   / +----------+  \   +---+
           /                 \
    +---+ /                   \ +---+
    | c |/                     \| 3 |
    +---+                       +---+

Messages sent by client sockets are fairly queued and sent in a round-robin
fashion to the server sockets. Here, for example, messages would be sent to 1,
then 2, then 3, then 1, then 2, and so on. When a server socket receives a
message, it can respond with a message of its own that is routed back to the
original sender.

### Client
Clients repeatedly

- read commands from stdin (e.g. `BEGIN TRANSACTION`, `GET x`, `PUT x foo`),
- parse the commands,
- serialize them into protocol buffers,
- send the protos to the message broker,
- await a response from a server, and
- pretty print the response.

Different clients perform different tricks to achieve their respective
isolation level.

### Server
The server runs some number of threads, and each thread maintains a copy of a
key-value store represented by a map from strings to timestamped strings; this
happens to be a semilattice. For example,

    {
        // key: (timestamp, value)
        "foo": (42, "moo"),
        "bar": (10, "meow"),
        ...
    }

Each server thread forms a connection to the message broker so that it can
receive messages from and send messages to the clients. It also forms a clique
of pub-sub connections with the other threads in order to gossip updates to the
key-value store. These pub-sub connections are made within the process and pass
data around via pointers to data allocated on the heap. Currently, gossip is
only between threads, not between servers.

[hat]: https://scholar.google.com/scholar?cluster=5290590427752770563&hl=en&as_sdt=0,5
