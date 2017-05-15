#### Distributed Key Value Store

This is a small concept multi-node key value store written in Go. It provides (bravely attempts to) the Atomicity, Consistency, and Independence in ACID. It uses strict 2 phase committing with key locking over the nodes and the client API to achieve this.

The system is able to continue working through multiple node failures.

#### KVService

KVService exposes the store's API to a client Go application.

#### KVNode

KVNode is run by all the nodes in the system. All the nodes are given a file of all other nodes ip:port to communicate to each other.