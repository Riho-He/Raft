# Raft

This is my implementation of the Raft consensus algorithm in Go. It handles leader election, log replication, and keeps things running even when nodes fail.

## Project Structure

```
raft/
  cmd/
    raft_cluster/    - Spin up a local cluster for testing
    raft_client/     - Interactive client to send commands
    raft_node/       - Run a single Raft node
  pkg/
    node.go          - Core Raft node logic
    node_follower_state.go
    node_candidate_state.go
    node_leader_state.go
    client.go        - Client implementation
    cluster.go       - Cluster management
    hashmachine/     - Example state machine (hash chain)
    bolt_store.go    - Persistent storage using BoltDB
    memory_store.go  - In-memory storage for testing
  test/              - Integration tests
```

## Getting Started

You will need Go 1.13 or later.

## How It Works

The basic idea is pretty simple:

1. Every node starts as a follower, just waiting around for instructions
2. If a follower stops hearing from a leader, it gets impatient and decides to run for leader itself
3. It asks other nodes to vote for it. If it gets enough votes, it wins and becomes the new leader
4. The leader then takes charge of handling requests and making sure everyone stays in sync
5. If the leader ever goes down, the whole election process kicks off again

### State Machine

I included a simple hash chain as an example state machine. You can initialize it with a value and then keep adding to the chain using MD5. If you want to use something else, just implement the StateMachine interface and plug it in.

### Storage

There are two ways to store data:

- In-memory, which is great for testing since nothing persists
- BoltDB, which writes to disk if you need things to survive restarts
