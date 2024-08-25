raftbare
========

[![raftbare](https://img.shields.io/crates/v/raftbare.svg)](https://crates.io/crates/raftbare)
[![Documentation](https://docs.rs/raftbare/badge.svg)](https://docs.rs/raftbare)
[![Actions Status](https://github.com/sile/raftbare/workflows/CI/badge.svg)](https://github.com/sile/raftbare/actions)
![License](https://img.shields.io/crates/l/raftbare)


`raftbare` is a minimal but feature-complete, I/O-free implementation of the [Raft] distributed consensus algorithm.

[Raft]: https://raft.github.io/

[`Node`] is the main struct that represents a Raft node.
It offers methods for creating a cluster, proposing commands, updating cluster configurations,
handling incoming messages, snapshotting, and more.

[`Node`] itself does not execute I/O operations.
Instead, it generates [`Action`]s that represent pending I/O operations.
How to execute these actions is up to the crate user.

Except for a few optimizations, `raftbare` is a very straightforward (yet efficient) implementation of the Raft algorithm.
This crate focuses on the core part of the algorithm.
So, offering various convenience features (which are not described in the Raft paper) is left to the crate user.

[`Node`]: https://docs.rs/raftbare/latest/raftbare/struct.Node.html
[`Action`]: https://docs.rs/raftbare/latest/raftbare/struct.Action.html

The following example outlines a basic usage flow of this crate:
```rust
use raftbare::{Action, Node, NodeId};

// Start a node.
let mut node = Node::start(NodeId::new(0));

// Create a three nodes cluster.
let mut promise = node.create_cluster(&[NodeId::new(0), NodeId::new(1), NodeId::new(2)]);

// Execute actions requested by the node until the cluster creation is complete.
while promise.poll(&mut node).is_pending() {
    for action in node.actions_mut() {
        // How to execute actions is up to the crate user.
        match action {
           Action::SetElectionTimeout => { /* ... */ },
           Action::SaveCurrentTerm => { /* ... */ },
           Action::SaveVotedFor => { /* ... */ },
           Action::AppendLogEntries(_) => { /* ... */ },
           Action::BroadcastMessage(_) => { /* ... */ },
           Action::SendMessage(_, _) => { /* ... */ },
           Action::InstallSnapshot(_) => { /* ... */ },
        }
    }

    // If the election timeout is expired, handle it.
    if is_election_timeout_expired() {
        node.handle_election_timeout();
    }

    // If a message is received, handle it.
    while let Some(message) = try_receive_message() {
        node.handle_message(message);
    }
    # break;
}

// Propose a user-defined command.
let promise = node.propose_command();

// Execute actions as before.
```

Coverage
--------

```console
$ rustup component add llvm-tools-preview
$ cargo install cargo-llvm-cov

$ git describe --tags
0.1.0

$ cargo llvm-cov
Filename                      Regions    Missed Regions     Cover   Functions  Missed Functions  Executed       Lines      Missed Lines     Cover    Branches   Missed Branches     Cover
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
action.rs                          78                13    83.33%           5                 1    80.00%         152                10    93.42%           0                 0         -
config.rs                          36                 9    75.00%          14                 2    85.71%          76                18    76.32%           0                 0         -
lib.rs                              3                 0   100.00%           3                 0   100.00%           9                 0   100.00%           0                 0         -
log.rs                            155                13    91.61%          49                 1    97.96%         362                15    95.86%           0                 0         -
message.rs                         40                10    75.00%          10                 0   100.00%          97                19    80.41%           0                 0         -
node.rs                           305                51    83.28%          48                 1    97.92%         631                83    86.85%           0                 0         -
promise.rs                         56                21    62.50%          11                 3    72.73%          52                16    69.23%           0                 0         -
quorum.rs                          61                 5    91.80%          15                 1    93.33%         111                 4    96.40%           0                 0         -
role.rs                            12                 5    58.33%           3                 1    66.67%           9                 3    66.67%           0                 0         -
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
TOTAL                             746               127    82.98%         158                10    93.67%        1499               168    88.79%           0                 0         -
```
