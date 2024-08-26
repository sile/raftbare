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
0.1.0-11-g5ac288c

$ cargo llvm-cov
Filename                      Regions    Missed Regions     Cover   Functions  Missed Functions  Executed       Lines      Missed Lines     Cover    Branches   Missed Branches     Cover
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
action.rs                          78                 3    96.15%           5                 0   100.00%         152                 0   100.00%           0                 0         -
config.rs                          36                 2    94.44%          14                 0   100.00%          76                 3    96.05%           0                 0         -
lib.rs                              3                 0   100.00%           3                 0   100.00%           9                 0   100.00%           0                 0         -
log.rs                            157                 7    95.54%          50                 0   100.00%         371                 5    98.65%           0                 0         -
message.rs                         40                 4    90.00%          10                 0   100.00%         100                 6    94.00%           0                 0         -
node.rs                           306                29    90.52%          49                 0   100.00%         637                37    94.19%           0                 0         -
promise.rs                         56                12    78.57%          11                 1    90.91%          52                 7    86.54%           0                 0         -
quorum.rs                          61                 5    91.80%          15                 1    93.33%         111                 4    96.40%           0                 0         -
role.rs                            16                 2    87.50%           4                 0   100.00%          14                 0   100.00%           0                 0         -
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
TOTAL                             753                64    91.50%         161                 2    98.76%        1522                62    95.93%           0                 0         -
```
