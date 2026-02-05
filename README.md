noraft
========

[![noraft](https://img.shields.io/crates/v/noraft.svg)](https://crates.io/crates/noraft)
[![Documentation](https://docs.rs/noraft/badge.svg)](https://docs.rs/noraft)
[![Actions Status](https://github.com/sile/noraft/workflows/CI/badge.svg)](https://github.com/sile/noraft/actions)
![License](https://img.shields.io/crates/l/noraft)


noraft: Minimal, feature-complete Raft for Rust - no I/O, no dependencies.

`noraft` is a minimal but feature-complete, sans I/O implementation of the [Raft] distributed consensus algorithm.

[Raft]: https://raft.github.io/

[`Node`] is the main struct that represents a Raft node.
It offers methods for creating a cluster, proposing commands, updating cluster configurations,
handling incoming messages, snapshotting, and more.

[`Node`] itself does not execute I/O operations.
Instead, it generates [`Action`]s that represent pending I/O operations.
How to execute these actions is up to the crate user.

`noraft` keeps its API surface minimal and lets users choose and integrate their own I/O layer freely.

Except for a few optimizations, `noraft` is a very straightforward (yet efficient) implementation of the Raft algorithm.
This crate focuses on the core part of the algorithm.
So, offering various convenience features (which are not described in the Raft paper) is left to the crate user.

[`Node`]: https://docs.rs/noraft/latest/noraft/struct.Node.html
[`Action`]: https://docs.rs/noraft/latest/noraft/struct.Action.html

The following example outlines a basic usage flow of this crate:
```rust
// Start a node.
let mut node = noraft::Node::start(noraft::NodeId::new(0));

// Create a three nodes cluster.
let commit_position = node.create_cluster(&[
    noraft::NodeId::new(0),
    noraft::NodeId::new(1),
    noraft::NodeId::new(2),
]);

// Execute actions requested by the node until the cluster creation is complete.
while node.get_commit_status(commit_position).is_in_progress() {
    for action in node.actions_mut() {
        // How to execute actions is up to the crate user.
        match action {
           noraft::Action::SetElectionTimeout => { /* ... */ },
           noraft::Action::SaveCurrentTerm => { /* ... */ },
           noraft::Action::SaveVotedFor => { /* ... */ },
           noraft::Action::BroadcastMessage(_) => { /* ... */ },
           noraft::Action::AppendLogEntries(_) => { /* ... */ },
           noraft::Action::SendMessage(_, _) => { /* ... */ },
           noraft::Action::InstallSnapshot(_) => { /* ... */ },
        }
    }

    // If the election timeout is expired, handle it.
    if is_election_timeout_expired() {
        node.handle_election_timeout();
    }

    // If a message is received, handle it.
    while let Some(message) = try_receive_message() {
        node.handle_message(&message);
    }
}

// Propose a user-defined command.
let commit_position = node.propose_command();

// Execute actions as before.
```

I/O Implementation
-------------------

[`raftpico`] is a reference implementation for the I/O layer.

[`raftpico`]: https://github.com/sile/raftpico

Coverage
--------

```console
$ rustup component add llvm-tools-preview
$ cargo install cargo-llvm-cov

$ git describe --tags
0.2.0

$ cargo llvm-cov
Filename                      Regions    Missed Regions     Cover   Functions  Missed Functions  Executed       Lines      Missed Lines     Cover    Branches   Missed Branches     Cover
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
action.rs                          78                 3    96.15%           5                 0   100.00%         152                 0   100.00%           0                 0         -
config.rs                          36                 2    94.44%          14                 0   100.00%          76                 3    96.05%           0                 0         -
lib.rs                              9                 6    33.33%           9                 6    33.33%          27                18    33.33%           0                 0         -
log.rs                            202                26    87.13%          68                 9    86.76%         442                42    90.50%           0                 0         -
message.rs                         50                14    72.00%          11                 1    90.91%         125                31    75.20%           0                 0         -
node.rs                           318                42    86.79%          54                 6    88.89%         647                61    90.57%           0                 0         -
quorum.rs                          35                 1    97.14%           9                 0   100.00%          64                 1    98.44%           0                 0         -
role.rs                            16                 2    87.50%           4                 0   100.00%          14                 0   100.00%           0                 0         -
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
TOTAL                             744                96    87.10%         174                22    87.36%        1547               156    89.92%           0                 0         -
```
