//! `raftbare` is a minimal but feature-complete, I/O-free implementation of the [Raft] distributed consensus algorithm.
//!
//! [Raft]: https://raft.github.io/
//!
//! [`Node`] is the main struct that represents a Raft node.
//! It offers methods for creating a cluster, proposing commands, updating cluster configurations,
//! handling incoming messages, snapshotting, and more.
//!
//! [`Node`] itself does not execute I/O operations.
//! Instead, it generates [`Action`]s that represent pending I/O operations.
//! How to execute these actions is up to the crate user.
//!
//! Except for a few optimizations, `raftbare` is a very straightforward (yet efficient) implementation of the Raft algorithm.
//! This crate focuses on the core part of the algorithm.
//! So, offering various convenience features (which are not described in the Raft paper) is left to the crate user.
//!
//! The following example outlines a basic usage flow of this crate:
//! ```
//! use raftbare::{Action, Node, NodeId};
//!
//! // Start a node.
//! let mut node = Node::start(NodeId::new(0));
//!
//! // Create a three nodes cluster.
//! let commit_position = node.create_cluster(&[NodeId::new(0), NodeId::new(1), NodeId::new(2)]);
//!
//! // Execute actions requested by the node until the cluster creation is complete.
//! while node.get_commit_status(commit_position).is_in_progress() {
//!     for action in node.actions_mut() {
//!         // How to execute actions is up to the crate user.
//!         match action {
//!            Action::SetElectionTimeout => { /* ... */ },
//!            Action::SaveCurrentTerm => { /* ... */ },
//!            Action::SaveVotedFor => { /* ... */ },
//!            Action::BroadcastMessage(_) => { /* ... */ },
//!            Action::AppendLogEntries(_) => { /* ... */ },
//!            Action::SendMessage(_, _) => { /* ... */ },
//!            Action::InstallSnapshot(_) => { /* ... */ },
//!         }
//!     }
//!
//!     // If the election timeout is expired, handle it.
//!     if is_election_timeout_expired() {
//!         node.handle_election_timeout();
//!     }
//!
//!     // If a message is received, handle it.
//!     while let Some(message) = try_receive_message() {
//!         node.handle_message(&message);
//!     }
//!     # break;
//! }
//!
//! // Propose a user-defined command.
//! let commit_position = node.propose_command();
//!
//! // Execute actions as before.
//!
//! # fn is_election_timeout_expired() -> bool { true }
//! # fn try_receive_message() -> Option<raftbare::Message> { None }
//! ```
#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod action;
mod config;
mod log;
mod message;
mod node;
mod quorum;
mod role;

pub use action::{Action, Actions};
pub use config::ClusterConfig;
pub use log::{CommitStatus, Log, LogEntries, LogEntry, LogIndex, LogPosition};
pub use message::{Message, MessageHeader};
pub use node::{Node, NodeGeneration, NodeId};
pub use role::Role;

/// Term.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Term(u64);

impl Term {
    /// The initial term.
    pub const ZERO: Self = Self(0);

    /// Makes a new [`Term`] instance.
    pub const fn new(t: u64) -> Self {
        Self(t)
    }

    /// Returns the value of this term.
    pub const fn get(self) -> u64 {
        self.0
    }

    pub(crate) const fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

impl From<u64> for Term {
    fn from(value: u64) -> Self {
        Self::new(value)
    }
}

impl From<Term> for u64 {
    fn from(value: Term) -> Self {
        value.get()
    }
}

impl std::ops::Add for Term {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self::new(self.0 + rhs.0)
    }
}

impl std::ops::AddAssign for Term {
    fn add_assign(&mut self, rhs: Self) {
        self.0 += rhs.0;
    }
}

impl std::ops::Sub for Term {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        Self::new(self.0 - rhs.0)
    }
}

impl std::ops::SubAssign for Term {
    fn sub_assign(&mut self, rhs: Self) {
        self.0 -= rhs.0;
    }
}
