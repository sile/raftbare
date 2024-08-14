#![forbid(unsafe_code)]

mod action;
mod config;
mod log;
pub mod message; // TODO:
mod node;
mod quorum;

pub use action::Action;
pub use config::ClusterConfig;
pub use log::{LogEntries, LogEntry, LogIndex, LogPosition, Snapshot};
pub use message::{Message, MessageSeqNum};
pub use node::{ChangeClusterConfigError, HeartbeatPromise, Node, NodeId, Role};

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
