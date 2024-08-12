mod action;
mod config;
mod log;
pub mod message; // TODO:
mod node;
mod quorum;

pub use action::Action;
pub use config::ClusterConfig;
pub use log::{LogEntries, LogEntry, LogEntryRef, LogIndex, Snapshot};
pub use message::{Message, MessageSeqNum};
pub use node::{ChangeClusterConfigError, HeartbeatPromise, Node, NodeId, Role};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Term(u64);

impl Term {
    pub const fn new(v: u64) -> Self {
        Self(v)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }
}
