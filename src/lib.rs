#![forbid(unsafe_code)]

mod action;
mod config;
mod log;
mod message;
mod node;
mod promise;
mod quorum;
mod role;

pub use action::{Action, Actions};
pub use config::ClusterConfig;
pub use log::{Log, LogEntries, LogEntry, LogIndex, LogPosition};
pub use message::{
    AppendEntriesReply, AppendEntriesRequest, Message, MessageSeqNo, RequestVoteReply,
    RequestVoteRequest,
};
pub use node::{ChangeClusterConfigError, Node, NodeId};
pub use promise::{CommitPromise, HeartbeatPromise};
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
