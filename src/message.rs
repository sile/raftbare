use crate::{
    Term,
    log::{LogEntries, LogIndex, LogPosition},
    node::{NodeGeneration, NodeId},
};

/// Message for RPC.
///
/// Note that this enum does not include the InstallSnapshot RPC,
/// as the specifics of snapshot installation depend heavily on
/// each individual application and are not managed by this crate.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Message {
    /// RequestVote RPC call.
    RequestVoteCall {
        /// Message header.
        header: MessageHeader,

        /// Last log position of the candidate.
        last_position: LogPosition,
    },

    /// RequestVote RPC reply.
    RequestVoteReply {
        /// Message header.
        header: MessageHeader,

        /// Whether the vote is granted or not.
        vote_granted: bool,
    },

    /// AppendEntries RPC call.
    AppendEntriesCall {
        /// Message header.
        header: MessageHeader,

        /// Leader's commit index.
        commit_index: LogIndex,

        /// Entries to append.
        ///
        /// Note that if the entries are too large to fit in a single message,
        /// it can be shrinked by calling [`LogEntries::truncate()`] before sending.
        entries: LogEntries,
    },

    /// AppendEntries RPC reply.
    AppendEntriesReply {
        /// Message header.
        header: MessageHeader,

        /// Generation of the sender.
        generation: NodeGeneration,

        /// Last log position of the follower.
        ///
        /// Instead of replying a boolean `success` as defined in the Raft paper,
        /// this crate replies the last log position of the follower.
        /// With this adjustment, the leader can quickly determine the appropriate match index of the follower,
        /// even if the follower is significantly behind the leader.
        last_position: LogPosition,
    },
}

impl Message {
    /// Returns the sender node ID of the message.
    pub fn from(&self) -> NodeId {
        match self {
            Self::RequestVoteCall { header, .. } => header.from,
            Self::RequestVoteReply { header, .. } => header.from,
            Self::AppendEntriesCall { header, .. } => header.from,
            Self::AppendEntriesReply { header, .. } => header.from,
        }
    }

    /// Returns the term of the message.
    pub fn term(&self) -> Term {
        match self {
            Self::RequestVoteCall { header, .. } => header.term,
            Self::RequestVoteReply { header, .. } => header.term,
            Self::AppendEntriesCall { header, .. } => header.term,
            Self::AppendEntriesReply { header, .. } => header.term,
        }
    }

    pub(crate) fn request_vote_call(
        term: Term,
        from: NodeId,
        last_position: LogPosition,
    ) -> Self {
        Self::RequestVoteCall {
            header: MessageHeader { term, from },
            last_position,
        }
    }

    pub(crate) fn request_vote_reply(
        term: Term,
        from: NodeId,
        vote_granted: bool,
    ) -> Self {
        Self::RequestVoteReply {
            header: MessageHeader { from, term },
            vote_granted,
        }
    }

    pub(crate) fn append_entries_call(
        term: Term,
        from: NodeId,
        commit_index: LogIndex,
        entries: LogEntries,
    ) -> Self {
        Self::AppendEntriesCall {
            header: MessageHeader { from, term },
            commit_index,
            entries,
        }
    }

    pub(crate) fn append_entries_reply(
        term: Term,
        from: NodeId,
        generation: NodeGeneration,
        last_position: LogPosition,
    ) -> Self {
        Self::AppendEntriesReply {
            header: MessageHeader { term, from },
            generation,
            last_position,
        }
    }

    pub(crate) fn merge(&mut self, other: Self) {
        debug_assert_eq!(self.from(), other.from());

        if other.term() < self.term() {
            return;
        }

        if other.term() > self.term() {
            *self = other;
            return;
        }

        let Self::AppendEntriesCall {
            header: header0,
            commit_index: leader_commit0,
            entries: entries0,
        } = self
        else {
            *self = other;
            return;
        };
        let Self::AppendEntriesCall {
            header: header1,
            commit_index: leader_commit1,
            entries: entries1,
        } = other
        else {
            *self = other;
            return;
        };

        *header0 = header1;
        *leader_commit0 = leader_commit1;
        if entries0.contains(entries1.prev_position()) {
            entries0.append(&entries1);
        } else {
            *entries0 = entries1;
        }
    }

    pub(crate) fn handle_snapshot_installed(&mut self, last_included_position: LogPosition) {
        match self {
            Message::RequestVoteCall {
                header,
                last_position,
            } => {
                header.term = header.term.max(last_included_position.term);
                if last_position.index < last_included_position.index {
                    *last_position = last_included_position;
                }
            }
            Message::RequestVoteReply { header, .. } => {
                header.term = header.term.max(last_included_position.term);
            }
            Message::AppendEntriesCall {
                header, entries, ..
            } => {
                header.term = header.term.max(last_included_position.term);
                entries.handle_snapshot_installed(last_included_position);
            }
            Message::AppendEntriesReply {
                header,
                generation: _,
                last_position,
            } => {
                header.term = header.term.max(last_included_position.term);
                if last_position.index < last_included_position.index {
                    *last_position = last_included_position;
                }
            }
        }
    }
}

/// Common header for all messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MessageHeader {
    /// Sender of the message.
    pub from: NodeId,

    /// Term of the sender.
    pub term: Term,
}
