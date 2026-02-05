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
        /// Sender of the message.
        from: NodeId,

        /// Term of the sender.
        term: Term,

        /// Last log position of the candidate.
        last_position: LogPosition,
    },

    /// RequestVote RPC reply.
    RequestVoteReply {
        /// Sender of the message.
        from: NodeId,

        /// Term of the sender.
        term: Term,

        /// Whether the vote is granted or not.
        vote_granted: bool,
    },

    /// AppendEntries RPC call.
    AppendEntriesCall {
        /// Sender of the message.
        from: NodeId,

        /// Term of the sender.
        term: Term,

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
        /// Sender of the message.
        from: NodeId,

        /// Term of the sender.
        term: Term,

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
            Self::RequestVoteCall { from, .. } => *from,
            Self::RequestVoteReply { from, .. } => *from,
            Self::AppendEntriesCall { from, .. } => *from,
            Self::AppendEntriesReply { from, .. } => *from,
        }
    }

    /// Returns the term of the message.
    pub fn term(&self) -> Term {
        match self {
            Self::RequestVoteCall { term, .. } => *term,
            Self::RequestVoteReply { term, .. } => *term,
            Self::AppendEntriesCall { term, .. } => *term,
            Self::AppendEntriesReply { term, .. } => *term,
        }
    }

    pub(crate) fn request_vote_call(term: Term, from: NodeId, last_position: LogPosition) -> Self {
        Self::RequestVoteCall {
            from,
            term,
            last_position,
        }
    }

    pub(crate) fn request_vote_reply(term: Term, from: NodeId, vote_granted: bool) -> Self {
        Self::RequestVoteReply {
            from,
            term,
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
            from,
            term,
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
            from,
            term,
            generation,
            last_position,
        }
    }

    pub(crate) fn merge(&mut self, other: Self) {
        debug_assert_eq!(self.from(), other.from());
        debug_assert!(self.term() <= other.term());

        let Self::AppendEntriesCall {
            from: from0,
            term: term0,
            commit_index: leader_commit0,
            entries: entries0,
        } = self
        else {
            *self = other;
            return;
        };
        let Self::AppendEntriesCall {
            from: from1,
            term: term1,
            commit_index: leader_commit1,
            entries: entries1,
        } = other
        else {
            *self = other;
            return;
        };

        *from0 = from1;
        *term0 = term1;
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
                term,
                last_position,
                ..
            } => {
                *term = (*term).max(last_included_position.term);
                if last_position.index < last_included_position.index {
                    *last_position = last_included_position;
                }
            }
            Message::RequestVoteReply { term, .. } => {
                *term = (*term).max(last_included_position.term);
            }
            Message::AppendEntriesCall { term, entries, .. } => {
                *term = (*term).max(last_included_position.term);
                entries.handle_snapshot_installed(last_included_position);
            }
            Message::AppendEntriesReply {
                term,
                generation: _,
                last_position,
                ..
            } => {
                *term = (*term).max(last_included_position.term);
                if last_position.index < last_included_position.index {
                    *last_position = last_included_position;
                }
            }
        }
    }
}
