use crate::{
    log::{LogEntries, LogIndex, LogPosition},
    node::NodeId,
    Term,
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
        header: MessageHeader,
        last_position: LogPosition,
    },

    /// RequestVote RPC reply.
    RequestVoteReply {
        header: MessageHeader,
        vote_granted: bool,
    },

    /// AppendEntries RPC call.
    AppendEntriesCall {
        header: MessageHeader,
        leader_commit: LogIndex, // TOOD: rename(?)

        /// Note that if the entries are too large to fit in a single message,
        /// it can be shrinked by calling [`LogEntries::truncate()`] before sending.
        entries: LogEntries,
    },

    /// AppendEntries RPC reply.
    AppendEntriesReply {
        header: MessageHeader,

        /// Instead of replying a boolean `success` as defined in the Raft paper,
        /// this crate replies the last log position of the follower.
        /// With this adjustment, the leader can quickly determine the appropriate match index of the follower,
        /// even if the follower is significantly behind the leader.
        last_position: LogPosition,
    },
}

impl Message {
    pub fn term(&self) -> Term {
        match self {
            Self::RequestVoteCall { header, .. } => header.term,
            Self::RequestVoteReply { header, .. } => header.term,
            Self::AppendEntriesCall { header, .. } => header.term,
            Self::AppendEntriesReply { header, .. } => header.term,
        }
    }

    pub fn from(&self) -> NodeId {
        match self {
            Self::RequestVoteCall { header, .. } => header.from,
            Self::RequestVoteReply { header, .. } => header.from,
            Self::AppendEntriesCall { header, .. } => header.from,
            Self::AppendEntriesReply { header, .. } => header.from,
        }
    }

    pub fn seqno(&self) -> MessageSeqNo {
        match self {
            Self::RequestVoteCall { header, .. } => header.seqno,
            Self::RequestVoteReply { header, .. } => header.seqno,
            Self::AppendEntriesCall { header, .. } => header.seqno,
            Self::AppendEntriesReply { header, .. } => header.seqno,
        }
    }

    pub fn request_vote_call(
        term: Term,
        from: NodeId,
        seqno: MessageSeqNo,
        last_position: LogPosition,
    ) -> Self {
        Self::RequestVoteCall {
            header: MessageHeader { term, from, seqno },
            last_position,
        }
    }

    pub fn request_vote_reply(
        term: Term,
        from: NodeId,
        seqno: MessageSeqNo,
        vote_granted: bool,
    ) -> Self {
        Self::RequestVoteReply {
            header: MessageHeader { from, term, seqno },
            vote_granted,
        }
    }

    pub fn append_entries_call(
        term: Term,
        from: NodeId,
        leader_commit: LogIndex,
        seqno: MessageSeqNo,
        entries: LogEntries,
    ) -> Self {
        Self::AppendEntriesCall {
            header: MessageHeader { from, term, seqno },
            leader_commit,
            entries,
        }
    }

    pub fn append_entries_reply(
        term: Term,
        from: NodeId,
        seqno: MessageSeqNo,
        last_entry: LogPosition,
    ) -> Self {
        Self::AppendEntriesReply {
            header: MessageHeader { term, from, seqno },
            last_position: last_entry,
        }
    }

    // TODO: test
    pub(crate) fn merge(&mut self, other: Self) {
        let Self::AppendEntriesCall {
            header: header0,
            leader_commit: leader_commit0,
            entries: entries0,
        } = self
        else {
            *self = other;
            return;
        };
        let Self::AppendEntriesCall {
            header: header1,
            leader_commit: leader_commit1,
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
}

/// Common header for all messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MessageHeader {
    /// Sender of the message.
    pub from: NodeId,

    /// Term of the sender.
    pub term: Term,

    /// Sequence number of the message.
    pub seqno: MessageSeqNo,
}

/// Message sequence number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MessageSeqNo(u64);

impl MessageSeqNo {
    pub(crate) const UNKNOWN: Self = Self(0);
    pub(crate) const INIT: Self = Self(1);

    /// Makes a new [`MessageSeqNo`] instance.
    pub const fn new(seqno: u64) -> Self {
        Self(seqno)
    }

    /// Returns the value of the sequence number.
    pub const fn get(self) -> u64 {
        self.0
    }

    pub(crate) const fn next(self) -> Self {
        Self(self.0 + 1)
    }

    pub(crate) fn fetch_and_increment(&mut self) -> Self {
        let v = *self;
        self.0 += 1;
        v
    }
}
