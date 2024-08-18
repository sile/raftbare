use crate::{
    log::{LogEntries, LogIndex, LogPosition},
    node::NodeId,
    Term,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Message {
    RequestVoteRequest(RequestVoteRequest),
    RequestVoteReply(RequestVoteReply),
    AppendEntriesRequest(AppendEntriesRequest),
    AppendEntriesReply(AppendEntriesReply),
}

impl Message {
    pub fn term(&self) -> Term {
        match self {
            Self::RequestVoteRequest(m) => m.term,
            Self::RequestVoteReply(m) => m.term,
            Self::AppendEntriesRequest(m) => m.term,
            Self::AppendEntriesReply(m) => m.term,
        }
    }

    pub fn from(&self) -> NodeId {
        match self {
            Self::RequestVoteRequest(m) => m.from,
            Self::RequestVoteReply(m) => m.from,
            Self::AppendEntriesRequest(m) => m.from,
            Self::AppendEntriesReply(m) => m.from,
        }
    }

    pub fn request_vote_request(
        term: Term,
        candidate_id: NodeId,
        last_position: LogPosition,
    ) -> Self {
        Self::RequestVoteRequest(RequestVoteRequest {
            term,
            from: candidate_id,
            last_entry: last_position,
        })
    }

    pub fn request_vote_reply(term: Term, from: NodeId, vote_granted: bool) -> Self {
        Self::RequestVoteReply(RequestVoteReply {
            term,
            from,
            vote_granted,
        })
    }

    pub fn append_entries_request(
        term: Term,
        leader_id: NodeId,
        leader_commit: LogIndex,
        leader_sn: MessageSeqNo,
        entries: LogEntries,
    ) -> Self {
        Self::AppendEntriesRequest(AppendEntriesRequest {
            term,
            from: leader_id,
            leader_commit,
            leader_sn,
            entries,
        })
    }

    pub fn append_entries_reply(
        term: Term,
        from: NodeId,
        leader_sn: MessageSeqNo,
        last_entry: LogPosition,
    ) -> Self {
        Self::AppendEntriesReply(AppendEntriesReply {
            term,
            from,
            leader_sn,
            last_entry,
        })
    }

    // TODO: test
    pub(crate) fn merge(&mut self, other: Self) {
        let Self::AppendEntriesRequest(req0) = self else {
            *self = other;
            return;
        };
        let Self::AppendEntriesRequest(req1) = other else {
            *self = other;
            return;
        };
        req0.term = req1.term;
        req0.leader_commit = req1.leader_commit;
        req0.leader_sn = req1.leader_sn;
        if req0.entries.contains(req1.entries.prev_position()) {
            req0.entries.append(&req1.entries);
        } else {
            req0.entries = req1.entries;
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MessageSeqNo(u64);

impl MessageSeqNo {
    pub const fn new() -> Self {
        Self(1)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub const fn from_u64(v: u64) -> Self {
        Self(v)
    }

    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RequestVoteRequest {
    pub term: Term,
    pub from: NodeId,
    pub last_entry: LogPosition,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RequestVoteReply {
    pub term: Term,
    pub from: NodeId,
    pub vote_granted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AppendEntriesRequest {
    pub term: Term,
    pub from: NodeId,
    pub leader_commit: LogIndex,
    pub leader_sn: MessageSeqNo,
    pub entries: LogEntries,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AppendEntriesReply {
    pub term: Term,
    pub from: NodeId,
    pub leader_sn: MessageSeqNo,
    pub last_entry: LogPosition,
}

// TODO: MessageHeader
