use crate::{
    log::{LogEntries, LogIndex, LogPosition},
    node::NodeId,
    Term,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Message {
    RequestVoteRequest {
        header: MessageHeader,
        last_position: LogPosition,
    },
    RequestVoteReply {
        header: MessageHeader,
        vote_granted: bool,
    },
    AppendEntriesRequest(AppendEntriesRequest),
    AppendEntriesReply(AppendEntriesReply),
}

impl Message {
    pub fn term(&self) -> Term {
        match self {
            Self::RequestVoteRequest { header, .. } => header.term,
            Self::RequestVoteReply { header, .. } => header.term,
            Self::AppendEntriesRequest(m) => m.header.term,
            Self::AppendEntriesReply(m) => m.header.term,
        }
    }

    pub fn from(&self) -> NodeId {
        match self {
            Self::RequestVoteRequest { header, .. } => header.from,
            Self::RequestVoteReply { header, .. } => header.from,
            Self::AppendEntriesRequest(m) => m.header.from,
            Self::AppendEntriesReply(m) => m.header.from,
        }
    }

    pub fn seqno(&self) -> MessageSeqNo {
        match self {
            Self::RequestVoteRequest { header, .. } => header.seqno,
            Self::RequestVoteReply { header, .. } => header.seqno,
            Self::AppendEntriesRequest(m) => m.header.seqno,
            Self::AppendEntriesReply(m) => m.header.seqno,
        }
    }

    pub fn request_vote_request(
        term: Term,
        from: NodeId,
        seqno: MessageSeqNo,
        last_position: LogPosition,
    ) -> Self {
        Self::RequestVoteRequest {
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

    pub fn append_entries_request(
        term: Term,
        from: NodeId,
        leader_commit: LogIndex,
        seqno: MessageSeqNo,
        entries: LogEntries,
    ) -> Self {
        Self::AppendEntriesRequest(AppendEntriesRequest {
            header: MessageHeader { from, term, seqno },
            leader_commit,
            entries,
        })
    }

    pub fn append_entries_reply(
        term: Term,
        from: NodeId,
        seqno: MessageSeqNo,
        last_entry: LogPosition,
    ) -> Self {
        Self::AppendEntriesReply(AppendEntriesReply {
            header: MessageHeader { term, from, seqno },
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
        req0.header = req1.header;
        req0.leader_commit = req1.leader_commit;
        if req0.entries.contains(req1.entries.prev_position()) {
            req0.entries.append(&req1.entries);
        } else {
            req0.entries = req1.entries;
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MessageHeader {
    pub from: NodeId,
    pub term: Term,
    pub seqno: MessageSeqNo,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
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

    pub const fn prev(self) -> Self {
        Self(self.0 - 1)
    }

    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }

    pub fn fetch_and_increment(&mut self) -> Self {
        let v = *self;
        self.0 += 1;
        v
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AppendEntriesRequest {
    pub header: MessageHeader,
    pub leader_commit: LogIndex,
    pub entries: LogEntries,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AppendEntriesReply {
    pub header: MessageHeader,
    pub last_entry: LogPosition,
}
