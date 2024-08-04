use crate::{
    log::{LogEntries, LogEntryRef, LogIndex},
    node::NodeId,
    Term,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Message {
    RequestVoteRequest { from: NodeId, term: Term },
    AppendEntriesRequest(AppendEntriesRequest),
    AppendEntriesReply(AppendEntriesReply),
}

impl Message {
    pub fn term(&self) -> Term {
        match self {
            Self::RequestVoteRequest { term, .. } => *term,
            Self::AppendEntriesRequest(m) => m.term,
            Self::AppendEntriesReply(m) => m.term,
        }
    }

    pub fn from(&self) -> NodeId {
        match self {
            Self::RequestVoteRequest {
                from: sender_id, ..
            } => *sender_id,
            Self::AppendEntriesRequest(m) => m.from,
            Self::AppendEntriesReply(m) => m.from,
        }
    }

    pub fn append_entries_request(
        term: Term,
        leader_id: NodeId,
        leader_commit: LogIndex,
        entries: LogEntries,
    ) -> Self {
        Self::AppendEntriesRequest(AppendEntriesRequest {
            term,
            from: leader_id,
            leader_commit,
            entries,
        })
    }

    pub fn append_entries_reply(
        term: Term,
        from: NodeId,
        last_entry: LogEntryRef,
        success: bool,
    ) -> Self {
        Self::AppendEntriesReply(AppendEntriesReply {
            term,
            from,
            last_entry,
            success,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AppendEntriesRequest {
    pub term: Term,
    pub from: NodeId,
    pub leader_commit: LogIndex,
    pub entries: LogEntries,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AppendEntriesReply {
    pub term: Term,
    pub from: NodeId,
    pub last_entry: LogEntryRef,
    pub success: bool,
}
