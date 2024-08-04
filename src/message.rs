use crate::{
    log::{LogEntries, LogIndex},
    node::NodeId,
    Term,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Message {
    RequestVoteRequest { sender_id: NodeId, term: Term },
    AppendEntriesRequest(AppendEntriesRequest),
}

impl Message {
    pub fn term(&self) -> Term {
        match self {
            Self::RequestVoteRequest { term, .. } => *term,
            Self::AppendEntriesRequest(m) => m.term,
        }
    }

    pub fn from(&self) -> NodeId {
        match self {
            Self::RequestVoteRequest { sender_id, .. } => *sender_id,
            Self::AppendEntriesRequest(m) => m.leader_id,
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
            leader_id,
            leader_commit,
            entries,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AppendEntriesRequest {
    pub term: Term,
    pub leader_id: NodeId, // TODO: rename
    pub leader_commit: LogIndex,
    pub entries: LogEntries,
}
