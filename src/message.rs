use crate::{
    log::{LogEntries, LogIndex},
    node::NodeId,
    Term,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Message {
    AppendEntriesRequest(AppendEntriesRequest),
}

impl Message {
    pub fn term(&self) -> Term {
        match self {
            Message::AppendEntriesRequest(m) => m.term,
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
    pub leader_id: NodeId,
    pub leader_commit: LogIndex,
    pub entries: LogEntries,
}
