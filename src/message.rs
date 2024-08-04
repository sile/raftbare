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
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AppendEntriesRequest {
    pub term: Term,
    pub leader_id: NodeId,
    pub leader_commit: LogIndex,
    pub entries: LogEntries,
}
