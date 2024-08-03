use crate::{
    log::{LogEntries, LogEntry, LogEntryRef},
    node::NodeId,
    Term,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Action {
    SaveCurrentTerm(Term),
    SaveVotedFor(NodeId),
    AppendLogEntries(LogEntries),
    InstallSnapshot,
    UnicastMessage,
    BroadcastMessage,
    NotifyCommited,
    SetTimeout,
}

impl Action {
    pub fn append_log_entry(prev: LogEntryRef, entry: LogEntry) -> Self {
        Self::AppendLogEntries(LogEntries::single(prev, entry))
    }
}
