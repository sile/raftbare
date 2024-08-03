use crate::{
    log::{LogEntries, LogEntry, LogIndex},
    node::NodeId,
    Term,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Action {
    SaveCurrentTerm(Term),
    SaveVotedFor(Option<NodeId>),
    CreateLog(LogEntry),
    AppendLogEntries(LogEntries),
    InstallSnapshot, // {LogEntries)
    UnicastMessage,
    BroadcastMessage,
    SetTimeout,
    NotifyCommitted(LogIndex),
    // NotifyLogTruncated
}
