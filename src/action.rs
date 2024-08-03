use crate::{
    log::{LogEntries, LogEntry},
    node::NodeId,
    Term,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Action {
    SaveCurrentTerm(Term),
    SaveVotedFor(Option<NodeId>),
    CreateLog(LogEntry),
    AppendLogEntries(LogEntries),
    InstallSnapshot,
    UnicastMessage,
    BroadcastMessage,
    SetTimeout,
    NotifyCommited,
}
