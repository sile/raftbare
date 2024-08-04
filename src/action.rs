use crate::{
    log::{LogEntries, LogEntry, LogIndex},
    message::Message,
    node::NodeId,
    Term,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Action {
    SaveCurrentTerm(Term),
    SaveVotedFor(Option<NodeId>),
    CreateLog(LogEntry),
    AppendLogEntries(LogEntries),
    UnicastMessage(NodeId, Message),
    BroadcastMessage(Message),
    SetTimeout,
    InstallSnapshot, // {LogEntries)
    NotifyCommitted(LogIndex),
    // NotifyLogTruncated or NotifyRejected or NotifyCanceled
}
