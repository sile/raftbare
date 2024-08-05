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

    // Synchronous action (if async, the consistency is not guaranteed)
    AppendLogEntries(LogEntries),

    // Can drop this message especially if there is another ongoing AppendEntriesRPC
    UnicastMessage(NodeId, Message),

    BroadcastMessage(Message),
    SetElectionTimeout,
    InstallSnapshot, // {LogEntries)
    NotifyCommitted(LogIndex),
    // NotifyLogTruncated or NotifyRejected or NotifyCanceled
}
