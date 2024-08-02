use crate::{node::NodeId, Term};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Action {
    SaveCurrentTerm(Term),
    SaveVotedFor(NodeId),
    AppendLogEntries,
    TruncateLog,
    InstallSnapshot,
    UnicastMessage,
    BroadcastMessage,
    NotifyCommited,
    SetTimeout,
}
