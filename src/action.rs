use crate::{log::LogEntry, node::NodeId, Term};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Action {
    SaveCurrentTerm(Term),
    SaveVotedFor(NodeId),
    AppendLogEntries(AppendLogEntriesAction),
    TruncateLog,
    InstallSnapshot,
    UnicastMessage,
    BroadcastMessage,
    NotifyCommited,
    SetTimeout,
}

impl Action {
    pub fn append_log_entry(entry: LogEntry) -> Self {
        Self::AppendLogEntries(AppendLogEntriesAction::Signle(entry))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AppendLogEntriesAction {
    Signle(LogEntry),
    Multi(Vec<LogEntry>),
}

impl AppendLogEntriesAction {
    pub fn entries(&self) -> &[LogEntry] {
        match self {
            AppendLogEntriesAction::Signle(entry) => std::slice::from_ref(entry),
            AppendLogEntriesAction::Multi(entries) => entries,
        }
    }
}
