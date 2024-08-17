use crate::{log::LogEntries, message::Message, node::NodeId};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Action {
    SetElectionTimeout,

    // Synchronous actions (if async, the Raft properties are not guaranteed)
    SaveCurrentTerm,
    SaveVotedFor,
    AppendLogEntries(LogEntries),

    // Can drop this message especially if there is another ongoing AppendEntriesRPC
    BroadcastMessage(Message), // TODO: remove(?)
    SendMessage(NodeId, Message),
    InstallSnapshot(NodeId),
}

#[derive(Debug, Default, Clone)]
pub struct Actions {
    pub set_election_timeout: bool,
    pub save_current_term: bool,
    pub save_voted_for: bool,
    pub append_log_entries: Option<LogEntries>,
    pub send_messages: BTreeMap<NodeId, Message>,
    pub install_snapshot: BTreeSet<NodeId>,
}

impl Iterator for Actions {
    type Item = Action;

    fn next(&mut self) -> Option<Self::Item> {
        if self.set_election_timeout {
            self.set_election_timeout = false;
            return Some(Action::SetElectionTimeout);
        }
        if self.save_current_term {
            self.save_current_term = false;
            return Some(Action::SaveCurrentTerm);
        }
        if self.save_voted_for {
            self.save_voted_for = false;
            return Some(Action::SaveVotedFor);
        }
        if let Some(log_entries) = self.append_log_entries.take() {
            return Some(Action::AppendLogEntries(log_entries));
        }
        if let Some((node_id, message)) = self.send_messages.pop_first() {
            return Some(Action::SendMessage(node_id, message));
        }
        if let Some(node_id) = self.install_snapshot.pop_first() {
            return Some(Action::InstallSnapshot(node_id));
        }
        None
    }
}
