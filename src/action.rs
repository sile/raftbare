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
    BroadcastMessage(Message),
    SendMessage(NodeId, Message),
    InstallSnapshot(NodeId),
}

#[derive(Debug, Default, Clone)]
pub struct Actions {
    pub set_election_timeout: bool,
    pub save_current_term: bool,
    pub save_voted_for: bool,
    pub append_log_entries: Option<LogEntries>,
    pub broadcast_message: Option<Message>,
    pub send_messages: BTreeMap<NodeId, Message>,
    pub install_snapshot: BTreeSet<NodeId>,
}

impl Actions {
    pub(crate) fn set(&mut self, action: Action) {
        match action {
            Action::SetElectionTimeout => self.set_election_timeout = true,
            Action::SaveCurrentTerm => self.save_current_term = true,
            Action::SaveVotedFor => self.save_voted_for = true,
            Action::AppendLogEntries(log_entries) => {
                // TODO: merge
                self.append_log_entries = Some(log_entries)
            }
            Action::BroadcastMessage(message) => {
                // TODO: merge
                self.broadcast_message = Some(message)
            }
            Action::SendMessage(node_id, message) => {
                // TODO: merge
                self.send_messages.insert(node_id, message);
            }
            Action::InstallSnapshot(node_id) => {
                self.install_snapshot.insert(node_id);
            }
        }
    }
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
        if let Some(broadcast_message) = self.broadcast_message.take() {
            return Some(Action::BroadcastMessage(broadcast_message));
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
