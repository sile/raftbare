use crate::{log::LogEntries, message::Message, node::NodeId};
use std::collections::{BTreeMap, BTreeSet};

/// [`Action`] represents the I/O operations for [`Node`](crate::Node) that crate users need to execute.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Action {
    /// Set an election timeout.
    ///
    /// When the timeout expires, please call [`Node::handle_election_timeout()`](crate::Node::handle_election_timeout).
    ///
    /// An existing timeout is cancelled when a new one is set.
    ///
    /// The user needs to set different timeouts for each node based on its [`Role`](crate::Role) as follows:
    /// - For [`Role::Leader`](crate::Role::Leader):
    ///   - When the timeout expires, the leader sends heartbeat messages to followers.
    ///   - To maintain the role of leader, the timeout should be shorter than the election timeouts of the followers.
    /// - For [`Role::Candidate`](crate::Role::Candidate):
    ///   - When the timeout expires, the candidate starts a new election.
    ///   - The timeout should have some randomness to avoid conflicts with other candidates.
    /// - For [`Role::Follower`](crate::Role::Follower):
    ///   - When the timeout expires, the follower starts a new election.
    ///   - The timeout should be longer than the heartbeat timeout of the leader.
    ///
    /// Note that the appropriate timeout values depend on the specific application.
    SetElectionTimeout,

    /// Save the current term ([`Node::current_term()`](crate::Node::current_term())) to persistent storage.
    ///
    /// To guarantee properties by the Raft algorithm, the value must be saved before responding to users or sending messages to other nodes.
    SaveCurrentTerm,

    /// Save the voted-for node ID ([`Node::voted_for()`](crate::Node::voted_for())) to persistent storage.
    ///
    /// To guarantee properties by the Raft algorithm, the value must be saved before responding to users or sending messages to other nodes.
    SaveVotedFor,

    /// Append log entries to the node-local log on persistent storage.
    ///
    /// Note that previously written log suffix entries may be overwritten by these new entries.
    /// In other words, the [`LogEntries::last_position().index`](crate::LogEntries::last_position) can be any value within the range from the start index to the end index of the local log.
    ///
    /// To guarantee properties by the Raft algorithm, the entries must be appended before responding to users or sending messages to other nodes.
    /// (However, because writing all log entries to persistent storage synchronously could be too costly, in reality, the entries are often written asynchronously.)
    AppendLogEntries(LogEntries),

    /// Broadcast a message to all other nodes ([`Node::peers()`](crate::Node::peers)).
    ///
    /// On the receiving side, the message is handled by [`Node::handle_message()`](crate::Node::handle_message).
    ///
    /// Unlike storage-related actions, this action can be executed asynchronously and can be discarded if the communication link is busy.
    BroadcastMessage(Message),

    /// Send a message to a specific node.
    ///
    /// On the receiving side, the message is handled by [`Node::handle_message()`](crate::Node::handle_message).
    ///
    /// Unlike storage-related actions, this action can be executed asynchronously and can be discarded if the communication link is busy.
    ///
    /// Additionally, if an AppendEntriesRPC contains too many entries to be sent in a single message,
    /// they can be safely truncated using [`LogEntries::truncate()`](crate::LogEntries::truncate) before sending the message.
    SendMessage(NodeId, Message),

    /// Install a snapshot on a specific node.
    ///
    /// The user is responsible for managing the details of sending and installing the snapshots.
    ///
    /// Note that once the snapshot installation is complete, the user needs to call [`Node::handle_snapshot_installed()`](crate::Node::handle_snapshot_installed).
    InstallSnapshot(NodeId),
}

// doc about pipeline
#[derive(Debug, Default, Clone)]
pub struct Actions {
    pub set_election_timeout: bool,
    pub save_current_term: bool,
    pub save_voted_for: bool,
    pub append_log_entries: Option<LogEntries>,
    pub broadcast_message: Option<Message>,
    pub send_messages: BTreeMap<NodeId, Message>,
    pub install_snapshots: BTreeSet<NodeId>,
}

impl Actions {
    pub(crate) fn set(&mut self, action: Action) {
        match action {
            Action::SetElectionTimeout => self.set_election_timeout = true,
            Action::SaveCurrentTerm => self.save_current_term = true,
            Action::SaveVotedFor => self.save_voted_for = true,
            Action::AppendLogEntries(log_entries) => {
                if let Some(existing) = &mut self.append_log_entries {
                    existing.append(&log_entries);
                } else {
                    self.append_log_entries = Some(log_entries);
                }
            }
            Action::BroadcastMessage(message) => {
                if let Some(existing) = &mut self.broadcast_message {
                    existing.merge(message);
                } else {
                    self.broadcast_message = Some(message);
                }
            }
            Action::SendMessage(node_id, message) => {
                if let Some(existing) = self.send_messages.get_mut(&node_id) {
                    existing.merge(message);
                } else {
                    self.send_messages.insert(node_id, message);
                }
            }
            Action::InstallSnapshot(node_id) => {
                self.install_snapshots.insert(node_id);
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
        if let Some(node_id) = self.install_snapshots.pop_first() {
            return Some(Action::InstallSnapshot(node_id));
        }
        None
    }
}
