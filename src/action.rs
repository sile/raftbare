use crate::{
    log::{LogEntries, LogIndex},
    message::Message,
    node::NodeId,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Action {
    SetElectionTimeout,

    // Synchronous actions (if async, the Raft properties are not guaranteed)
    SaveCurrentTerm,
    SaveVotedFor,
    AppendLogEntries(LogEntries),

    // TODO: delete
    NotifyCommitted(LogIndex),

    // Can drop this message especially if there is another ongoing AppendEntriesRPC
    BroadcastMessage(Message), // TODO: remove(?)
    UnicastMessage(NodeId, Message),
    InstallSnapshot(NodeId),
}

// TODO
//
// #[derive(Debug, Default, Clone)]
// pub struct Actions {
//     pub set_election_timeout: bool,
// }

// impl Iterator for Actions {
//     type Item = Action;

//     fn next(&mut self) -> Option<Self::Item> {
//         if self.set_election_timeout {
//             self.set_election_timeout = false;
//             return Some(Action::SetElectionTimeout);
//         }

//         None
//     }
// }
