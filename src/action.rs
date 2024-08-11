use crate::{
    log::{LogEntries, LogEntry, LogIndex},
    message::Message,
    node::{Heartbeat, NodeId},
    Term,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Action {
    SetElectionTimeout,

    // Synchronous actions (if async, the consistency is not guaranteed)
    SaveCurrentTerm(Term),
    SaveVotedFor(Option<NodeId>),
    // SaveClusterConfig
    CreateLog(LogEntry),
    AppendLogEntries(LogEntries),
    //InstallSnapshot, // {LogEntries)

    // TODO: delete
    NotifyCommitted(LogIndex),
    NotifyHeartbeatSucceeded(Heartbeat),
    // NotifyLogTruncated or NotifyRejected or NotifyCanceled

    // Can drop this message especially if there is another ongoing AppendEntriesRPC
    BroadcastMessage(Message),
    UnicastMessage(NodeId, Message),
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
