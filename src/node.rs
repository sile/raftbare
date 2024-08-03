use crate::{
    action::Action,
    event::Event,
    log::{LogEntries, LogEntry, LogEntryRef, LogIndex},
    Term,
};
use std::collections::VecDeque;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId(u64);

impl NodeId {
    pub const fn new(id: u64) -> Self {
        NodeId(id)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone)]
pub struct Node {
    id: NodeId,
    action_queue: VecDeque<Action>,
    role: Role,
    voted_for: Option<NodeId>,
    current_term: Term,
    log: LogEntries,
}

impl Node {
    pub fn start(id: NodeId) -> Self {
        let term = Term::new(0);
        let index = LogIndex::new(0);
        let mut this = Self {
            id,
            action_queue: VecDeque::new(),
            role: Role::Follower,
            voted_for: None,
            current_term: term,
            log: LogEntries::new(LogEntryRef::new(term, index)),
        };
        this.enqueue_action(Action::append_log_entry(
            this.log.last,
            LogEntry::Term(term),
        ));
        this
    }

    // TODO: restart

    pub fn create_cluster(&mut self) -> bool {
        if self.current_term != Term::new(0) {
            return false;
        }

        true
    }

    pub fn id(&self) -> NodeId {
        self.id
    }

    pub fn role(&self) -> Role {
        self.role
    }

    pub fn voted_for(&self) -> Option<NodeId> {
        self.voted_for
    }

    pub fn current_term(&self) -> Term {
        self.current_term
    }

    pub fn log(&self) -> &LogEntries {
        &self.log
    }

    pub fn handle_event(&mut self, _event: Event) {}

    pub fn next_action(&mut self) -> Option<Action> {
        self.action_queue.pop_front()
    }

    fn enqueue_action(&mut self, action: Action) {
        self.action_queue.push_back(action);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}
