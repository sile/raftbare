use crate::{action::Action, event::Event, log::Log};
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
    log: Log,
}

impl Node {
    pub fn start(id: NodeId) -> Self {
        Self {
            id,
            action_queue: VecDeque::new(),
            role: Role::Follower,
            log: Log::new(),
        }
    }

    // TODO: restart

    pub fn create_cluster(&mut self) -> bool {
        true
    }

    pub fn id(&self) -> NodeId {
        self.id
    }

    pub fn role(&self) -> Role {
        self.role
    }

    pub fn log(&self) -> &Log {
        &self.log
    }

    pub fn handle_event(&mut self, _event: Event) {}

    pub fn next_action(&mut self) -> Option<Action> {
        self.action_queue.pop_front()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}
