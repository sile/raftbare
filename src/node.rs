use crate::action::Action;
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

#[derive(Debug)]
pub struct Node {
    id: NodeId,
    action_queue: VecDeque<Action>,
}

impl Node {
    pub fn new(id: NodeId) -> Self {
        Self {
            id,
            action_queue: VecDeque::new(),
        }
    }

    pub fn id(&self) -> &NodeId {
        &self.id
    }

    pub fn next_action(&mut self) -> Option<Action> {
        self.action_queue.pop_front()
    }
}
