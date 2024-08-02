use crate::action::Action;
use std::{collections::VecDeque, sync::Arc};

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId(Arc<String>);

impl NodeId {
    pub fn new(id: &str) -> Self {
        NodeId(Arc::new(id.to_owned()))
    }

    pub fn get(&self) -> &str {
        &self.0
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
