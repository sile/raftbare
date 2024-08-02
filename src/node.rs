use std::sync::Arc;

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
}

impl Node {
    pub fn new(id: NodeId) -> Self {
        Self { id }
    }

    pub fn id(&self) -> &NodeId {
        &self.id
    }
}
