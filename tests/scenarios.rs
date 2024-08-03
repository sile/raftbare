use raftbare::node::{Node, NodeId};

#[test]
fn single_node_start() {
    let _node = Node::start(id(0));
}

const fn id(id: u64) -> NodeId {
    NodeId::new(id)
}
