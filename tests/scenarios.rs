use raftbare::node::{Node, NodeId, Role};

macro_rules! assert_no_action {
    ($node:expr) => {
        assert_eq!(None, $node.next_action());
    };
}

#[test]
fn single_node_start() {
    let mut node = Node::start(id(0));
    assert_eq!(node.role(), Role::Follower);
    assert_no_action!(node);
}

const fn id(id: u64) -> NodeId {
    NodeId::new(id)
}
