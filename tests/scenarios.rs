use raftbare::{
    action::Action,
    log::LogEntry,
    node::{Node, NodeId, Role},
    Term,
};

macro_rules! assert_no_action {
    ($node:expr) => {
        assert_eq!($node.next_action(), None);
    };
}

macro_rules! assert_action {
    ($node:expr, $action:expr) => {
        assert_eq!($node.next_action(), Some($action));
    };
}

#[test]
fn single_node_start() {
    let mut node = Node::start(id(0));
    assert_eq!(node.role(), Role::Follower);
    assert_action!(node, append_log_entry_action(term_entry(t(0))));
    assert_no_action!(node);
}

fn id(id: u64) -> NodeId {
    NodeId::new(id)
}

fn t(term: u64) -> Term {
    Term::new(term)
}

fn append_log_entry_action(entry: LogEntry) -> Action {
    Action::append_log_entry(entry)
}

fn term_entry(term: Term) -> LogEntry {
    LogEntry::Term(term)
}
