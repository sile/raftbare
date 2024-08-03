use raftbare::{
    action::Action,
    log::{LogEntries, LogEntry, LogEntryRef, LogIndex},
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

macro_rules! assert_last_action {
    ($node:expr, $action:expr) => {
        assert_action!($node, $action);
        assert_no_action!($node);
    };
}

#[test]
fn single_node_start() {
    let mut node = Node::start(id(0));
    assert_eq!(node.role(), Role::Follower);
    assert_last_action!(node, create_log());
}

#[test]
fn create_single_node_cluster() {
    let mut node = Node::start(id(0));
    assert_last_action!(node, create_log());

    // Create cluster.
    assert!(node.create_cluster());
    assert_action!(node, save_current_term(t(1)));
    assert_action!(node, save_voted_for(Some(node.id())));
    assert_last_action!(node, append_log_entry(prev(t(0), i(0)), term_entry(t(1))));

    // Cannot create cluster again.
    assert!(!node.create_cluster());
}

fn id(id: u64) -> NodeId {
    NodeId::new(id)
}

fn t(term: u64) -> Term {
    Term::new(term)
}

fn i(index: u64) -> LogIndex {
    LogIndex::new(index)
}

fn prev(term: Term, index: LogIndex) -> LogEntryRef {
    LogEntryRef::new(term, index)
}

fn term_entry(term: Term) -> LogEntry {
    LogEntry::Term(term)
}

fn create_log() -> Action {
    Action::CreateLog(LogEntry::Term(t(0)))
}

fn append_log_entry(prev: LogEntryRef, entry: LogEntry) -> Action {
    Action::AppendLogEntries(LogEntries::single(prev, entry))
}

fn save_current_term(term: Term) -> Action {
    Action::SaveCurrentTerm(term)
}

fn save_voted_for(voted_for: Option<NodeId>) -> Action {
    Action::SaveVotedFor(voted_for)
}
