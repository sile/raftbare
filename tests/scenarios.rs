use raftbare::{
    action::Action,
    config::ClusterConfig,
    log::{LogEntries, LogEntry, LogEntryRef, LogIndex},
    message::Message,
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
    assert_action!(node, create_log());
    assert_no_action!(node);
}

#[test]
fn create_single_node_cluster() {
    let mut node = Node::start(id(0));
    assert_action!(node, create_log());
    assert_no_action!(node);

    // Create cluster.
    assert!(node.create_cluster());
    assert_action!(node, save_current_term(t(1)));
    assert_action!(node, save_voted_for(Some(node.id())));
    assert_action!(node, append_log_entry(prev(t(0), i(0)), term_entry(t(1))));
    assert_action!(
        node,
        append_log_entry(prev(t(1), i(1)), cluster_config_entry(voters(&[node.id()])))
    );
    assert_action!(node, committed(i(2)));
    assert_no_action!(node);

    assert_eq!(node.role(), Role::Leader);
    assert_eq!(
        node.cluster_config().members().collect::<Vec<_>>(),
        &[node.id()]
    );
    assert_eq!(node.cluster_config().voters.len(), 1);
    assert_eq!(node.cluster_config().non_voters.len(), 0);
    assert_eq!(node.cluster_config().new_voters.len(), 0);

    // Cannot create cluster again.
    assert!(!node.create_cluster());
    assert_no_action!(node);
}

#[test]
fn create_two_nodes_cluster() {
    let mut node0 = Node::start(id(0));
    let mut node1 = Node::start(id(1));
    assert_eq!(node0.take_actions().count(), 1);
    assert_eq!(node1.take_actions().count(), 1);

    // Create single node cluster.
    assert!(node0.create_cluster());
    assert_eq!(node0.take_actions().count(), 5);

    // Update cluster configuration.
    let prev_entry = node0.log().last;
    let next_index = node0.log().last.index.next();
    let new_config = joint(&[node0.id()], &[node0.id(), node1.id()]);
    let msg = append_entries_request(
        node0.current_term(),
        node0.id(),
        node0.commit_index(),
        LogEntries::single(prev_entry, &cluster_config_entry(new_config.clone())),
    );
    assert_eq!(Ok(next_index), node0.change_cluster_config(&new_config));
    assert_action!(
        node0,
        append_log_entry(prev_entry, cluster_config_entry(new_config.clone()))
    );
    assert_action!(node0, broadcast_message(&msg));
    assert_action!(node0, set_election_timeout());
    assert_no_action!(node0);

    node1.handle_message(&msg);
    let reply = append_entries_reply(node1.current_term(), node1.id(), node1.log().last);
    assert_action!(node1, save_current_term(node0.current_term()));
    assert_action!(node1, save_voted_for(Some(node0.id())));
    assert_action!(node1, set_election_timeout());
    assert_action!(node1, unicast_message(node0.id(), &reply));
    assert_no_action!(node1);

    node0.handle_message(&reply);
    let entries = entries(
        prev(t(0), i(0)), // == node1.log().last
        &[
            term_entry(t(1)),
            cluster_config_entry(voters(&[node0.id()])),
            cluster_config_entry(joint(&[node0.id()], &[node0.id(), node1.id()])),
        ],
    );
    let msg = append_entries_request(
        node0.current_term(),
        node0.id(),
        node0.commit_index(),
        entries.clone(),
    );
    assert_action!(node0, unicast_message(node1.id(), &msg));
    assert_no_action!(node0);

    node1.handle_message(&msg);
    assert_eq!(node1.log().last, node0.log().last);
    let reply = append_entries_reply(node1.current_term(), node1.id(), node1.log().last);
    assert_action!(node1, append_log_entries(&entries));
    assert_action!(node1, committed(i(2)));
    assert_action!(node1, unicast_message(node0.id(), &reply));
    assert_no_action!(node1);

    let new_config = voters(&[node0.id(), node1.id()]);
    let prev_entry = node0.log().last;
    let request = append_entries_request(
        node0.current_term(),
        node0.id(),
        i(3),
        LogEntries::single(prev_entry, &cluster_config_entry(new_config.clone())),
    );
    node0.handle_message(&reply);
    assert_action!(node0, committed(i(3)));
    assert_action!(
        node0,
        append_log_entry(prev_entry, cluster_config_entry(new_config.clone()))
    );
    assert_action!(node0, broadcast_message(&request));
    assert_action!(node0, set_election_timeout());
    assert_action!(node0, unicast_message(node1.id(), &request)); // TODO: Remove this redundant action if possible.
    assert_no_action!(node0);

    let reply = append_entries_reply(node1.current_term(), node1.id(), node1.log().last.next());
    node1.handle_message(&request);
    assert_action!(
        node1,
        append_log_entry(prev_entry, cluster_config_entry(new_config.clone()))
    );
    assert_action!(node1, committed(i(3)));
    assert_action!(node1, unicast_message(node0.id(), &reply));
    assert_no_action!(node1);

    assert!(!node0.cluster_config().is_joint_consensus());
    assert_eq!(node0.cluster_config(), node1.cluster_config());
}

fn entries(prev: LogEntryRef, entries: &[LogEntry]) -> LogEntries {
    let mut log = LogEntries::new(prev);
    for entry in entries {
        log.append_entry(entry);
    }
    log
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

fn joint(old: &[NodeId], new: &[NodeId]) -> ClusterConfig {
    let mut config = ClusterConfig::new();
    config.voters.extend(old.iter().copied());
    config.new_voters.extend(new.iter().copied());
    config
}

fn voters(ids: &[NodeId]) -> ClusterConfig {
    let mut config = ClusterConfig::new();
    config.voters.extend(ids.iter().copied());
    config
}

fn term_entry(term: Term) -> LogEntry {
    LogEntry::Term(term)
}

fn cluster_config_entry(config: ClusterConfig) -> LogEntry {
    LogEntry::ClusterConfig(config)
}

fn create_log() -> Action {
    Action::CreateLog(LogEntry::Term(t(0)))
}

fn append_entries_request(
    term: Term,
    leader_id: NodeId,
    commit_index: LogIndex,
    entries: LogEntries,
) -> Message {
    Message::append_entries_request(term, leader_id, commit_index, entries)
}

fn append_entries_reply(term: Term, from: NodeId, entry: LogEntryRef) -> Message {
    Message::append_entries_reply(term, from, entry)
}

fn unicast_message(destination: NodeId, message: &Message) -> Action {
    Action::UnicastMessage(destination, message.clone())
}

fn broadcast_message(message: &Message) -> Action {
    Action::BroadcastMessage(message.clone())
}

fn set_election_timeout() -> Action {
    Action::SetElectionTimeout
}

fn append_log_entry(prev: LogEntryRef, entry: LogEntry) -> Action {
    Action::AppendLogEntries(LogEntries::single(prev, &entry))
}

fn append_log_entries(entries: &LogEntries) -> Action {
    Action::AppendLogEntries(entries.clone())
}

fn save_current_term(term: Term) -> Action {
    Action::SaveCurrentTerm(term)
}

fn save_voted_for(voted_for: Option<NodeId>) -> Action {
    Action::SaveVotedFor(voted_for)
}

fn committed(index: LogIndex) -> Action {
    Action::NotifyCommitted(index)
}
