use std::ops::{Deref, DerefMut};

use raftbare::{
    action::Action,
    config::ClusterConfig,
    log::{LogEntries, LogEntry, LogEntryRef, LogIndex},
    message::{AppendEntriesRequest, Message},
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
    TestNode::asserted_start(id(0));
}

#[test]
fn create_single_node_cluster() {
    let mut node = TestNode::asserted_start(id(0));

    // Create cluster.
    node.asserted_create_cluster();

    // Cannot create cluster again.
    assert!(!node.inner.create_cluster());
    assert_no_action!(node.inner);
}

#[test]
fn create_two_nodes_cluster() {
    let mut node0 = TestNode::asserted_start(id(0));
    let mut node1 = TestNode::asserted_start(id(1));

    // Create single node cluster.
    node0.asserted_create_cluster();

    // Update cluster configuration.
    let request =
        node0.asserted_change_cluster_config(joint(&[node0.id()], &[node0.id(), node1.id()]));
    let reply = node1.asserted_handle_first_append_entries_request(&request);

    let request = node0.asserted_handle_append_entries_reply_failure(&reply);
    let reply = node1.asserted_handle_append_entries_request_success(&request);

    let request =
        node0.asserted_handle_append_entries_reply_success_with_joint_config_committed(&reply);
    let reply = node1.asserted_handle_append_entries_request_success(&request);
    node0.asserted_handle_append_entries_reply_success(&reply, true);

    assert!(!node0.cluster_config().is_joint_consensus());
    assert_eq!(node0.cluster_config(), node1.cluster_config());
}

#[test]
fn create_three_nodes_cluster() {
    let mut node0 = TestNode::asserted_start(id(0));
    let mut node1 = TestNode::asserted_start(id(1));
    let mut node2 = TestNode::asserted_start(id(2));

    // Create single node cluster.
    node0.asserted_create_cluster();

    // Update cluster configuration.
    let request = node0.asserted_change_cluster_config(joint(
        &[node0.id()],
        &[node0.id(), node1.id(), node2.id()],
    ));

    for node in &mut [&mut node1, &mut node2] {
        let reply = node0.asserted_handle_first_append_entries_request(&request);
        let request = node0.asserted_handle_append_entries_reply_failure(&reply);
        let reply = node.asserted_handle_append_entries_request_success(&request);
        if node.id() == id(1) {
            let request = node0
                .asserted_handle_append_entries_reply_success_with_joint_config_committed(&reply);
            let reply = node.asserted_handle_append_entries_request_success(&request);
            node0.asserted_handle_append_entries_reply_success(&reply, true);
        } else {
            node0.asserted_handle_append_entries_reply_success(&reply, false);
        }
    }

    assert!(!node0.cluster_config().is_joint_consensus());
    assert_eq!(node0.cluster_config(), node1.cluster_config());
    assert_eq!(node0.cluster_config(), node2.cluster_config());
}

// TODO: election
// TODO: snapshot

#[derive(Debug)]
struct TestNode {
    inner: Node,
}

impl TestNode {
    fn asserted_start(id: NodeId) -> Self {
        let mut node = Node::start(id);
        assert_eq!(node.role(), Role::Follower);
        assert_eq!(node.current_term(), t(0));
        assert_eq!(node.voted_for(), None);
        assert_action!(node, create_log());
        assert_no_action!(node);
        Self { inner: node }
    }

    fn asserted_create_cluster(&mut self) {
        assert!(self.create_cluster());

        assert_action!(self, save_current_term(t(1)));
        assert_action!(self, save_voted_for(Some(self.id())));
        assert_action!(self, append_log_entry(prev(t(0), i(0)), term_entry(t(1))));
        assert_action!(
            self,
            append_log_entry(prev(t(1), i(1)), cluster_config_entry(voters(&[self.id()])))
        );
        assert_action!(self, committed(i(2)));
        assert_no_action!(self);

        assert_eq!(self.role(), Role::Leader);
        assert_eq!(
            self.cluster_config().members().collect::<Vec<_>>(),
            &[self.id()]
        );
        assert_eq!(self.cluster_config().voters.len(), 1);
        assert_eq!(self.cluster_config().non_voters.len(), 0);
        assert_eq!(self.cluster_config().new_voters.len(), 0);
    }

    fn asserted_change_cluster_config(&mut self, new_config: ClusterConfig) -> Message {
        let prev_entry = self.log().last;
        let next_index = self.log().last.index.next();
        let msg = append_entries_request(
            self.current_term(),
            self.id(),
            self.commit_index(),
            LogEntries::single(prev_entry, &cluster_config_entry(new_config.clone())),
        );
        assert_eq!(Ok(next_index), self.change_cluster_config(&new_config));
        assert_action!(
            self,
            append_log_entry(prev_entry, cluster_config_entry(new_config.clone()))
        );
        assert_action!(self, broadcast_message(&msg));
        assert_action!(self, set_election_timeout());
        assert_no_action!(self);

        msg
    }

    fn asserted_handle_first_append_entries_request(&mut self, msg: &Message) -> Message {
        assert!(matches!(msg, Message::AppendEntriesRequest(_)));

        self.handle_message(msg);
        let reply = append_entries_reply(self.current_term(), self.id(), self.log().last);
        assert_action!(self, save_current_term(msg.term()));
        assert_action!(self, save_voted_for(Some(msg.from())));
        assert_action!(self, set_election_timeout());
        assert_action!(self, unicast_message(msg.from(), &reply));
        assert_no_action!(self);

        reply
    }

    fn asserted_handle_append_entries_request_success(&mut self, msg: &Message) -> Message {
        assert!(matches!(msg, Message::AppendEntriesRequest(_)));

        self.handle_message(msg);
        let Message::AppendEntriesRequest(AppendEntriesRequest {
            entries,
            leader_commit,
            ..
        }) = msg
        else {
            unreachable!();
        };
        assert_eq!(self.log().last, entries.last);

        let reply = append_entries_reply(self.current_term(), self.id(), self.log().last);
        assert_action!(self, append_log_entries(&entries));
        assert_action!(self, committed(*leader_commit));
        assert_action!(self, unicast_message(msg.from(), &reply));
        assert_no_action!(self);

        reply
    }

    fn asserted_handle_append_entries_reply_failure(&mut self, reply: &Message) -> Message {
        assert!(matches!(reply, Message::AppendEntriesReply(_)));
        self.handle_message(reply);

        let Message::AppendEntriesReply(reply) = reply else {
            unreachable!();
        };
        let Some(entries) = self.log().since(reply.last_entry) else {
            panic!("Needs snapshot");
        };

        let request = append_entries_request(
            self.current_term(),
            self.id(),
            self.commit_index(),
            entries.clone(),
        );
        assert_action!(self, unicast_message(reply.from, &request));
        assert_no_action!(self);

        request
    }

    fn asserted_handle_append_entries_reply_success_with_joint_config_committed(
        &mut self,
        reply: &Message,
    ) -> Message {
        assert!(matches!(reply, Message::AppendEntriesReply(_)));
        assert!(self.cluster_config().is_joint_consensus());

        let prev_entry = self.log().last;
        let mut new_config = self.cluster_config().clone();
        new_config.voters = std::mem::take(&mut new_config.new_voters);

        self.handle_message(reply);

        let Message::AppendEntriesReply(reply) = reply else {
            unreachable!();
        };
        let request = append_entries_request(
            self.current_term(),
            self.id(),
            reply.last_entry.index,
            LogEntries::single(prev_entry, &cluster_config_entry(new_config.clone())),
        );
        assert_action!(self, committed(reply.last_entry.index));
        assert_action!(
            self,
            append_log_entry(prev_entry, cluster_config_entry(new_config.clone()))
        );
        assert_action!(self, broadcast_message(&request));
        assert_action!(self, set_election_timeout());
        assert_no_action!(self);

        request
    }

    fn asserted_handle_append_entries_reply_success(
        &mut self,
        reply: &Message,
        commit_index_will_be_updated: bool,
    ) {
        assert!(matches!(reply, Message::AppendEntriesReply(_)));
        self.handle_message(reply);

        let Message::AppendEntriesReply(reply) = reply else {
            unreachable!();
        };
        if commit_index_will_be_updated {
            assert_action!(self, committed(reply.last_entry.index));
        }
        assert_no_action!(self);
    }
}

impl Deref for TestNode {
    type Target = Node;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for TestNode {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
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
