use raftbare::{
    action::Action,
    config::ClusterConfig,
    log::{LogEntries, LogEntry, LogEntryRef, LogIndex},
    message::{AppendEntriesRequest, Message, SequenceNumber},
    node::{Heartbeat, Node, NodeId, Role},
    Term,
};
use std::ops::{Deref, DerefMut};

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
    let mut cluster = ThreeNodeCluster::new();
    cluster.init_cluster();

    assert!(!cluster.node0.cluster_config().is_joint_consensus());
    assert_eq!(
        cluster.node0.cluster_config(),
        cluster.node1.cluster_config()
    );
    assert_eq!(
        cluster.node0.cluster_config(),
        cluster.node2.cluster_config()
    );
}

#[test]
fn election() {
    let mut cluster = ThreeNodeCluster::new();
    cluster.init_cluster();

    // Trigger a new election.
    let request = cluster.node1.asserted_follower_election_timeout();
    let reply = cluster
        .node0
        .asserted_handle_request_vote_request_success(&request);

    let request = cluster
        .node1
        .asserted_handle_request_vote_reply_majority_vote_granted(&reply);
    let reply_from_node2 = cluster
        .node2
        .asserted_handle_append_entries_request_success_new_leader(&request);
    let reply_from_node0 = cluster
        .node0
        .asserted_handle_append_entries_request_success(&request);

    cluster
        .node1
        .asserted_handle_append_entries_reply_success(&reply_from_node0, true);
    cluster
        .node1
        .asserted_handle_append_entries_reply_success(&reply_from_node2, false);

    // Manual heartbeat.
    let (heartbeat, request) = cluster.node1.asserted_heartbeat();
    let reply = cluster
        .node0
        .asserted_handle_append_entries_request_success(&request);
    cluster.node1.handle_message(&reply);
    assert_action!(cluster.node1, Action::NotifyHeartbeatSucceeded(heartbeat));
    assert_no_action!(cluster.node1);

    // Periodic heartbeat.
    cluster.node1.handle_election_timeout();
    let request = append_entries_request(&cluster.node1, LogEntries::new(cluster.node1.log().last));
    assert_action!(cluster.node1, broadcast_message(&request));

    let reply = cluster
        .node2
        .asserted_handle_append_entries_request_success(&request);
    cluster.node1.handle_message(&reply);
    assert_no_action!(cluster.node1);
}

#[test]
fn restart() {
    let mut cluster = ThreeNodeCluster::new();
    cluster.init_cluster();
    cluster.propose_command();

    // Restart node1.
    assert_eq!(cluster.node1.role(), Role::Follower);
    cluster.node1.inner = Node::restart(
        cluster.node1.id(),
        cluster.node1.current_term(),
        cluster.node1.voted_for(),
        cluster.node1.cluster_config().clone(),
        cluster.node1.log().clone(),
    );

    cluster.propose_command();
}

#[test]
fn truncate_log() {
    let mut cluster = ThreeNodeCluster::new();
    cluster.init_cluster();
    cluster.propose_command();

    // Propose a command, but not broadcast the message.
    assert_eq!(cluster.node0.role(), Role::Leader);
    let Some(command_index) = cluster.node0.propose_command() else {
        unreachable!()
    };
    while let Some(_) = cluster.node0.next_action() {}

    // Make node2 the leader.
    let _request = cluster.node2.asserted_follower_election_timeout();
    let request = cluster.node2.asserted_candidate_election_timeout(); // Increase term.

    // The log index of node0 is greater than node2 => failed.
    cluster
        .node0
        .asserted_handle_request_vote_request_failed(&request);
    assert_eq!(cluster.node0.role(), Role::Follower);

    // The log index of node1 is equal to node2 => granted.
    let _ = cluster.node1.asserted_follower_election_timeout();
    let reply = cluster
        .node1
        .asserted_handle_request_vote_request_success(&request);
    let request = cluster
        .node2
        .asserted_handle_request_vote_reply_majority_vote_granted(&reply);
    assert_eq!(cluster.node2.role(), Role::Leader);

    // The uncommitted log entries on node0 are truncated.
    let reply = cluster
        .node0
        .asserted_handle_append_entries_request_success(&request);
    assert_ne!(
        Some(LogEntry::Command),
        cluster.node0.log().get_entry(command_index)
    );

    cluster
        .node2
        .asserted_handle_append_entries_reply_success(&reply, true);

    assert_no_action!(cluster.node0);
    assert_no_action!(cluster.node1);
    assert_no_action!(cluster.node2);
}

// TODO: snapshot

#[derive(Debug)]
struct ThreeNodeCluster {
    node0: TestNode,
    node1: TestNode,
    node2: TestNode,
}

impl ThreeNodeCluster {
    fn new() -> Self {
        Self {
            node0: TestNode::asserted_start(id(0)),
            node1: TestNode::asserted_start(id(1)),
            node2: TestNode::asserted_start(id(2)),
        }
    }

    fn init_cluster(&mut self) {
        // Create single node cluster.
        self.node0.asserted_create_cluster();

        // Update cluster configuration.
        let request = self.node0.asserted_change_cluster_config(joint(
            &[self.node0.id()],
            &[self.node0.id(), self.node1.id(), self.node2.id()],
        ));

        for node in &mut [&mut self.node1, &mut self.node2] {
            let reply = node.asserted_handle_first_append_entries_request(&request);
            let request = self
                .node0
                .asserted_handle_append_entries_reply_failure(&reply);
            let reply = node.asserted_handle_append_entries_request_success(&request);
            if node.id() == id(1) {
                let request = self
                    .node0
                    .asserted_handle_append_entries_reply_success_with_joint_config_committed(
                        &reply,
                    );
                let reply = node.asserted_handle_append_entries_request_success(&request);
                self.node0
                    .asserted_handle_append_entries_reply_success(&reply, true);
            } else {
                self.node0
                    .asserted_handle_append_entries_reply_success(&reply, false);
            }
        }
    }

    fn propose_command(&mut self) {
        let mut commit_index = None;
        let mut request = None;
        for node in &mut [&mut self.node0, &mut self.node1, &mut self.node2] {
            if node.role() != Role::Leader {
                continue;
            }
            commit_index = node.inner.propose_command();
            assert_action!(
                node.inner,
                append_log_entry(node.inner.log().last.prev(), LogEntry::Command)
            );
            let msg = append_entries_request(
                &node.inner,
                LogEntries::single(node.inner.log().last.prev(), &LogEntry::Command),
            );
            assert_action!(node.inner, broadcast_message(&msg));
            assert_action!(node.inner, set_election_timeout());
            assert_no_action!(node.inner);
            request = Some(msg);
            break;
        }

        let (Some(commit_index), Some(request)) = (commit_index, request) else {
            panic!("No leader found.");
        };

        let mut replies = Vec::new();
        for node in &mut [&mut self.node0, &mut self.node1, &mut self.node2] {
            if node.role() == Role::Leader {
                continue;
            }

            replies.push(node.asserted_handle_append_entries_request_success(&request));
        }

        let mut first = true;
        for node in &mut [&mut self.node0, &mut self.node1, &mut self.node2] {
            if node.role() != Role::Leader {
                continue;
            }

            for reply in replies {
                node.asserted_handle_append_entries_reply_success(&reply, first);
                assert_eq!(node.commit_index(), commit_index);
                first = false;
            }
            break;
        }
    }
}

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
        assert_eq!(Ok(next_index), self.change_cluster_config(&new_config));
        let msg = append_entries_request(
            self,
            LogEntries::single(prev_entry, &cluster_config_entry(new_config.clone())),
        );

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
        let reply = append_entries_reply(msg, self);
        assert_action!(self, save_current_term(msg.term()));
        assert_action!(self, save_voted_for(Some(msg.from())));
        assert_action!(self, set_election_timeout());
        assert_action!(self, unicast_message(msg.from(), &reply));
        assert_no_action!(self);

        reply
    }

    fn asserted_handle_append_entries_request_success(&mut self, msg: &Message) -> Message {
        assert!(matches!(msg, Message::AppendEntriesRequest(_)));

        let Message::AppendEntriesRequest(AppendEntriesRequest {
            entries,
            leader_commit,
            ..
        }) = msg
        else {
            unreachable!();
        };

        let prev_commit_index = self.commit_index();
        let prev_voted_for = self.voted_for();

        self.handle_message(msg);
        assert_eq!(self.log().last, entries.last);
        if prev_voted_for != Some(msg.from()) {
            assert_action!(self, save_voted_for(Some(msg.from())));
        }

        let reply = append_entries_reply(msg, self);
        if !entries.is_empty() {
            assert_action!(self, append_log_entries(&entries));
        }
        if prev_commit_index < *leader_commit && prev_commit_index <= self.log().last.index {
            assert_action!(self, committed(self.log().last.index.min(*leader_commit)));
        }
        assert_action!(self, unicast_message(msg.from(), &reply));
        assert_no_action!(self);

        reply
    }

    fn asserted_handle_append_entries_reply_failure(&mut self, msg: &Message) -> Message {
        assert!(matches!(msg, Message::AppendEntriesReply(_)));

        let Message::AppendEntriesReply(reply) = msg else {
            unreachable!();
        };
        let Some(entries) = self.log().since(reply.last_entry) else {
            panic!("Needs snapshot");
        };

        self.handle_message(msg);
        let request = append_entries_request(self, entries.clone());

        assert_action!(self, unicast_message(reply.from, &request));
        assert_no_action!(self);

        request
    }

    fn asserted_handle_append_entries_reply_success_with_joint_config_committed(
        &mut self,
        msg: &Message,
    ) -> Message {
        assert!(matches!(msg, Message::AppendEntriesReply(_)));
        assert!(self.cluster_config().is_joint_consensus());

        let prev_entry = self.log().last;
        let mut new_config = self.cluster_config().clone();
        new_config.voters = std::mem::take(&mut new_config.new_voters);

        let Message::AppendEntriesReply(reply) = msg else {
            unreachable!();
        };

        self.handle_message(msg);
        let request = append_entries_request(
            self,
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

    fn asserted_follower_election_timeout(&mut self) -> Message {
        assert_eq!(self.role(), Role::Follower);

        let prev_term = self.current_term();
        self.handle_election_timeout();
        assert_eq!(self.role(), Role::Candidate);
        assert_eq!(self.current_term(), prev_term.next());

        let request = request_vote_request(self.current_term(), self.id(), self.log().last);
        assert_action!(self, save_current_term(prev_term.next()));
        assert_action!(self, save_voted_for(Some(self.id())));
        assert_action!(self, broadcast_message(&request));
        assert_action!(self, set_election_timeout());
        assert_no_action!(self);

        request
    }

    fn asserted_candidate_election_timeout(&mut self) -> Message {
        assert_eq!(self.role(), Role::Candidate);

        let prev_term = self.current_term();
        self.handle_election_timeout();
        assert_eq!(self.role(), Role::Candidate);
        assert_eq!(self.current_term(), prev_term.next());

        let request = request_vote_request(self.current_term(), self.id(), self.log().last);
        assert_action!(self, save_current_term(prev_term.next()));
        assert_action!(self, save_voted_for(Some(self.id())));
        assert_action!(self, broadcast_message(&request));
        assert_action!(self, set_election_timeout());
        assert_no_action!(self);

        request
    }

    fn asserted_handle_request_vote_request_success(&mut self, msg: &Message) -> Message {
        assert!(matches!(msg, Message::RequestVoteRequest(_)));

        self.handle_message(&msg);

        let reply = request_vote_reply(msg.term(), self.id(), true);
        assert_action!(self, save_current_term(msg.term()));
        assert_action!(self, save_voted_for(Some(msg.from())));
        assert_action!(self, set_election_timeout());
        assert_action!(self, unicast_message(msg.from(), &reply));
        assert_no_action!(self);

        reply
    }

    fn asserted_handle_request_vote_request_failed(&mut self, msg: &Message) {
        assert!(matches!(msg, Message::RequestVoteRequest(_)));

        self.handle_message(&msg);
        assert_action!(self, save_current_term(msg.term()));
        assert_action!(self, Action::SaveVotedFor(None));
        assert_action!(self, set_election_timeout());
        assert_no_action!(self);
    }

    fn asserted_handle_request_vote_reply_majority_vote_granted(
        &mut self,
        msg: &Message,
    ) -> Message {
        assert!(matches!(msg, Message::RequestVoteReply(_)));

        let tail = self.log().last;
        self.handle_message(&msg);
        let request = append_entries_request(
            self,
            LogEntries::single(tail, &term_entry(self.current_term())),
        );
        assert_action!(
            self,
            append_log_entry(tail, term_entry(self.current_term()))
        );
        assert_action!(self, broadcast_message(&request));
        assert_action!(self, set_election_timeout());
        assert_no_action!(self);

        request
    }

    fn asserted_handle_append_entries_request_success_new_leader(
        &mut self,
        msg: &Message,
    ) -> Message {
        assert!(matches!(msg, Message::AppendEntriesRequest(_)));

        let tail = self.log().last;
        self.handle_message(&msg);
        let reply = append_entries_reply(&msg, self);
        assert_action!(self, save_current_term(msg.term()));
        assert_action!(self, save_voted_for(Some(msg.from())));
        assert_action!(self, set_election_timeout());
        assert_action!(self, append_log_entry(tail, term_entry(msg.term())));
        assert_action!(self, unicast_message(msg.from(), &reply));
        assert_no_action!(self);

        reply
    }

    fn asserted_heartbeat(&mut self) -> (Heartbeat, Message) {
        let heartbeat = self.heartbeat();
        let request = append_entries_request(self, LogEntries::new(self.log().last));
        assert_action!(self, broadcast_message(&request));
        assert_no_action!(self);
        (heartbeat, request)
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

fn request_vote_request(term: Term, from: NodeId, last_entry: LogEntryRef) -> Message {
    Message::request_vote_request(term, from, last_entry)
}

fn request_vote_reply(term: Term, from: NodeId, vote_granted: bool) -> Message {
    Message::request_vote_reply(term, from, vote_granted)
}

fn append_entries_request(leader: &Node, entries: LogEntries) -> Message {
    Message::append_entries_request(
        leader.current_term(),
        leader.id(),
        leader.commit_index(),
        SequenceNumber::from_u64(leader.leader_sn.get() - 1),
        entries,
    )
}

fn append_entries_reply(request: &Message, node: &Node) -> Message {
    let Message::AppendEntriesRequest(request) = request else {
        panic!();
    };
    Message::append_entries_reply(
        node.current_term(),
        node.id(),
        request.leader_sn,
        node.log().last,
    )
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
