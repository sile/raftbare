use raftbare::{
    Action, Actions, ClusterConfig, LogEntries, LogEntry, LogIndex, LogPosition, Message,
    MessageHeader, Node, NodeGeneration, NodeId, Role, Term,
};
use std::ops::{Deref, DerefMut};

macro_rules! assert_no_action {
    ($node:expr) => {
        assert_eq!($node.actions_mut().next(), None);
        assert!($node.actions().is_empty());
    };
}

macro_rules! assert_action {
    ($node:expr, $action:expr) => {
        let action = $action;
        assert_eq!(
            next_same_kind_action($node.actions_mut(), &action),
            Some(action)
        );
    };
}

#[test]
fn single_node_start() {
    TestNode::asserted_start(id(0), &[id(0)]);
}

#[test]
fn create_two_nodes_cluster() {
    let initial_voters = [id(0), id(1)];
    let mut node0 = TestNode::asserted_start(id(0), &initial_voters);
    let mut node1 = TestNode::asserted_start(id(1), &[]);

    // Setup cluster.
    node0.handle_election_timeout();
    assert_eq!(node0.role(), Role::Candidate);
    assert_action!(node0, set_election_timeout());
    assert_action!(node0, save_current_term());
    assert_action!(node0, save_voted_for());

    let Some(Action::BroadcastMessage(call @ Message::RequestVoteCall { .. })) =
        node0.actions_mut().next()
    else {
        panic!("Expected RequestVoteCall message");
    };
    assert_no_action!(node0);

    let reply = node1.asserted_handle_request_vote_call_success(&call);
    let call = node0.asserted_handle_request_vote_reply_majority_vote_granted(&reply);
    let reply = node1.asserted_handle_append_entries_call_failure(&call);
    let call = node0.asserted_handle_append_entries_reply_failure(&reply);

    assert!(!node0.config().is_joint_consensus());
    assert_eq!(node0.config().voters, initial_voters.into_iter().collect());

    assert_eq!(node1.config().unique_nodes().count(), 0);

    let reply = node1.asserted_handle_append_entries_call_success(&call);
    node0.asserted_handle_append_entries_reply_success(&reply, true, false);
    assert_eq!(node0.config(), node1.config());
}

#[test]
fn create_three_nodes_cluster() {
    let mut cluster = ThreeNodeCluster::new();
    cluster.init_cluster();

    assert!(!cluster.node0.config().is_joint_consensus());
    assert_eq!(cluster.node0.config(), cluster.node1.config());
    assert_eq!(cluster.node0.config(), cluster.node2.config());
}

#[test]
fn election() {
    let mut cluster = ThreeNodeCluster::new();
    cluster.init_cluster();

    // Trigger a new election.
    let _call = cluster.node1.asserted_follower_election_timeout();
    let _call = cluster.node2.asserted_follower_election_timeout();
    let call = cluster.node1.asserted_candidate_election_timeout();

    let reply = cluster
        .node2
        .asserted_handle_request_vote_call_success(&call);

    let call = cluster
        .node1
        .asserted_handle_request_vote_reply_majority_vote_granted(&reply);
    let reply_from_node2 = cluster
        .node2
        .asserted_handle_append_entries_call_success(&call);
    let reply_from_node0 = cluster
        .node0
        .asserted_handle_append_entries_call_success_new_leader(&call);

    cluster
        .node1
        .asserted_handle_append_entries_reply_success(&reply_from_node0, true, false);
    cluster
        .node1
        .asserted_handle_append_entries_reply_success(&reply_from_node2, false, false);

    // Manual heartbeat.
    let call = cluster.node1.asserted_heartbeat();
    let reply = cluster
        .node0
        .asserted_handle_append_entries_call_success(&call);
    cluster.node1.handle_message(&reply);
    assert_no_action!(cluster.node1);

    // Periodic heartbeat.
    cluster.node1.handle_election_timeout();
    let call = append_entries_call(
        &cluster.node1,
        LogEntries::new(cluster.node1.log().entries().last_position()),
    );
    assert_action!(cluster.node1, set_election_timeout());
    assert_action!(cluster.node1, broadcast_message(&call));

    let reply = cluster
        .node2
        .asserted_handle_append_entries_call_success(&call);
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
        NodeGeneration::new(cluster.node1.inner.generation().get() + 1),
        cluster.node1.current_term(),
        cluster.node1.voted_for(),
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
    let commit_position = cluster.node0.propose_command();
    assert_eq!(commit_position, cluster.node0.log().last_position(),);
    while let Some(_) = cluster.node0.actions_mut().next() {}

    // Make node2 the leader.
    let _call = cluster.node2.asserted_follower_election_timeout();
    let call = cluster.node2.asserted_candidate_election_timeout(); // Increase term.

    // As  node0 is the leader, it ignores RequestVoteRPC even it if has a higher term.
    cluster.node0.handle_message(&call);
    assert_eq!(cluster.node0.role(), Role::Leader);
    assert_no_action!(cluster.node0);

    // The log index of node1 is equal to node2 => granted.
    let _ = cluster.node1.asserted_follower_election_timeout();
    let reply = cluster
        .node1
        .asserted_handle_request_vote_call_success(&call);
    let call = cluster
        .node2
        .asserted_handle_request_vote_reply_majority_vote_granted(&reply);
    assert_eq!(cluster.node2.role(), Role::Leader);

    // The uncommitted log entries on node0 are truncated.
    let reply = cluster
        .node0
        .asserted_handle_append_entries_call_success(&call);
    assert!(
        cluster
            .node0
            .get_commit_status(commit_position)
            .is_in_progress()
    );

    cluster
        .node2
        .asserted_handle_append_entries_reply_success(&reply, true, false);

    let call = cluster.node2.asserted_heartbeat();
    let _reply = cluster
        .node0
        .asserted_handle_append_entries_call_success(&call);
    assert!(
        cluster
            .node0
            .get_commit_status(commit_position)
            .is_rejected()
    );

    assert_no_action!(cluster.node0);
    assert_no_action!(cluster.node1);
    assert_no_action!(cluster.node2);
}

#[test]
fn snapshot() {
    let mut cluster = ThreeNodeCluster::new();
    cluster.init_cluster();
    cluster.propose_command();
    assert_eq!(cluster.node0.role(), Role::Leader);

    // Take a snapshot.
    for node in &mut [&mut cluster.node0, &mut cluster.node1, &mut cluster.node2] {
        assert_eq!(node.log().entries().prev_position().index, LogIndex::new(0));
        let snapshot_config = node.log().latest_config().clone();
        let snapshot_position = node.log().entries().last_position();
        assert!(node.handle_snapshot_installed(snapshot_position, snapshot_config));
        assert_ne!(node.log().entries().prev_position().index, LogIndex::new(0));
    }

    // Add a new node and remove two nodes.
    let mut node3 = TestNode::asserted_start(id(3), &[]);
    let config = joint(
        &[cluster.node0.id(), cluster.node1.id(), cluster.node2.id()],
        &[cluster.node0.id(), node3.id()],
    );
    let call = cluster.node0.asserted_change_cluster_config(config);
    for node in &mut [&mut cluster.node1, &mut cluster.node2] {
        let reply = node.asserted_handle_append_entries_call_success(&call);
        cluster
            .node0
            .asserted_handle_append_entries_reply_success(&reply, false, false);
    }

    // Cannot append (need snapshot).
    let reply = node3.asserted_handle_append_entries_call_failure(&call);
    let (snapshot_config, snapshot_position) = cluster
        .node0
        .asserted_handle_append_entries_reply_failure_need_snapshot(&reply);
    assert!(node3.handle_snapshot_installed(snapshot_position, snapshot_config));

    // Append after snapshot.
    let call = cluster.node0.asserted_heartbeat();
    let reply = node3.asserted_handle_append_entries_call_failure(&call);

    let call = cluster
        .node0
        .asserted_handle_append_entries_reply_failure(&reply);
    let reply = node3.asserted_handle_append_entries_call_success(&call);
    cluster
        .node0
        .asserted_handle_append_entries_reply_success_with_joint_config_committed(&reply);
}

#[derive(Debug)]
struct ThreeNodeCluster {
    node0: TestNode,
    node1: TestNode,
    node2: TestNode,
}

impl ThreeNodeCluster {
    fn new() -> Self {
        let initial_voters = &[id(0), id(1), id(2)];
        Self {
            node0: TestNode::asserted_start(id(0), initial_voters),
            node1: TestNode::asserted_start(id(1), &[]),
            node2: TestNode::asserted_start(id(2), &[]),
        }
    }

    fn init_cluster(&mut self) {
        // Setup  cluster.
        self.node0.handle_election_timeout();
        assert_eq!(self.node0.role(), Role::Candidate);
        assert_action!(self.node0, set_election_timeout());
        assert_action!(self.node0, save_current_term());
        assert_action!(self.node0, save_voted_for());
        let call = self
            .node0
            .actions_mut()
            .broadcast_message
            .take()
            .expect("broadcast");
        assert_no_action!(self.node0);

        for node in &mut [&mut self.node1, &mut self.node2] {
            let reply = node.asserted_handle_request_vote_call_success(&call);
            if node.id() == id(1) {
                self.node0
                    .asserted_handle_request_vote_reply_majority_vote_granted(&reply);
            }
        }
        assert_eq!(self.node0.role(), Role::Leader);

        let call = self.node0.take_broadcast_message();
        for node in &mut [&mut self.node1, &mut self.node2] {
            let reply = node.asserted_handle_append_entries_call_failure(&call);
            let call = self
                .node0
                .asserted_handle_append_entries_reply_failure(&reply);
            let reply = node.asserted_handle_append_entries_call_success(&call);
            if node.id() == id(1) {
                self.node0
                    .asserted_handle_append_entries_reply_success(&reply, true, false);
            }
        }
        assert_eq!(self.node0.config(), self.node1.config());
        assert_eq!(self.node0.config(), self.node2.config());
    }

    fn propose_command(&mut self) {
        let mut commit_position = None;
        let mut call = None;
        for node in &mut [&mut self.node0, &mut self.node1, &mut self.node2] {
            if node.role() != Role::Leader {
                continue;
            }
            commit_position = Some(node.propose_command());
            assert_action!(
                node.inner,
                append_log_entry(
                    log_prev(node.log().entries().last_position()),
                    LogEntry::Command
                )
            );
            let msg = append_entries_call(
                &node.inner,
                LogEntries::from_iter(
                    log_prev(node.log().entries().last_position()),
                    std::iter::once(LogEntry::Command),
                ),
            );
            assert_action!(node, broadcast_message(&msg));
            assert_action!(node, set_election_timeout());
            assert_no_action!(node);
            call = Some(msg);
            break;
        }

        let (Some(commit_position), Some(call)) = (commit_position, call) else {
            panic!("No leader found.");
        };

        let mut replies = Vec::new();
        for node in &mut [&mut self.node0, &mut self.node1, &mut self.node2] {
            if node.role() == Role::Leader {
                continue;
            }

            replies.push(node.asserted_handle_append_entries_call_success(&call));
        }

        let mut first = true;
        for node in &mut [&mut self.node0, &mut self.node1, &mut self.node2] {
            if node.role() != Role::Leader {
                continue;
            }

            for reply in replies {
                node.asserted_handle_append_entries_reply_success(&reply, first, false);
                assert_eq!(node.commit_index(), commit_position.index);
                first = false;
            }
            break;
        }
    }
}

#[derive(Debug)]
struct TestNode {
    inner: Node,
    actions: Actions,
}

impl TestNode {
    fn take_broadcast_message(&mut self) -> Message {
        self.actions
            .broadcast_message
            .take()
            .expect("No broadcast message.")
    }

    fn asserted_start(id: NodeId, initial_voters: &[NodeId]) -> Self {
        let mut node = Node::start(id);
        assert_eq!(node.role(), Role::Follower);
        assert_eq!(node.current_term(), t(0));
        assert_eq!(node.voted_for(), None);
        assert_no_action!(node);

        if !initial_voters.is_empty() {
            assert_ne!(node.create_cluster(initial_voters), LogPosition::INVALID);

            assert_action!(node, set_election_timeout());
            assert_action!(node, save_current_term());
            assert_action!(node, save_voted_for());

            if initial_voters == [id] {
                assert_eq!(node.role(), Role::Leader);
                assert_action!(
                    node,
                    append_log_entries(&LogEntries::from_iter(
                        prev(t(0), i(0)),
                        [
                            cluster_config_entry(joint(initial_voters, &[])),
                            term_entry(t(1))
                        ]
                        .into_iter()
                    ))
                );
            } else {
                assert_eq!(node.role(), Role::Candidate);
                assert_action!(
                    node,
                    append_log_entries(&LogEntries::from_iter(
                        prev(t(0), i(0)),
                        [cluster_config_entry(joint(initial_voters, &[]))].into_iter()
                    ))
                );
                assert!(matches!(
                    node.actions_mut().next(),
                    Some(Action::BroadcastMessage(Message::RequestVoteCall { .. }))
                ));
            }
            assert_no_action!(node);
        }
        Self {
            inner: node,
            actions: Actions::default(),
        }
    }

    fn asserted_change_cluster_config(&mut self, new_config: ClusterConfig) -> Message {
        let prev_entry = self.log().entries().last_position();
        let next_index = next_index(self.log().entries().last_position().index);
        let next_position = log_pos(self.current_term(), next_index);
        assert_eq!(next_position, self.propose_config(new_config.clone()));
        let msg = append_entries_call(
            self,
            LogEntries::from_iter(
                prev_entry,
                std::iter::once(cluster_config_entry(new_config.clone())),
            ),
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

    fn asserted_handle_append_entries_call_success(&mut self, msg: &Message) -> Message {
        assert!(matches!(msg, Message::AppendEntriesCall { .. }));
        let old_role = self.role();

        let Message::AppendEntriesCall {
            entries,
            commit_index: leader_commit,
            ..
        } = msg
        else {
            unreachable!();
        };

        let prev_commit_index = self.commit_index();
        let prev_voted_for = self.voted_for();

        self.handle_message(msg);
        assert_eq!(
            self.log().entries().last_position(),
            entries.last_position()
        );
        if prev_voted_for != Some(msg.from()) {
            assert_action!(self, save_voted_for());
            assert_eq!(self.voted_for(), Some(msg.from()));
        }

        let reply = append_entries_reply(msg, self);
        if !entries.is_empty() {
            assert_action!(self, append_log_entries(&entries));
        }
        if prev_commit_index < *leader_commit
            && prev_commit_index <= self.log().entries().last_position().index
        {
            assert_eq!(
                self.commit_index(),
                self.log()
                    .entries()
                    .last_position()
                    .index
                    .min(*leader_commit)
            );
        }
        assert_action!(self, send_message(msg.from(), &reply));
        assert_action!(self, set_election_timeout());
        if old_role.is_leader() {
            assert_action!(self, save_current_term());
        }
        assert_no_action!(self);

        reply
    }

    fn asserted_handle_append_entries_call_failure(&mut self, msg: &Message) -> Message {
        assert!(matches!(msg, Message::AppendEntriesCall { .. }));

        let Message::AppendEntriesCall { entries, .. } = msg else {
            unreachable!();
        };

        let prev_voted_for = self.voted_for();
        let prev_term = self.current_term();

        self.handle_message(msg);
        assert_ne!(
            self.log().entries().last_position(),
            entries.last_position()
        );
        if prev_term < msg.term() {
            assert_action!(self, save_current_term());
            assert_eq!(self.current_term(), msg.term());
        }
        if prev_voted_for != Some(msg.from()) {
            assert_action!(self, save_voted_for());
            assert_eq!(self.voted_for(), Some(msg.from()));
        }
        assert_action!(self, set_election_timeout());

        let reply = append_entries_reply(msg, self);
        assert_action!(self, send_message(msg.from(), &reply));
        assert_no_action!(self);

        reply
    }

    fn asserted_handle_append_entries_reply_failure_need_snapshot(
        &mut self,
        msg: &Message,
    ) -> (ClusterConfig, LogPosition) {
        assert!(matches!(msg, Message::AppendEntriesReply { .. }));

        let Message::AppendEntriesReply {
            header,
            last_position,
            ..
        } = msg
        else {
            unreachable!();
        };
        assert!(since(self.log().entries(), *last_position).is_none());

        self.handle_message(msg);
        assert_action!(self, Action::InstallSnapshot(header.from));
        assert_no_action!(self);

        (
            self.log().snapshot_config().clone(),
            self.log().entries().prev_position(),
        )
    }

    fn asserted_handle_append_entries_reply_success_with_joint_config_committed(
        &mut self,
        msg: &Message,
    ) -> Message {
        assert!(matches!(msg, Message::AppendEntriesReply { .. }));
        assert!(self.config().is_joint_consensus());

        let prev_entry = self.log().entries().last_position();
        let mut new_config = self.config().clone();
        new_config.voters = std::mem::take(&mut new_config.new_voters);

        let Message::AppendEntriesReply { last_position, .. } = msg else {
            unreachable!();
        };

        self.handle_message(msg);
        let call = append_entries_call(
            self,
            LogEntries::from_iter(
                prev_entry,
                std::iter::once(cluster_config_entry(new_config.clone())),
            ),
        );
        assert_eq!(self.commit_index(), last_position.index);
        assert_action!(
            self,
            append_log_entry(prev_entry, cluster_config_entry(new_config.clone()))
        );
        assert_action!(self, broadcast_message(&call));
        assert_action!(self, set_election_timeout());
        assert_no_action!(self);

        call
    }

    fn asserted_handle_append_entries_reply_success(
        &mut self,
        reply: &Message,
        commit_index_will_be_updated: bool,
        joint_consensus_will_be_finalized: bool,
    ) {
        assert!(matches!(reply, Message::AppendEntriesReply { .. }));

        let old_last_position = self.log().entries().last_position();
        self.handle_message(reply);
        self.actions = self.inner.actions().clone();

        let Message::AppendEntriesReply { last_position, .. } = reply else {
            unreachable!();
        };
        if commit_index_will_be_updated {
            assert_eq!(self.commit_index(), last_position.index);
        }
        if joint_consensus_will_be_finalized {
            assert_action!(self, set_election_timeout());

            let config = self.config().clone();
            assert_action!(
                self,
                append_log_entry(old_last_position, cluster_config_entry(config.clone()))
            );
            assert_action!(
                self,
                broadcast_message(&append_entries_call(
                    self,
                    LogEntries::from_iter(
                        old_last_position,
                        std::iter::once(cluster_config_entry(config.clone()))
                    )
                ))
            );
        }
        assert_no_action!(self);
    }

    fn asserted_handle_append_entries_reply_failure(&mut self, reply: &Message) -> Message {
        assert!(matches!(reply, Message::AppendEntriesReply { .. }));

        self.handle_message(reply);
        let Some(call) = self.actions_mut().send_messages.remove(&reply.from()) else {
            panic!("No send message action");
        };
        assert_no_action!(self);

        call
    }

    fn asserted_follower_election_timeout(&mut self) -> Message {
        assert_eq!(self.role(), Role::Follower);

        let prev_term = self.current_term();
        self.handle_election_timeout();
        assert_eq!(self.role(), Role::Candidate);
        assert_eq!(self.current_term(), next_term(prev_term));

        let call = request_vote_call(
            self.current_term(),
            self.id(),
            self.log().entries().last_position(),
        );
        assert_action!(self, save_current_term());
        assert_eq!(self.current_term(), next_term(prev_term));
        assert_action!(self, save_voted_for());
        assert_eq!(self.voted_for(), Some(self.id()));
        assert_action!(self, broadcast_message(&call));
        assert_action!(self, set_election_timeout());
        assert_no_action!(self);

        call
    }

    fn asserted_candidate_election_timeout(&mut self) -> Message {
        assert_eq!(self.role(), Role::Candidate);

        let prev_term = self.current_term();
        self.handle_election_timeout();
        assert_eq!(self.role(), Role::Candidate);
        assert_eq!(self.current_term(), next_term(prev_term));

        let call = request_vote_call(
            self.current_term(),
            self.id(),
            self.log().entries().last_position(),
        );
        assert_action!(self, save_current_term());
        assert_eq!(self.current_term(), next_term(prev_term));
        assert_action!(self, save_voted_for());
        assert_eq!(self.voted_for(), Some(self.id()));
        assert_action!(self, broadcast_message(&call));
        assert_action!(self, set_election_timeout());
        assert_no_action!(self);

        call
    }

    fn asserted_handle_request_vote_call_success(&mut self, msg: &Message) -> Message {
        assert!(matches!(msg, Message::RequestVoteCall { .. }));

        self.handle_message(msg);

        let reply = request_vote_reply(msg.term(), self.id(), true);
        assert_action!(self, save_current_term());
        assert_eq!(self.current_term(), msg.term());
        assert_action!(self, save_voted_for());
        assert_eq!(self.voted_for(), Some(msg.from()));
        assert_action!(self, set_election_timeout());
        assert_action!(self, send_message(msg.from(), &reply));
        assert_no_action!(self);

        reply
    }

    fn asserted_handle_request_vote_reply_majority_vote_granted(
        &mut self,
        msg: &Message,
    ) -> Message {
        assert!(matches!(msg, Message::RequestVoteReply { .. }));

        let tail = self.log().entries().last_position();
        self.handle_message(msg);
        self.actions = self.inner.actions().clone();
        let call = append_entries_call(
            self,
            LogEntries::from_iter(tail, std::iter::once(term_entry(self.current_term()))),
        );
        assert_action!(
            self,
            append_log_entry(tail, term_entry(self.current_term()))
        );
        assert_action!(self, broadcast_message(&call));
        assert_action!(self, set_election_timeout());
        assert_no_action!(self);

        call
    }

    fn asserted_handle_append_entries_call_success_new_leader(&mut self, msg: &Message) -> Message {
        assert!(matches!(msg, Message::AppendEntriesCall { .. }));

        let tail = self.log().entries().last_position();
        self.handle_message(msg);
        let reply = append_entries_reply(&msg, self);
        assert_action!(self, save_current_term());
        assert_eq!(self.current_term(), msg.term());
        assert_action!(self, save_voted_for());
        assert_eq!(self.voted_for(), Some(msg.from()));
        assert_action!(self, set_election_timeout());
        assert_action!(self, append_log_entry(tail, term_entry(msg.term())));
        assert_action!(self, send_message(msg.from(), &reply));
        assert_no_action!(self);

        reply
    }

    fn asserted_heartbeat(&mut self) -> Message {
        assert!(self.heartbeat());
        let call = append_entries_call(self, LogEntries::new(self.log().entries().last_position()));
        assert_action!(self, set_election_timeout());
        assert_action!(self, broadcast_message(&call));
        assert_no_action!(self);
        call
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

fn prev(term: Term, index: LogIndex) -> LogPosition {
    log_pos(term, index)
}

fn joint(old: &[NodeId], new: &[NodeId]) -> ClusterConfig {
    let mut config = ClusterConfig::new();
    config.voters.extend(old.iter().copied());
    config.new_voters.extend(new.iter().copied());
    config
}

fn term_entry(term: Term) -> LogEntry {
    LogEntry::Term(term)
}

fn cluster_config_entry(config: ClusterConfig) -> LogEntry {
    LogEntry::ClusterConfig(config)
}

fn request_vote_call(
    term: Term,
    from: NodeId,
    last_position: LogPosition,
) -> Message {
    Message::RequestVoteCall {
        header: MessageHeader { term, from },
        last_position,
    }
}

fn request_vote_reply(
    term: Term,
    from: NodeId,
    vote_granted: bool,
) -> Message {
    Message::RequestVoteReply {
        header: MessageHeader { from, term },
        vote_granted,
    }
}

fn append_entries_call(leader: &Node, entries: LogEntries) -> Message {
    let term = leader.current_term();
    let from = leader.id();
    let commit_index = leader.commit_index();
    Message::AppendEntriesCall {
        header: MessageHeader { from, term },
        commit_index,
        entries,
    }
}

fn append_entries_reply(call: &Message, node: &Node) -> Message {
    let Message::AppendEntriesCall { .. } = call else {
        panic!();
    };

    let term = node.current_term();
    let from = node.id();
    let generation = node.generation();
    let last_position = node.log().entries().last_position();
    Message::AppendEntriesReply {
        header: MessageHeader { term, from },
        generation,
        last_position,
    }
}

fn send_message(destination: NodeId, message: &Message) -> Action {
    Action::SendMessage(destination, message.clone())
}

fn broadcast_message(message: &Message) -> Action {
    Action::BroadcastMessage(message.clone())
}

fn set_election_timeout() -> Action {
    Action::SetElectionTimeout
}

fn append_log_entry(prev: LogPosition, entry: LogEntry) -> Action {
    Action::AppendLogEntries(LogEntries::from_iter(prev, std::iter::once(entry)))
}

fn append_log_entries(entries: &LogEntries) -> Action {
    Action::AppendLogEntries(entries.clone())
}

fn save_current_term() -> Action {
    Action::SaveCurrentTerm
}

fn save_voted_for() -> Action {
    Action::SaveVotedFor
}

fn next_term(term: Term) -> Term {
    Term::new(term.get() + 1)
}

fn next_index(index: LogIndex) -> LogIndex {
    LogIndex::new(index.get() + 1)
}

fn log_pos(term: Term, index: LogIndex) -> LogPosition {
    LogPosition { term, index }
}

fn log_prev(entry: LogPosition) -> LogPosition {
    log_pos(entry.term, LogIndex::new(entry.index.get() - 1))
}

fn since(entries: &LogEntries, position: LogPosition) -> Option<LogEntries> {
    if !entries.contains(position) {
        return None;
    }
    Some(LogEntries::from_iter(
        position,
        entries
            .iter()
            .skip(position.index.get() as usize - position.index.get() as usize),
    ))
}

fn next_same_kind_action(actions: &mut Actions, expected: &Action) -> Option<Action> {
    match expected {
        Action::SetElectionTimeout if actions.set_election_timeout => {
            actions.set_election_timeout = false;
            Some(Action::SetElectionTimeout)
        }
        Action::SaveCurrentTerm if actions.save_current_term => {
            actions.save_current_term = false;
            Some(Action::SaveCurrentTerm)
        }
        Action::SaveVotedFor if actions.save_voted_for => {
            actions.save_voted_for = false;
            Some(Action::SaveVotedFor)
        }
        Action::AppendLogEntries(_) => actions
            .append_log_entries
            .take()
            .map(Action::AppendLogEntries),
        Action::BroadcastMessage(_) => actions
            .broadcast_message
            .take()
            .map(Action::BroadcastMessage),
        Action::SendMessage(node_id, _) => actions
            .send_messages
            .remove(node_id)
            .map(|msg| Action::SendMessage(*node_id, msg)),
        Action::InstallSnapshot(node_id) => actions
            .install_snapshots
            .remove(node_id)
            .then(|| Action::InstallSnapshot(*node_id)),
        _ => None,
    }
}
