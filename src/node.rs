use crate::{
    action::{Action, Actions},
    config::ClusterConfig,
    log::{LogEntries, LogEntry, LogIndex, LogPosition},
    message::{Message, MessageSeqNo},
    quorum::Quorum,
    CommitPromise, HeartbeatPromise, Log, MessageHeader, Role, Term,
};
use std::collections::{BTreeMap, BTreeSet};

/// Node identifier ([`u64`]).
///
/// Note that if you want to distinguish nodes by their names (not integers),
/// mapping node names to identifiers is out of the scope of this crate.
///
/// Besides, each [`Node`] in a cluster can have a different mapping of names to identifiers.
/// In this case, it is necessary to remap [`NodeId`]s in [`Message`]s before delivering them to other nodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId(u64);

impl NodeId {
    /// Makes a new [`NodeId`] instance.
    pub const fn new(id: u64) -> Self {
        NodeId(id)
    }

    /// Returns the value of this identifier.
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Raft node.
#[derive(Debug, Clone)]
pub struct Node {
    id: NodeId,
    voted_for: Option<NodeId>,
    current_term: Term,
    log: Log,
    commit_index: LogIndex,
    seqno: MessageSeqNo,
    actions: Actions,
    role: RoleState,
}

impl Node {
    /// Starts a new node.
    ///
    /// To create a new cluster, please call [`Node::create_cluster()`] after starting the node.
    ///
    /// If the node has already been part of a cluster, please use [`Node::restart()`] instead.
    ///
    /// # Examples
    ///
    /// ```
    /// use raftbare::{LogPosition, Node, NodeId};
    ///
    /// // Starts three nodes.
    /// let mut node0 = Node::start(NodeId::new(0));
    /// let node1 = Node::start(NodeId::new(1));
    /// let node2 = Node::start(NodeId::new(2));
    ///
    /// for node in [&node0, &node1, &node2] {
    ///     assert!(node.role().is_follower());
    ///     assert_eq!(node.config().unique_nodes().count(), 0);
    ///     assert_eq!(node.log().last_position(), LogPosition::ZERO);
    ///     assert!(node.actions().is_empty());
    /// }
    ///
    /// // Creates a new cluster.
    /// node0.create_cluster(&[node0.id(), node1.id(), node2.id()]);
    ///
    /// assert!(node0.role().is_candidate());
    /// assert_eq!(node0.config().unique_nodes().count(), 3);
    /// assert_ne!(node0.log().last_position(), LogPosition::ZERO);
    /// assert!(!node0.actions().is_empty());
    ///
    /// // [NOTE] To complete the cluster creation, the user needs to handle the queued actions.
    /// ```
    pub fn start(id: NodeId) -> Self {
        Self::new(id)
    }

    /// Restarts a node.
    ///
    /// `current_term`, `voted_for`, and `log` are restored from persistent storage.
    /// Note that managing the persistent storage is outside the scope of this crate.
    ///
    /// # Notes
    ///
    /// Raft algorithm assumes the persistent storage is reliable.
    /// So, for example, if the local log of the node has corrupted or lost some log tail entries,
    /// it is safest to remove the node from the cluster then add it back as a new node.
    ///
    /// In practice, such storage failures are usually tolerable when the majority of nodes in the cluster
    /// are healthy (i.e., the restarted node can restored its previous state).
    ///
    /// But be careful, whether the degraded safety guarantee is acceptable or not highly depends on
    /// the application.
    ///
    /// # Examples
    /// ```
    /// use raftbare::{Node, NodeId};
    ///
    /// // Loads the persistent state.
    /// let current_term = /* ... ; */
    /// # raftbare::Term::new(1);
    /// let voted_for = /* ... ; */
    /// # None;
    /// let log = /* ... ; */
    /// # raftbare::Log::new(raftbare::ClusterConfig::new(), raftbare::LogEntries::new(raftbare::LogPosition::ZERO));
    ///
    /// // Restarts a node.
    /// let node = Node::restart(NodeId::new(0), current_term, voted_for, log);
    /// assert!(node.role().is_follower());
    ///
    /// // Unlike `Node::start()`, the restarted node has actions to execute.
    /// assert!(!node.actions().is_empty());
    /// ```
    pub fn restart(id: NodeId, current_term: Term, voted_for: Option<NodeId>, log: Log) -> Self {
        let mut node = Self::new(id);

        node.current_term = current_term;
        node.voted_for = voted_for;
        node.log = log;
        node.actions.set(Action::SetElectionTimeout);

        node
    }

    /// Creates a new cluster.
    ///
    /// This method returns a [`CommitPromise`] that will be accepted
    /// when the initial cluster configuration is successfully committed.
    ///
    /// To proceed the cluster creation, the user needs to handle the queued actions after calling this method.
    ///
    /// # Preconditions
    ///
    /// This method returns `CommitPromise::Rejected(LogPosition::NEVER)` if the following preconditions are not met:
    /// - This node (`self`) is a newly started node.
    /// - `initial_voters` contains at least one node.
    ///
    /// Theoretically, it is acceptable to exclude the self node from `initial_voters`
    /// (although it is not practical).
    ///
    /// # Notes
    ///
    /// Raft algorithm assumes that each node in a cluster belongs to only one cluster at a time.
    /// Therefore, including nodes that are already part of another cluster in the `initial_voters`
    /// will result in undefined behavior.
    pub fn create_cluster(&mut self, initial_voters: &[NodeId]) -> CommitPromise {
        if self.log.entries().last_position() != LogPosition::ZERO {
            return CommitPromise::Rejected(LogPosition::INVALID);
        }
        if !self.config().voters.is_empty() {
            return CommitPromise::Rejected(LogPosition::INVALID);
        }
        if initial_voters.is_empty() {
            return CommitPromise::Rejected(LogPosition::INVALID);
        }

        let mut config = ClusterConfig::new();
        config.voters.extend(initial_voters.iter().copied());
        let entry = LogEntry::ClusterConfig(config);
        self.actions
            .set(Action::AppendLogEntries(LogEntries::from_iter(
                LogPosition::ZERO,
                std::iter::once(entry.clone()),
            )));
        self.log.entries_mut().push(entry.clone());

        self.transition_to_candidate();

        CommitPromise::Pending(self.log.last_position())
    }

    fn new(id: NodeId) -> Self {
        let term = Term::new(0);
        let index = LogIndex::new(0);
        let config = ClusterConfig::new();
        Self {
            id,
            voted_for: None,
            current_term: term,
            log: Log::new(config, LogEntries::new(LogPosition::new(term, index))),
            commit_index: LogIndex::new(0),
            seqno: MessageSeqNo::INIT,
            actions: Actions::default(),
            role: RoleState::Follower,
        }
    }

    /// Returns the identifier of this node.
    pub fn id(&self) -> NodeId {
        self.id
    }

    /// Returns the role of this node.
    pub fn role(&self) -> Role {
        match self.role {
            RoleState::Follower => Role::Follower,
            RoleState::Candidate { .. } => Role::Candidate,
            RoleState::Leader { .. } => Role::Leader,
        }
    }

    /// Returns the current term of this node.
    pub fn current_term(&self) -> Term {
        self.current_term
    }

    /// Returns the identifier of the node for which this node voted in the current term.
    ///
    /// If `self.role()` is not [`Role::Candidate`], the returned node may be the leader of the current term.
    pub fn voted_for(&self) -> Option<NodeId> {
        self.voted_for
    }

    /// Returns the in-memory representation of the local log of this node.
    pub fn log(&self) -> &Log {
        &self.log
    }

    /// Returns the commit index of this node.
    ///
    /// [`LogEntry::Command`] entries up to this index are safely applied to the state machine managed by the user.
    pub fn commit_index(&self) -> LogIndex {
        self.commit_index
    }

    /// Returns the current cluster configuration of this node.
    ///
    /// This is shorthand for `self.log().latest_config()`.
    pub fn config(&self) -> &ClusterConfig {
        self.log.latest_config()
    }

    /// Returns an iterator over the identifiers of the peers of this node.
    ///
    /// "Peers" means all unique nodes in the current cluster configuration except for this node.
    pub fn peers(&self) -> impl '_ + Iterator<Item = NodeId> {
        self.config()
            .unique_nodes()
            .filter(move |&node| node != self.id)
    }

    /// Returns a reference to the pending actions for this node.
    pub fn actions(&self) -> &Actions {
        &self.actions
    }

    /// Returns a mutable reference to the pending actions for this node.
    ///
    /// # Note
    ///
    /// It is the user's responsibility to execute these actions.
    pub fn actions_mut(&mut self) -> &mut Actions {
        &mut self.actions
    }

    fn transition_to_leader(&mut self) {
        debug_assert_eq!(self.voted_for, Some(self.id));

        let config = self.log.latest_config();
        let quorum = Quorum::new(config);
        let followers = config
            .unique_nodes()
            .filter(|id| *id != self.id)
            .map(|id| (id, Follower::new()))
            .collect();
        self.role = RoleState::Leader { followers, quorum };

        self.propose(LogEntry::Term(self.current_term));
    }

    fn transition_to_candidate(&mut self) {
        self.set_current_term(self.current_term.next());
        self.set_voted_for(Some(self.id));
        self.role = RoleState::Candidate {
            granted_votes: std::iter::once(self.id).collect(),
        };

        let seqno = self.seqno.fetch_and_increment();
        self.actions
            .set(Action::BroadcastMessage(Message::request_vote_call(
                self.current_term,
                self.id,
                seqno,
                self.log.entries().last_position(),
            )));
        self.actions.set(Action::SetElectionTimeout);
    }

    fn transition_to_follower(&mut self, term: Term) {
        self.set_current_term(term);
        self.set_voted_for(None);
        self.role = RoleState::Follower;
        self.actions.set(Action::SetElectionTimeout);
    }

    /// Proposes a user-defined command ([`LogEntry::Command`]).
    ///
    /// This method returns a [`CommitPromise`] that will be resolved when the command is committed or rejected.
    /// Committed commands can be applied to the state machine managed by the user.
    ///
    /// The [`CommitPromise`] is useful for determining when to send the command result back to the client
    /// that triggered the command (if such a client exists).
    /// To detect all committed commands that need to be applied to the state machine,
    /// it is recommended to use [`Node::commit_index()`] since it considers commands proposed by other nodes.
    ///
    /// Note that this crate does not manage the detail of user-defined commands,
    /// so this method takes no arguments.
    /// It is the user's responsibility to mapping the log index of the proposed command to
    /// the actual command data.
    ///
    /// # Preconditions
    ///
    /// This method returns `CommitPromise::Rejected(LogPosition::NEVER)` if the following preconditions are not met:
    /// - `self.role().is_leader()` is [`true`].
    ///
    /// # Pipelining
    ///
    /// [`Node::propose_command()`] can be called multiple times before any action is executed.
    /// In such cases, the pending actions are consolidated, reducing the overall I/O cost.
    ///
    /// # Examples
    ///
    /// ```
    /// use raftbare::{LogEntry, LogIndex, NodeId, Node};
    ///
    /// let mut node = /* ... ; */
    /// # Node::start(NodeId::new(0));
    ///
    /// let mut commit_promise = node.propose_command();
    /// if commit_promise.is_rejected() {
    ///     // `node` is not the leader.
    ///     if let Some(maybe_leader) = node.voted_for() {
    ///         // Retry with the possible leader or reply to the client that the command is rejected.
    ///         // ...
    ///     }
    ///     return;
    /// }
    ///
    /// // Need to map the log index to the actual command data for
    /// // exeucting `Action::AppendLogEntries(_)` queued by the node.
    /// assert!(node.actions().append_log_entries.is_some());
    /// let index = commit_promise.log_position().index;
    /// # let _ = index;
    /// // ... executing actions ...
    ///
    /// while commit_promise.poll(&mut node).is_pending() {
    ///     // ... executing actions ...
    /// }
    ///
    /// if commit_promise.is_rejected() {
    ///    // Retry with another node or reply to the client that the command is rejected.
    ///    // ...
    ///    return;
    /// }
    /// assert!(commit_promise.is_accepted());
    ///
    /// // Apply all committed commands to the state machine.
    /// let last_applied_index = /* ...; */
    /// # raftbare::LogIndex::ZERO;
    /// for index in (last_applied_index.get() - 1)..=node.commit_index().get() {
    ///     let index = LogIndex::new(index);
    ///     if node.log().entries().get_entry(index) != Some(LogEntry::Command) {
    ///         continue;
    ///     }
    ///     // Apply the command to the state machine.
    ///     // ...
    ///
    ///     if index == commit_promise.log_position().index {
    ///         // Reply to the client that the command is committed.
    ///         // ...
    ///     }
    /// }
    /// ```
    pub fn propose_command(&mut self) -> CommitPromise {
        if !matches!(self.role, RoleState::Leader { .. }) {
            return CommitPromise::Rejected(LogPosition::INVALID);
        }
        self.propose(LogEntry::Command)
    }

    fn propose(&mut self, entry: LogEntry) -> CommitPromise {
        debug_assert!(matches!(self.role, RoleState::Leader { .. }));

        let prev_entry = self.log.entries().last_position();
        self.append_log_entry(&entry);

        let RoleState::Leader { quorum, followers } = &mut self.role else {
            unreachable!();
        };

        if !followers.is_empty() {
            let old_seqno = self.seqno.fetch_and_increment();
            let call = Message::append_entries_call(
                self.current_term,
                self.id,
                self.commit_index,
                self.seqno,
                LogEntries::from_iter(prev_entry, std::iter::once(entry)),
            );
            self.actions.set(Action::BroadcastMessage(call));
            quorum.update_seqno(self.log.latest_config(), self.id, old_seqno, self.seqno);
        }
        self.actions.set(Action::SetElectionTimeout);
        self.update_commit_index_if_possible(); // TODO: check single node

        let index = self.log.entries().last_position().index;
        let term = self.current_term;
        let position = LogPosition { term, index };
        CommitPromise::new(position)
    }

    fn rebuild_followers(&mut self) {
        // TODO: refactor
        let RoleState::Leader { followers, .. } = &mut self.role else {
            unreachable!();
        };
        let config = self.log.latest_config();
        for id in config.unique_nodes() {
            if id == self.id || followers.contains_key(&id) {
                continue;
            }
            followers.insert(id, Follower::new());
        }
        followers.retain(|id, _| config.contains(*id));
    }

    fn rebuild_quorum(&mut self) {
        // TODO: refactor
        let RoleState::Leader { quorum, followers } = &mut self.role else {
            unreachable!();
        };

        let config = self.log.latest_config();
        *quorum = Quorum::new(config);

        let zero = LogIndex::new(0);
        quorum.update_match_index(
            config,
            self.id,
            zero,
            self.log.entries().last_position().index,
        );
        quorum.update_seqno(config, self.id, MessageSeqNo::UNKNOWN, self.seqno);

        for (&id, follower) in followers {
            quorum.update_match_index(config, id, zero, follower.match_index);
            quorum.update_seqno(config, id, MessageSeqNo::UNKNOWN, follower.max_sn);
        }
    }

    fn update_commit_index_if_possible(&mut self) {
        let RoleState::Leader { quorum, .. } = &mut self.role else {
            unreachable!();
        };

        let new_commit_index = quorum.smallest_majority_index();
        if self.commit_index < new_commit_index
            && self.log.entries().get_term(new_commit_index) == Some(self.current_term)
        {
            self.commit_index = new_commit_index;

            if self.log.latest_config().is_joint_consensus()
                && self.log.latest_config_index() <= new_commit_index
            {
                self.finalize_joint_consensus();
            }
        }
    }

    fn finalize_joint_consensus(&mut self) {
        debug_assert!(self.role().is_leader());
        debug_assert!(self.log.latest_config().is_joint_consensus());

        let mut new_config = self.log.latest_config().clone();
        new_config.voters = std::mem::take(&mut new_config.new_voters);
        self.propose(LogEntry::ClusterConfig(new_config));

        // TODO: leader not in new config steps down when the new config is committed
    }

    /// Proposes a new cluster configuration ([`LogEntry::ClusterConfig`]).
    ///
    /// If `new_config.new_voters` is not empty, the cluster will transition into a joint consensus state.
    /// In this state, leader elections and commit proposals require a majority from both the old and
    /// new voters independently.
    /// Once `new_config` is committed, a new configuration, which includes only the new voters
    /// (and any non-voters, if any), will be automatically proposed to finalize the joint consensus.
    ///
    /// `new_config.new_voters` does not need to include the self node.
    /// If it does not, the leader self node will transition to a follower
    /// when the final configuration is committed.
    ///
    /// Note that a change in `new_config.non_voters` does not require a joint consensus.
    ///
    /// # Preconditions
    ///
    /// This method returns `CommitPromise::Rejected(LogPosition::NEVER)` if the following preconditions are not met:
    /// - `self.role().is_leader()` is [`true`].
    /// - `new_config.voters` is equal to `self.config().voters`.
    /// - `self.config().is_joint_consensus()` is [`false`] (i.e., there is no other configuration change in progress).
    ///
    /// # Examples
    ///
    /// ```
    /// use raftbare::NodeId;
    ///
    /// let mut node = /* ... ; */
    /// # raftbare::Node::start(NodeId::new(1));
    ///
    /// // Propose a new configuration with adding node 4 and removing node 2.
    /// let new_config = node.config().to_joint_consensus(&[NodeId::new(4)], &[NodeId::new(2)]);
    /// node.propose_config(new_config);
    /// ```
    pub fn propose_config(&mut self, new_config: ClusterConfig) -> CommitPromise {
        if !self.role().is_leader() {
            return CommitPromise::Rejected(self.log().last_position().next());
        }
        if self.log.latest_config().voters != new_config.voters {
            return CommitPromise::Rejected(self.log().last_position().next());
        }
        if self.log.latest_config().is_joint_consensus() {
            return CommitPromise::Rejected(self.log().last_position().next());
        }

        self.propose(LogEntry::ClusterConfig(new_config))
    }

    /// Sends a heartbeat to all followers.
    ///
    /// This method returns a [`HeartbeatPromise`] that will be accepted when a majority of followers
    /// respond with success. If the term changes, the promise will be rejected.
    ///
    /// Typically, this method is used to confirm that the current node is still the leader
    /// before processing a query request about the state machine managed by the user.
    ///
    /// # Preconditions
    ///
    /// This method returns [`HeartbeatPromise::Rejected`] if the following preconditions are not met:
    /// - `self.role().is_leader()` is [`true`].
    pub fn heartbeat(&mut self) -> HeartbeatPromise {
        let RoleState::Leader { quorum, .. } = &mut self.role else {
            return HeartbeatPromise::Rejected;
        };

        // TODO: handle single node case
        let old_seqno = self.seqno.fetch_and_increment();
        let call = Message::append_entries_call(
            self.current_term,
            self.id,
            self.commit_index,
            self.seqno,
            LogEntries::new(self.log.entries().last_position()),
        );
        quorum.update_seqno(self.log.latest_config(), self.id, old_seqno, self.seqno);
        self.actions.set(Action::BroadcastMessage(call));

        HeartbeatPromise::new(self.current_term, self.seqno)
    }

    fn append_log_entry(&mut self, entry: &LogEntry) {
        debug_assert!(self.role().is_leader());

        let prev_index = self.log.entries().last_position().index;
        self.actions
            .set(Action::AppendLogEntries(LogEntries::from_iter(
                self.log.entries().last_position(),
                std::iter::once(entry.clone()),
            )));
        self.log.entries_mut().push(entry.clone());

        // TODO: unnecessary condition?
        if let RoleState::Leader { quorum, .. } = &mut self.role {
            quorum.update_match_index(
                self.log.latest_config(),
                self.id,
                prev_index,
                self.log.entries().last_position().index,
            );
        }

        if let LogEntry::ClusterConfig(_) = entry {
            // TODO: unnecessary condition?
            if self.role().is_leader() {
                self.rebuild_followers();
                self.rebuild_quorum();
            }
        }
    }

    fn try_append_log_entries(&mut self, entries: &LogEntries) -> bool {
        debug_assert!(self.role().is_follower());

        if self.log.entries().contains(entries.last_position()) {
            // Already up-to-date.
            return true;
        }
        if !self.log.entries().contains(entries.prev_position()) {
            // Cannot append.
            return false;
        }

        // Append.
        self.actions.set(Action::AppendLogEntries(entries.clone()));
        self.log.entries_mut().append(entries);
        true
    }

    fn set_current_term(&mut self, term: Term) {
        self.current_term = term;
        self.actions.set(Action::SaveCurrentTerm);
    }

    fn set_voted_for(&mut self, voted_for: Option<NodeId>) {
        self.voted_for = voted_for;
        self.actions.set(Action::SaveVotedFor);
    }

    /// Handles an incoming message from other nodes.
    ///
    /// # Examples
    ///
    /// ```
    /// let mut node = /* ... ; */
    /// # raftbare::Node::start(raftbare::NodeId::new(1));
    ///
    /// let msg = /* ... ; */
    /// # raftbare::Message::RequestVoteReply { header: raftbare::MessageHeader { from: raftbare::NodeId::new(1), term: raftbare::Term::new(1), seqno: raftbare::MessageSeqNo::new(3) }, vote_granted: true };
    /// node.handle_message(msg);
    ///
    /// // Execute actions queued by the message handling.
    /// for action in node.actions_mut() {
    ///     // ...
    /// }
    /// ```
    pub fn handle_message(&mut self, msg: Message) {
        if self.current_term < msg.term() {
            if matches!(msg, Message::RequestVoteCall { .. })
                && self.role().is_follower()
                && self.voted_for.map_or(false, |id| id != msg.from())
            {
                return;
            }
            self.transition_to_follower(msg.term());
        }

        match msg {
            Message::RequestVoteCall {
                header,
                last_position,
            } => self.handle_request_vote_call(header, last_position),
            Message::RequestVoteReply {
                header,
                vote_granted,
            } => self.handle_request_vote_reply(header, vote_granted),
            Message::AppendEntriesCall {
                header,
                commit_index,
                entries,
            } => self.handle_append_entries_call(header, commit_index, entries),
            Message::AppendEntriesReply {
                header,
                last_position,
            } => self.handle_append_entries_reply(header, last_position),
        }
    }

    fn handle_request_vote_call(&mut self, header: MessageHeader, last_position: LogPosition) {
        if header.term < self.current_term {
            let reply =
                Message::request_vote_reply(self.current_term, self.id, header.seqno, false);
            self.actions.set(Action::SendMessage(header.from, reply));
            return;
        }
        if self.log.entries().last_position().index > last_position.index {
            return;
        }

        if self.voted_for.is_none() {
            self.set_voted_for(Some(header.from));
        }
        if self.voted_for != Some(header.from) {
            return;
        }

        let reply = Message::request_vote_reply(self.current_term, self.id, header.seqno, true);
        self.actions.set(Action::SendMessage(header.from, reply));
    }

    fn handle_request_vote_reply(&mut self, header: MessageHeader, vote_granted: bool) {
        let RoleState::Candidate { granted_votes } = &mut self.role else {
            return;
        };
        if !vote_granted {
            return;
        }
        granted_votes.insert(header.from);

        let config = self.log.latest_config();
        let n = config
            .voters
            .iter()
            .filter(|v| granted_votes.contains(v))
            .count();
        if n < self.log.latest_config().voter_majority_count() {
            return;
        }

        let n = config
            .new_voters
            .iter()
            .filter(|v| granted_votes.contains(v))
            .count();
        if n < config.new_voter_majority_count() {
            return;
        }

        self.transition_to_leader();
    }

    /// Handles an election timeout.
    ///
    /// This method is typically invoked when the timeout set by [`Action::SetElectionTimeout`] expires.
    /// However, it can also be invoked by other means, such as to trigger a new election
    /// as quickly as possible when the crate user knows there is no leader.
    ///
    /// # Examples
    ///
    /// ```
    /// let mut node = /* ... ; */
    /// # raftbare::Node::start(raftbare::NodeId::new(1));
    ///
    /// node.handle_election_timeout();
    ///
    /// // Execute actions queued by the timeout handling.
    /// for action in node.actions_mut() {
    ///     // ...
    /// }
    /// ```
    pub fn handle_election_timeout(&mut self) {
        match self.role {
            RoleState::Follower => {
                self.transition_to_candidate();
            }
            RoleState::Candidate { .. } => {
                self.transition_to_candidate();
            }
            RoleState::Leader { .. } => {
                self.heartbeat();
            }
        }
    }

    fn is_valid_snapshot(
        &self,
        last_included_config: &ClusterConfig,
        last_included_position: LogPosition,
    ) -> bool {
        // TODO(?): Remove this redundant check.
        if last_included_position.index < self.log.entries().prev_position().index {
            return false;
        }
        if self.log.entries().last_position().index < last_included_position.index {
            return self.role() != Role::Leader;
        }
        if !self.log.entries().contains(last_included_position) {
            return false;
        }
        self.log.entries().get_config(last_included_position.index) == Some(last_included_config)
    }

    /// Updates this node's log ([`Log`]) to reflect the installation of a snapshot.
    ///
    /// If the node log contains `last_included_position`, log entries up to `last_included_position` are removed.
    /// If `last_included_position` is greater than the last log position, the log is replaced with an empty log starting at `last_included_position`.
    ///
    /// Note that how to install a snapshot is outside of the scope of this crate.
    ///
    /// # Preconditions
    ///
    /// This method returns [`false`] and ignores the installation if the following conditions are not met:
    /// - `last_included_position` is valid, which means:
    ///   - `self.log.entries().contains(last_included_position)` is [`true`].
    ///   - Additionally, if `self.role().is_leader()` is [`false`], it is also acceptable if `last_included_position.index` is greater than `self.log.last_position().index`.
    /// - `last_included_config` is the configuration at `last_included_position.index`.
    pub fn handle_snapshot_installed(
        &mut self,
        last_included_config: ClusterConfig,
        last_included_position: LogPosition,
    ) -> bool {
        if !self.is_valid_snapshot(&last_included_config, last_included_position) {
            return false;
        }
        if let Some(entries) = self.log.entries().since(last_included_position) {
            self.log = Log::new(last_included_config, entries);
        } else {
            self.log = Log::new(
                last_included_config,
                LogEntries::new(last_included_position),
            );
            self.actions.append_log_entries = None;
        }
        true
    }

    fn reply_append_entries(&mut self, call: MessageHeader) {
        let reply = Message::append_entries_reply(
            self.current_term,
            self.id,
            call.seqno,
            self.log.last_position(),
        );
        self.actions.set(Action::SendMessage(call.from, reply));
    }

    fn handle_append_entries_call(
        &mut self,
        header: MessageHeader,
        leader_commit: LogIndex,
        entries: LogEntries,
    ) {
        if !self.role().is_follower() {
            return;
        }
        if header.term < self.current_term {
            // Stale request.
            self.reply_append_entries(header);
            return;
        }

        if self.voted_for.is_none() {
            self.set_voted_for(Some(header.from));
        }

        if self.try_append_log_entries(&entries) {
            let next_commit_index = leader_commit
                .min(self.log.entries().last_position().index)
                .min(entries.last_position().index); // TODO: Add note comment (entries could be truncated by action implementor)
            if self.commit_index < next_commit_index {
                self.commit_index = next_commit_index;
            }
        }

        // TODO(?): Don't reply if request.leader_sn is old
        //          (the reply will be discarded in the leader side anyway)
        self.reply_append_entries(header);
        self.actions.set(Action::SetElectionTimeout);
    }

    fn handle_append_entries_reply(&mut self, header: MessageHeader, last_position: LogPosition) {
        let RoleState::Leader { followers, quorum } = &mut self.role else {
            return;
        };

        let Some(follower) = followers.get_mut(&header.from) else {
            // Replies from unknown nodes are ignored.
            return;
        };

        // TODO: Add doc about seqno A/B problem is not occured here
        //      (high seqno request sent by before-restarting node is too delayed to be replied, and
        //       delivered the reply to the same restarted node. => max_sn is wrongly updated?
        //       => no term check prohibits this case)
        // TODO: Add old term check

        if follower.max_sn < header.seqno {
            quorum.update_seqno(
                self.log.latest_config(),
                header.from,
                follower.max_sn,
                header.seqno,
            );
            follower.max_sn = header.seqno;
        }

        if last_position.index < follower.match_index {
            // Maybe delayed reply.
            // (or the follower's storage has been corrupted. Raft does not handle this case though.)
            // TODO: consider follower.last_sn instead of match index here
            return;
        };

        let self_last_position = self.log.entries().last_position(); // Save the current last entry before (maybe) updating it.
        if self.log.entries().contains(last_position) {
            if follower.match_index < last_position.index {
                let old_match_index = follower.match_index;
                follower.match_index = last_position.index;

                quorum.update_match_index(
                    self.log.latest_config(),
                    header.from,
                    old_match_index,
                    follower.match_index,
                );

                if self.commit_index < follower.match_index {
                    self.update_commit_index_if_possible();
                }
            }

            if last_position.index == self.log.entries().last_position().index {
                // Up-to-date.
                return;
            }
        }

        if last_position.index < self.log.entries().prev_position().index {
            // Send snapshot
            self.actions.set(Action::InstallSnapshot(header.from));
        } else if last_position.index < self_last_position.index {
            // send delta
            let Some(delta) = self.log.entries().since(last_position) else {
                // TODO: handle this case (decrement index and retry to find out ...)
                return;
            };
            let old_seqno = self.seqno.fetch_and_increment();
            let call = Message::append_entries_call(
                self.current_term,
                self.id,
                self.commit_index,
                self.seqno,
                delta,
            );
            self.actions.set(Action::SendMessage(header.from, call));

            let RoleState::Leader { quorum, .. } = &mut self.role else {
                return;
            };
            quorum.update_seqno(self.log.latest_config(), self.id, old_seqno, self.seqno);
        }
    }

    pub(crate) fn quorum(&self) -> Option<&Quorum> {
        if let RoleState::Leader { quorum, .. } = &self.role {
            Some(quorum)
        } else {
            None
        }
    }
}

#[derive(Debug, Clone)]
pub enum RoleState {
    Follower,
    Candidate {
        granted_votes: BTreeSet<NodeId>,
    },
    Leader {
        followers: BTreeMap<NodeId, Follower>,
        quorum: Quorum,
    },
}

#[derive(Debug, Clone)]
pub struct Follower {
    pub match_index: LogIndex,
    pub max_sn: MessageSeqNo,
}

impl Follower {
    pub fn new() -> Self {
        Self {
            match_index: LogIndex::new(0),
            max_sn: MessageSeqNo::UNKNOWN,
        }
    }
}
