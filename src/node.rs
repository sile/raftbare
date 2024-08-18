use crate::{
    action::{Action, Actions},
    config::ClusterConfig,
    log::{LogEntries, LogEntry, LogIndex, LogPosition},
    message::{
        AppendEntriesReply, AppendEntriesRequest, Message, MessageSeqNo, RequestVoteReply,
        RequestVoteRequest,
    },
    quorum::Quorum,
    CommitPromise, HeartbeatPromise, Log, Role, Term,
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

#[derive(Debug, Clone)]
pub struct Node {
    id: NodeId,
    role: Role,
    voted_for: Option<NodeId>,
    current_term: Term,
    log: Log,
    commit_index: LogIndex,
    actions: Actions,

    // TODO: Factor out role specific states in an enum
    // Candidate state
    granted_votes: BTreeSet<NodeId>,

    // Leader state
    leader_index: LogIndex,
    followers: BTreeMap<NodeId, Follower>,
    quorum: Quorum,
    pub leader_sn: MessageSeqNo, // TODO: priv
}

impl Node {
    pub fn start(id: NodeId) -> Self {
        let term = Term::new(0);
        let index = LogIndex::new(0);
        let config = ClusterConfig::new();
        let quorum = Quorum::new(&config);
        Self {
            id,
            role: Role::Follower,
            voted_for: None,
            current_term: term,
            log: Log::new(config, LogEntries::new(LogPosition::new(term, index))),
            commit_index: LogIndex::new(0),
            actions: Actions::default(),

            // candidate state
            granted_votes: BTreeSet::new(),

            // leader state
            leader_index: LogIndex::new(0),
            followers: BTreeMap::new(),
            quorum,
            leader_sn: MessageSeqNo::new(),
        }
    }

    pub fn restart(id: NodeId, current_term: Term, voted_for: Option<NodeId>, log: Log) -> Self {
        let mut node = Self::start(id);

        node.current_term = current_term;
        node.voted_for = voted_for;
        node.log = log;
        node
    }

    pub fn create_cluster(&mut self) -> bool {
        if self.current_term != Term::new(0) {
            return false;
        }

        // TODO: factor out (with enter_leader())
        self.role = Role::Leader;
        self.set_current_term(self.current_term.next());
        self.set_voted_for(Some(self.id));

        let mut config = self.log.latest_config().clone();
        config.voters.insert(self.id);

        self.quorum = Quorum::new(&config);

        // Optimized propose (TODO)
        self.append_log_entry(&LogEntry::Term(self.current_term));
        self.leader_index = self.log.entries().last_position().index;

        self.append_log_entry(&LogEntry::ClusterConfig(config));
        self.commit(self.log.entries().last_position().index);

        debug_assert!(self.followers.is_empty());
        debug_assert_eq!(self.quorum.commit_index(), self.commit_index);

        true
    }

    fn commit(&mut self, index: LogIndex) {
        self.commit_index = index;
    }

    // TODOO: rename(?)
    pub fn heartbeat(&mut self) -> HeartbeatPromise {
        // TODO: handle single node case

        let sn = self.leader_sn;
        let request = Message::append_entries_request(
            self.current_term,
            self.id,
            self.commit_index,
            sn,
            LogEntries::new(self.log.entries().last_position()),
        );
        self.quorum.update_seqnum(
            self.log.latest_config(),
            self.id,
            self.leader_sn,
            self.leader_sn.next(),
        );
        self.leader_sn = sn.next();
        self.broadcast_message(request);

        let heartbeat = HeartbeatPromise::new(self.current_term, sn);

        heartbeat
    }

    pub fn propose_command(&mut self) -> CommitPromise {
        if self.role != Role::Leader {
            return CommitPromise::Rejected;
        }
        self.propose(LogEntry::Command)
    }

    fn propose(&mut self, entry: LogEntry) -> CommitPromise {
        debug_assert_eq!(self.role, Role::Leader);

        // TODO: Create LogEnties instance only once
        let prev_entry = self.log.entries().last_position();
        self.append_log_entry(&entry); // TODO: merge the same kind actions
        self.broadcast_message(Message::append_entries_request(
            self.current_term,
            self.id,
            self.commit_index,
            self.leader_sn,
            LogEntries::from_iter(prev_entry, std::iter::once(entry)),
        ));
        self.quorum.update_seqnum(
            self.log.latest_config(),
            self.id,
            self.leader_sn,
            self.leader_sn.next(),
        );
        self.leader_sn = self.leader_sn.next();
        self.enqueue_action(Action::SetElectionTimeout); // TODO: merge the same kind actions
        self.update_commit_index_if_possible(); // TODO: check single node

        let index = self.log.entries().last_position().index;
        let term = self.current_term;
        let position = LogPosition { term, index };
        CommitPromise::new(position)
    }

    fn rebuild_followers(&mut self) {
        let config = self.log.latest_config();
        for id in config.unique_nodes() {
            if id == self.id {
                continue;
            }
            self.followers.insert(id, Follower::new());
        }
        self.followers.retain(|id, _| config.contains(*id));
    }

    fn rebuild_quorum(&mut self) {
        let config = self.log.latest_config();
        self.quorum = Quorum::new(config);

        let zero = LogIndex::new(0);
        self.quorum.update_match_index(
            config,
            self.id,
            zero,
            self.log.entries().last_position().index,
        );
        self.quorum
            .update_seqnum(config, self.id, MessageSeqNo::new(), self.leader_sn);

        for (&id, follower) in &mut self.followers {
            self.quorum
                .update_match_index(config, id, zero, follower.match_index);
            self.quorum
                .update_seqnum(config, id, MessageSeqNo::new(), follower.max_sn);
        }
    }

    fn broadcast_message(&mut self, message: Message) {
        self.enqueue_action(Action::BroadcastMessage(message));
    }

    fn send_message(&mut self, destination: NodeId, message: Message) {
        self.enqueue_action(Action::SendMessage(destination, message));
    }

    fn update_commit_index_if_possible(&mut self) {
        debug_assert!(self.role.is_leader());

        let new_commit_index = self.quorum.commit_index();
        if self.commit_index < new_commit_index
            && self.log.entries().get_term(new_commit_index) == Some(self.current_term)
        {
            self.commit(new_commit_index);

            if self.log.latest_config().is_joint_consensus()
                && self.log.latest_config_index() <= new_commit_index
            {
                self.finalize_joint_consensus();
            }
        }
    }

    fn finalize_joint_consensus(&mut self) {
        debug_assert!(self.role.is_leader());
        debug_assert!(self.log.latest_config().is_joint_consensus());

        let mut new_config = self.log.latest_config().clone();
        new_config.voters = std::mem::take(&mut new_config.new_voters);
        self.propose(LogEntry::ClusterConfig(new_config));

        // TODO: leader not in new config steps down when the new config is committed
    }

    pub fn change_cluster_config(
        &mut self,
        new_config: &ClusterConfig,
    ) -> Result<CommitPromise, ChangeClusterConfigError> {
        if !self.role.is_leader() {
            return Err(ChangeClusterConfigError::NotLeader);
        }
        if self.log.latest_config().voters != new_config.voters {
            return Err(ChangeClusterConfigError::VotersMismatched);
        }
        if self.log.latest_config().is_joint_consensus() {
            return Err(ChangeClusterConfigError::JointConsensusInProgress);
        }

        Ok(self.propose(LogEntry::ClusterConfig(new_config.clone())))
    }

    fn append_log_entry(&mut self, entry: &LogEntry) {
        debug_assert!(self.role.is_leader());

        let prev_index = self.log.entries().last_position().index;
        self.enqueue_action(Action::AppendLogEntries(LogEntries::from_iter(
            self.log.entries().last_position(),
            std::iter::once(entry.clone()),
        )));
        self.log.entries_mut().push(entry.clone());

        // TODO: unnecessary condition?
        if self.role.is_leader() {
            self.quorum.update_match_index(
                self.log.latest_config(),
                self.id,
                prev_index,
                self.log.entries().last_position().index,
            );
        }

        if let LogEntry::ClusterConfig(_) = entry {
            // TODO: unnecessary condition?
            if self.role.is_leader() {
                self.rebuild_followers();
                self.rebuild_quorum();
            }
        }
    }

    fn try_append_log_entries(&mut self, entries: &LogEntries) -> bool {
        debug_assert!(self.role.is_follower());

        if self.log.entries().contains(entries.last_position()) {
            // Already up-to-date.
            return true;
        }
        if !self.log.entries().contains(entries.prev_position()) {
            // Cannot append.
            return false;
        }

        // Append.
        self.enqueue_action(Action::AppendLogEntries(entries.clone()));
        self.log.entries_mut().append(entries);
        true
    }

    fn set_current_term(&mut self, term: Term) {
        self.current_term = term;
        self.enqueue_action(Action::SaveCurrentTerm);
    }

    fn set_voted_for(&mut self, voted_for: Option<NodeId>) {
        self.voted_for = voted_for;
        self.enqueue_action(Action::SaveVotedFor);
    }

    // TODO: split into handle_request_vote_request, ... to make it possible to return errors
    // or keep this method but return errors
    pub fn handle_message(&mut self, msg: &Message) {
        if self.current_term < msg.term() {
            if matches!(msg, Message::RequestVoteRequest { .. })
                && self.role.is_follower()
                && self.voted_for.map_or(false, |id| id != msg.from())
            {
                return;
            }
            self.enter_follower(msg.term());
        }

        match msg {
            Message::RequestVoteRequest(msg) => self.handle_request_vote_request(msg),
            Message::RequestVoteReply(msg) => self.handle_request_vote_reply(msg),
            Message::AppendEntriesRequest(msg) => self.handle_append_entries_request(msg),
            Message::AppendEntriesReply(msg) => {
                self.handle_append_entries_reply(msg);
            }
        }
    }

    fn handle_request_vote_request(&mut self, request: &RequestVoteRequest) {
        if request.term < self.current_term {
            self.send_message(
                request.from,
                Message::request_vote_reply(self.current_term, self.id, false),
            );
            return;
        }
        if self.log.entries().last_position().index > request.last_entry.index {
            return;
        }

        if self.voted_for.is_none() {
            self.set_voted_for(Some(request.from));
        }
        if self.voted_for != Some(request.from) {
            return;
        }
        self.send_message(
            request.from,
            Message::request_vote_reply(self.current_term, self.id, true),
        );
    }

    fn handle_request_vote_reply(&mut self, reply: &RequestVoteReply) {
        if !reply.vote_granted {
            return;
        }
        self.granted_votes.insert(reply.from);

        let config = self.log.latest_config();
        let n = config
            .voters
            .iter()
            .filter(|v| self.granted_votes.contains(v))
            .count();
        if n < self.log.latest_config().voter_majority_count() {
            return;
        }

        let n = config
            .new_voters
            .iter()
            .filter(|v| self.granted_votes.contains(v))
            .count();
        if n < config.new_voter_majority_count() {
            return;
        }

        self.enter_leader();
    }

    pub fn handle_election_timeout(&mut self) {
        match self.role {
            Role::Follower => {
                self.start_new_election();
            }
            Role::Candidate => {
                self.start_new_election();
            }
            Role::Leader => {
                self.heartbeat();
            }
        }
    }

    fn is_valid_snapshot(
        &self,
        last_included_config: &ClusterConfig,
        last_included_position: LogPosition,
    ) -> bool {
        if last_included_position.index < self.log.entries().prev_position().index {
            return false;
        }
        if self.log.entries().last_position().index < last_included_position.index {
            return self.role != Role::Leader;
        }
        if !self.log.entries().contains(last_included_position) {
            return false;
        }
        self.log.entries().get_config(last_included_position.index) == Some(last_included_config)
    }

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

    fn start_new_election(&mut self) {
        self.role = Role::Candidate;
        self.set_current_term(self.current_term.next());
        self.set_voted_for(Some(self.id));
        self.granted_votes.clear();
        self.granted_votes.insert(self.id);

        self.broadcast_message(Message::request_vote_request(
            self.current_term,
            self.id,
            self.log.entries().last_position(),
        ));
        self.enqueue_action(Action::SetElectionTimeout);
    }

    fn enter_leader(&mut self) {
        debug_assert_eq!(self.voted_for, Some(self.id));

        self.role = Role::Leader;
        self.followers.clear();
        self.rebuild_followers();
        self.rebuild_quorum();
        self.leader_sn = MessageSeqNo::new();

        self.propose(LogEntry::Term(self.current_term));
    }

    fn enter_follower(&mut self, term: Term) {
        self.role = Role::Follower;
        self.set_current_term(term);
        self.set_voted_for(None);
        // self.quorum.clear();
        self.followers.clear();
        self.enqueue_action(Action::SetElectionTimeout);
    }

    fn reply_append_entries(&mut self, request: &AppendEntriesRequest) {
        self.send_message(
            request.from,
            Message::append_entries_reply(
                self.current_term,
                self.id,
                request.leader_sn,
                self.log.entries().last_position(),
            ),
        );
    }

    fn handle_append_entries_request(&mut self, request: &AppendEntriesRequest) {
        if !self.role.is_follower() {
            return;
        }
        if request.term < self.current_term {
            // Stale request.
            self.reply_append_entries(request);
            return;
        }

        if self.voted_for.is_none() {
            self.set_voted_for(Some(request.from));
        }

        if self.try_append_log_entries(&request.entries) {
            let next_commit_index = request
                .leader_commit
                .min(self.log.entries().last_position().index)
                .min(request.entries.last_position().index); // TODO: Add note comment (entries could be truncated by action implementor)
            if self.commit_index < next_commit_index {
                self.commit(next_commit_index);
            }
        }

        // TODO(?): Don't reply if request.leader_sn is old
        //          (the reply will be discarded in the leader side anyway)
        self.reply_append_entries(request);
        // TODO: reset election timeout
    }

    fn handle_append_entries_reply(&mut self, reply: &AppendEntriesReply) {
        if !self.role.is_leader() {
            return;
        }

        let Some(follower) = self.followers.get_mut(&reply.from) else {
            // Replies from unknown nodes are ignored.
            return;
        };
        if follower.max_sn < reply.leader_sn {
            self.quorum.update_seqnum(
                self.log.latest_config(),
                reply.from,
                follower.max_sn,
                reply.leader_sn,
            );
            follower.max_sn = reply.leader_sn;
        }

        if reply.last_entry.index < follower.match_index {
            // Maybe delayed reply.
            // (or the follower's storage has been corrupted. Raft does not handle this case though.)
            // TODO: consider follower.last_sn instead of match index here
            return;
        };

        let self_last_entry = self.log.entries().last_position(); // Save the current last entry before (maybe) updating it.
        if self.log.entries().contains(reply.last_entry) {
            if follower.match_index < reply.last_entry.index {
                let old_match_index = follower.match_index;
                follower.match_index = reply.last_entry.index;

                self.quorum.update_match_index(
                    self.log.latest_config(),
                    reply.from,
                    old_match_index,
                    follower.match_index,
                );

                if self.commit_index < follower.match_index {
                    self.update_commit_index_if_possible();
                }
            }

            if reply.last_entry.index == self.log.entries().last_position().index {
                // Up-to-date.
                return;
            }
        }

        let last_entry = reply.last_entry;
        if last_entry.index < self.log.entries().prev_position().index {
            // Send snapshot
            self.enqueue_action(Action::InstallSnapshot(reply.from));
        } else if last_entry.index < self_last_entry.index {
            // send delta
            let Some(delta) = self.log.entries().since(last_entry) else {
                // Wrong reply.
                return;
            };
            let msg = Message::append_entries_request(
                self.current_term,
                self.id,
                self.commit_index,
                self.leader_sn,
                delta,
            );
            self.send_message(reply.from, msg);
            self.quorum.update_seqnum(
                self.log.latest_config(),
                self.id,
                self.leader_sn,
                self.leader_sn.next(),
            );
            self.leader_sn = self.leader_sn.next();
        }
    }

    pub fn id(&self) -> NodeId {
        self.id
    }

    pub fn role(&self) -> Role {
        self.role
    }

    pub fn commit_index(&self) -> LogIndex {
        self.commit_index
    }

    pub fn voted_for(&self) -> Option<NodeId> {
        self.voted_for
    }

    pub fn current_term(&self) -> Term {
        self.current_term
    }

    pub fn config(&self) -> &ClusterConfig {
        self.log.latest_config()
    }

    pub fn peers(&self) -> impl '_ + Iterator<Item = NodeId> {
        self.config()
            .unique_nodes()
            .filter(move |&node| node != self.id)
    }

    pub fn log(&self) -> &Log {
        &self.log
    }

    pub fn actions(&self) -> &Actions {
        &self.actions
    }

    pub fn actions_mut(&mut self) -> &mut Actions {
        &mut self.actions
    }

    pub fn next_action(&mut self) -> Option<Action> {
        self.actions.next()
    }

    // TODO: rename
    fn enqueue_action(&mut self, action: Action) {
        self.actions.set(action);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChangeClusterConfigError {
    NotLeader,
    VotersMismatched,
    JointConsensusInProgress,
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
            max_sn: MessageSeqNo::from_u64(0),
        }
    }
}
