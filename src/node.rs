use crate::{
    action::Action,
    config::ClusterConfig,
    log::{LogEntries, LogEntry, LogEntryRef, LogIndex},
    message::{
        AppendEntriesReply, AppendEntriesRequest, Message, RequestVoteReply, RequestVoteRequest,
        SequenceNumber,
    },
    quorum::Quorum,
    Term,
};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId(u64);

impl NodeId {
    pub const fn new(id: u64) -> Self {
        NodeId(id)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone)]
pub struct Node {
    id: NodeId,
    action_queue: VecDeque<Action>, // TODO: BTreeMap<ActionKind,Action>
    role: Role,
    voted_for: Option<NodeId>,
    current_term: Term,
    log: LogEntries,
    config: ClusterConfig, // TODO: leader state
    commit_index: LogIndex,

    // TODO: Factor out role specific states in an enum
    // Candidate state
    granted_votes: BTreeSet<NodeId>,

    // Leader state
    leader_index: LogIndex,
    followers: BTreeMap<NodeId, Follower>,
    quorum: Quorum,
    pub leader_sn: SequenceNumber, // TODO: priv
    ongoing_heartbeats: VecDeque<Heartbeat>,
}

impl Node {
    pub fn start(id: NodeId) -> Self {
        let term = Term::new(0);
        let index = LogIndex::new(0);
        let config = ClusterConfig::new();
        let quorum = Quorum::new(&config);
        let mut this = Self {
            id,
            action_queue: VecDeque::new(),
            role: Role::Follower,
            voted_for: None,
            current_term: term,
            log: LogEntries::new(LogEntryRef::new(term, index)),
            config,
            commit_index: LogIndex::new(0),

            // candidate state
            granted_votes: BTreeSet::new(),

            // leader state
            leader_index: LogIndex::new(0),
            followers: BTreeMap::new(),
            quorum,
            leader_sn: SequenceNumber::new(),
            ongoing_heartbeats: VecDeque::new(),
        };
        this.enqueue_action(Action::CreateLog(LogEntry::Term(term)));
        this
    }

    pub fn restart(
        id: NodeId,
        current_term: Term,
        voted_for: Option<NodeId>,
        config: ClusterConfig, // TODO: remove(?)
        log: LogEntries,
    ) -> Self {
        // TODO: Return Err(_) if the log doesn't contain cluster config
        let mut node = Self::start(id);
        node.action_queue.clear();

        node.current_term = current_term;
        node.voted_for = voted_for;
        node.log = log;
        node.config = config;
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
        self.config.voters.insert(self.id);

        self.quorum = Quorum::new(&self.config);

        // Optimized propose
        self.append_log_entry(&LogEntry::Term(self.current_term));
        self.leader_index = self.log.last.index;

        self.append_log_entry(&LogEntry::ClusterConfig(self.config.clone()));
        self.commit(self.log.last.index);

        debug_assert!(self.followers.is_empty());
        debug_assert_eq!(self.quorum.commit_index(), self.commit_index);

        true
    }

    fn commit(&mut self, index: LogIndex) {
        self.commit_index = index;
        self.enqueue_action(Action::NotifyCommitted(index));
    }

    // TODOO: rename(?)
    pub fn heartbeat(&mut self) -> Heartbeat {
        // TODO: handle single node case

        let sn = self.leader_sn;
        let request = Message::append_entries_request(
            self.current_term,
            self.id,
            self.commit_index,
            sn,
            LogEntries::new(self.log.last),
        );
        self.quorum
            .update_seqnum(&self.config, self.id, self.leader_sn, self.leader_sn.next());
        self.leader_sn = sn.next();
        self.broadcast_message(request);

        let heartbeat = Heartbeat::new(self.current_term, sn);
        self.ongoing_heartbeats.push_back(heartbeat);

        heartbeat
    }

    pub fn propose_command(&mut self) -> Option<LogIndex> {
        // TODO: Return CommitPromise or else
        if self.role != Role::Leader {
            return None;
        }
        Some(self.propose(LogEntry::Command))
    }

    fn propose(&mut self, entry: LogEntry) -> LogIndex {
        debug_assert_eq!(self.role, Role::Leader);

        // TODO: Create LogEnties instance only once
        let prev_entry = self.log.last;
        self.append_log_entry(&entry); // TODO: merge the same kind actions
        self.broadcast_message(Message::append_entries_request(
            self.current_term,
            self.id,
            self.commit_index,
            self.leader_sn,
            LogEntries::single(prev_entry, &entry),
        ));
        self.quorum
            .update_seqnum(&self.config, self.id, self.leader_sn, self.leader_sn.next());
        self.leader_sn = self.leader_sn.next();
        self.check_heartbeat();
        self.enqueue_action(Action::SetElectionTimeout); // TODO: merge the same kind actions
        self.update_commit_index_if_possible(); // TODO: check single node

        self.log.last.index
    }

    fn rebuild_followers(&mut self) {
        for id in self.config.members() {
            if id == self.id {
                continue;
            }
            self.followers.insert(id, Follower::new());
        }
        self.followers.retain(|id, _| self.config.contains(*id));
    }

    fn rebuild_quorum(&mut self) {
        self.quorum = Quorum::new(&self.config);

        let zero = LogIndex::new(0);
        self.quorum
            .update_match_index(&self.config, self.id, zero, self.log.last.index);
        self.quorum
            .update_seqnum(&self.config, self.id, SequenceNumber::new(), self.leader_sn);

        for (&id, follower) in &mut self.followers {
            self.quorum
                .update_match_index(&self.config, id, zero, follower.match_index);
            self.quorum
                .update_seqnum(&self.config, id, SequenceNumber::new(), follower.max_sn);
        }
    }

    fn broadcast_message(&mut self, message: Message) {
        self.enqueue_action(Action::BroadcastMessage(message));
    }

    fn unicast_message(&mut self, destination: NodeId, message: Message) {
        self.enqueue_action(Action::UnicastMessage(destination, message));
    }

    fn update_commit_index_if_possible(&mut self) {
        debug_assert!(self.role.is_leader());

        let new_commit_index = self.quorum.commit_index();
        if self.commit_index < new_commit_index && self.log.term_index() <= new_commit_index {
            self.commit(new_commit_index);

            if self.config.is_joint_consensus()
                && self.log.latest_config_index() <= new_commit_index
            {
                self.finalize_joint_consensus();
            }
        }
    }

    fn finalize_joint_consensus(&mut self) {
        debug_assert!(self.role.is_leader());
        debug_assert!(self.config.is_joint_consensus());

        let mut new_config = self.config.clone();
        new_config.voters = std::mem::take(&mut new_config.new_voters);
        self.propose(LogEntry::ClusterConfig(new_config));
    }

    pub fn change_cluster_config(
        &mut self,
        new_config: &ClusterConfig,
    ) -> Result<LogIndex, ChangeClusterConfigError> {
        if !self.role.is_leader() {
            return Err(ChangeClusterConfigError::NotLeader);
        }
        if self.config.voters != new_config.voters {
            return Err(ChangeClusterConfigError::VotersMismatched);
        }
        if self.config.is_joint_consensus() {
            return Err(ChangeClusterConfigError::JointConsensusInProgress);
        }

        let index = self.propose(LogEntry::ClusterConfig(new_config.clone()));
        Ok(index)
    }

    fn append_log_entry(&mut self, entry: &LogEntry) {
        debug_assert!(self.role.is_leader());

        let prev_index = self.log.last.index;
        self.enqueue_action(Action::AppendLogEntries(LogEntries::single(
            self.log.last,
            entry,
        )));
        self.log.append_entry(entry);

        // TODO: unnecessary condition?
        if self.role.is_leader() {
            self.quorum
                .update_match_index(&self.config, self.id, prev_index, self.log.last.index);
        }

        if let LogEntry::ClusterConfig(new_config) = entry {
            self.config = new_config.clone();
            // TODO: unnecessary condition?
            if self.role.is_leader() {
                self.rebuild_followers();
                self.rebuild_quorum();
            }
        }
    }

    fn try_append_log_entries(&mut self, entries: &LogEntries) -> bool {
        debug_assert!(self.role.is_follower());

        if self.log.contains(entries.last) {
            // Already up-to-date.
            return true;
        }
        if !self.log.contains(entries.prev) {
            // Cannot append.
            return false;
        }

        // Append.
        let truncated = self.log.last.index != entries.prev.index;
        self.enqueue_action(Action::AppendLogEntries(entries.clone()));
        self.log.append_entries(entries);
        if let Some((_, new_config)) = entries.configs.last_key_value() {
            self.config = new_config.clone();
        } else if truncated {
            if let Some(latest_config) = self.log.configs.last_key_value().map(|x| x.1.clone()) {
                self.config = latest_config;
            }
        }
        true
    }

    fn set_current_term(&mut self, term: Term) {
        self.current_term = term;
        self.enqueue_action(Action::SaveCurrentTerm(term));
    }

    fn set_voted_for(&mut self, voted_for: Option<NodeId>) {
        self.voted_for = voted_for;

        for action in &mut self.action_queue {
            if matches!(action, Action::SaveVotedFor(..)) {
                *action = Action::SaveVotedFor(voted_for);
                return;
            }
        }
        self.enqueue_action(Action::SaveVotedFor(voted_for));
    }

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
                self.check_heartbeat(); // TODO: call only if needed
            }
        }
    }

    fn handle_request_vote_request(&mut self, request: &RequestVoteRequest) {
        if request.term < self.current_term {
            self.unicast_message(
                request.from,
                Message::request_vote_reply(self.current_term, self.id, false),
            );
            return;
        }
        if self.log.last.index > request.last_entry.index {
            return;
        }

        if self.voted_for.is_none() {
            self.set_voted_for(Some(request.from));
        }
        if self.voted_for != Some(request.from) {
            return;
        }
        self.unicast_message(
            request.from,
            Message::request_vote_reply(self.current_term, self.id, true),
        );
    }

    fn handle_request_vote_reply(&mut self, reply: &RequestVoteReply) {
        if !reply.vote_granted {
            return;
        }
        self.granted_votes.insert(reply.from);

        let n = self
            .config
            .voters
            .iter()
            .filter(|v| self.granted_votes.contains(v))
            .count();
        if n < self.config.voter_majority_count() {
            return;
        }

        let n = self
            .config
            .new_voters
            .iter()
            .filter(|v| self.granted_votes.contains(v))
            .count();
        if n < self.config.new_voter_majority_count() {
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
                self.ongoing_heartbeats.pop_back(); // TODO: optimize
            }
        }
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
            self.log.last,
        ));
        self.enqueue_action(Action::SetElectionTimeout);
    }

    fn enter_leader(&mut self) {
        debug_assert_eq!(self.voted_for, Some(self.id));

        self.role = Role::Leader;
        self.followers.clear();
        self.rebuild_followers();
        self.rebuild_quorum();
        self.leader_sn = SequenceNumber::new();
        self.ongoing_heartbeats = VecDeque::new();

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
        self.unicast_message(
            request.from,
            Message::append_entries_reply(
                self.current_term,
                self.id,
                request.leader_sn,
                self.log.last,
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
                .min(self.log.last.index)
                .min(request.entries.last.index); // TODO: Add note comment (entries could be truncated by action implementor)
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
            self.quorum
                .update_seqnum(&self.config, reply.from, follower.max_sn, reply.leader_sn);
            follower.max_sn = reply.leader_sn;
        }

        if reply.last_entry.index < follower.match_index {
            // Maybe delayed reply.
            // (or the follower's storage has been corrupted. Raft does not handle this case though.)
            // TODO: consider follower.last_sn instead of match index here
            return;
        };

        let self_last_entry = self.log.last; // Save the current last entry before (maybe) updating it.
        let last_entry = if self.log.contains(reply.last_entry) {
            if follower.match_index < reply.last_entry.index {
                let old_match_index = follower.match_index;
                follower.match_index = reply.last_entry.index;

                self.quorum.update_match_index(
                    &self.config,
                    reply.from,
                    old_match_index,
                    follower.match_index,
                );

                if self.commit_index < follower.match_index {
                    self.update_commit_index_if_possible();
                }
            }

            if reply.last_entry.index == self.log.last.index {
                // Up-to-date.
                return;
            }
            reply.last_entry
        } else {
            // TODO: find matched log entry (decrement next_index and send empty entries)
            todo!()
        };
        if last_entry.index < self.log.prev.index {
            // send snapshot
            todo!()
        } else if last_entry.index < self_last_entry.index {
            // send delta
            let Some(delta) = self.log.since(last_entry) else {
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
            self.unicast_message(reply.from, msg);
            self.quorum
                .update_seqnum(&self.config, self.id, self.leader_sn, self.leader_sn.next());
            self.check_heartbeat();
            self.leader_sn = self.leader_sn.next();
        }
    }

    fn check_heartbeat(&mut self) {
        if self.ongoing_heartbeats.is_empty() {
            return;
        }

        let accepted_sn = self.quorum.smallest_majority_seqnum();
        while let Some(h) = self.ongoing_heartbeats.front().copied() {
            if accepted_sn <= h.sn {
                self.ongoing_heartbeats.pop_front();
                self.enqueue_action(Action::NotifyHeartbeatSucceeded(h));
            } else {
                break;
            }
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

    pub fn cluster_config(&self) -> &ClusterConfig {
        &self.config
    }

    pub fn log(&self) -> &LogEntries {
        &self.log
    }

    pub fn next_action(&mut self) -> Option<Action> {
        self.action_queue.pop_front()
    }

    pub fn take_actions(&mut self) -> impl '_ + Iterator<Item = Action> {
        self.action_queue.drain(..)
    }

    fn enqueue_action(&mut self, action: Action) {
        self.action_queue.push_back(action);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

impl Role {
    pub const fn is_leader(self) -> bool {
        matches!(self, Self::Leader)
    }

    pub const fn is_follower(self) -> bool {
        matches!(self, Self::Follower)
    }

    pub const fn is_candidate(self) -> bool {
        matches!(self, Self::Candidate)
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
    pub max_sn: SequenceNumber,
}

impl Follower {
    pub fn new() -> Self {
        Self {
            match_index: LogIndex::new(0),
            max_sn: SequenceNumber::from_u64(0),
        }
    }
}

// TODO: s/../HeartbeatPromise (or else)/
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Heartbeat {
    pub term: Term,
    pub sn: SequenceNumber,
}

impl Heartbeat {
    fn new(term: Term, sn: SequenceNumber) -> Self {
        Self { term, sn }
    }
}
