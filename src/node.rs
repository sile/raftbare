use crate::{
    action::Action,
    config::ClusterConfig,
    log::{LogEntries, LogEntry, LogEntryRef, LogIndex},
    message::{AppendEntriesRequest, Message},
    quorum::Quorum,
    Term,
};
use std::collections::{BTreeMap, VecDeque};

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
    action_queue: VecDeque<Action>,
    role: Role,
    voted_for: Option<NodeId>,
    current_term: Term,
    log: LogEntries,
    config: ClusterConfig,
    commit_index: LogIndex,

    // Leader state
    leader_index: LogIndex,
    followers: BTreeMap<NodeId, Follower>,
    quorum: Quorum,
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

            // leader state
            leader_index: LogIndex::new(0),
            followers: BTreeMap::new(),
            quorum,
        };
        this.enqueue_action(Action::CreateLog(LogEntry::Term(term)));
        this
    }

    // TODO: restart (id: NodeId, log_since_snapshot: LogEntries) -> Self
    // TODO: consistent_query() or heartbeat() -> Heartbeat
    // impl Heartbeat { pub fn handle_event(&mut self,.. ); pub fn is_latest_leader(&self) -> Option<bool>; }

    pub fn create_cluster(&mut self) -> bool {
        if self.current_term != Term::new(0) {
            return false;
        }

        // TODO: factor out
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

        // TODO: set heartbeat timeout

        debug_assert!(self.followers.is_empty());
        debug_assert_eq!(self.quorum.commit_index(), self.commit_index);

        true
    }

    fn commit(&mut self, index: LogIndex) {
        self.commit_index = index;
        self.enqueue_action(Action::NotifyCommitted(index));
    }

    // TODO: fn propose_command(&mut self, n: usize) -> Result<()>;

    fn propose(&mut self, entry: LogEntry) -> LogIndex {
        debug_assert_eq!(self.role, Role::Leader);

        // TODO: Create LogEnties instance only once
        let prev_entry = self.log.last;
        self.append_log_entry(&entry);
        if let LogEntry::ClusterConfig(new_config) = &entry {
            self.config = new_config.clone();
            self.rebuild_quorum();
        }
        self.broadcast_message(Message::append_entries_request(
            self.current_term,
            self.id,
            self.commit_index,
            LogEntries::single(prev_entry, &entry),
        ));
        self.update_commit_index_if_possible();

        self.log.last.index
    }

    fn rebuild_quorum(&mut self) {
        self.quorum = Quorum::new(&self.config);

        let zero = LogIndex::new(0);
        self.quorum
            .update_match_index(&self.config, self.id, zero, self.log.last.index);

        for (&id, follower) in &mut self.followers {
            self.quorum
                .update_match_index(&self.config, id, zero, follower.match_index);
        }
    }

    fn broadcast_message(&mut self, message: Message) {
        self.enqueue_action(Action::BroadcastMessage(message));
    }

    fn unicast_message(&mut self, destination: NodeId, message: Message) {
        self.enqueue_action(Action::UnicastMessage(destination, message));
    }

    fn update_commit_index_if_possible(&mut self) {
        let new_commit_index = self.quorum.commit_index();
        if self.commit_index < new_commit_index {
            self.commit(new_commit_index);
        }
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
        let prev_index = self.log.last.index;
        self.enqueue_action(Action::AppendLogEntries(LogEntries::single(
            self.log.last,
            entry,
        )));
        self.log.append_entry(entry);

        if self.role.is_leader() {
            self.quorum
                .update_match_index(&self.config, self.id, prev_index, self.log.last.index);
        }
    }

    fn set_current_term(&mut self, term: Term) {
        self.current_term = term;
        self.enqueue_action(Action::SaveCurrentTerm(term));
    }

    fn set_voted_for(&mut self, voted_for: Option<NodeId>) {
        self.voted_for = voted_for;
        self.enqueue_action(Action::SaveVotedFor(voted_for));
    }

    pub fn handle_message(&mut self, message: &Message) {
        if self.current_term < message.term() {
            if matches!(message, Message::RequestVoteRequest { .. })
                && self.role.is_follower()
                && self.voted_for.map_or(false, |id| id != message.from())
            {
                // TODO: note comment
                return;
            }
            //
        }

        match message {
            Message::RequestVoteRequest { .. } => todo!(),
            Message::AppendEntriesRequest(msg) => self.handle_append_entries_request(msg),
            Message::AppendEntriesReply { .. } => todo!(),
        }
    }

    fn reply_append_entries(&mut self, request: &AppendEntriesRequest, success: bool) {
        self.unicast_message(
            request.from,
            Message::append_entries_reply(self.current_term, self.id, self.log.last, success),
        );
    }

    fn handle_append_entries_request(&mut self, request: &AppendEntriesRequest) {
        if request.term < self.current_term {
            self.reply_append_entries(request, false);
            return;
        }
        if !self.log.contains(request.entries.prev) {
            self.reply_append_entries(request, false);
            return;
        }
        todo!()
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
    pub next_index: LogIndex,
    pub match_index: LogIndex,
}

impl Follower {
    pub fn new(next_index: LogIndex) -> Self {
        Self {
            next_index,
            match_index: LogIndex::new(0),
        }
    }
}
