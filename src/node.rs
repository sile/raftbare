use crate::{
    action::Action,
    config::ClusterConfig,
    event::Event,
    log::{LogEntries, LogEntry, LogEntryRef, LogIndex},
    Term,
};
use std::collections::VecDeque;

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
}

impl Node {
    pub fn start(id: NodeId) -> Self {
        let term = Term::new(0);
        let index = LogIndex::new(0);
        let mut this = Self {
            id,
            action_queue: VecDeque::new(),
            role: Role::Follower,
            voted_for: None,
            current_term: term,
            log: LogEntries::new(LogEntryRef::new(term, index)),
            config: ClusterConfig::new(),
            commit_index: LogIndex::new(0),
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

        self.role = Role::Leader;
        self.set_current_term(self.current_term.next());
        self.set_voted_for(Some(self.id));
        self.config.voters.insert(self.id);

        // Optimized propose
        self.append_log_entry(&LogEntry::Term(self.current_term));
        self.append_log_entry(&LogEntry::ClusterConfig(self.config.clone()));
        self.commit(self.log.last.index);

        true
    }

    fn commit(&mut self, index: LogIndex) {
        self.commit_index = index;
        self.enqueue_action(Action::NotifyCommitted(index));
    }

    // TODO: fn propose_command(&mut self, n: usize) -> Result<()>;

    fn propose(&mut self, entry: LogEntry) {
        debug_assert_eq!(self.role, Role::Leader);

        self.append_log_entry(&entry);
        self.update_commit_index_if_possible();
        // TODO: self.broadcast_message();
    }

    fn update_commit_index_if_possible(&mut self) {
        // TODO
    }

    pub fn change_cluster_config(
        &mut self,
        new_config: ClusterConfig,
    ) -> Result<(), ChangeClusterConfigError> {
        if !self.role.is_leader() {
            return Err(ChangeClusterConfigError::NotLeader);
        }
        if self.config.voters != new_config.voters {
            return Err(ChangeClusterConfigError::VotersMismatched);
        }
        if self.config.is_joint_consensus() {
            return Err(ChangeClusterConfigError::JointConsensusInProgress);
        }

        self.propose(LogEntry::ClusterConfig(new_config.clone()));
        self.config = new_config;

        Ok(())
    }

    fn append_log_entry(&mut self, entry: &LogEntry) {
        self.enqueue_action(Action::AppendLogEntries(LogEntries::single(
            self.log.last,
            entry,
        )));
        self.log.append_entry(entry);
    }

    fn set_current_term(&mut self, term: Term) {
        self.current_term = term;
        self.enqueue_action(Action::SaveCurrentTerm(term));
    }

    fn set_voted_for(&mut self, voted_for: Option<NodeId>) {
        self.voted_for = voted_for;
        self.enqueue_action(Action::SaveVotedFor(voted_for));
    }

    pub fn id(&self) -> NodeId {
        self.id
    }

    pub fn role(&self) -> Role {
        self.role
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

    pub fn handle_event(&mut self, _event: Event) {}

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
