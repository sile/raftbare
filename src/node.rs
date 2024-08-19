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
    pub fn start(id: NodeId) -> Self {
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

    // # Important
    //
    // - Raft assumes the persistent storage is reliable.
    // - If the storage has corrupted or lost some log data, it's safe to remove the node then add it back to the cluster as a new node.
    // - Changing the node's ID is recommended.
    // - (But this crate has limited support for recovering from the corrupted log data)
    pub fn restart(id: NodeId, current_term: Term, voted_for: Option<NodeId>, log: Log) -> Self {
        let mut node = Self::start(id);

        node.current_term = current_term;
        node.voted_for = voted_for;
        node.log = log;
        node
    }

    // TODO: remove(?)
    pub fn create_cluster(&mut self) -> bool {
        if self.current_term != Term::ZERO {
            return false;
        }

        let mut config = self.log.latest_config().clone();
        config.voters.insert(self.id);

        let entry = LogEntry::ClusterConfig(config);
        self.actions
            .set(Action::AppendLogEntries(LogEntries::from_iter(
                LogPosition::ZERO,
                std::iter::once(entry.clone()),
            )));
        self.log.entries_mut().push(entry.clone());

        self.set_current_term(Term::new(1));
        self.set_voted_for(Some(self.id));
        self.transition_to_leader();

        true
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

    // TODO: remove
    fn commit(&mut self, index: LogIndex) {
        self.commit_index = index;
    }

    pub fn heartbeat(&mut self) -> HeartbeatPromise {
        let RoleState::Leader { quorum, .. } = &mut self.role else {
            return HeartbeatPromise::Rejected;
        };

        // TODO: handle single node case
        let sn = self.seqno;
        let call = Message::append_entries_call(
            self.current_term,
            self.id,
            self.commit_index,
            sn,
            LogEntries::new(self.log.entries().last_position()),
        );
        quorum.update_seqnum(
            self.log.latest_config(),
            self.id,
            self.seqno,
            self.seqno.next(),
        );
        self.seqno = sn.next();
        self.broadcast_message(call);

        HeartbeatPromise::new(self.current_term, sn)
    }

    pub fn propose_command(&mut self) -> CommitPromise {
        if !matches!(self.role, RoleState::Leader { .. }) {
            return CommitPromise::Rejected;
        }
        self.propose(LogEntry::Command)
    }

    fn propose(&mut self, entry: LogEntry) -> CommitPromise {
        debug_assert!(matches!(self.role, RoleState::Leader { .. }));

        // TODO: Create LogEnties instance only once
        let prev_entry = self.log.entries().last_position();
        self.append_log_entry(&entry); // TODO: merge the same kind actions

        let RoleState::Leader { quorum, followers } = &mut self.role else {
            unreachable!();
        };

        if !followers.is_empty() {
            self.actions
                .set(Action::BroadcastMessage(Message::append_entries_call(
                    self.current_term,
                    self.id,
                    self.commit_index,
                    self.seqno,
                    LogEntries::from_iter(prev_entry, std::iter::once(entry)),
                )));
            quorum.update_seqnum(
                self.log.latest_config(),
                self.id,
                self.seqno,
                self.seqno.next(),
            );
            self.seqno = self.seqno.next();
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
        quorum.update_seqnum(config, self.id, MessageSeqNo::UNKNOWN, self.seqno);

        for (&id, follower) in followers {
            quorum.update_match_index(config, id, zero, follower.match_index);
            quorum.update_seqnum(config, id, MessageSeqNo::UNKNOWN, follower.max_sn);
        }
    }

    fn broadcast_message(&mut self, message: Message) {
        self.actions.set(Action::BroadcastMessage(message));
    }

    fn send_message(&mut self, destination: NodeId, message: Message) {
        self.actions.set(Action::SendMessage(destination, message));
    }

    fn update_commit_index_if_possible(&mut self) {
        let RoleState::Leader { quorum, .. } = &mut self.role else {
            unreachable!();
        };

        let new_commit_index = quorum.commit_index();
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
        debug_assert!(self.role().is_leader());
        debug_assert!(self.log.latest_config().is_joint_consensus());

        let mut new_config = self.log.latest_config().clone();
        new_config.voters = std::mem::take(&mut new_config.new_voters);
        self.propose(LogEntry::ClusterConfig(new_config));

        // TODO: leader not in new config steps down when the new config is committed
    }

    pub fn change_config(
        &mut self,
        new_config: &ClusterConfig,
    ) -> Result<CommitPromise, ChangeConfigError> {
        if !self.role().is_leader() {
            return Err(ChangeConfigError::NotLeader);
        }
        if self.log.latest_config().voters != new_config.voters {
            return Err(ChangeConfigError::VotersMismatched);
        }
        if self.log.latest_config().is_joint_consensus() {
            return Err(ChangeConfigError::JointConsensusInProgress);
        }

        Ok(self.propose(LogEntry::ClusterConfig(new_config.clone())))
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

    // TODO: split into handle_request_vote_call, ... to make it possible to return errors
    // or keep this method but return errors
    pub fn handle_message(&mut self, msg: &Message) {
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
            } => self.handle_request_vote_call(*header, *last_position),
            Message::RequestVoteReply {
                header,
                vote_granted,
            } => self.handle_request_vote_reply(*header, *vote_granted),
            Message::AppendEntriesCall {
                header,
                commit_index,
                entries,
            } => self.handle_append_entries_call(*header, *commit_index, entries),
            Message::AppendEntriesReply {
                header,
                last_position,
            } => self.handle_append_entries_reply(*header, *last_position),
        }
    }

    fn handle_request_vote_call(&mut self, header: MessageHeader, last_position: LogPosition) {
        if header.term < self.current_term {
            self.send_message(
                header.from,
                Message::request_vote_reply(self.current_term, self.id, header.seqno, false),
            );
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
        self.send_message(
            header.from,
            Message::request_vote_reply(self.current_term, self.id, header.seqno, true),
        );
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

    pub fn handle_election_timeout(&mut self) {
        match self.role {
            RoleState::Follower => {
                self.start_new_election();
            }
            RoleState::Candidate { .. } => {
                self.start_new_election();
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

    // TODO: rename
    fn start_new_election(&mut self) {
        self.set_current_term(self.current_term.next());
        self.set_voted_for(Some(self.id));
        self.role = RoleState::Candidate {
            granted_votes: std::iter::once(self.id).collect(),
        };

        let seqno = self.seqno.fetch_and_increment();
        self.broadcast_message(Message::request_vote_call(
            self.current_term,
            self.id,
            seqno,
            self.log.entries().last_position(),
        ));
        self.actions.set(Action::SetElectionTimeout);
    }

    fn transition_to_follower(&mut self, term: Term) {
        self.set_current_term(term);
        self.set_voted_for(None);
        self.role = RoleState::Follower;
        self.actions.set(Action::SetElectionTimeout);
    }

    fn reply_append_entries(&mut self, header: MessageHeader) {
        self.send_message(
            header.from,
            Message::append_entries_reply(
                self.current_term,
                self.id,
                header.seqno,
                self.log.entries().last_position(),
            ),
        );
    }

    fn handle_append_entries_call(
        &mut self,
        header: MessageHeader,
        leader_commit: LogIndex,
        entries: &LogEntries,
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

        if self.try_append_log_entries(entries) {
            let next_commit_index = leader_commit
                .min(self.log.entries().last_position().index)
                .min(entries.last_position().index); // TODO: Add note comment (entries could be truncated by action implementor)
            if self.commit_index < next_commit_index {
                self.commit(next_commit_index);
            }
        }

        // TODO(?): Don't reply if request.leader_sn is old
        //          (the reply will be discarded in the leader side anyway)
        self.reply_append_entries(header);
        // TODO: reset election timeout
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
            quorum.update_seqnum(
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
            let msg = Message::append_entries_call(
                self.current_term,
                self.id,
                self.commit_index,
                self.seqno,
                delta,
            );
            self.actions.set(Action::SendMessage(header.from, msg));

            let RoleState::Leader { quorum, .. } = &mut self.role else {
                return;
            };
            quorum.update_seqnum(
                self.log.latest_config(),
                self.id,
                self.seqno,
                self.seqno.next(),
            );
            self.seqno = self.seqno.next();
        }
    }

    pub fn id(&self) -> NodeId {
        self.id
    }

    pub fn role(&self) -> Role {
        match self.role {
            RoleState::Follower => Role::Follower,
            RoleState::Candidate { .. } => Role::Candidate,
            RoleState::Leader { .. } => Role::Leader,
        }
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

// TODO: remove
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChangeConfigError {
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
            max_sn: MessageSeqNo::UNKNOWN,
        }
    }
}
