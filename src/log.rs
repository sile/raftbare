use crate::{config::ClusterConfig, Term};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogEntries {
    // TODO: private
    pub prev: LogEntryId, // TODO: prev_entry
    pub last: LogEntryId,
    pub terms: BTreeMap<LogIndex, Term>,
    pub configs: BTreeMap<LogIndex, ClusterConfig>,
}

impl LogEntries {
    // TODO: remove
    pub fn new(prev: LogEntryId) -> Self {
        Self {
            prev,
            last: prev,
            terms: BTreeMap::new(),
            configs: BTreeMap::new(),
        }
    }

    // TODO: rename
    pub fn from_snapshot(snapshot: Snapshot) -> Self {
        let mut this = Self::new(snapshot.last_entry);
        this.configs
            .insert(snapshot.last_entry.index, snapshot.cluster_config);
        this
    }

    pub fn is_empty(&self) -> bool {
        self.prev == self.last
    }

    pub fn get_entry(&self, i: LogIndex) -> Option<LogEntry> {
        if !self.contains_index(i) {
            return None;
        }
        if let Some(term) = self.terms.get(&i) {
            Some(LogEntry::Term(*term))
        } else if let Some(config) = self.configs.get(&i) {
            Some(LogEntry::ClusterConfig(config.clone()))
        } else {
            Some(LogEntry::Command)
        }
    }

    pub fn get_config(&self, i: LogIndex) -> Option<&ClusterConfig> {
        self.configs.range(..=i).map(|x| x.1).rev().next()
    }

    pub fn latest_config_index(&self) -> LogIndex {
        self.configs
            .last_key_value()
            .map(|(k, _)| *k)
            .unwrap_or(self.prev.index)
    }

    pub fn term_index(&self) -> LogIndex {
        self.terms
            .last_key_value()
            .map(|(k, _)| *k)
            .unwrap_or(self.prev.index)
    }

    pub fn single(prev: LogEntryId, entry: &LogEntry) -> Self {
        let mut this = Self::new(prev);
        this.append_entry(&entry);
        this
    }

    // TODO: add unit test
    pub fn since(&self, new_prev: LogEntryId) -> Option<Self> {
        if !self.contains(new_prev) {
            return None;
        }

        let mut this = self.clone();
        this.prev = new_prev;

        // TODO: optimize(?) => use split_off()
        this.terms.retain(|index, _| index > &new_prev.index);
        this.configs.retain(|index, _| index > &new_prev.index);

        Some(this)
    }

    pub fn contains(&self, entry: LogEntryId) -> bool {
        if !self.contains_index(entry.index) {
            return false;
        }

        let term = self
            .terms
            .range(..=entry.index)
            .rev()
            .next()
            .map(|(_, term)| *term)
            .unwrap_or(self.prev.term);
        term == entry.term
    }

    pub fn contains_index(&self, index: LogIndex) -> bool {
        (self.prev.index..=self.last.index).contains(&index)
    }

    pub fn append_entry(&mut self, entry: &LogEntry) {
        self.last = self.last.next();
        match entry {
            LogEntry::Term(term) => {
                self.terms.insert(self.last.index, *term);
                self.last.term = *term;
            }
            LogEntry::ClusterConfig(config) => {
                self.configs.insert(self.last.index, config.clone());
            }
            LogEntry::Command => {}
        }
    }

    pub fn append_entries(&mut self, entries: &Self) {
        if self.last != entries.prev {
            // Truncate
            debug_assert!(self.contains(entries.prev));
            self.last = entries.prev;
            self.terms.split_off(&self.last.index);
            self.configs.split_off(&self.last.index);
        }

        // TODO: use append()
        self.terms.extend(&entries.terms);
        self.configs
            .extend(entries.configs.iter().map(|(k, v)| (k.clone(), v.clone())));
        self.last = entries.last;
    }

    // TODO: move to Node (or add struct Log)
    pub fn current_snapshot(&self) -> Snapshot {
        Snapshot {
            last_entry: self.prev,
            cluster_config: self // TODO
                .configs
                .first_key_value()
                .map(|(_, v)| v.clone())
                .unwrap_or_else(|| ClusterConfig::new()),
        }
    }

    pub fn snapshot_at_last_entry(&self) -> Snapshot {
        Snapshot {
            last_entry: self.last,
            cluster_config: self // TODO
                .configs
                .last_key_value()
                .map(|(_, v)| v.clone())
                .unwrap_or_else(|| ClusterConfig::new()),
        }
    }
}

/// Log index.
///
/// Unlike the Raft paper, this index is 0-based, with a sentinel entry at index 0.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LogIndex(u64);

impl LogIndex {
    /// Makes a new [`LogIndex`] instance.
    pub const fn new(i: u64) -> Self {
        Self(i)
    }

    /// Returns the inner of this index.
    pub const fn get(self) -> u64 {
        self.0
    }

    pub(crate) const fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

/// Identifier that uniquely indicates a [`LogEntry`] stored within a cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LogEntryId {
    /// Term of the log entry.
    pub term: Term,

    /// Index of the log entry.
    pub index: LogIndex,
}

impl LogEntryId {
    pub(crate) const fn new(term: Term, index: LogIndex) -> Self {
        Self { term, index }
    }

    pub(crate) const fn next(self) -> Self {
        Self::new(self.term, self.index.next())
    }
}

/// Log entry.
///
/// Each log entry within a cluster is uniquely identified by a [`LogEntryId`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum LogEntry {
    /// A log entry to indicate the start of a new term with a new leader.
    Term(Term),

    /// A log entry for a new cluster configuration.
    ClusterConfig(ClusterConfig),

    /// A log entry for a user-defined command.
    ///
    /// # Note
    ///
    /// This crate does not handle the content of user-defined commands.
    /// Therefore, this variant is represented as a unit.
    /// It is the user's responsibility to manage the mapping from each [`LogEntry::Command`] to
    /// an actual command data.
    Command,
}

/// Snapshot of a state machine replicated using Raft.
///
/// # Note
///
/// This crate does not handle the content of snapshots.
/// Consequently, this struct does not contain the actual snapshot data.
/// Users are responsible for managing their own snapshot data.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Snapshot {
    /// Last log entry included in this snapshot.
    pub last_entry: LogEntryId,

    /// Cluster configuration at the time of this snapshot was taken.
    pub cluster_config: ClusterConfig,
}
