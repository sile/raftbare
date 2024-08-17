use crate::{config::ClusterConfig, Term};
use std::collections::BTreeMap;

/// In-memory representation of a [`Node`][crate::Node] local log.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Log {
    snapshot_config: ClusterConfig,
    entries: LogEntries,
}

impl Log {
    /// Makes a new [`Log`] instance with the given cluster configuration and entries.
    ///
    /// # Examples
    ///
    /// ```
    /// use raftbare::{ClusterConfig, Log, LogEntries, LogEntry, LogPosition, NodeId, Term};
    ///
    /// let empty_config = ClusterConfig::new();
    /// let mut single_config = ClusterConfig::new();
    /// single_config.voters.insert(NodeId::new(1));
    ///
    /// let entries = LogEntries::from_iter(
    ///     LogPosition::ZERO,
    ///     vec![
    ///         LogEntry::Term(Term::ZERO),
    ///         LogEntry::ClusterConfig(single_config.clone()),
    ///         LogEntry::Command,
    ///     ],
    /// );
    /// let log = Log::new(empty_config.clone(), entries);
    ///
    /// assert_eq!(log.snapshot_position(), LogPosition::ZERO);
    /// assert_eq!(log.snapshot_config(), &empty_config);
    /// assert_eq!(log.latest_config(), &single_config);
    /// ```
    pub const fn new(snapshot_config: ClusterConfig, entries: LogEntries) -> Self {
        Self {
            snapshot_config,
            entries,
        }
    }

    /// Returns a reference to the entries in this log.
    pub fn entries(&self) -> &LogEntries {
        &self.entries
    }

    pub(crate) fn entries_mut(&mut self) -> &mut LogEntries {
        &mut self.entries
    }

    /// Returns the log position where the snapshot was taken.
    ///
    /// This is equivalent to `self.entries().prev_position`.
    pub fn snapshot_position(&self) -> LogPosition {
        self.entries.prev_position
    }

    /// Returns a reference to the cluster configuration at the time the snapshot was taken.
    pub fn snapshot_config(&self) -> &ClusterConfig {
        &self.snapshot_config
    }

    /// Returns a reference to the cluster configuration located at the highest index in this log.
    pub fn latest_config(&self) -> &ClusterConfig {
        self.entries
            .configs
            .last_key_value()
            .map(|(_, v)| v)
            .unwrap_or(&self.snapshot_config)
    }

    pub(crate) fn latest_config_index(&self) -> LogIndex {
        self.entries
            .configs
            .last_key_value()
            .map(|(i, _)| *i)
            .unwrap_or(self.entries.prev_position.index)
    }
}

/// Log entries.
///
/// This representation is compact and only requires `O(|terms|) + O(|configs|)` memory,
/// where `|terms|` is the number of [`LogEntry::Term`] entries and
/// `|configs|` is the number of [`LogEntry::ClusterConfig`] entries.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogEntries {
    prev_position: LogPosition,
    last_position: LogPosition,
    terms: BTreeMap<LogIndex, Term>,
    configs: BTreeMap<LogIndex, ClusterConfig>,
}

impl LogEntries {
    /// Makes a new empty [`LogEntries`] instance at the given position.
    ///
    /// # Examples
    ///
    /// ```
    /// use raftbare::{LogEntries, LogEntry, LogPosition};
    ///
    /// let entries = LogEntries::new(LogPosition::ZERO);
    /// assert!(entries.is_empty());
    /// assert_eq!(entries.len(), 0);
    /// assert_eq!(entries.iter().count(), 0);
    /// assert_eq!(entries.prev_position(), LogPosition::ZERO);
    /// assert_eq!(entries.last_position(), LogPosition::ZERO);
    /// ```
    pub const fn new(prev_position: LogPosition) -> Self {
        Self {
            prev_position,
            last_position: prev_position,
            terms: BTreeMap::new(),
            configs: BTreeMap::new(),
        }
    }

    /// Makes a new [`LogEntries`] instance with the given entries.
    ///
    /// # Examples
    ///
    /// ```
    /// use raftbare::{LogEntries, LogEntry, LogIndex, LogPosition, Term};
    ///
    /// let entries = LogEntries::from_iter(
    ///     LogPosition::ZERO,
    ///     vec![
    ///         LogEntry::Term(Term::ZERO),
    ///         LogEntry::Command,
    ///         LogEntry::Command,
    ///     ],
    /// );
    /// assert!(!entries.is_empty());
    /// assert_eq!(entries.len(), 3);
    /// assert_eq!(entries.iter().count(), 3);
    /// assert_eq!(entries.prev_position(), LogPosition::ZERO);
    /// assert_eq!(entries.last_position(), LogPosition { term: Term::ZERO, index: LogIndex::new(3) });
    /// ```
    pub fn from_iter<I>(prev_position: LogPosition, entries: I) -> Self
    where
        I: IntoIterator<Item = LogEntry>,
    {
        let mut this = Self::new(prev_position);
        this.extend(entries);
        this
    }

    /// Returns the number of entries in this [`LogEntries`] instance.
    pub fn len(&self) -> usize {
        self.last_position.index.get() as usize - self.prev_position.index.get() as usize
    }

    /// Returns [`true`] if the log entries is empty (i.e., the previous and last positions are the same).
    pub fn is_empty(&self) -> bool {
        self.prev_position == self.last_position
    }

    /// Returns the position immediately before the first entry in this [`LogEntries`] instance.
    pub fn prev_position(&self) -> LogPosition {
        self.prev_position
    }

    /// Returns the position of the last entry in this [`LogEntries`] instance.
    pub fn last_position(&self) -> LogPosition {
        self.last_position
    }

    /// Returns an iterator over the entries in this [`LogEntries`] instance.
    pub fn iter(&self) -> impl '_ + Iterator<Item = LogEntry> {
        (self.prev_position.index.get() + 1..=self.last_position.index.get()).map(|i| {
            let i = LogIndex::new(i);
            if let Some(term) = self.terms.get(&i).copied() {
                LogEntry::Term(term)
            } else if let Some(config) = self.configs.get(&i).cloned() {
                LogEntry::ClusterConfig(config)
            } else {
                LogEntry::Command
            }
        })
    }

    /// Returns an iterator over the entries in this [`LogEntries`] instance with their positions.
    ///
    /// # Examples
    /// ```
    /// use raftbare::{LogEntries, LogEntry, LogIndex, LogPosition, Term};
    ///
    /// let mut entries = LogEntries::new(LogPosition::ZERO);
    /// entries.push(LogEntry::Command);
    /// entries.push(LogEntry::Term(Term::new(1)));
    /// entries.push(LogEntry::Command);
    ///
    /// fn pos(term: u64, index: u64) -> LogPosition {
    ///     LogPosition { term: Term::new(term), index: LogIndex::new(index) }
    /// }
    ///
    /// let mut iter = entries.iter_with_positions();
    /// assert_eq!(iter.next(), Some((pos(0, 1), LogEntry::Command)));
    /// assert_eq!(iter.next(), Some((pos(1, 2), LogEntry::Term(Term::new(1)))));
    /// assert_eq!(iter.next(), Some((pos(1, 3), LogEntry::Command)));
    /// assert_eq!(iter.next(), None);
    /// ```
    pub fn iter_with_positions(&self) -> impl '_ + Iterator<Item = (LogPosition, LogEntry)> {
        let base_index = self.prev_position.index.get() + 1;
        let mut term = self.prev_position.term;
        self.iter().enumerate().map(move |(i, entry)| {
            if let LogEntry::Term(t) = entry {
                term = t;
            }
            let index = LogIndex::new(base_index + i as u64);
            let position = LogPosition { term, index };
            (position, entry)
        })
    }

    /// Returns [`true`] if the given position is within the range of this entries.
    ///
    /// # Examples
    ///
    /// ```
    /// use raftbare::{LogEntries, LogEntry, LogIndex, LogPosition, Term};
    ///
    /// fn pos(term: u64, index: u64) -> LogPosition {
    ///     LogPosition { term: Term::new(term), index: LogIndex::new(index) }
    /// }
    ///
    /// let entries = LogEntries::from_iter(
    ///     LogPosition::ZERO,
    ///     vec![
    ///         LogEntry::Term(Term::ZERO),
    ///         LogEntry::Command,
    ///         LogEntry::Term(Term::new(1)),
    ///         LogEntry::Command,
    ///     ],
    /// );
    /// assert!(entries.contains(pos(0, 0))); // Including the previous position
    /// assert!(entries.contains(pos(1, 4))); // Including the last position
    /// assert!(!entries.contains(pos(0, 4))); // Index is within the range but term is different
    /// assert!(!entries.contains(pos(1, 5))); // Index is out of range
    /// ```
    pub fn contains(&self, position: LogPosition) -> bool {
        Some(position.term) == self.get_term(position.index)
    }

    /// Returns [`true`] if the given index is within the range of this entries.
    ///
    /// Unlike [`LogEntries::contains()`], this method does not check the term of the given index.
    ///
    /// # Examples
    ///
    /// ```
    /// use raftbare::{LogEntries, LogEntry, LogIndex, LogPosition, Term};
    ///
    /// let entries = LogEntries::from_iter(
    ///     LogPosition::ZERO,
    ///     vec![
    ///         LogEntry::Term(Term::ZERO),
    ///         LogEntry::Command,
    ///         LogEntry::Term(Term::new(1)),
    ///         LogEntry::Command,
    ///     ],
    /// );
    /// assert!(entries.contains_index(LogIndex::ZERO)); // Including the previous index
    /// assert!(entries.contains_index(LogIndex::new(1)));
    /// assert!(entries.contains_index(LogIndex::new(4))); // Including the last index
    /// assert!(!entries.contains_index(LogIndex::new(5)));
    /// ```
    pub fn contains_index(&self, index: LogIndex) -> bool {
        (self.prev_position.index..=self.last_position.index).contains(&index)
    }

    /// Returns the term of the given index if it is within the range of this entries.
    pub fn get_term(&self, index: LogIndex) -> Option<Term> {
        self.contains_index(index).then(|| {
            self.terms
                .range(..=index)
                .rev()
                .next()
                .map(|(_, term)| *term)
                .unwrap_or(self.prev_position.term)
        })
    }

    /// Returns the entry at the given index if it is within the range of this entries.
    ///
    /// Note that if the index is equal to the previous index, this method returns [`None`].
    ///
    /// # Examples
    ///
    /// ```
    /// use raftbare::{LogEntries, LogEntry, LogIndex, LogPosition, Term};
    ///
    /// let entries = LogEntries::from_iter(
    ///     LogPosition::ZERO,
    ///     vec![
    ///         LogEntry::Term(Term::ZERO),
    ///         LogEntry::Command,
    ///         LogEntry::Term(Term::new(1)),
    ///     ],
    /// );
    /// assert_eq!(entries.get_entry(LogIndex::ZERO), None);
    /// assert_eq!(entries.get_entry(LogIndex::new(1)), Some(LogEntry::Term(Term::ZERO)));
    /// assert_eq!(entries.get_entry(LogIndex::new(2)), Some(LogEntry::Command));
    /// assert_eq!(entries.get_entry(LogIndex::new(3)), Some(LogEntry::Term(Term::new(1))));
    /// assert_eq!(entries.get_entry(LogIndex::new(4)), None);
    /// ```
    pub fn get_entry(&self, index: LogIndex) -> Option<LogEntry> {
        if !self.contains_index(index) || self.prev_position.index == index {
            None
        } else if let Some(term) = self.terms.get(&index).copied() {
            Some(LogEntry::Term(term))
        } else if let Some(config) = self.configs.get(&index).cloned() {
            Some(LogEntry::ClusterConfig(config))
        } else {
            Some(LogEntry::Command)
        }
    }

    pub(crate) fn get_config(&self, index: LogIndex) -> Option<&ClusterConfig> {
        self.configs.range(..=index).map(|x| x.1).rev().next()
    }

    /// Appends an entry to the back of this entries.
    ///
    /// # Examples
    ///
    /// ```
    /// use raftbare::{LogEntries, LogEntry, LogIndex, LogPosition, Term};
    ///
    /// let mut entries = LogEntries::new(LogPosition::ZERO);
    /// entries.push(LogEntry::Term(Term::ZERO));
    /// entries.push(LogEntry::Command);
    ///
    /// assert_eq!(entries.get_entry(LogIndex::new(1)), Some(LogEntry::Term(Term::ZERO)));
    /// assert_eq!(entries.last_position(), LogPosition { term: Term::ZERO, index: LogIndex::new(2) });
    /// ```
    pub fn push(&mut self, entry: LogEntry) {
        self.last_position = self.last_position.next();
        match entry {
            LogEntry::Term(term) => {
                self.terms.insert(self.last_position.index, term);
                self.last_position.term = term;
            }
            LogEntry::ClusterConfig(config) => {
                self.configs
                    .insert(self.last_position.index, config.clone());
            }
            LogEntry::Command => {}
        }
    }

    // TODO: add unit test
    pub fn since(&self, new_prev: LogPosition) -> Option<Self> {
        if !self.contains(new_prev) {
            return None;
        }

        let mut this = self.clone();
        this.prev_position = new_prev;

        // TODO: optimize(?) => use split_off()
        this.terms.retain(|index, _| index > &new_prev.index);
        this.configs.retain(|index, _| index > &new_prev.index);

        Some(this)
    }

    pub(crate) fn append(&mut self, entries: &Self) {
        if self.last_position != entries.prev_position {
            // Truncate
            debug_assert!(self.contains(entries.prev_position));
            self.last_position = entries.prev_position;
            self.terms.split_off(&self.last_position.index);
            self.configs.split_off(&self.last_position.index);
        }

        self.terms.extend(&entries.terms);
        self.configs
            .extend(entries.configs.iter().map(|(k, v)| (*k, v.clone())));
        self.last_position = entries.last_position;
    }
}

impl std::iter::Extend<LogEntry> for LogEntries {
    fn extend<T: IntoIterator<Item = LogEntry>>(&mut self, iter: T) {
        for entry in iter {
            self.push(entry);
        }
    }
}

/// Log index.
///
/// According to the Raft paper, index 0 serves as a sentinel value,
/// and the actual log entries start from index 1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LogIndex(u64);

impl LogIndex {
    /// The initial log index (sentinel value)
    pub const ZERO: Self = Self(0);

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

/// Log position ([`Term`] and [`LogIndex`]).
///
/// A [`LogPosition`] uniquely identifies a [`LogEntry`] stored within a cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LogPosition {
    /// Term of the log entry.
    pub term: Term,

    /// Index of the log entry.
    pub index: LogIndex,
}

impl LogPosition {
    /// The initial log position ([`Term::ZERO`] and [`LogIndex::ZERO`]).
    pub const ZERO: Self = Self::new(Term::ZERO, LogIndex::ZERO);

    pub(crate) const fn new(term: Term, index: LogIndex) -> Self {
        Self { term, index }
    }

    pub(crate) const fn next(self) -> Self {
        Self::new(self.term, self.index.next())
    }
}

/// Log entry.
///
/// Each log entry within a cluster is uniquely identified by a [`LogPosition`].
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
