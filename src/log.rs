use crate::{config::ClusterConfig, Term};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogEntries {
    // TODO: private
    pub prev: LogEntryRef, // TODO: LogIndex
    pub last: LogEntryRef,
    pub terms: BTreeMap<LogIndex, Term>,
    pub configs: BTreeMap<LogIndex, ClusterConfig>,
}

impl LogEntries {
    // TODO: remove
    pub fn new(prev: LogEntryRef) -> Self {
        Self {
            prev,
            last: prev,
            terms: BTreeMap::new(),
            configs: BTreeMap::new(),
        }
    }

    pub fn single(prev: LogEntryRef, entry: &LogEntry) -> Self {
        let mut this = Self::new(prev);
        this.append_entry(&entry);
        this
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LogIndex(u64);

impl LogIndex {
    pub const fn new(i: u64) -> Self {
        Self(i)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

// TODO(?): Remove
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LogEntryRef {
    pub term: Term,
    pub index: LogIndex,
}

impl LogEntryRef {
    pub const fn new(term: Term, index: LogIndex) -> Self {
        Self { term, index }
    }

    pub const fn next(self) -> Self {
        Self::new(self.term, self.index.next())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum LogEntry {
    Term(Term),
    ClusterConfig(ClusterConfig),
    Command,
}
