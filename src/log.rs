use crate::Term;
use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub struct Log {
    // TODO: private
    pub prev: LogEntryRef,
    pub next: LogEntryRef,
    pub terms: BTreeMap<LogIndex, Term>,
    // TODO: cluster_configs
}

impl Log {
    pub fn new(prev: LogEntryRef) -> Self {
        let mut terms = BTreeMap::new();
        terms.insert(prev.index, prev.term);
        Self {
            prev,
            next: prev.next(),
            terms,
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
    // TODO: ClusterConfig,
    Command,
}
