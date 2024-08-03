use crate::Term;

#[derive(Debug, Clone)]
pub struct Log {
    pub snapshot_term: Term,
    pub snapshot_index: LogIndex,
}

impl Log {
    pub fn new() -> Self {
        Self {
            snapshot_term: Term::new(0),
            snapshot_index: LogIndex::new(0),
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
