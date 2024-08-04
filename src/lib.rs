// TODO: private
pub mod action;
pub mod config;
pub mod event;
pub mod log;
pub mod message;
pub mod node;
pub mod quorum;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Term(u64);

impl Term {
    pub const fn new(v: u64) -> Self {
        Self(v)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }
}
