use crate::node::NodeId;
use std::{
    collections::{btree_set::Iter, BTreeSet},
    iter::Peekable,
};

#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
pub struct ClusterConfig {
    pub voters: BTreeSet<NodeId>,
    pub old_voters: BTreeSet<NodeId>,
    pub non_voters: BTreeSet<NodeId>,
}

impl ClusterConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn members(&self) -> Members {
        Members {
            voters: self.voters.iter().peekable(),
            old_voters: self.old_voters.iter().peekable(),
            non_voters: self.non_voters.iter().peekable(),
        }
    }
}

#[derive(Debug)]
pub struct Members<'a> {
    voters: Peekable<Iter<'a, NodeId>>,
    old_voters: Peekable<Iter<'a, NodeId>>,
    non_voters: Peekable<Iter<'a, NodeId>>,
}

impl<'a> Iterator for Members<'a> {
    type Item = NodeId;

    fn next(&mut self) -> Option<Self::Item> {
        let a = self.voters.peek().copied();
        let b = self.old_voters.peek().copied();
        let c = self.non_voters.peek().copied();
        match (a, b, c) {
            (None, None, None) => None,
            (Some(a), None, None) => {
                self.voters.next();
                Some(*a)
            }
            (None, Some(b), None) => {
                self.old_voters.next();
                Some(*b)
            }
            (None, None, Some(c)) => {
                self.non_voters.next();
                Some(*c)
            }
            (Some(a), Some(b), None) => {
                if a < b {
                    self.voters.next();
                    Some(*a)
                } else {
                    self.old_voters.next();
                    Some(*b)
                }
            }
            (Some(a), None, Some(c)) => {
                if a < c {
                    self.voters.next();
                    Some(*a)
                } else {
                    self.non_voters.next();
                    Some(*c)
                }
            }
            (None, Some(b), Some(c)) => {
                if b < c {
                    self.old_voters.next();
                    Some(*b)
                } else {
                    self.non_voters.next();
                    Some(*c)
                }
            }
            (Some(a), Some(b), Some(c)) => {
                if a < b && a < c {
                    self.voters.next();
                    Some(*a)
                } else if b < a && b < c {
                    self.old_voters.next();
                    Some(*b)
                } else {
                    self.non_voters.next();
                    Some(*c)
                }
            }
        }
    }
}
