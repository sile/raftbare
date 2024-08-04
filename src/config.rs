use crate::node::NodeId;
use std::{
    collections::{btree_set::Iter, BTreeSet},
    iter::Peekable,
};

#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
pub struct ClusterConfig {
    // TODO: private
    pub voters: BTreeSet<NodeId>,
    pub new_voters: BTreeSet<NodeId>, // Empty means no voters are being added or removed.
    pub non_voters: BTreeSet<NodeId>,
}

impl ClusterConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn members(&self) -> Members {
        Members {
            voters: self.voters.iter().peekable(),
            new_voters: self.new_voters.iter().peekable(),
            non_voters: self.non_voters.iter().peekable(),
        }
    }

    pub fn is_joint_consensus(&self) -> bool {
        !self.new_voters.is_empty()
    }
}

#[derive(Debug)]
pub struct Members<'a> {
    voters: Peekable<Iter<'a, NodeId>>,
    new_voters: Peekable<Iter<'a, NodeId>>,
    non_voters: Peekable<Iter<'a, NodeId>>,
}

impl<'a> Iterator for Members<'a> {
    type Item = NodeId;

    fn next(&mut self) -> Option<Self::Item> {
        let a = self.voters.peek().copied();
        let b = self.new_voters.peek().copied();
        let c = self.non_voters.peek().copied();
        match (a, b, c) {
            (None, None, None) => None,
            (Some(a), None, None) => {
                self.voters.next();
                Some(*a)
            }
            (None, Some(b), None) => {
                self.new_voters.next();
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
                    self.new_voters.next();
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
                    self.new_voters.next();
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
                    self.new_voters.next();
                    Some(*b)
                } else {
                    self.non_voters.next();
                    Some(*c)
                }
            }
        }
    }
}
