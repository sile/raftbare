use crate::node::NodeId;
use std::{
    collections::{btree_set::Iter, BTreeSet},
    iter::Peekable,
};

/// Cluster configuration (membership).
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
pub struct ClusterConfig {
    /// Voting nodes.
    ///
    /// For a cluster to be available,
    /// the majority of the voters must be alive and able to communicate with each other.
    pub voters: BTreeSet<NodeId>,

    /// New voting nodes during joint consensus.
    ///
    /// When a cluster changes its configuration, it enters a joint consensus state.
    /// In that state, `voters` and `new_voters` represent the old and new configurations, respectively.
    ///
    /// During joint consensus, the cluster requires the majority of both the old and new voters to be available
    /// to proceed with leader election and log entry commit.
    ///
    /// Once the log entry for this joint configuration is committed, `voters` takes the value of `new_voters`,
    /// and the cluster exits the joint consensus state.
    ///
    /// If `new_voters` is empty, it means there are no configuration changes in progress.
    pub new_voters: BTreeSet<NodeId>,

    /// Non-voting nodes.
    ///
    /// Non-voting nodes do not participate in the leader election and log entry commit quorum,
    /// but they do replicate log entries from the leader.
    ///
    /// When adding a new node to a cluster with a large snapshot or many log entries,
    /// it is recommended to first add the node as a non-voter.
    /// Allow the node to catch up with the leader, and then promote it to a voter.
    ///
    /// Note that adding or removing non-voters does not require a joint consensus.
    pub non_voters: BTreeSet<NodeId>,
}

impl ClusterConfig {
    /// Makes a new empty [`ClusterConfig`] instance.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns `true` if the given node is in this configuration.
    pub fn contains(&self, id: NodeId) -> bool {
        self.voters.contains(&id) || self.new_voters.contains(&id) || self.non_voters.contains(&id)
    }

    /// Returns `true` if this configuration represents a joint consensus state.
    pub fn is_joint_consensus(&self) -> bool {
        !self.new_voters.is_empty()
    }

    /// Gets an iterator over all unique [`NodeId`]s in this configuration.
    pub fn unique_nodes(&self) -> impl '_ + Iterator<Item = NodeId> {
        UniqueNodes {
            voters: self.voters.iter().peekable(),
            new_voters: self.new_voters.iter().peekable(),
            non_voters: self.non_voters.iter().peekable(),
        }
    }

    pub(crate) fn voter_majority_count(&self) -> usize {
        self.voters.len() / 2 + 1
    }

    pub(crate) fn new_voter_majority_count(&self) -> usize {
        if self.new_voters.is_empty() {
            0
        } else {
            self.new_voters.len() / 2 + 1
        }
    }
}

#[derive(Debug)]
pub struct UniqueNodes<'a> {
    voters: Peekable<Iter<'a, NodeId>>,
    new_voters: Peekable<Iter<'a, NodeId>>,
    non_voters: Peekable<Iter<'a, NodeId>>,
}

impl<'a> Iterator for UniqueNodes<'a> {
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
