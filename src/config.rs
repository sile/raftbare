use crate::node::NodeId;
use std::{
    collections::{
        btree_set::{Iter, Union},
        BTreeSet,
    },
    iter::Peekable,
};

/// Cluster configuration (membership).
///
/// # Examples
///
/// ```
/// use raftbare::{ClusterConfig, NodeId};
///
/// // Makes a new cluster configuration with two voting nodes.
/// let mut config = ClusterConfig::new();
/// config.voters.insert(NodeId::new(0));
/// config.voters.insert(NodeId::new(1));
/// assert!(!config.is_joint_consensus());
///
/// // Adds a new non-voting node.
/// config.non_voters.insert(NodeId::new(2));
/// assert!(!config.is_joint_consensus());
///
/// // Updates the configuration to add a new voting node.
/// config.new_voters = config.voters.clone();
/// config.new_voters.insert(NodeId::new(3));
/// assert!(config.is_joint_consensus());
/// ```
///
/// The `config` value in the example above can be applied to a cluster via [`Node::propose_config()`][crate::Node::propose_config].
///
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
    /// When adding more than half of the current number of nodes to a cluster that has a large snapshot or many log entries,
    /// it is recommended to first add the new nodes as non-voters to avoid blocking subsequent log commits.
    /// Allow these nodes to catch up with the leader before promoting them to voters.
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

    /// Gets an iterator over all unique [`NodeId`]s in this configuration, in sorted order.
    pub fn unique_nodes(&self) -> impl '_ + Iterator<Item = NodeId> {
        UniqueNodes {
            voters: self.voters.union(&self.new_voters).peekable(),
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
    voters: Peekable<Union<'a, NodeId>>,
    non_voters: Peekable<Iter<'a, NodeId>>,
}

impl<'a> Iterator for UniqueNodes<'a> {
    type Item = NodeId;

    fn next(&mut self) -> Option<Self::Item> {
        let a = self.voters.peek().copied();
        let b = self.non_voters.peek().copied();
        match (a, b) {
            (None, None) => None,
            (Some(a), None) => {
                self.voters.next();
                Some(*a)
            }
            (None, Some(b)) => {
                self.non_voters.next();
                Some(*b)
            }
            (Some(a), Some(b)) if a == b => {
                self.voters.next();
                self.non_voters.next();
                Some(*a)
            }
            (Some(a), Some(b)) if a < b => {
                self.voters.next();
                Some(*a)
            }
            (Some(_), Some(b)) => {
                self.non_voters.next();
                Some(*b)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unique_nodes() {
        let mut config = ClusterConfig::new();
        config.voters.insert(id(1));
        config.voters.insert(id(2));
        config.new_voters.insert(id(2));
        config.new_voters.insert(id(3));
        config.non_voters.insert(id(4));
        config.non_voters.insert(id(5));
        config.non_voters.insert(id(6));

        let nodes: Vec<_> = config.unique_nodes().map(|n| n.get()).collect();
        assert_eq!(nodes, vec![1, 2, 3, 4, 5, 6]);
    }

    fn id(n: u64) -> NodeId {
        NodeId::new(n)
    }
}
