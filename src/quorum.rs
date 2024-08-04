use std::collections::BTreeSet;

use crate::{config::ClusterConfig, log::LogIndex, node::NodeId};

// TOOD: struct(?)
#[derive(Debug, Clone)]
pub enum Quorum {
    Single {
        voters: BTreeSet<NodeId>,
        majority_indices: BTreeSet<(LogIndex, NodeId)>,
    },
    Joint {
        old_voters: BTreeSet<NodeId>,
        new_voters: BTreeSet<NodeId>,
        old_majority_indices: BTreeSet<(LogIndex, NodeId)>,
        new_majority_indices: BTreeSet<(LogIndex, NodeId)>,
    },
}

impl Quorum {
    pub fn new(config: &ClusterConfig) -> Self {
        if config.is_joint_consensus() {
            Self::Joint {
                old_voters: config.voters.clone(),
                new_voters: config.new_voters.clone(),
                old_majority_indices: BTreeSet::new(),
                new_majority_indices: BTreeSet::new(),
            }
        } else {
            Self::Single {
                voters: config.voters.clone(),
                majority_indices: BTreeSet::new(),
            }
        }
    }

    pub fn update_index(&mut self, node_id: &NodeId, old_index: LogIndex, index: LogIndex) {
        match self {
            Quorum::Single {
                voters,
                majority_indices,
            } => {
                if voters.contains(node_id)
                    && majority_indices.first().map_or(false, |(i, _)| *i < index)
                {
                    majority_indices.insert((index, node_id.clone()));
                    if !majority_indices.remove(&(old_index, node_id.clone())) {
                        majority_indices.pop_first();
                    }
                }
            }
            Quorum::Joint {
                old_voters,
                new_voters,
                old_majority_indices,
                new_majority_indices,
            } => {
                if old_voters.contains(node_id)
                    && old_majority_indices
                        .first()
                        .map_or(false, |(i, _)| *i < index)
                {
                    old_majority_indices.insert((index, node_id.clone()));
                    if !old_majority_indices.remove(&(old_index, node_id.clone())) {
                        old_majority_indices.pop_first();
                    }
                }
                if new_voters.contains(node_id)
                    && new_majority_indices
                        .first()
                        .map_or(false, |(i, _)| *i < index)
                {
                    new_majority_indices.insert((index, node_id.clone()));
                    if !new_majority_indices.remove(&(old_index, node_id.clone())) {
                        new_majority_indices.pop_first();
                    }
                }
            }
        }
    }

    pub fn commit_index(&self) -> LogIndex {
        match self {
            Self::Single {
                majority_indices, ..
            } => majority_indices
                .first()
                .map_or(LogIndex::new(0), |(index, _)| *index),
            Self::Joint {
                old_majority_indices,
                new_majority_indices,
                ..
            } => std::cmp::min(
                old_majority_indices
                    .first()
                    .map_or(LogIndex::new(0), |(index, _)| *index),
                new_majority_indices
                    .first()
                    .map_or(LogIndex::new(0), |(index, _)| *index),
            ),
        }
    }
}
