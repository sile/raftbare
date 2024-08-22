use crate::{config::ClusterConfig, log::LogIndex, message::MessageSeqNo, node::NodeId};
use std::collections::BTreeSet;

#[derive(Debug, Clone)]
pub struct Quorum {
    majority_indices: BTreeSet<(LogIndex, NodeId)>,
    new_majority_indices: BTreeSet<(LogIndex, NodeId)>,

    majority_seqnos: BTreeSet<(MessageSeqNo, NodeId)>,
    new_majority_seqnos: BTreeSet<(MessageSeqNo, NodeId)>,
}

impl Quorum {
    pub fn new(config: &ClusterConfig) -> Self {
        let majority_indices = config
            .voters
            .iter()
            .take(config.voters.len() / 2 + 1)
            .copied()
            .map(|id| (LogIndex::new(0), id))
            .collect::<BTreeSet<_>>();
        let new_majority_indices = config
            .new_voters
            .iter()
            .take(config.new_voters.len() / 2 + 1)
            .copied()
            .map(|id| (LogIndex::new(0), id))
            .collect::<BTreeSet<_>>();

        let majority_seqnos = config
            .voters
            .iter()
            .take(config.voters.len() / 2 + 1)
            .copied()
            .map(|id| (MessageSeqNo::UNKNOWN, id))
            .collect::<BTreeSet<_>>();
        let new_majority_seqnos = config
            .new_voters
            .iter()
            .take(config.new_voters.len() / 2 + 1)
            .copied()
            .map(|id| (MessageSeqNo::UNKNOWN, id))
            .collect::<BTreeSet<_>>();

        Self {
            majority_indices,
            new_majority_indices,
            majority_seqnos,
            new_majority_seqnos,
        }
    }

    // TODO: return Some(LogIndex) if least value is updated
    pub fn update_match_index(
        &mut self,
        config: &ClusterConfig,
        node_id: NodeId,
        old_index: LogIndex,
        index: LogIndex,
    ) {
        if config.voters.contains(&node_id)
            && self.majority_indices.first().map(|(i, _)| *i) < Some(index)
        {
            self.majority_indices.insert((index, node_id));
            if !self.majority_indices.remove(&(old_index, node_id)) {
                self.majority_indices.pop_first();
            }
        }

        if config.new_voters.contains(&node_id)
            && self.new_majority_indices.first().map(|(i, _)| *i) < Some(index)
        {
            self.new_majority_indices.insert((index, node_id));
            if !self.new_majority_indices.remove(&(old_index, node_id)) {
                self.new_majority_indices.pop_first();
            }
        }
    }

    // TODO: return Some(LogIndex) if least value is updated
    pub fn update_seqno(
        &mut self,
        config: &ClusterConfig,
        node_id: NodeId,
        old_seqno: MessageSeqNo,
        seqno: MessageSeqNo,
    ) {
        if config.voters.contains(&node_id)
            && self.majority_seqnos.first().map(|(i, _)| *i) < Some(seqno)
        {
            self.majority_seqnos.insert((seqno, node_id));
            if !self.majority_seqnos.remove(&(old_seqno, node_id)) {
                self.majority_seqnos.pop_first();
            }
        }

        if config.new_voters.contains(&node_id)
            && self.new_majority_seqnos.first().map(|(i, _)| *i) < Some(seqno)
        {
            self.new_majority_seqnos.insert((seqno, node_id));
            if !self.new_majority_seqnos.remove(&(old_seqno, node_id)) {
                self.new_majority_seqnos.pop_first();
            }
        }
    }

    pub fn smallest_majority_seqno(&self) -> MessageSeqNo {
        let Some(i0) = self.majority_seqnos.first().map(|(i, _)| *i) else {
            unreachable!();
        };
        if let Some(i1) = self.new_majority_seqnos.first().map(|(i, _)| *i) {
            i0.min(i1)
        } else {
            i0
        }
    }

    pub fn commit_index(&self) -> LogIndex {
        let Some(i0) = self.majority_indices.first().map(|(i, _)| *i) else {
            unreachable!();
        };
        if let Some(i1) = self.new_majority_indices.first().map(|(i, _)| *i) {
            i0.min(i1)
        } else {
            i0
        }
    }
}
