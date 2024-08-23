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

    pub fn update_match_index(
        &mut self,
        config: &ClusterConfig,
        node_id: NodeId,
        old_index: LogIndex,
        index: LogIndex,
    ) {
        debug_assert!(old_index <= index);

        let old_entry = (old_index, node_id);
        let new_entry = (index, node_id);

        if config.voters.contains(&node_id)
            && self.majority_indices.first().map(|(i, _)| *i) < Some(index)
        {
            update_majority_set(&mut self.majority_indices, old_entry, new_entry);
        }

        if config.new_voters.contains(&node_id)
            && self.new_majority_indices.first().map(|(i, _)| *i) < Some(index)
        {
            update_majority_set(&mut self.new_majority_indices, old_entry, new_entry);
        }
    }

    pub fn update_seqno(
        &mut self,
        config: &ClusterConfig,
        node_id: NodeId,
        old_seqno: MessageSeqNo,
        seqno: MessageSeqNo,
    ) {
        debug_assert!(old_seqno <= seqno);

        let old_entry = (old_seqno, node_id);
        let new_entry = (seqno, node_id);

        if config.voters.contains(&node_id)
            && self.majority_seqnos.first().map(|(i, _)| *i) < Some(seqno)
        {
            update_majority_set(&mut self.majority_seqnos, old_entry, new_entry);
        }

        if config.new_voters.contains(&node_id)
            && self.new_majority_seqnos.first().map(|(i, _)| *i) < Some(seqno)
        {
            update_majority_set(&mut self.new_majority_seqnos, old_entry, new_entry);
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

    pub fn smallest_majority_index(&self) -> LogIndex {
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

fn update_majority_set<T: Ord>(set: &mut BTreeSet<T>, old_entry: T, new_entry: T) {
    set.insert(new_entry);
    if !set.remove(&old_entry) {
        set.pop_first();
    }
}
