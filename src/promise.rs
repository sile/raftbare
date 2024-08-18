use crate::{LogPosition, MessageSeqNum, Node, Term};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CommitPromise {
    Pending(LogPosition),
    Rejected,
    Accepted,
}

impl CommitPromise {
    pub(crate) fn new(position: LogPosition) -> Self {
        Self::Pending(position)
    }

    pub fn poll(&mut self, node: &Node) -> Self {
        let Self::Pending(position) = *self else {
            return *self;
        };
        if position.index <= node.commit_index() {
            *self = Self::Accepted;
        } else if !node.log().entries().contains(position) {
            *self = Self::Rejected;
        }
        *self
    }

    pub fn is_pending(&self) -> bool {
        matches!(self, Self::Pending(..))
    }

    pub fn is_rejected(&self) -> bool {
        matches!(self, Self::Rejected)
    }

    pub fn is_accepted(&self) -> bool {
        matches!(self, Self::Accepted)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HeartbeatPromise {
    Pending { term: Term, sn: MessageSeqNum },
    Rejected,
    Accepted,
}

impl HeartbeatPromise {
    pub(crate) fn new(term: Term, sn: MessageSeqNum) -> Self {
        Self::Pending { term, sn }
    }

    pub fn poll(&mut self, node: &Node) -> Self {
        let Self::Pending { term, sn } = *self else {
            return *self;
        };
        if node.current_term() != term {
            *self = Self::Rejected;
        } else if sn <= node.leader_sn {
            *self = Self::Accepted;
        }
        *self
    }

    pub fn is_pending(&self) -> bool {
        matches!(self, Self::Pending { .. })
    }

    pub fn is_rejected(&self) -> bool {
        matches!(self, Self::Rejected)
    }

    pub fn is_accepted(&self) -> bool {
        matches!(self, Self::Accepted)
    }
}
