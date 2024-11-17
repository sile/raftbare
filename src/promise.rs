use crate::{LogPosition, MessageSeqNo, Node, Term};

/// Promise of a commit that results in either rejection or acceptance.
// TODO: struct CommitPromise(LogPosition);
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CommitPromise {
    /// The promise is pending.
    ///
    /// This state can be updated to either [`CommitPromise::Accepted`] or [`CommitPromise::Rejected`] by invoking [`CommitPromise::poll()`].
    Pending(LogPosition),

    /// The promise is rejected.
    Rejected(LogPosition),

    /// The promise is accepted.
    Accepted(LogPosition),
    // TODO: Add Unknown variant (for snapshot)
}

impl CommitPromise {
    pub(crate) fn new(position: LogPosition) -> Self {
        Self::Pending(position)
    }

    /// Polls the promise to update its state.
    ///
    /// For convinience, the updated promise is returned.
    ///
    /// Note that `node` can be a different node than the one that the promise was created for.
    pub fn poll(&mut self, node: &Node) -> Self {
        let Self::Pending(position) = *self else {
            return *self;
        };

        if position.index <= node.commit_index() {
            if node.log().entries().contains(position) {
                *self = Self::Accepted(position);
            } else {
                *self = Self::Rejected(position);
            }
        } else if let Some(term) = node.log().entries().get_term(node.commit_index()) {
            if position.term < term {
                *self = Self::Rejected(position);
            }
        }

        *self
    }

    /// Returns the log position that the commit promise is associated with.
    pub fn log_position(self) -> LogPosition {
        match self {
            Self::Pending(position) => position,
            Self::Accepted(position) => position,
            Self::Rejected(position) => position,
        }
    }

    /// Returns [`true`] if the promise is pending.
    pub fn is_pending(self) -> bool {
        matches!(self, Self::Pending(_))
    }

    /// Returns [`true`] if the promise is rejected.
    pub fn is_rejected(self) -> bool {
        matches!(self, Self::Rejected(_))
    }

    /// Returns [`true`] if the promise is accepted.
    pub fn is_accepted(self) -> bool {
        matches!(self, Self::Accepted(_))
    }
}

/// Promise of a heartbeat that results in either rejection or acceptance.
// TODO: consider to remove this enum
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HeartbeatPromise {
    /// The promise is pending.
    ///
    /// This state can be updated to either [`HeartbeatPromise::Accepted`] or [`HeartbeatPromise::Rejected`] by invoking [`HeartbeatPromise::poll()`].
    Pending(Term, MessageSeqNo),

    /// The promise is rejected.
    Rejected,

    /// The promise is accepted.
    Accepted,
}

impl HeartbeatPromise {
    pub(crate) fn new(term: Term, seqno: MessageSeqNo) -> Self {
        Self::Pending(term, seqno)
    }

    /// Polls the promise to update its state.
    ///
    /// For convinience, the updated promise is returned.
    pub fn poll(&mut self, node: &Node) -> Self {
        let Self::Pending(term, seqno) = *self else {
            return *self;
        };
        if node.current_term() != term {
            *self = Self::Rejected;
        } else if let Some(quorum) = node.quorum() {
            if seqno <= quorum.smallest_majority_seqno() {
                *self = Self::Accepted;
            }
        }
        *self
    }

    /// Returns [`true`] if the promise is pending.
    pub fn is_pending(self) -> bool {
        matches!(self, Self::Pending { .. })
    }

    /// Returns [`true`] if the promise is rejected.
    pub fn is_rejected(self) -> bool {
        matches!(self, Self::Rejected)
    }

    /// Returns [`true`] if the promise is accepted.
    pub fn is_accepted(self) -> bool {
        matches!(self, Self::Accepted)
    }
}
