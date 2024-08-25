/// Role of a Raft node (follower, candidate, or leader).
///
/// Note that the Raft paper refers to this concept as "state".
/// However, this crate uses the term "role" as it is more specific and less ambiguous.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Role {
    /// Follower.
    Follower,

    /// Candidate.
    Candidate,

    /// Leader.
    Leader,
}

impl Role {
    /// Returns `true` if the role is leader.
    pub const fn is_leader(self) -> bool {
        matches!(self, Self::Leader)
    }

    /// Returns `true` if the role is follower.
    pub const fn is_follower(self) -> bool {
        matches!(self, Self::Follower)
    }

    /// Returns `true` if the role is candidate.
    pub const fn is_candidate(self) -> bool {
        matches!(self, Self::Candidate)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn role_is() {
        assert!(Role::Leader.is_leader());
        assert!(Role::Follower.is_follower());
        assert!(Role::Candidate.is_candidate());
    }
}
