use raftbare::{Node, NodeId};

#[derive(Debug)]
pub struct TestCluster {
    pub nodes: Vec<TestNode>,
}

#[derive(Debug)]
pub struct TestNode {
    pub inner: Node,
    pub running: bool,
    pub timeout_expire_time: Option<TestClock>,
    pub storage_finish_time: Option<TestClock>,
}

impl TestNode {
    pub fn start(id: NodeId) -> TestNode {
        TestNode {
            inner: Node::start(id),
            running: true,
            timeout_expire_time: None,
            storage_finish_time: None,
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TestClock(u64);

impl TestClock {
    pub fn new() -> TestClock {
        TestClock(0)
    }

    pub fn tick(&mut self) {
        self.0 += 1;
    }
}

#[derive(Debug)]
pub enum Step {
    AddVoterNode,
    RemoveVoterNode,
    AddNonVoterNode,
    RemoveNonVoterNode,
    ChangeConfig,
    Heartbeat,
    ProposeCommand,
    InstallSnapshot,
    SetLinkDelay,
    SetLinkPacketLoss,
    SetStorageLatency,
    RestartNode,
    RestartNodeWithLogTruncate,
}
