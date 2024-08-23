use raftbare::Node;

#[derive(Debug)]
pub struct TestCluster {
    pub nodes: Vec<Node>,
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
