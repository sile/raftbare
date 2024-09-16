use raftbare::{ClusterConfig, LogIndex, LogPosition, Message, Node, NodeId, Role};
use rand::{
    distributions::uniform::SampleRange, prelude::RngCore, rngs::StdRng, seq::SliceRandom, Rng,
    SeedableRng,
};
use std::collections::BTreeMap;

#[test]
fn propose_commands() {
    let seed = rand::thread_rng().gen();
    let rng = StdRng::seed_from_u64(seed);
    dbg!(seed);

    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];

    // Create a cluster.
    let mut cluster = TestCluster::new(&node_ids, rng);
    assert!(cluster
        .random_node_mut()
        .create_cluster(&node_ids)
        .is_pending());

    let deadline = cluster.clock.add(10000);
    let satisfied = cluster.run_until(deadline, |cluster| cluster.leader_node().is_some());
    assert!(satisfied, "Create cluster timeout");

    // Propose commands.
    let mut promises = Vec::new();
    for _ in 0..100 {
        let Some(leader) = cluster.leader_node_mut() else {
            panic!("No leader");
        };
        promises.push(leader.propose_command());

        let ticks = MinMax::new(1, 10).sample_single(&mut cluster.rng);
        cluster.run(cluster.clock.add(ticks));
    }
    assert_eq!(promises.len(), 100);

    for mut promise in promises {
        for _ in 0..1000 {
            let Some(leader) = cluster.leader_node_mut() else {
                panic!("No leader");
            };
            if promise.poll(leader).is_accepted() {
                break;
            }
            cluster.run(cluster.clock.add(10));
        }
        assert!(promise.is_accepted());
    }

    let deadline = cluster.clock.add(1000);
    let satisfied = cluster.run_until(deadline, |cluster| {
        cluster.nodes[0].inner.commit_index() == cluster.nodes[1].inner.commit_index()
            && cluster.nodes[0].inner.commit_index() == cluster.nodes[2].inner.commit_index()
    });
    assert!(satisfied, "Commit indices are not synchronized");

    // Links are stable, so the leader should not change.
    assert_eq!(cluster.nodes[0].inner.current_term().get(), 1);
}

#[test]
fn unstable_network() {
    let seed = rand::thread_rng().gen();
    let rng = StdRng::seed_from_u64(seed);
    dbg!(seed);

    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];

    // Create a cluster.
    let mut cluster = TestCluster::new(&node_ids, rng);

    // [NOTE] This test is almost the same as `propose_commands()`, but the network is very unstable.
    cluster.default_link_options.drop_rate = 0.3;
    cluster.default_link_options.latency_ticks = MinMax::new(1, 1000);

    assert!(cluster
        .random_node_mut()
        .create_cluster(&node_ids)
        .is_pending());

    let deadline = cluster.clock.add(100000);
    let satisfied = cluster.run_until(deadline, |cluster| cluster.leader_node().is_some());
    assert!(satisfied, "Create cluster timeout");

    // Propose commands.
    let mut promises = Vec::new();
    for _ in 0..100 {
        cluster.run_while_leader_absent(cluster.clock.add(100_000));
        let Some(leader) = cluster.leader_node_mut() else {
            panic!("No leader");
        };
        promises.push(leader.propose_command());

        let ticks = MinMax::new(1, 10).sample_single(&mut cluster.rng);
        cluster.run(cluster.clock.add(ticks));
    }
    assert_eq!(promises.len(), 100);

    for mut promise in promises {
        for _ in 0..10000 {
            cluster.run_while_leader_absent(cluster.clock.add(100_000));
            let Some(leader) = cluster.leader_node_mut() else {
                panic!("No leader");
            };
            if promise.poll(leader).is_accepted() {
                break;
            }
            cluster.run(cluster.clock.add(10));
        }
        assert!(promise.is_accepted());
    }

    let deadline = cluster.clock.add(100000);
    let satisfied = cluster.run_until(deadline, |cluster| {
        cluster.nodes[0].inner.commit_index() == cluster.nodes[1].inner.commit_index()
            && cluster.nodes[0].inner.commit_index() == cluster.nodes[2].inner.commit_index()
    });
    assert!(satisfied, "Commit indices are not synchronized");
}

#[test]
fn node_restart() {
    let seed = rand::thread_rng().gen();
    let rng = StdRng::seed_from_u64(seed);
    dbg!(seed);

    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];

    // Create a cluster.
    let mut cluster = TestCluster::new(&node_ids, rng);

    cluster.nodes[0].options.running_ticks = MinMax::new(800, 5000);
    cluster.nodes[0].options.stopping_ticks = MinMax::new(800, 5000);

    assert!(cluster
        .random_node_mut()
        .create_cluster(&node_ids)
        .is_pending());

    let deadline = cluster.clock.add(10000);
    let satisfied = cluster.run_until(deadline, |cluster| cluster.leader_node().is_some());
    assert!(satisfied, "Create cluster timeout");

    // Propose commands.
    let mut promises = Vec::new();
    for _ in 0..100 {
        cluster.run_while_leader_absent(cluster.clock.add(10000));
        let Some(leader) = cluster.leader_node_mut() else {
            panic!("No leader");
        };
        promises.push(leader.propose_command());

        let ticks = MinMax::new(1, 10).sample_single(&mut cluster.rng);
        cluster.run(cluster.clock.add(ticks));
    }
    assert_eq!(promises.len(), 100);

    for mut promise in promises {
        for _ in 0..1000 {
            cluster.run_while_leader_absent(cluster.clock.add(10000));
            let Some(leader) = cluster.leader_node_mut() else {
                panic!("No leader");
            };
            if promise.poll(leader).is_accepted() {
                break;
            }
            cluster.run(cluster.clock.add(10));
        }
        assert!(promise.is_accepted());
    }

    let deadline = cluster.clock.add(50000);
    let satisfied = cluster.run_until(deadline, |cluster| {
        cluster.nodes[0].inner.commit_index() == cluster.nodes[1].inner.commit_index()
            && cluster.nodes[0].inner.commit_index() == cluster.nodes[2].inner.commit_index()
    });
    assert!(satisfied, "Commit indices are not synchronized");
}

#[test]
fn pipelining() {
    let seed = rand::thread_rng().gen();
    let rng = StdRng::seed_from_u64(seed);
    dbg!(seed);

    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];

    // Create a cluster.
    let mut cluster = TestCluster::new(&node_ids, rng);
    assert!(cluster
        .random_node_mut()
        .create_cluster(&node_ids)
        .is_pending());

    let deadline = cluster.clock.add(10000);
    let satisfied = cluster.run_until(deadline, |cluster| cluster.leader_node().is_some());
    assert!(satisfied, "Create cluster timeout");

    // Propose commands and trigger heartbeats.
    let mut promises = Vec::new();
    for _ in 0..100 {
        let pipeline_command = cluster.rng.gen_bool(0.8);
        let do_hearbeat = cluster.rng.gen_bool(0.5);

        cluster.run_while_leader_absent(cluster.clock.add(10000));
        let Some(leader) = cluster.leader_node_mut() else {
            panic!("No leader");
        };
        promises.push(leader.propose_command());
        if do_hearbeat {
            let promise = leader.heartbeat();
            assert!(promise.is_pending());
        }

        if !pipeline_command {
            let ticks = MinMax::new(0, 5).sample_single(&mut cluster.rng);
            cluster.run(cluster.clock.add(ticks));
        }
    }
    assert_eq!(promises.len(), 100);

    for mut promise in promises {
        for _ in 0..1000 {
            cluster.run_while_leader_absent(cluster.clock.add(10000));
            let Some(leader) = cluster.leader_node_mut() else {
                panic!("No leader");
            };
            if promise.poll(leader).is_accepted() {
                break;
            }
            cluster.run(cluster.clock.add(10));
        }
        assert!(promise.is_accepted());
    }

    let deadline = cluster.clock.add(10000);
    let satisfied = cluster.run_until(deadline, |cluster| {
        cluster.nodes[0].inner.commit_index() == cluster.nodes[1].inner.commit_index()
            && cluster.nodes[0].inner.commit_index() == cluster.nodes[2].inner.commit_index()
    });
    assert!(satisfied, "Commit indices are not synchronized");
}

#[test]
fn storage_repair_without_snapshot() {
    let seed = rand::thread_rng().gen();
    let rng = StdRng::seed_from_u64(seed);
    dbg!(seed);

    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];

    // Create a cluster.
    let mut cluster = TestCluster::new(&node_ids, rng);
    assert!(cluster
        .random_node_mut()
        .create_cluster(&node_ids)
        .is_pending());

    let deadline = cluster.clock.add(10000);
    let satisfied = cluster.run_until(deadline, |cluster| cluster.leader_node().is_some());
    assert!(satisfied, "Create cluster timeout");

    // Propose commands.
    let mut promises = Vec::new();
    for i in 0..100 {
        if i == 50 {
            for node in cluster.nodes.iter_mut() {
                if !node.inner.role().is_leader() {
                    // Reset the node.
                    node.inner = Node::start(node.inner.id());
                }
            }
        }

        cluster.run_while_leader_absent(cluster.clock.add(10000));
        let Some(leader) = cluster.leader_node_mut() else {
            panic!("No leader");
        };
        promises.push(leader.propose_command());

        let ticks = MinMax::new(1, 10).sample_single(&mut cluster.rng);
        cluster.run(cluster.clock.add(ticks));
    }
    assert_eq!(promises.len(), 100);

    for mut promise in promises {
        for _ in 0..1000 {
            let Some(leader) = cluster.leader_node_mut() else {
                panic!("No leader");
            };
            if promise.poll(leader).is_accepted() {
                break;
            }
            cluster.run(cluster.clock.add(10));
        }
        assert!(promise.is_accepted());
    }

    let deadline = cluster.clock.add(1_000_000);
    let satisfied = cluster.run_until(deadline, |cluster| {
        cluster.nodes[0].inner.commit_index() == cluster.nodes[1].inner.commit_index()
            && cluster.nodes[0].inner.commit_index() == cluster.nodes[2].inner.commit_index()
    });
    assert!(satisfied, "Commit indices are not synchronized");
}

#[test]
fn storage_repair_with_snapshot() {
    let seed = rand::thread_rng().gen();
    let rng = StdRng::seed_from_u64(seed);
    dbg!(seed);

    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];

    // Create a cluster.
    let mut cluster = TestCluster::new(&node_ids, rng);
    assert!(cluster
        .random_node_mut()
        .create_cluster(&node_ids)
        .is_pending());

    let deadline = cluster.clock.add(10000);
    let satisfied = cluster.run_until(deadline, |cluster| cluster.leader_node().is_some());
    assert!(satisfied, "Create cluster timeout");

    // Propose commands.
    let mut promises = Vec::new();
    let mut snapshot_index = LogIndex::ZERO;
    for i in 0..100 {
        if i == 25 {
            // Take snapshot
            cluster.run_until(cluster.clock.add(10000), |cluster| {
                cluster
                    .nodes
                    .iter()
                    .all(|node| node.inner.commit_index().get() > 0)
            });
            for node in cluster.nodes.iter_mut() {
                let (position, config) = node
                    .inner
                    .log()
                    .get_position_and_config(node.inner.commit_index())
                    .expect("unreachable");
                assert!(node
                    .inner
                    .handle_snapshot_installed(position, config.clone()));
                if node.inner.role().is_leader() {
                    snapshot_index = position.index;
                }
            }
        }
        if i == 50 {
            for node in cluster.nodes.iter_mut() {
                if !node.inner.role().is_leader() {
                    // Reset the node.
                    node.inner = Node::start(node.inner.id());
                }
            }
        }

        cluster.run_while_leader_absent(cluster.clock.add(10000));
        let Some(leader) = cluster.leader_node_mut() else {
            panic!("No leader");
        };
        promises.push(leader.propose_command());

        let ticks = MinMax::new(1, 10).sample_single(&mut cluster.rng);
        cluster.run(cluster.clock.add(ticks));
    }
    assert_eq!(promises.len(), 100);

    for mut promise in promises {
        for _ in 0..1000 {
            let Some(leader) = cluster.leader_node_mut() else {
                panic!("No leader");
            };
            if !promise.poll(leader).is_pending() {
                break;
            }
            cluster.run(cluster.clock.add(10));
        }

        if promise.log_position().index < snapshot_index {
            assert!(promise.is_rejected());
        } else {
            assert!(promise.is_accepted());
        }
    }

    let deadline = cluster.clock.add(1_000_000);
    let satisfied = cluster.run_until(deadline, |cluster| {
        cluster.nodes[0].inner.commit_index() == cluster.nodes[1].inner.commit_index()
            && cluster.nodes[0].inner.commit_index() == cluster.nodes[2].inner.commit_index()
    });
    assert!(satisfied, "Commit indices are not synchronized");
}

#[test]
fn dynamic_membership() {
    let seed = rand::thread_rng().gen();
    let rng = StdRng::seed_from_u64(seed);
    dbg!(seed);

    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];

    // Create a cluster.
    let mut cluster = TestCluster::new(&node_ids, rng);

    cluster.default_link_options.drop_rate = 0.3;
    cluster.default_link_options.latency_ticks = MinMax::new(1, 1000);

    assert!(cluster
        .random_node_mut()
        .create_cluster(&node_ids)
        .is_pending());

    let deadline = cluster.clock.add(100000);
    let satisfied = cluster.run_until(deadline, |cluster| cluster.leader_node().is_some());
    assert!(satisfied, "Create cluster timeout");

    for i in 0..10 {
        // Change the cluster configuration.
        cluster.run_while_leader_absent(cluster.clock.add(1000_000));
        if cluster.rng.gen_bool(0.7) {
            // Add.
            let node_id = NodeId::new(3 + i);
            let voter = cluster.rng.gen_bool(0.5);
            let mut node = TestNode::new(node_id);
            node.voter = voter;
            cluster.nodes.push(node);

            let Some(leader) = cluster.leader_node_mut() else {
                unreachable!();
            };
            let new_config = if voter {
                leader.config().to_joint_consensus(&[node_id], &[])
            } else {
                let mut new_config = leader.config().clone();
                new_config.non_voters.insert(node_id);
                new_config
            };
            let promise = leader.propose_config(new_config);
            assert!(promise.is_pending());
        } else if cluster.nodes.iter().filter(|n| n.voter).count() > 2 {
            // Remove.
            let node_ids = cluster
                .nodes
                .iter()
                .map(|n| n.inner.id())
                .collect::<Vec<_>>();
            let node_id = node_ids
                .choose(&mut cluster.rng)
                .copied()
                .expect("unreachable");

            let Some(leader) = cluster.leader_node_mut() else {
                unreachable!();
            };
            let new_config = if leader.config().non_voters.contains(&node_id) {
                let mut new_config = leader.config().clone();
                new_config.non_voters.remove(&node_id);
                new_config
            } else {
                leader.config().to_joint_consensus(&[], &[node_id])
            };
            let promise = leader.propose_config(new_config);
            assert!(promise.is_pending());
        }

        // Propose commands.
        let mut promises = Vec::new();
        for _ in 0..10 {
            cluster.run_while_leader_absent(cluster.clock.add(1000_000));
            let Some(leader) = cluster.leader_node_mut() else {
                panic!("No leader");
            };
            promises.push(leader.propose_command());

            let ticks = MinMax::new(1, 10).sample_single(&mut cluster.rng);
            cluster.run(cluster.clock.add(ticks));
        }
        assert_eq!(promises.len(), 10);

        let mut success_count = 0;
        for mut promise in promises {
            for _ in 0..10000 {
                cluster.run_while_leader_absent(cluster.clock.add(1000_000));
                let Some(leader) = cluster.leader_node_mut() else {
                    panic!("No leader");
                };
                if !promise.poll(leader).is_pending() {
                    break;
                }
                cluster.run(cluster.clock.add(10));
            }
            assert!(!promise.is_pending());
            if promise.is_accepted() {
                success_count += 1;
            }
        }
        assert!(success_count > 5);
    }
}

#[test]
fn truncate_divergence_log() {
    let seed = rand::thread_rng().gen();
    let rng = StdRng::seed_from_u64(seed);
    dbg!(seed);

    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];

    // Create a cluster.
    let mut cluster = TestCluster::new(&node_ids, rng);
    assert!(cluster
        .random_node_mut()
        .create_cluster(&node_ids)
        .is_pending());

    let deadline = cluster.clock.add(10000);
    let satisfied = cluster.run_until(deadline, |cluster| cluster.leader_node().is_some());
    assert!(satisfied, "Create cluster timeout");

    // Propose 20 commands as usual.
    let mut promises = Vec::new();
    for _ in 0..20 {
        let Some(leader) = cluster.leader_node_mut() else {
            panic!("No leader");
        };
        promises.push(leader.propose_command());

        let ticks = MinMax::new(1, 10).sample_single(&mut cluster.rng);
        cluster.run(cluster.clock.add(ticks));
    }

    // Propose next 20 commands without calling `cluster.run()`
    for _ in 0..20 {
        let Some(leader) = cluster.leader_node_mut() else {
            panic!("No leader");
        };
        promises.push(leader.propose_command());
    }

    // Isolate the leader from the cluster.
    let leader_node_index = cluster
        .nodes
        .iter()
        .position(|n| n.inner.role().is_leader())
        .expect("unreachable");
    let old_leader = cluster.nodes.remove(leader_node_index);

    // Elect a new leader.
    cluster.run_while_leader_absent(cluster.clock.add(1000_000));

    // Propose remaining commands.
    for _ in 0..60 {
        let Some(leader) = cluster.leader_node_mut() else {
            panic!("No leader");
        };
        promises.push(leader.propose_command());
    }
    assert_eq!(promises.len(), 100);

    // Rejoin the old leader.
    cluster.nodes.push(old_leader);

    let mut success_count = 0;
    for mut promise in promises {
        for _ in 0..1000 {
            let Some(leader) = cluster.leader_node_mut() else {
                panic!("No leader");
            };
            if !promise.poll(leader).is_pending() {
                break;
            }
            cluster.run(cluster.clock.add(10));
        }
        assert!(!promise.is_pending());
        if promise.is_accepted() {
            success_count += 1;
        }
    }
    assert!(60 <= success_count);
    assert!(success_count <= 80);

    let deadline = cluster.clock.add(1000);
    let satisfied = cluster.run_until(deadline, |cluster| {
        cluster.nodes[0].inner.commit_index() == cluster.nodes[1].inner.commit_index()
            && cluster.nodes[0].inner.commit_index() == cluster.nodes[2].inner.commit_index()
    });
    assert!(satisfied, "Commit indices are not synchronized");
}

#[derive(Debug)]
pub struct TestCluster {
    pub nodes: Vec<TestNode>,
    pub clock: Clock,
    pub rng: StdRng,
    pub default_link_options: TestLinkOptions,
    seqno: u64,
}

impl TestCluster {
    pub fn new(node_ids: &[NodeId], rng: StdRng) -> Self {
        Self {
            nodes: node_ids.iter().map(|&id| TestNode::new(id)).collect(),
            clock: Clock::new(),
            rng,
            default_link_options: TestLinkOptions::default(),
            seqno: 0,
        }
    }

    pub fn leader_node(&self) -> Option<&Node> {
        self.nodes
            .iter()
            .find(|node| node.inner.role().is_leader())
            .map(|node| &node.inner)
    }

    pub fn leader_node_mut(&mut self) -> Option<&mut Node> {
        self.nodes
            .iter_mut()
            .find(|node| node.inner.role().is_leader())
            .map(|node| &mut node.inner)
    }

    pub fn random_node_mut(&mut self) -> &mut Node {
        let index = self.rng.gen_range(0..self.nodes.len());
        &mut self.nodes[index].inner
    }

    pub fn run_while_leader_absent(&mut self, deadline: Clock) {
        self.run_until(deadline, |cluster| cluster.leader_node().is_some());
    }

    pub fn run(&mut self, deadline: Clock) {
        self.run_until(deadline, |_| false);
    }

    pub fn run_until<F>(&mut self, deadline: Clock, condition: F) -> bool
    where
        F: Fn(&TestCluster) -> bool,
    {
        while self.clock < deadline && !condition(self) {
            self.run_tick();
        }
        self.clock < deadline
    }

    pub fn run_tick(&mut self) {
        self.clock.tick();
        let mut messages = Vec::new();
        let mut snapshots = Vec::new();

        // Run nodes.
        for node in &mut self.nodes {
            node.run_tick(&mut self.rng, self.clock);

            let src = node.inner.id();
            let mut actions = std::mem::take(node.inner.actions_mut());
            if let Some(msg) = actions.broadcast_message.take() {
                for dst in node.inner.peers() {
                    messages.push((src, dst, msg.clone()));
                }
            }
            for (dst, msg) in actions.send_messages {
                messages.push((src, dst, msg));
            }
            for dst in actions.install_snapshots {
                snapshots.push((
                    src,
                    dst,
                    node.inner.log().snapshot_position(),
                    node.inner.log().snapshot_config().clone(),
                ));
                // for node in &mut self.nodes {
                //     if node.inner.id() == id {
                //         continue;
                //     }
                //     if node.snapshot_finish_time.is_some() {
                //         break;
                //     }

                //     // node.snapshot_finish_time =
                //     // node.inner.handle_snapshot_installed(
                //     //     node.inner.commit_position(),
                //     //     node.inner.config().clone(),
                //     // );
                //     break;
                // }
            }
        }

        // Deliver messages.
        for (src, dst, msg) in messages {
            self.send_message(src, dst, msg);
        }

        // Deliver snapshots.
        for (src, dst, position, config) in snapshots {
            self.send_snashot(src, dst, position, config);
        }
    }

    fn send_message(&mut self, _src: NodeId, dst: NodeId, msg: Message) {
        let options = &self.default_link_options;

        if self.rng.gen_bool(options.drop_rate) {
            return;
        }

        let latency = options.latency_ticks.sample_single(&mut self.rng) * message_size(&msg);
        for node in &mut self.nodes {
            if node.inner.id() == dst {
                node.incoming_messages
                    .insert((self.clock.add(latency), self.seqno), msg);
                self.seqno += 1;
                return;
            }
        }
    }

    fn send_snashot(
        &mut self,
        _src: NodeId,
        dst: NodeId,
        position: LogPosition,
        config: ClusterConfig,
    ) {
        for node in &mut self.nodes {
            if node.inner.id() == dst {
                if node.snapshot_finish_time.is_some() {
                    return;
                }

                node.snapshot_finish_time = Some((
                    self.clock.add(
                        node.options
                            .install_snapshot_ticks
                            .sample_single(&mut self.rng),
                    ),
                    position,
                    config,
                ));
                return;
            }
        }
    }
}

fn message_size(msg: &Message) -> usize {
    match msg {
        Message::AppendEntriesCall { entries, .. } => entries.len(),
        Message::AppendEntriesReply { .. } => 1,
        Message::RequestVoteCall { .. } => 1,
        Message::RequestVoteReply { .. } => 1,
    }
}

#[derive(Debug, Clone)]
pub struct TestLinkOptions {
    pub latency_ticks: MinMax,
    pub drop_rate: f64,
}

impl Default for TestLinkOptions {
    fn default() -> Self {
        Self {
            latency_ticks: MinMax::new(5, 20),
            drop_rate: 0.01,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TestNodeOptions {
    pub election_timeout_ticks: MinMax,
    pub storage_latency_ticks: MinMax,
    pub install_snapshot_ticks: MinMax,
    pub running_ticks: MinMax,
    pub stopping_ticks: MinMax,
    pub log_entries_lost: MinMax,
    pub max_entries_per_rpc: usize,
    pub voter: bool,
}

impl Default for TestNodeOptions {
    fn default() -> Self {
        Self {
            election_timeout_ticks: MinMax::new(100, 1000),
            storage_latency_ticks: MinMax::new(1, 10),
            install_snapshot_ticks: MinMax::new(1000, 10_000),
            running_ticks: MinMax::constant(usize::MAX),
            stopping_ticks: MinMax::constant(usize::MAX),
            log_entries_lost: MinMax::constant(0),
            max_entries_per_rpc: 100,
            voter: true,
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct MinMax {
    pub min: usize,
    pub max: usize,
}

impl MinMax {
    pub fn new(min: usize, max: usize) -> MinMax {
        assert!(min <= max);
        MinMax { min, max }
    }

    pub fn constant(value: usize) -> MinMax {
        MinMax::new(value, value)
    }
}

impl SampleRange<usize> for MinMax {
    fn sample_single<R: RngCore + ?Sized>(self, rng: &mut R) -> usize {
        rng.gen_range(self.min..=self.max)
    }

    fn is_empty(&self) -> bool {
        self.min > self.max
    }
}

#[derive(Debug)]
pub struct TestNode {
    pub inner: Node,
    pub options: TestNodeOptions,
    pub running: bool,
    pub timeout_expire_time: Option<Clock>,
    pub storage_finish_time: Option<Clock>,
    pub snapshot_finish_time: Option<(Clock, LogPosition, ClusterConfig)>,
    pub incoming_messages: BTreeMap<(Clock, u64), Message>,
    pub stop_time: Option<Clock>,
    pub start_time: Option<Clock>,
    pub voter: bool,
}

impl TestNode {
    pub fn new(id: NodeId) -> TestNode {
        TestNode {
            inner: Node::start(id),
            options: TestNodeOptions::default(),
            running: true,
            timeout_expire_time: None,
            storage_finish_time: None,
            snapshot_finish_time: None,
            incoming_messages: BTreeMap::new(),
            stop_time: None,
            start_time: None,
            voter: true,
        }
    }

    pub fn run_tick(&mut self, rng: &mut StdRng, now: Clock) {
        if !self.voter {
            assert!(self.inner.role().is_follower());
        }

        if !self.running {
            if self.start_time.take_if(|t| *t <= now).is_some() {
                self.running = true;

                while let Some(entry) = self.incoming_messages.first_entry() {
                    if entry.key().0 < now {
                        entry.remove();
                    } else {
                        break;
                    }
                }

                self.inner = Node::restart(
                    self.inner.id(),
                    self.inner.current_term(),
                    self.inner.voted_for(),
                    self.inner.log().clone(),
                );
            } else {
                return;
            }
        }
        if self.stop_time.is_none() {
            self.stop_time = Some(now.add(self.options.running_ticks.sample_single(rng)));
        }
        if self.stop_time.take_if(|t| *t <= now).is_some() {
            self.running = false;
            self.timeout_expire_time = None;
            self.storage_finish_time = None;
            self.start_time = Some(now.add(self.options.stopping_ticks.sample_single(rng)));
            return;
        }

        self.storage_finish_time.take_if(|t| *t <= now);
        if self.storage_finish_time.is_some() {
            // Storage operations are synchronous, so we can't do anything else until they finish.
            return;
        }

        if self.timeout_expire_time.take_if(|t| *t <= now).is_some() {
            self.inner.handle_election_timeout();
        }

        if let Some((_, position, config)) =
            self.snapshot_finish_time.take_if(|(t, _, _)| *t <= now)
        {
            let _succeeded = self.inner.handle_snapshot_installed(position, config);
        }

        while let Some(entry) = self.incoming_messages.first_entry() {
            if entry.key().0 <= now {
                let message = entry.remove();
                self.inner.handle_message(message);
            } else {
                break;
            }
        }

        if std::mem::take(&mut self.inner.actions_mut().set_election_timeout) {
            self.reset_election_timeout(rng, now);
        }
        if std::mem::take(&mut self.inner.actions_mut().save_current_term) {
            self.extend_storage_finish_time(rng, now, 1);
        }
        if std::mem::take(&mut self.inner.actions_mut().save_voted_for) {
            self.extend_storage_finish_time(rng, now, 1);
        }
        if let Some(entries) = self.inner.actions_mut().append_log_entries.take() {
            self.extend_storage_finish_time(rng, now, entries.len());
        }
    }

    fn reset_election_timeout(&mut self, rng: &mut StdRng, now: Clock) {
        let timeout = match self.inner.role() {
            Role::Leader => self.options.election_timeout_ticks.min,
            Role::Candidate => self.options.election_timeout_ticks.sample_single(rng),
            Role::Follower => self.options.election_timeout_ticks.max,
        };
        self.timeout_expire_time = Some(now.add(timeout));
    }

    fn extend_storage_finish_time(&mut self, rng: &mut StdRng, now: Clock, n: usize) {
        let remaining_latency = self.storage_finish_time.map_or(0, |t| t.0 - now.0);
        let additional_latency = self.options.storage_latency_ticks.sample_single(rng) * n;
        let latency = remaining_latency + additional_latency;
        self.storage_finish_time = Some(now.add(latency));
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Clock(usize);

impl Clock {
    pub fn new() -> Clock {
        Clock(0)
    }

    pub fn tick(&mut self) {
        self.0 += 1;
    }

    pub fn add(&self, ticks: usize) -> Clock {
        Clock(self.0.saturating_add(ticks))
    }
}
