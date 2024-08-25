use std::collections::BTreeMap;

use raftbare::{ClusterConfig, LogPosition, Message, Node, NodeId, Role};
use rand::{distributions::uniform::SampleRange, prelude::RngCore, rngs::StdRng, Rng, SeedableRng};

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
    cluster.run_until(deadline, |cluster| cluster.leader_node().is_some());
    assert!(cluster.clock < deadline, "Create cluster timeout");
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

    pub fn random_node_mut(&mut self) -> &mut Node {
        let index = self.rng.gen_range(0..self.nodes.len());
        &mut self.nodes[index].inner
    }

    pub fn run_until<F>(&mut self, deadline: Clock, condition: F)
    where
        F: Fn(&TestCluster) -> bool,
    {
        while self.clock < deadline && !condition(self) {
            self.run_tick();
        }
    }

    pub fn run_tick(&mut self) {
        self.clock.tick();
        let mut messages = Vec::new();

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
            for _id in actions.install_snapshots {
                todo!();
            }
        }

        // Deliver messages.
        for (src, dst, msg) in messages {
            self.send_message(src, dst, msg);
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
            drop_rate: 0.05,
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
    // TODO: snapshot_install_interval_ticks
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
    //pub running: bool,
    pub timeout_expire_time: Option<Clock>,
    pub storage_finish_time: Option<Clock>,
    pub snapshot_finish_time: Option<(Clock, ClusterConfig, LogPosition)>,
    pub incoming_messages: BTreeMap<(Clock, u64), Message>,
}

impl TestNode {
    pub fn new(id: NodeId) -> TestNode {
        TestNode {
            inner: Node::start(id),
            options: TestNodeOptions::default(),
            //running: true,
            timeout_expire_time: None,
            storage_finish_time: None,
            snapshot_finish_time: None,
            incoming_messages: BTreeMap::new(),
        }
    }

    pub fn run_tick(&mut self, rng: &mut StdRng, now: Clock) {
        self.storage_finish_time.take_if(|t| *t <= now);
        if self.storage_finish_time.is_some() {
            // Storage operations are synchronous, so we can't do anything else until they finish.
            return;
        }

        if self.timeout_expire_time.take_if(|t| *t <= now).is_some() {
            self.inner.handle_election_timeout();
        }

        if let Some((_, config, position)) =
            self.snapshot_finish_time.take_if(|(t, _, _)| *t <= now)
        {
            let _succeeded = self.inner.handle_snapshot_installed(config, position);
        }

        if let Some(entry) = self.incoming_messages.first_entry() {
            if entry.key().0 <= now {
                let message = entry.remove();
                self.inner.handle_message(message);
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
        Clock(self.0 + ticks)
    }
}
