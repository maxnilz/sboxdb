use std::cell::RefCell;
use std::collections::HashMap;
use std::time::Duration;

use log::debug;
use tokio::sync::oneshot;
use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_stream::StreamExt as _;

use crate::error::{Error, Result};
use crate::raft::message::{Address, Event, Message};
use crate::raft::node::follower::Follower;
use crate::raft::node::{Node, NodeId, NodeState, ProposalId, RawNode, TICK_INTERVAL};
use crate::raft::persister::Persister;
use crate::raft::transport::Transport;
use crate::raft::{Command, CommandResult};
use crate::storage::state::State;

struct Request {
    command: Command,
    timeout: Option<Duration>,
    tx: oneshot::Sender<CommandResult>,
}

struct EventLoopContext {
    id: NodeId,
    /// role-based node, the role of a node
    /// is changing according to raft protocol.
    node: Box<dyn Node>,
    /// overlay channel for receiving in-process
    /// message from node, paired with the node_tx
    /// inside raw node.
    node_rx: mpsc::UnboundedReceiver<Message>,
    /// channel for receiving node state query,
    /// paired with state_tx, will be consumed
    /// into eventloop.
    state_rx: mpsc::UnboundedReceiver<((), oneshot::Sender<NodeState>)>,
    /// channel for receiving client command,
    /// paired with command_tx, will be consumed
    /// into eventloop.
    command_rx: mpsc::UnboundedReceiver<Request>,
    /// transport act as the exchange for sending
    /// and receiving messages to/from raft peers.
    transport: Box<dyn Transport>,
}

pub struct Server {
    #[allow(unused)]
    id: NodeId,

    /// channel for query node state, paired with
    /// the state_rx in the eventloop.
    state_tx: mpsc::UnboundedSender<((), oneshot::Sender<NodeState>)>,

    /// channel for client command, paired with
    /// the command_rx in the eventloop.
    command_tx: mpsc::UnboundedSender<Request>,

    /// eventloop context, will be consumed by
    /// the eventloop, use RefCell here to make
    /// `Server` achieve the interior mutability.
    /// Since the context will not be used across
    /// threads(the actual content is moved into
    /// eventloop and never used again), so it is
    /// safe to assert `Server` is `Sync` explicitly.
    context: RefCell<Option<EventLoopContext>>,
}

unsafe impl Sync for Server {}

impl Server {
    pub fn new(
        id: NodeId,
        peers: Vec<NodeId>,
        persister: Persister,
        transport: Box<dyn Transport>,
        state: Box<dyn State>,
    ) -> Result<Server> {
        let (node_tx, node_rx) = mpsc::unbounded_channel();
        let rn = RawNode::new(id, peers, persister, node_tx, state)?;
        // create server as follower at the very beginning.
        let follower = Follower::new(rn);
        let node: Box<dyn Node> = Box::new(follower);
        let (state_tx, state_rx) = mpsc::unbounded_channel();
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let context = EventLoopContext { id, node, node_rx, state_rx, command_rx, transport };
        Ok(Server { id, state_tx, command_tx, context: RefCell::new(Some(context)) })
    }

    pub async fn serve(&self, done: broadcast::Receiver<()>) -> Result<()> {
        let context = self.context.borrow_mut().take().unwrap();
        let eventloop = tokio::spawn(Self::eventloop(context, done));
        eventloop.await?
    }

    /// run the event loop for message processing.
    async fn eventloop(context: EventLoopContext, mut done: broadcast::Receiver<()>) -> Result<()> {
        let node_id = context.id;
        let mut node = context.node;
        let mut node_rx = UnboundedReceiverStream::new(context.node_rx);

        let mut state_rx = UnboundedReceiverStream::new(context.state_rx);
        let mut command_rx = UnboundedReceiverStream::new(context.command_rx);

        let mut transport = context.transport;
        let mut transport_rx = transport.receiver().await?;

        let mut proposals: HashMap<ProposalId, oneshot::Sender<CommandResult>> = HashMap::new();

        debug!("node {} eventloop on {:?}", node_id, std::thread::current().id());

        let mut ticker = tokio::time::interval(TICK_INTERVAL);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    node = node.tick()?;
                },

                Some(msg) = transport_rx.next() => {
                    node = node.step(msg)?
                }

                Some(msg) = node_rx.next() => {
                    match msg {
                        Message{to: Address::Node(_), ..} => transport.send(msg).await?,
                        Message{to: Address::Broadcast, ..} => transport.send(msg).await?,
                        Message{to: Address::Localhost, event: Event::ProposalResponse {id,result}, ..} => {
                            if let Some(tx) = proposals.remove(&id) {
                                if let Err(_) = tx.send(result.into()) {
                                    return Err(Error::internal("command oneshot receiver dropped"))
                                }
                            }
                        }
                        _ => return Err(Error::internal(format!("unexpected message to localhost {:?}", msg)))
                    }
                },

                Some((_, tx)) = state_rx.next() => {
                    let ns = node.get_state();
                    if let Err(_) = tx.send(ns) {
                        return Err(Error::internal("state response receiver dropped"));
                    }
                }

                Some(req) = command_rx.next() => {
                    let id = ProposalId::new();
                    let message = Message {
                        term: 0,
                        from: Address::Localhost,
                        to: Address::Node(node_id),
                        event: Event::ProposalRequest {
                            id: id.clone(),
                            command: req.command,
                            timeout: req.timeout,
                        }
                    };
                    node = node.step(message)?;
                    proposals.insert(id, req.tx);
                }

                _ = done.recv() => {
                    return Ok(())
                },
            }
        }
    }

    pub fn get_state(&self) -> Result<NodeState> {
        let (tx, rx) = oneshot::channel();
        if let Err(_) = self.state_tx.send(((), tx)) {
            return Err(Error::internal("state channel is closed or dropped"));
        }
        let ns = futures::executor::block_on(rx)?;
        Ok(ns)
    }

    pub fn execute_command(
        &self,
        command: Vec<u8>,
        timeout: Option<Duration>,
    ) -> Result<CommandResult> {
        let (tx, rx) = oneshot::channel();
        let req = Request { command: command.into(), timeout, tx };
        if let Err(_) = self.command_tx.send(req) {
            return Err(Error::internal("command channel is closed or dropped"));
        }
        Ok(futures::executor::block_on(rx)?)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::ops::{Add, Mul};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, SystemTime};

    use log::{debug, error};
    use rand::{thread_rng, Rng};

    use crate::error::Result;
    use crate::raft::node::{ELECTION_TIMEOUT_RANGE, HEARTBEAT_INTERVAL, ROUND_TRIP_INTERVAL};
    use crate::raft::transport::tests::LabNetMesh;
    use crate::raft::Index;
    use crate::raft::Term;
    use crate::storage::state::ApplyMsg;
    use crate::storage::{new_storage, StorageType};

    use super::*;

    fn max_election_timeout() -> Duration {
        let ticks = ELECTION_TIMEOUT_RANGE.end + HEARTBEAT_INTERVAL;
        TICK_INTERVAL.mul(ticks as u32)
    }

    #[derive(Debug)]
    struct Inner {
        states: HashMap<Index, Command>,
        messages: Vec<ApplyMsg>,
    }

    #[derive(Debug)]
    struct KvState {
        inner: Mutex<Inner>,
    }

    impl KvState {
        fn new() -> KvState {
            let inner = Inner { states: HashMap::new(), messages: Vec::new() };
            KvState { inner: Mutex::new(inner) }
        }
    }

    impl KvState {
        fn get_command(&self, index: Index) -> Option<Command> {
            let gard = self.inner.lock().unwrap();
            gard.states.get(&index).cloned()
        }
    }

    impl State for Arc<KvState> {
        fn apply(&mut self, msg: ApplyMsg) -> Result<Command> {
            let mut gard = self.inner.lock().unwrap();
            gard.messages.push(msg.clone());
            gard.states.insert(msg.index, msg.command.clone());
            Ok(msg.command)
        }
    }

    fn new_server(
        id: NodeId,
        peers: Vec<NodeId>,
        mesh: &LabNetMesh,
        state: Arc<KvState>,
    ) -> Result<Server> {
        let ns = id.to_string();
        let storage = new_storage(StorageType::Memory)?;
        let persister = Persister::new(ns, storage)?;
        let transport = Box::new(mesh.get(id)?);
        let state: Box<dyn State> = Box::new(state);
        let server = Server::new(id, peers, persister, transport, state)?;
        Ok(server)
    }

    struct Cluster {
        nodes: Vec<NodeId>,
        net_mesh: LabNetMesh,
        states: Vec<Arc<KvState>>,
        servers: Vec<Arc<Server>>,

        threads: HashMap<NodeId, (broadcast::Sender<()>, std::thread::JoinHandle<()>)>,
    }

    impl Cluster {
        fn new(n: u8) -> Result<Self> {
            let mut nodes = Vec::new();
            for i in 0..n {
                // node id is equal to the index of array.
                nodes.push(i as NodeId);
            }
            let net_mesh = LabNetMesh::new(nodes.clone());
            let mut states = Vec::new();
            let mut servers = Vec::new();
            for &id in nodes.iter() {
                let peers: Vec<_> =
                    nodes.iter().filter_map(|&x| if x == id { None } else { Some(x) }).collect();
                let state = Arc::new(KvState::new());
                states.push(Arc::clone(&state));
                let server = new_server(id, peers, &net_mesh, Arc::clone(&state))?;
                servers.push(Arc::new(server));
            }
            Ok(Self { nodes, net_mesh, states, servers, threads: HashMap::new() })
        }

        fn start_node(&mut self, id: NodeId) {
            let server = &self.servers[id as usize];
            let server = Arc::clone(server);
            let (tx, rx) = broadcast::channel(1);
            let th = std::thread::spawn(move || {
                debug!("node {} start on {:?}", id, std::thread::current().id());
                let rt =
                    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
                rt.block_on(async move {
                    debug!("node {} block on {:?}", id, std::thread::current().id());
                    if let Err(err) = server.serve(rx).await {
                        error!("node {} failed {}", id, err)
                    }
                })
            });
            self.threads.insert(id, (tx, th));
        }

        fn close_node(&mut self, id: NodeId) {
            let (tx, th) = self.threads.remove(&id).unwrap();
            if let Err(_) = tx.send(()) {
                // receiver dropped, do nothing
            }
            if let Err(_) = th.join() {
                // join thread error, do nothing
            }
        }

        fn start(&mut self) {
            for id in self.nodes.clone() {
                self.start_node(id)
            }
        }

        fn close(&mut self) {
            for id in self.nodes.clone() {
                self.close_node(id)
            }
        }

        fn get_node_state(&self, id: NodeId) -> NodeState {
            let server = &self.servers[id as usize];
            server.get_state().unwrap()
        }

        fn check_no_leader(&self) -> Result<()> {
            for &id in &self.nodes {
                if !self.net_mesh.is_connected(id) {
                    continue;
                }
                let server = &self.servers[id as usize];
                let ns = server.get_state().unwrap();
                if !ns.leader.is_none() {
                    #[rustfmt::skip]
                    return Err(Error::internal(format!("expected no leader among connected servers, but {} claims to be leader", ns.leader.unwrap())));
                }
            }
            Ok(())
        }

        // check that one of the connected servers thinks
        // it is the leader, and that no other connected
        // server thinks otherwise.
        //
        // try a few times in case re-elections are needed.
        fn check_one_leader(&self) -> Result<NodeId> {
            for _ in 0..10 {
                // wait at lease max election timeout so that
                // we will have at least one election.
                std::thread::sleep(max_election_timeout());

                let mut terms: HashMap<Term, HashSet<NodeId>> = HashMap::new();
                for &id in &self.nodes {
                    if !self.net_mesh.is_connected(id) {
                        continue;
                    }
                    let server = &self.servers[id as usize];
                    let ns = server.get_state().unwrap();
                    if let Some(leader) = ns.leader {
                        if let Some(leaders) = terms.get_mut(&ns.term) {
                            leaders.insert(leader);
                        } else {
                            let mut leaders = HashSet::new();
                            leaders.insert(leader);
                            terms.insert(ns.term, leaders);
                        }
                    }
                }
                let mut latest_term = 0;
                let mut leader: Option<NodeId> = None;
                for (&term, leaders) in terms.iter() {
                    if leaders.len() > 1 {
                        #[rustfmt::skip]
                        return Err(Error::internal(format!("term {} have {}(>1) leaders", term, leaders.len())));
                    }
                    if latest_term < term {
                        latest_term = term;
                        let leaders = leaders.iter().map(|&x| x).collect::<Vec<NodeId>>();
                        leader = Some(leaders[0])
                    }
                }
                if let Some(leader) = leader {
                    return Ok(leader);
                }
            }
            Err(Error::internal("expect one leader, got none"))
        }

        // check that everyone agrees on the term.
        fn check_terms(&self) -> Result<Term> {
            let mut term = 0;
            for &id in &self.nodes {
                if !self.net_mesh.is_connected(id) {
                    continue;
                }
                let server = &self.servers[id as usize];
                let ns = server.get_state().unwrap();
                if term == 0 {
                    term = ns.term;
                    continue;
                }
                if term != ns.term {
                    return Err(Error::internal("servers disagree on term"));
                }
            }
            Ok(term)
        }

        // how many servers think a log entry is applied at the given index
        fn napplied(&self, index: Index) -> Result<(u8, Option<Command>)> {
            let mut n = 0;
            let mut ans: Option<Command> = None;
            for &id in &self.nodes {
                let state = &self.states[id as usize];
                let cmd = state.get_command(index);
                if cmd.is_none() {
                    continue;
                }
                let cmd = cmd.unwrap();
                #[rustfmt::skip]
                if let Some(c) = &ans && n > 1 {
                    if *c != cmd {
                        return Err(Error::internal(format!("applied values do not match: index {}, {:?}, {:?}", index, *c, cmd)));
                    }
                }
                n += 1;
                ans = Some(cmd);
            }
            Ok((n, ans))
        }

        // do a complete agreement. since our raft implementation would forward
        // command to the leader as long as the election is done. so we try to
        // send the command to raft to each of servers in a loop. the command
        // might get drop because of the leader election or leader change, we have
        // to re-submit the command in this case. keep retrying in 10 seconds before
        // entirely giving up.
        // if retry == ture, we may submit the command multiple times, in case a
        // leader fails just after submit.
        // if retry == false, just do a success submit only once.
        fn one(&self, command: Vec<u8>, n: u8, retry: bool) -> Result<Index> {
            let mut ind = 0;
            let tm = SystemTime::now().add(Duration::from_secs(10));
            loop {
                let server = &self.servers[ind];
                // set the agreement timeout to be 3 times of round trip interval
                let timeout = TICK_INTERVAL.mul(3 * ROUND_TRIP_INTERVAL as u32);
                let res = server.execute_command(command.clone(), Some(timeout))?;
                match res {
                    CommandResult::Dropped => {
                        // command get dropped by raft, sleep a while
                        // then continue with another server.
                        std::thread::sleep(Duration::from_millis(50));
                        ind = (ind + 1) % self.servers.len();
                        continue;
                    }
                    CommandResult::Ongoing(index) => {
                        // somebody claimed to be the leader and to have
                        // submitted our command, however, after the timeout
                        // it still not reach agreement yet.
                        //
                        // check if we have retry setup, otherwise consider this
                        // as fail to agreement and return err.
                        if retry == false {
                            #[rustfmt::skip]
                            return Err(Error::internal(format!( "failed to reach agreement {:?} at index {}", command, index)));
                        }
                    }
                    CommandResult::Applied { index, .. } => {
                        // since we will get applied response back as soon as the
                        // leader applied the command to the state machine after
                        // the cluster think the command is replicated/logged to
                        // majority.
                        // wait a while to for the follower to apply the command.
                        let d = max_election_timeout();
                        let until = SystemTime::now().add(d);
                        let mut m: u8;
                        let mut cmd: Option<Command>;
                        loop {
                            (m, cmd) = self.napplied(index)?;
                            if m >= n && cmd == Some(command.clone().into()) {
                                return Ok(index);
                            }
                            if SystemTime::now().gt(&until) {
                                break;
                            }
                            std::thread::sleep(Duration::from_millis(50));
                        }
                        #[rustfmt::skip]
                        return Err(Error::internal(format!("failed to reach agreement {:?} at index {}, {}/{}, {:?}", command, index, m, n, cmd)));
                    }
                };

                if SystemTime::now().gt(&tm) {
                    break;
                }
            }
            Err(Error::internal(format!("failed to reach agreement {:?}", command)))
        }

        fn disconnect(&mut self, id: NodeId) {
            debug!("disconnect {}", id);
            self.net_mesh.disconnect(id).unwrap()
        }

        fn connect(&mut self, id: NodeId) {
            debug!("connect {}", id);
            self.net_mesh.connect(id).unwrap()
        }
    }

    #[test]
    fn test_initial_election_r1() -> Result<()> {
        env_logger::builder().init();

        let mut cluster = Cluster::new(3)?;
        cluster.start();

        // check if a leader elected.
        let leader1 = cluster.check_one_leader()?;

        // check all servers agree on a same term
        let term1 = cluster.check_terms()?;

        // does the leader+term stay the same if there is no network failure?
        std::thread::sleep(max_election_timeout());
        // the term should be the same
        let term2 = cluster.check_terms()?;
        assert_eq!(term1, term2);
        // the leader should be the same
        let leader2 = cluster.check_one_leader()?;
        assert_eq!(leader1, leader2);

        cluster.close();
        Ok(())
    }

    #[test]
    fn test_re_election_r1() -> Result<()> {
        env_logger::builder().init();

        let num_nodes = 3;

        let mut cluster = Cluster::new(num_nodes)?;
        cluster.start();

        let leader1 = cluster.check_one_leader()?;

        // if the leader disconnect, a new one should be elected.
        cluster.disconnect(leader1);
        let leader2 = cluster.check_one_leader()?;
        assert_ne!(leader2, leader1);

        // if old leader rejoins, that should not disturb the
        // new leader. and the old leader should switch to follower.
        cluster.connect(leader1);
        let leader3 = cluster.check_one_leader()?;
        assert_eq!(leader2, leader3);
        let ns = cluster.get_node_state(leader1);
        assert_eq!(Some(leader2), ns.leader);

        // if there is no quorum, no new leader should be elected.
        cluster.disconnect(leader2);
        cluster.disconnect(((leader2 as u8 + 1) % num_nodes) as NodeId);
        std::thread::sleep(max_election_timeout());

        // check that the one connected server does not think it is the leader.
        cluster.check_no_leader()?;

        // if quorum arise, it should elect a leader.
        cluster.connect(((leader2 as u8 + 1) % num_nodes) as NodeId);
        cluster.check_one_leader()?;

        // re-join of last node, should not prevent leader from existing.
        cluster.connect(leader2);
        cluster.check_one_leader()?;

        cluster.close();
        Ok(())
    }

    #[test]
    fn test_many_election_r1() -> Result<()> {
        env_logger::builder().init();

        let num_nodes = 7;
        let mut cluster = Cluster::new(num_nodes)?;
        cluster.start();

        cluster.check_one_leader()?;

        let iters = 10;
        for i in 0..iters {
            debug!("test many election iter {}", i);

            // disconnect three nodes
            let i1 = thread_rng().gen_range(0..num_nodes) as NodeId;
            let i2 = thread_rng().gen_range(0..num_nodes) as NodeId;
            let i3 = thread_rng().gen_range(0..num_nodes) as NodeId;

            cluster.disconnect(i1);
            cluster.disconnect(i2);
            cluster.disconnect(i3);

            // either the current leader should alive, or
            // the remaining four should elect a new one.
            cluster.check_one_leader()?;

            cluster.connect(i1);
            cluster.connect(i2);
            cluster.connect(i3);
        }

        cluster.check_one_leader()?;

        cluster.close();

        Ok(())
    }

    #[test]
    fn test_basic_agree_r2() -> Result<()> {
        env_logger::builder().init();

        let num_nodes = 3;
        let mut cluster = Cluster::new(num_nodes)?;
        cluster.start();

        for index in 1..=3 {
            let (n, _) = cluster.napplied(index)?;
            assert_eq!(n, 0, "some have committed before");
            let got = cluster.one(vec![index as u8], num_nodes, false)?;
            assert_eq!(index, got, "got index {}, expected {}", got, index);
        }

        cluster.close();

        Ok(())
    }

    #[test]
    fn test_fail_agree_r2() -> Result<()> {
        env_logger::builder().init();

        // a follower participates first, then disconnect and reconnect.

        let num_nodes = 3;
        let mut cluster = Cluster::new(num_nodes)?;
        cluster.start();

        cluster.one(vec![0x0b], num_nodes, false)?;

        // disconnect one follower from the network.
        let leader = cluster.check_one_leader()?;
        cluster.disconnect((leader + 1) % num_nodes);

        // the leader and the reaming follower should be
        // able to agree despite the disconnected follower.
        cluster.one(vec![0x0c], num_nodes - 1, false)?;
        cluster.one(vec![0x0d], num_nodes - 1, false)?;
        std::thread::sleep(max_election_timeout());
        cluster.one(vec![0x0e], num_nodes - 1, false)?;
        cluster.one(vec![0x0f], num_nodes - 1, false)?;

        // reconnect the disconnected follower.
        cluster.connect((leader + 1) % num_nodes);

        // the full set of servers should preserve previous
        // agreements, and be able to agree on new commands.
        cluster.one(vec![0x10], num_nodes - 1, true)?;
        std::thread::sleep(max_election_timeout());
        cluster.one(vec![0x11], num_nodes - 1, true)?;

        cluster.close();

        Ok(())
    }

    #[test]
    fn test_fail_no_agree_r2() -> Result<()> {
        env_logger::builder().init();

        // no agreement if too many followers disconnect

        let num_nodes = 5;
        let mut cluster = Cluster::new(num_nodes)?;
        cluster.start();

        cluster.one(vec![0x01], num_nodes, false)?;

        // 3 of 5 followers disconnect.
        let leader = cluster.check_one_leader()?;
        cluster.disconnect((leader + 1) % num_nodes);
        cluster.disconnect((leader + 2) % num_nodes);
        cluster.disconnect((leader + 3) % num_nodes);

        let server = &cluster.servers[leader as usize];

        // check no agreement can be made.
        let server = Arc::clone(server);
        let timeout = max_election_timeout();
        let res = server.execute_command(vec![0x02], Some(timeout))?;
        assert_eq!(res, CommandResult::Ongoing(2), "should block on index #2");

        std::thread::sleep(max_election_timeout());

        let (m, _) = cluster.napplied(2)?;
        assert_eq!(m, 0, "should have no apply");

        // repair
        cluster.connect((leader + 1) % num_nodes);
        cluster.connect((leader + 2) % num_nodes);
        cluster.connect((leader + 3) % num_nodes);

        let leader = cluster.check_one_leader()?;
        let res = server.execute_command(vec![0x03], Some(timeout))?;
        assert_eq!(
            res,
            CommandResult::Applied { index: 2, result: Ok(vec![0x03].into()) },
            "command should applied at index 2 or 3"
        );

        cluster.one(vec![0x04], num_nodes, false)?;

        cluster.close();

        Ok(())
    }
}
