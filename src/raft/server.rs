use std::cell::RefCell;
use std::collections::HashMap;
use std::time::Duration;

use log::debug;
use tokio::sync::oneshot;
use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_stream::StreamExt as _;
use uuid::Uuid;

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
                    let id: ProposalId = Uuid::new_v4().as_bytes().to_vec();
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
        let req = Request { command: Some(command), timeout, tx };
        if let Err(_) = self.command_tx.send(req) {
            return Err(Error::internal("command channel is closed or dropped"));
        }
        Ok(futures::executor::block_on(rx)?)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::ops::Mul;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use log::{debug, error};
    use rand::{thread_rng, Rng};

    use crate::error::Result;
    use crate::raft::node::{ELECTION_TIMEOUT_RANGE, HEARTBEAT_INTERVAL};
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

        // how many servers think a log entry is committed at the given index
        fn ncommitted(&self, index: Index) -> Result<(i32, Option<Command>)> {
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
                        return Err(Error::internal(format!("committed values do not match: index {}, {:?}, {:?}", index, *c, cmd)));
                    }
                }
                n += 1;
                ans = Some(cmd);
            }
            Ok((n, ans))
        }

        fn one(&self, command: Command, m: i32, retry: bool) -> Result<Index> {
            for _ in 0..10 {
                // wait at lease max election timeout so that
                // we will have at least one election.
                std::thread::sleep(max_election_timeout());

                // try to send a command to a server as a client,
                // if we get Error::Abort error back, we need to retry.
                for &id in &self.nodes {}
            }
            todo!()
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
    fn test_initial_election() -> Result<()> {
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
    fn test_re_election() -> Result<()> {
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
    fn test_many_election() -> Result<()> {
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
}
