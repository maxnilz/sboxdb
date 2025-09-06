use std::collections::HashMap;
use std::collections::HashSet;
use std::ops::Add;
use std::ops::Mul;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;

use log::debug;
use log::error;
use sboxdb::error::Error;
use sboxdb::raft::log::Log;
use sboxdb::raft::node::NodeId;
use sboxdb::raft::node::NodeState;
use sboxdb::raft::node::ELECTION_TIMEOUT_RANGE;
use sboxdb::raft::node::HEARTBEAT_INTERVAL;
use sboxdb::raft::node::ROUND_TRIP_INTERVAL;
use sboxdb::raft::node::TICK_INTERVAL;
use sboxdb::raft::server::Server;
use sboxdb::raft::Command;
use sboxdb::raft::CommandResult;
use sboxdb::raft::Index;
use sboxdb::raft::State;
use sboxdb::raft::Term;
use sboxdb::storage::new_storage;
use sboxdb::storage::StorageType;
use tokio::sync::broadcast;

use crate::raft::state::KvState;
use crate::raft::state::States;
use crate::raft::transport::LabNetMesh;
use crate::raft::transport::Noise;

pub fn max_election_timeout() -> Duration {
    let ticks = ELECTION_TIMEOUT_RANGE.end + HEARTBEAT_INTERVAL;
    TICK_INTERVAL.mul(ticks as u32)
}

fn new_server(id: NodeId, mesh: &LabNetMesh, state: Arc<KvState>) -> sboxdb::error::Result<Server> {
    let storage = new_storage(StorageType::Memory)?;
    let log = Log::new(id, storage)?;
    let transport = Box::new(mesh.get(id)?);
    let state: Box<dyn State> = Box::new(state);
    let server = Server::try_new(log, transport, state)?;
    Ok(server)
}

pub struct Cluster {
    nodes: Vec<NodeId>,
    net_mesh: LabNetMesh,
    states: Arc<States>,
    servers: Vec<Arc<Server>>,

    threads: HashMap<NodeId, (broadcast::Sender<()>, std::thread::JoinHandle<()>)>,
}

impl Cluster {
    pub fn new(n: u8, noise: Option<Noise>) -> sboxdb::error::Result<Self> {
        let mut nodes = Vec::new();
        for i in 0..n {
            // node id is equal to the index of array.
            nodes.push(i as NodeId);
        }
        let states = Arc::new(States::new(n));

        let net_mesh = LabNetMesh::new(nodes.clone(), noise);
        let mut servers = Vec::new();
        for &id in nodes.iter() {
            let states = Arc::clone(&states);
            let state = Arc::new(KvState::new(id, states));
            let server = new_server(id, &net_mesh, Arc::clone(&state))?;
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
            let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
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

    pub fn start(&mut self) {
        for id in self.nodes.clone() {
            self.start_node(id)
        }
    }

    pub fn close(&mut self) {
        for id in self.nodes.clone() {
            self.close_node(id)
        }
    }

    pub fn server(&self, id: NodeId) -> Arc<Server> {
        Arc::clone(&self.servers[id as usize])
    }

    pub fn get_node_state(&self, id: NodeId) -> NodeState {
        let server = &self.servers[id as usize];
        server.get_state().unwrap()
    }

    pub fn check_no_leader(&self) -> sboxdb::error::Result<()> {
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
    pub fn check_one_leader(&self) -> sboxdb::error::Result<NodeId> {
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
    pub fn check_terms(&self) -> sboxdb::error::Result<Term> {
        let mut term = 0;
        for &id in &self.nodes {
            if !self.net_mesh.is_connected(id) {
                continue;
            }
            let server = &self.servers[id as usize];
            let ns = server.get_state().unwrap();
            if ns.leader.is_none() {
                continue; // no leader yet.
            }
            if term == 0 {
                term = ns.term;
                continue;
            }
            if term != ns.term {
                #[rustfmt::skip]
                return Err(Error::internal(format!("servers disagree on term, {}/{}", term, ns.term)));
            }
        }
        Ok(term)
    }

    // how many servers think a log entry is applied at the given index
    pub fn napplied(&self, index: Index) -> sboxdb::error::Result<(u8, Option<Command>)> {
        self.states.napplied(index)
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
    pub fn one(&self, cmd: Vec<u8>, n: u8, retry: bool) -> sboxdb::error::Result<Index> {
        let command = Command::from(cmd);
        let mut ind = 0;
        let tm = SystemTime::now().add(Duration::from_secs(10));
        loop {
            if !self.is_connected(ind as NodeId) {
                ind = (ind + 1) % self.servers.len();
                continue;
            }
            let server = &self.servers[ind];
            // set the agreement timeout to be 3 times of round trip interval
            let timeout = TICK_INTERVAL.mul(3 * ROUND_TRIP_INTERVAL as u32);
            let res = server.execute_command(command.clone(), Some(timeout))?;
            match res {
                CommandResult::Dropped => {
                    // command get dropped by raft in case of election,
                    // sleep a while then continue with another server.
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
                        let msg =
                            format!("failed to reach agreement {} at index {}", command, index);
                        return Err(Error::internal(msg));
                    }
                }
                CommandResult::Applied { index, result } => {
                    if let Err(err) = result {
                        return Err(err);
                    }
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
                        if m >= n && cmd == Some(command.clone()) {
                            return Ok(index);
                        }
                        if SystemTime::now().gt(&until) {
                            break;
                        }
                        std::thread::sleep(Duration::from_millis(50));
                    }

                    #[rustfmt::skip]
                    let msg = format!("failed to reach agreement {} at index {}, {}/{}, {}",
                                          command, index, m, n, cmd.unwrap_or(Command(None)));
                    return Err(Error::internal(msg));
                }
            };

            if SystemTime::now().gt(&tm) {
                break;
            }
        }
        Err(Error::internal(format!("failed to reach agreement {:?}", command)))
    }

    pub fn exec_command(
        &self,
        id: NodeId,
        cmd: Vec<u8>,
        timeout: Option<Duration>,
    ) -> sboxdb::error::Result<CommandResult> {
        self.servers[id as usize].execute_command(cmd.into(), timeout)
    }

    pub fn disconnect(&mut self, id: NodeId) {
        debug!("disconnect {}", id);
        self.net_mesh.disconnect(id).unwrap()
    }

    pub fn connect(&mut self, id: NodeId) {
        debug!("connect {}", id);
        self.net_mesh.connect(id).unwrap()
    }

    fn is_connected(&self, id: NodeId) -> bool {
        self.net_mesh.is_connected(id)
    }
}
