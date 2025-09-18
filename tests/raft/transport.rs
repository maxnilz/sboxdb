use std::collections::HashMap;
use std::net::SocketAddr;
use std::ops::Range;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;

use async_trait::async_trait;
use futures::Stream;
use log::debug;
use log::error;
use rand::Rng;
use sboxdb::error::Error;
use sboxdb::error::Result;
use sboxdb::raft::message::Address;
use sboxdb::raft::message::Message;
use sboxdb::raft::node::NodeId;
use sboxdb::raft::transport::Transport;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio_stream::wrappers::ReceiverStream;

pub struct LabTransport {
    #[allow(unused)]
    id: NodeId,

    me: Arc<Mutex<mpsc::Receiver<Message>>>,
    peers: HashMap<NodeId, Arc<mpsc::Sender<Message>>>,

    net: Arc<LabNet>,
}

impl LabTransport {
    fn new(
        id: NodeId,
        me: Arc<Mutex<mpsc::Receiver<Message>>>,
        peers: HashMap<NodeId, Arc<mpsc::Sender<Message>>>,
        net: Arc<LabNet>,
    ) -> Self {
        Self { id, me, peers, net }
    }
}

#[async_trait]
impl Transport for LabTransport {
    fn me(&self) -> (NodeId, SocketAddr) {
        (self.id, SocketAddr::from_str("0.0.0.0:0").unwrap())
    }

    fn topology(&self) -> (NodeId, Vec<NodeId>) {
        (self.id, self.peers.iter().map(|(&node_id, _)| node_id).collect())
    }

    async fn receiver(&self) -> Result<Box<dyn Stream<Item = Message> + Unpin + Send>> {
        let (tx, rx) = mpsc::channel(100);
        let me = Arc::clone(&self.me);
        tokio::spawn(async move {
            loop {
                let mut guard = me.lock().await;
                let message = guard.recv().await;
                if let Some(msg) = message {
                    match tx.send(msg).await {
                        Ok(_) => {}
                        Err(err) => {
                            error!("failed sending inbound message {}", err)
                        }
                    }
                }
            }
        });
        Ok(Box::new(ReceiverStream::new(rx)))
    }

    async fn send(&mut self, message: Message) -> Result<()> {
        let from = message.from.unwrap_node_id();
        let txs: HashMap<_, _> = match message.to {
            Address::Broadcast => self.peers.iter().map(|(id, tx)| (*id, Arc::clone(tx))).collect(),
            Address::Node(id) => match self.peers.get(&id) {
                None => {
                    error!("sending outbound message to a unknown peer {}", id);
                    HashMap::new()
                }
                Some(tx) => HashMap::from_iter(vec![(id, Arc::clone(tx))]),
            },
            Address::Localhost => {
                return Err(Error::internal("outbound message to a local address"))
            }
        };
        for (id, tx) in txs {
            let state = self.net.get(from, id)?;
            match state {
                SocketState::Connected => tx.send(message.clone()).await?,
                SocketState::Disconnected => {
                    debug!("connection from {} to {} are disconnected", from, id)
                    // drop message quietly.
                }
                SocketState::Unstable(noise) => {
                    // mimic the packet loss ratio
                    let rng = rand::thread_rng().gen_range(0..100);
                    if rng < noise.loss_ratio {
                        // drop message quietly.
                        continue;
                    }

                    // mimic the package delay
                    let rng = rand::thread_rng().gen_range(noise.delay_ms);
                    tokio::time::sleep(Duration::from_millis(rng)).await;

                    tx.send(message.clone()).await?
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct Noise {
    // package loss ratio in percentage: [0, 100]
    loss_ratio: u8,
    delay_ms: Range<u64>,
}

impl Noise {
    pub fn new(loss_ratio: u8, delay_ms: Range<u64>) -> Self {
        Self { loss_ratio, delay_ms }
    }
}

#[derive(Debug, Clone)]
enum SocketState {
    Connected,
    Disconnected,
    // the wire between two peers are connected but unstable
    // with the given the packet loss ratio and range of delay.
    Unstable(Noise),
}

impl SocketState {
    fn from_noise(noise: Option<Noise>) -> SocketState {
        if noise.is_none() {
            SocketState::Connected
        } else {
            SocketState::Unstable(noise.clone().unwrap())
        }
    }
}

struct LabNet {
    sockets: Arc<RwLock<HashMap<(NodeId, NodeId), SocketState>>>,
}

impl LabNet {
    pub fn get(&self, from: NodeId, to: NodeId) -> Result<SocketState> {
        let r = self.sockets.read()?;
        match r.get(&(from, to)) {
            None => Ok(SocketState::Disconnected),
            Some(state) => Ok(state.clone()),
        }
    }

    fn set(&self, from: NodeId, to: NodeId, state: SocketState) -> Result<()> {
        let mut w = self.sockets.write()?;
        if let Some(value) = w.get_mut(&(from, to)) {
            *value = state
        }
        Ok(())
    }
}

pub struct LabNetMesh {
    nodes: Vec<NodeId>,
    // noise setup
    noise: Option<Noise>,
    // assuming node id is equal to the index of array.
    connected: Vec<bool>,

    txs: HashMap<NodeId, Arc<mpsc::Sender<Message>>>,
    rxs: HashMap<NodeId, Arc<Mutex<mpsc::Receiver<Message>>>>,

    net: Arc<LabNet>,
}

impl LabNetMesh {
    pub fn new(nodes: Vec<NodeId>, noise: Option<Noise>) -> Self {
        let mut rxs = HashMap::new();
        let mut txs = HashMap::new();
        for &id in &nodes {
            let (tx, rx) = mpsc::channel(100);
            txs.insert(id, Arc::new(tx));
            rxs.insert(id, Arc::new(Mutex::new(rx)));
        }
        let mut sockets = HashMap::new();
        for &from in &nodes {
            for &to in &nodes {
                // init the socket state with connected or being noisy.
                let state = SocketState::from_noise(noise.clone());
                sockets.insert((from, to), state);
            }
        }

        // connect all node by initially
        let connected = vec![true; nodes.len()];

        let net = LabNet { sockets: Arc::new(RwLock::new(sockets)) };
        let net = Arc::new(net);
        Self { nodes, noise, connected, txs, rxs, net }
    }

    pub fn get(&self, id: NodeId) -> sboxdb::error::Result<LabTransport> {
        let rx = self.rxs.get(&id).ok_or(Error::internal(format!("node {} not found", id)))?;
        let peers: HashMap<_, _> = self
            .txs
            .iter()
            .flat_map(
                |(node_id, tx)| {
                    if *node_id == id {
                        None
                    } else {
                        Some((*node_id, Arc::clone(tx)))
                    }
                },
            )
            .collect();
        Ok(LabTransport::new(id, Arc::clone(rx), peers, Arc::clone(&self.net)))
    }

    pub fn disconnect(&mut self, id: NodeId) -> sboxdb::error::Result<()> {
        for &peer in &self.nodes {
            // disconnect outgoing
            self.net.set(id, peer, SocketState::Disconnected)?;
            // disconnect incoming
            self.net.set(peer, id, SocketState::Disconnected)?;
        }
        // update connected state
        // assuming node id is equal to the index of array.
        self.connected[id as usize] = false;
        Ok(())
    }

    pub fn connect(&mut self, id: NodeId) -> sboxdb::error::Result<()> {
        // get connecting state with the mesh noise setup
        let state = SocketState::from_noise(self.noise.clone());
        for &peer in &self.nodes {
            if id != peer && self.connected[peer as usize] == false {
                continue;
            }
            // connect outgoing
            self.net.set(id, peer, state.clone())?;
            // connect incoming
            self.net.set(peer, id, state.clone())?;
        }
        // update connected state
        // assuming node id is equal to the index of array.
        self.connected[id as usize] = true;
        Ok(())
    }

    pub fn is_connected(&self, id: NodeId) -> bool {
        self.connected[id as usize]
    }
}
