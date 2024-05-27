use std::collections::HashMap;
use std::net::IpAddr;
use std::time::Duration;

use async_trait::async_trait;
use futures::SinkExt;
use log::{debug, error};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream, UnboundedReceiverStream};
use tokio_stream::Stream;
use tokio_stream::StreamExt as _;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use crate::error::Error;
use crate::error::Result;
use crate::raft::message::{Address, Message};
use crate::raft::NodeId;

// Transport act as the message exchange(send, receive) among
// raft peers.
#[async_trait]
pub trait Transport: Send {
    // The Unpin constraint ensures that the stream type
    // can be safely moved after being boxed.
    async fn receiver(&self) -> Result<Box<dyn Stream<Item = Message> + Unpin + Send>>;
    async fn send(&mut self, message: Message) -> Result<()>;
}

struct TcpTransport {
    me: (NodeId, IpAddr),
    peers: HashMap<NodeId, IpAddr>,

    /// outbound message first get buffered into
    /// channel via the sender, and the underlying
    /// socket client that manage connect/reconnect
    /// to raft peers will read the buffered message
    /// and send them to wire.
    txs: HashMap<NodeId, mpsc::Sender<Message>>,
}

impl TcpTransport {
    pub fn new(me: (NodeId, IpAddr), peers: HashMap<NodeId, IpAddr>) -> Result<Self> {
        let mut txs: HashMap<NodeId, mpsc::Sender<Message>> = HashMap::new();
        for (id, addr) in peers.clone().into_iter() {
            let (tx, rx) = mpsc::channel::<Message>(10000);
            txs.insert(id, tx);
            // TODO: close connection gracefully
            tokio::spawn(Self::connect(addr, rx));
        }

        Ok(Self { me, peers, txs })
    }

    /// Accept inbound TCP connection continuously.
    async fn serve(listener: TcpListener, tx: mpsc::UnboundedSender<Message>) -> Result<()> {
        let mut listener = TcpListenerStream::new(listener);
        while let Some(socket) = listener.try_next().await? {
            let peer = socket.peer_addr()?;
            let tx = tx.clone();
            tokio::spawn(async move {
                debug!("raft peer {} connected", peer);
                match Self::recv_msg(socket, tx).await {
                    Ok(()) => debug!("raft peer {} disconnected", peer),
                    Err(err) => error!("raft peer {} error: {}", peer, err.to_string()),
                }
            });
        }
        Ok(())
    }
    /// Receives inbound messages from a peer via TCP.
    async fn recv_msg(socket: TcpStream, tx: mpsc::UnboundedSender<Message>) -> Result<()> {
        let mut stream = tokio_serde::SymmetricallyFramed::<_, Message, _>::new(
            Framed::new(socket, LengthDelimitedCodec::new()),
            tokio_serde::formats::SymmetricalBincode::<Message>::default(),
        );
        while let Some(message) = stream.try_next().await? {
            tx.send(message)?;
        }
        Ok(())
    }

    /// Connects to a peer for sending outbound message, continuously reconnecting.
    async fn connect(addr: IpAddr, rx: mpsc::Receiver<Message>) -> Result<()> {
        let mut rx = ReceiverStream::new(rx);
        loop {
            match TcpStream::connect(addr.to_string()).await {
                Ok(socket) => {
                    debug!("connected to raft peer {}", addr);
                    match Self::send_msg(socket, &mut rx).await {
                        Ok(_) => break,
                        Err(err) => error!("failed sending to raft peer {}: {}", addr, err),
                    }
                }
                Err(err) => error!("failed connecting to raft peer {}: {}", addr, err),
            }
            tokio::time::sleep(Duration::from_millis(1000)).await
        }
        debug!("disconnected from raft peer {}", addr);
        Ok(())
    }

    /// Sends outbound messages to a peer via a TCP connection.
    async fn send_msg(socket: TcpStream, rx: &mut ReceiverStream<Message>) -> Result<()> {
        let mut stream = tokio_serde::SymmetricallyFramed::<_, Message, _>::new(
            Framed::new(socket, LengthDelimitedCodec::new()),
            tokio_serde::formats::SymmetricalBincode::<Message>::default(),
        );
        while let Some(message) = rx.next().await {
            stream.send(message).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl Transport for TcpTransport {
    async fn receiver(&self) -> Result<Box<dyn Stream<Item = Message> + Unpin + Send>> {
        let (_, addr) = self.me;
        let listener = TcpListener::bind(addr.to_string()).await?;
        let (tx, rx) = mpsc::unbounded_channel();

        // TODO: close connection gracefully
        let _ = tokio::spawn(async move {
            match Self::serve(listener, tx).await {
                Ok(()) => debug!("raft receiver {} stopped", addr),
                Err(err) => error!("raft receiver {} error: {}", addr, err),
            }
        });

        let rx = UnboundedReceiverStream::new(rx);
        Ok(Box::new(rx))
    }

    async fn send(&mut self, message: Message) -> Result<()> {
        let to = match message.to {
            Address::Broadcast => self.txs.keys().copied().collect(),
            Address::Node(peer) => vec![peer],
            Address::Localhost => {
                return Err(Error::internal("outbound message to a local address"))
            }
        };
        for id in to {
            match self.txs.get_mut(&id) {
                None => {
                    error!("sending outbound message to a unknown peer {}", id)
                }
                Some(tx) => match tx.try_send(message.clone()) {
                    Ok(_) => {}
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        debug!("full send buffer for peer {}, discarding message", id)
                    }
                    Err(err) => return Err(err.into()),
                },
            }
        }
        Ok(())
    }
}

#[cfg(test)]
pub mod tests {
    use std::collections::HashSet;
    use std::ops::Range;
    use std::os::linux::raw::stat;
    use std::sync::{Arc, RwLock};

    use async_trait::async_trait;
    use futures::Stream;
    use rand::Rng;
    use tokio::sync::Mutex;

    use crate::error::Error;
    use crate::error::Result;

    use super::*;

    pub struct LabTransport {
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
                Address::Broadcast => {
                    self.peers.iter().map(|(id, tx)| (*id, Arc::clone(tx))).collect()
                }
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
                        // drop message quietly.
                    }
                    SocketState::Unstable((loss, delay)) => {
                        // mimic the packet loss ratio
                        let rng = rand::thread_rng().gen_range(0..100);
                        if rng < loss {
                            // drop message quietly.
                            continue;
                        }

                        // mimic the package delay
                        let rng = rand::thread_rng()
                            .gen_range(delay.start.as_millis()..delay.end.as_millis());
                        tokio::time::sleep(Duration::from_millis(rng as u64)).await;

                        tx.send(message.clone()).await?
                    }
                }
            }
            Ok(())
        }
    }

    #[derive(Debug, Clone)]
    enum SocketState {
        Connected,
        Disconnected,
        // the wire between two peers are connected but unstable
        // with the given the packet loss ratio and range of delay.
        Unstable((u8, Range<Duration>)),
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
        // assuming node id is equal to the index of array.
        connected: Vec<bool>,

        txs: HashMap<NodeId, Arc<mpsc::Sender<Message>>>,
        rxs: HashMap<NodeId, Arc<Mutex<mpsc::Receiver<Message>>>>,

        net: Arc<LabNet>,
    }

    impl LabNetMesh {
        pub fn new(nodes: Vec<NodeId>) -> Self {
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
                    if from == to {
                        continue;
                    }
                    // init the socket state with connected.
                    sockets.insert((from, to), SocketState::Connected);
                }
            }

            // connect all node by initially
            let mut connected = Vec::new();
            for i in 0..nodes.len() {
                connected.push(true);
            }

            let net = LabNet { sockets: Arc::new(RwLock::new(sockets)) };
            let net = Arc::new(net);
            Self { nodes, connected, txs, rxs, net }
        }

        pub fn get(&self, id: NodeId) -> Result<LabTransport> {
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

        pub fn disconnect(&mut self, id: NodeId) -> Result<()> {
            // disconnect outgoing
            for &to in &self.nodes {
                self.net.set(id, to, SocketState::Disconnected)?;
            }
            // disconnect incoming
            for &from in &self.nodes {
                self.net.set(from, id, SocketState::Disconnected)?;
            }
            // update connected state
            // assuming node id is equal to the index of array.
            self.connected[id as usize] = false;
            Ok(())
        }

        pub fn connect(&mut self, id: NodeId) -> Result<()> {
            // disconnect outgoing
            for &to in &self.nodes {
                self.net.set(id, to, SocketState::Connected)?;
            }
            // disconnect incoming
            for &from in &self.nodes {
                self.net.set(from, id, SocketState::Connected)?;
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
}
