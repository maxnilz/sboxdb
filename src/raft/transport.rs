use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use async_trait::async_trait;
use futures::SinkExt;
use log::debug;
use log::error;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_stream::Stream;
use tokio_stream::StreamExt as _;
use tokio_util::codec::Framed;
use tokio_util::codec::LengthDelimitedCodec;

use super::message::Address;
use super::message::Message;
use super::node::NodeId;
use crate::error::Error;
use crate::error::Result;

/// Transport act as the message exchange(send, receive) among
/// raft peers.
#[async_trait]
pub trait Transport: Send {
    fn me(&self) -> (NodeId, SocketAddr);
    /// Inspect the transport nodes topology info, returns
    /// my node id and peers nodes id.
    fn topology(&self) -> (NodeId, Vec<NodeId>);
    /// The Unpin constraint ensures that the stream type
    /// can be safely moved after being boxed.
    async fn receiver(&self) -> Result<Box<dyn Stream<Item = Message> + Unpin + Send>>;
    /// Sends message to given address in message
    async fn send(&mut self, message: Message) -> Result<()>;
}

pub struct TcpTransport {
    me: (NodeId, SocketAddr),
    peers: HashMap<NodeId, SocketAddr>,

    /// outbound message first get buffered into
    /// channel via the sender, and the underlying
    /// socket client that manage connect/reconnect
    /// to raft peers will read the buffered message
    /// and send them to wire.
    txs: HashMap<NodeId, mpsc::Sender<Message>>,
}

impl TcpTransport {
    pub fn new(me: (NodeId, SocketAddr), peers: HashMap<NodeId, SocketAddr>) -> Result<Self> {
        let mut txs: HashMap<NodeId, mpsc::Sender<Message>> = HashMap::new();
        for (id, addr) in peers.clone().into_iter() {
            let (tx, rx) = mpsc::channel::<Message>(10000);
            txs.insert(id, tx);
            // TODO: close connection gracefully
            tokio::spawn(Self::connect(addr, rx));
        }

        Ok(Self { me, peers, txs })
    }

    /// Connects to a peer for sending outbound message, continuously reconnecting.
    async fn connect(addr: SocketAddr, rx: mpsc::Receiver<Message>) -> Result<()> {
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
}

#[async_trait]
impl Transport for TcpTransport {
    fn me(&self) -> (NodeId, SocketAddr) {
        self.me
    }

    fn topology(&self) -> (NodeId, Vec<NodeId>) {
        (self.me.0, self.peers.iter().map(|(node_id, _)| *node_id).collect())
    }

    async fn receiver(&self) -> Result<Box<dyn Stream<Item = Message> + Unpin + Send>> {
        let (_, addr) = self.me;
        let listener = TcpListener::bind(addr.to_string()).await?;
        let (tx, rx) = mpsc::unbounded_channel();

        // TODO: close connection gracefully
        tokio::spawn(async move {
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
