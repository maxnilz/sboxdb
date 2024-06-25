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
use crate::raft::State;
use crate::raft::{Command, CommandResult};
use crate::storage::kv::KvStorage;

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

/// A Raft server
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
    /// Create a Raft server,
    pub fn new(
        id: NodeId,
        peers: Vec<NodeId>,
        storage: Box<dyn KvStorage>,
        transport: Box<dyn Transport>,
        state: Box<dyn State>,
    ) -> Result<Server> {
        let persister = Persister::new(id, storage)?;
        let (node_tx, node_rx) = mpsc::unbounded_channel();
        let rn = RawNode::new(id, peers, persister, node_tx, state)?;
        // init server as follower at the very beginning.
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
                        _ => return Err(Error::internal(format!("unexpected message to localhost {}", msg)))
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
        command: Command,
        timeout: Option<Duration>,
    ) -> Result<CommandResult> {
        let (tx, rx) = oneshot::channel();
        let req = Request { command, timeout, tx };
        if let Err(_) = self.command_tx.send(req) {
            return Err(Error::internal("command channel is closed or dropped"));
        }
        Ok(futures::executor::block_on(rx)?)
    }
}
