use std::io::Write;
use std::sync::Mutex;

use tokio::sync::oneshot;
use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_stream::StreamExt as _;

use crate::error::{Error, Result};
use crate::raft::message::Message;
use crate::raft::node::follower::Follower;
use crate::raft::node::{Node, NodeState, RawNode, TICK_INTERVAL};
use crate::raft::persister::Persister;
use crate::raft::transport::Transport;
use crate::raft::NodeId;
use crate::storage::state::State;

struct Context {
    tx: broadcast::Sender<()>,
}

impl Context {
    fn new() -> Self {
        let (tx, _) = broadcast::channel(1);
        Self { tx }
    }

    fn close(self) {
        drop(self.tx)
    }

    fn done(&self) -> broadcast::Receiver<()> {
        self.tx.subscribe()
    }
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
    command_rx: mpsc::UnboundedReceiver<(Vec<u8>, oneshot::Sender<Result<Vec<u8>>>)>,
    /// transport act as the exchange for sending
    /// and receiving messages to/from raft peers.
    transport: Box<dyn Transport>,
}

pub struct Server {
    id: NodeId,

    /// channel for query node state, paired with
    /// the state_rx in the eventloop.
    state_tx: mpsc::UnboundedSender<((), oneshot::Sender<NodeState>)>,

    /// channel for client command, paired with
    /// the command_rx in the eventloop.
    command_tx: mpsc::UnboundedSender<(Vec<u8>, oneshot::Sender<Result<Vec<u8>>>)>,

    /// eventloop context, will be consumed by
    /// the eventloop, use mutex here for the
    /// interior mutability across threads.
    context: Mutex<Option<EventLoopContext>>,
}

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
        Ok(Server { id, state_tx, command_tx, context: Mutex::new(Some(context)) })
    }

    pub async fn serve(&self, done: broadcast::Receiver<()>) -> Result<()> {
        let context = self.context.lock().as_mut().unwrap().take().unwrap();
        let eventloop = tokio::spawn(Self::eventloop(context, done));
        eventloop.await?
    }

    /// run the event loop for message processing.
    async fn eventloop(context: EventLoopContext, mut done: broadcast::Receiver<()>) -> Result<()> {
        let id = context.id;
        let mut node = context.node;
        let mut node_rx = UnboundedReceiverStream::new(context.node_rx);

        let mut state_rx = UnboundedReceiverStream::new(context.state_rx);
        let mut command_rx = UnboundedReceiverStream::new(context.command_rx);

        let mut transport = context.transport;
        let mut transport_rx = transport.receiver().await?;

        let mut ticker = tokio::time::interval(TICK_INTERVAL);
        loop {
            tokio::select! {
                _ = done.recv() => {
                    return Ok(())
                }
                _ = ticker.tick() => {
                    node = node.tick()?;
                },

                Some(msg) = transport_rx.next() => {
                    node = node.step(msg)?
                }

                Some(msg) = node_rx.next() => {
                    transport.send(msg).await?
                },

                Some((_, res_tx)) = state_rx.next() => {
                    let ns = node.get_state();
                    if let Err(_) = res_tx.send(ns) {
                        return Err(Error::internal("state response receiver dropped"));
                    }
                }

                Some((command, res_tx)) = command_rx.next() => {
                    if let Err(_) = res_tx.send(Ok(command)) {
                        return Err(Error::internal("command response receiver dropped"));
                    }
                }

            }
        }
    }

    pub fn get_state(&self) -> Result<NodeState> {
        let (res_tx, res_rx) = oneshot::channel();
        if let Err(_) = self.state_tx.send(((), res_tx)) {
            return Err(Error::internal("state channel is closed or dropped"));
        }
        let ns = futures::executor::block_on(res_rx)?;
        Ok(ns)
    }

    pub fn execute_command(&self, command: Vec<u8>) -> Result<Vec<u8>> {
        let (res_tx, res_rx) = oneshot::channel();
        if let Err(_) = self.command_tx.send((command, res_tx)) {
            return Err(Error::internal("command channel is closed or dropped"));
        }
        futures::executor::block_on(res_rx)?
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::future::Future;
    use std::sync::Arc;
    use std::time::Duration;

    use log::error;

    use crate::error::Result;
    use crate::raft::transport::tests::LabNetMesh;
    use crate::storage::state::ApplyMsg;
    use crate::storage::{new_storage, StorageType};

    use super::*;

    #[derive(Debug, Clone)]
    struct KvState {
        messages: Vec<ApplyMsg>,
    }

    impl State for KvState {
        fn apply(&mut self, msg: ApplyMsg) -> Result<Vec<u8>> {
            self.messages.push(msg);
            Ok(vec![])
        }
    }

    fn new_server(id: NodeId, peers: Vec<NodeId>, mesh: &LabNetMesh) -> Result<Server> {
        let ns = id.to_string();
        let storage = new_storage(StorageType::Memory)?;
        let persister = Persister::new(ns, storage)?;
        let transport = Box::new(mesh.get(id)?);
        let state: Box<dyn State> = Box::new(KvState { messages: vec![] });
        let server = Server::new(id, peers, persister, transport, state)?;
        Ok(server)
    }

    struct Cluster {
        nodes: Vec<NodeId>,
        net_mesh: LabNetMesh,
        servers: Arc<HashMap<NodeId, Arc<Server>>>,

        threads: HashMap<NodeId, (broadcast::Sender<()>, std::thread::JoinHandle<()>)>,
    }

    impl Cluster {
        fn new(nodes: Vec<NodeId>) -> Result<Self> {
            let net_mesh = LabNetMesh::new(nodes.clone());
            let mut servers = HashMap::new();
            for &id in nodes.iter() {
                let peers: Vec<_> =
                    nodes.iter().filter_map(|&x| if x == id { None } else { Some(x) }).collect();
                let server = new_server(id, peers, &net_mesh)?;
                servers.insert(id, Arc::new(server));
            }
            let servers = Arc::new(servers);
            Ok(Self { nodes, net_mesh, servers, threads: HashMap::new() })
        }

        fn start_node(&mut self, id: NodeId) {
            let server = self.servers.get(&id).unwrap();
            let server = Arc::clone(server);
            let (tx, rx) = broadcast::channel(1);
            let th = std::thread::spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .thread_name(format!("ev-{}", id))
                    .build()
                    .unwrap();
                rt.block_on(async move {
                    if let Err(err) = server.serve(rx).await {
                        error!("server {} failed {}", id, err)
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

        fn get_num_leader(&self) -> i8 {
            let mut ans = 0;
            for (_, server) in self.servers.iter() {
                let ns = server.get_state().unwrap();
                ans = if ns.is_leader() { ans + 1 } else { ans }
            }
            ans
        }
    }

    #[test]
    fn test_eventloop() -> Result<()> {
        // initialize a three node cluster
        let nodes: Vec<NodeId> = vec![1, 2, 3];
        let mut cluster = Cluster::new(nodes)?;

        // start cluster
        cluster.start();

        // check num of leader
        for i in 0..10 {
            let ans = cluster.get_num_leader();
            println!("found {} leader", ans);
            std::thread::sleep(Duration::from_secs(1));
        }

        // close cluster
        cluster.close();

        Ok(())
    }
}
