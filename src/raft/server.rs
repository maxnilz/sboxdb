use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_stream::StreamExt as _;

use crate::error::{Error, Result};
use crate::raft::message::Message;
use crate::raft::node::follower::Follower;
use crate::raft::node::{Node, RawNode, TICK_INTERVAL};
use crate::raft::persister::Persister;
use crate::raft::transport::Transport;
use crate::raft::NodeId;
use crate::storage::state::State;

pub struct Server {
    node: Box<dyn Node + Send + Sync>,

    // receive channel for receiving in-process
    // message from node.
    node_rx: mpsc::UnboundedReceiver<Message>,

    // transport act as the exchange for sending
    // and receiving messages among raft peers.
    transport: Box<dyn Transport + Send + Sync>,
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
        let raw_node = RawNode::new(id, peers, persister, node_tx, state)?;
        // create server as follower at the very beginning.
        let follower = Follower::new(raw_node);
        let node: Box<dyn Node> = Box::new(follower);
        Ok(Server { node, node_rx, transport })
    }

    pub async fn serve(
        self,
        client_rx: mpsc::UnboundedReceiver<(Vec<u8>, oneshot::Sender<Vec<u8>>)>,
    ) -> Result<()> {
        let eventloop =
            tokio::spawn(Self::eventloop(self.node, self.transport, self.node_rx, client_rx));
        tokio::try_join!(eventloop)?;
        Ok(())
    }

    // run the event loop for message processing.
    async fn eventloop(
        mut node: Box<dyn Node + Send>,
        mut transport: Box<dyn Transport + Send>,
        node_rx: mpsc::UnboundedReceiver<Message>,
        client_rx: mpsc::UnboundedReceiver<(Vec<u8>, oneshot::Sender<Vec<u8>>)>,
    ) -> Result<()> {
        let mut node_rx = UnboundedReceiverStream::new(node_rx);
        let mut client_rx = UnboundedReceiverStream::new(client_rx);
        let mut transport_rx = transport.receiver();

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
                    transport.send(msg).await?
                },

                Some((command, res_tx)) = client_rx.next() => {
                    res_tx.send(command).map_err(|e| {
                        Error::Internal(format!("Fail to send response {:?}", e))
                    })?
                }
            }
        }
    }
}
