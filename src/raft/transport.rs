use async_trait::async_trait;
use futures::Stream;

use crate::error::Result;
use crate::raft::message::Message;

// Transport act as the message exchange(send, receive) among
// raft peers.
#[async_trait]
pub trait Transport: Send + Sync {
    // The Unpin constraint ensures that the stream type
    // can be safely moved after being boxed.
    fn receiver(&self) -> Box<dyn Stream<Item = Message> + Unpin + Send>;
    async fn send(&self, message: Message) -> Result<()>;
}

struct TcpTransport {}

#[async_trait]
impl Transport for TcpTransport {
    fn receiver(&self) -> Box<dyn Stream<Item = Message> + Unpin + Send> {
        todo!()
    }

    async fn send(&self, message: Message) -> Result<()> {
        todo!()
    }
}
