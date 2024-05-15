use crate::error::Result;
use crate::raft::message::Message;
use crate::raft::node::Node;
use crate::raft::node::RawNode;
use crate::storage::Storage;

pub struct Leader {
    rn: RawNode,
}

impl Leader {}

impl Node for Leader {
    fn tick(self: Box<Self>) -> Result<Box<dyn Node + Send + Sync>> {
        todo!()
    }

    fn step(self: Box<Self>, msg: Message) -> Result<Box<dyn Node + Send + Sync>> {
        todo!()
    }
}
