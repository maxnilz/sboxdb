use crate::error::Result;
use crate::raft::message::Message;
use crate::raft::node::Node;
use crate::raft::node::RawNode;
use crate::storage::Storage;

pub struct Candidate {
    rn: RawNode,
}

impl Candidate {}

impl Node for Candidate {
    fn tick(self: Box<Self>) -> Result<Box<dyn Node + Send + Sync>> {
        todo!()
    }

    fn step(self: Box<Self>, msg: Message) -> Result<Box<dyn Node + Send + Sync>> {
        todo!()
    }
}
