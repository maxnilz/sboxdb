use crate::error::Result;
use crate::raft::message::Message;
use crate::raft::node::RawNode;
use crate::raft::node::{Node, NodeState};

pub struct Leader {
    rn: RawNode,
}

impl Leader {}

impl Node for Leader {
    fn tick(self: Box<Self>) -> Result<Box<dyn Node>> {
        todo!()
    }

    fn step(self: Box<Self>, msg: Message) -> Result<Box<dyn Node>> {
        todo!()
    }

    fn get_state(&self) -> NodeState {
        NodeState::from(&self.rn)
    }
}
