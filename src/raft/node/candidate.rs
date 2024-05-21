use crate::error::Result;
use crate::raft::message::Message;
use crate::raft::node::RawNode;
use crate::raft::node::{Node, NodeState};

pub struct Candidate {
    rn: RawNode,
}

impl Candidate {}

impl Node for Candidate {
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
