use crate::error::Result;
use crate::raft::message::Message;
use crate::raft::node::{rand_election_timeout, Node, NodeState};
use crate::raft::node::{RawNode, Ticks};

pub struct Follower {
    rn: RawNode,
    // accumulate tick the clock ticked, every time we receive
    // a message from a valid leader, reset the tick to 0.
    tick: Ticks,
    // maximum number of tick before triggering an election.
    // i.e., if tick >= timeout, transit to a candidate then
    // fire an election.
    timeout: Ticks,
}

impl Follower {
    pub fn new(raw_node: RawNode) -> Follower {
        Follower { rn: raw_node, tick: 0, timeout: rand_election_timeout() }
    }
}

impl Node for Follower {
    fn tick(self: Box<Self>) -> Result<Box<dyn Node>> {
        Ok(self)
    }

    fn step(self: Box<Self>, msg: Message) -> Result<Box<dyn Node>> {
        Ok(self)
    }

    fn get_state(&self) -> NodeState {
        NodeState::from(&self.rn)
    }
}
