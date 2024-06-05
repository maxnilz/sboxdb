use std::fmt::Debug;

use crate::error::Result;
use crate::raft::{Command, Index};

// A message that passed from raft to state machine
// over the apply channel.
#[derive(Debug, Clone)]
pub struct ApplyMsg {
    pub index: Index,
    pub command: Command,
}

pub trait State: Debug + Send {
    fn apply(&mut self, msg: ApplyMsg) -> Result<Command>;
}
