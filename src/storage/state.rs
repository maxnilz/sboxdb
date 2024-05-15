use crate::error::Result;
use crate::raft::Index;

// A message that passed to raft to state machine
// over the apply channel.
pub struct ApplyMsg {
    index: Index,
    command: Vec<u8>,
}

pub trait State: Send + Sync {
    fn apply(&mut self, msg: ApplyMsg) -> Result<Vec<u8>>;
}
