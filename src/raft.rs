use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::error::Result;

pub type Index = u64;
pub type Term = u64;
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Command(pub Option<Vec<u8>>);

impl Command {
    pub fn unwrap(self) -> Vec<u8> {
        self.0.unwrap()
    }
}

impl From<Vec<u8>> for Command {
    fn from(cmd: Vec<u8>) -> Self {
        Self(Some(cmd))
    }
}

impl Display for Command {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(cmd) = &self.0 {
            for i in 0..cmd.len() {
                if i == 0 {
                    write!(f, "0x")?;
                }
                write!(f, "{:02x}", cmd[i])?;
            }
            return Ok(());
        }
        write!(f, "None")
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CommandResult {
    Dropped,
    Ongoing(Index),
    Applied { index: Index, result: Result<Command> },
}

// A message that passed from raft to state machine
// over the apply channel.
#[derive(Debug, Clone)]
pub struct ApplyMsg {
    pub index: Index,
    pub command: Command,
}

pub trait State: Debug + Send {
    fn apply(&self, msg: ApplyMsg) -> Result<Command>;
}

impl<T: State + Send + Sync> State for Arc<T> {
    fn apply(&self, msg: ApplyMsg) -> Result<Command> {
        (**self).apply(msg)
    }
}

mod persister;

pub mod message;
pub mod node;

pub mod server;
pub mod transport;
