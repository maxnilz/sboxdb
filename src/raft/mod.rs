use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::Arc;

use serde::Deserialize;
use serde::Serialize;

use crate::error::Error;
use crate::error::Result;

pub mod message;
pub mod node;

pub mod server;
pub mod transport;

mod persister;

pub type Index = u64;
pub type Term = u64;
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Command(pub Option<Vec<u8>>);

impl Command {
    pub fn unwrap(self) -> Vec<u8> {
        self.0.unwrap()
    }

    pub fn ok_or(self, err: Error) -> Result<Vec<u8>> {
        self.0.ok_or(err)
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
            for (i, it) in cmd.iter().enumerate() {
                if i == 0 {
                    write!(f, "0x")?;
                }
                write!(f, "{:02x}", it)?;
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
    /// Apply command to the state machine for replication
    /// FIXME: to increase throughput, we need
    ///  1. separate query and mutation from the command, have only the mutation go through
    ///   the raft log and get applied to state machine. for query command, read from leader
    ///   state without go through raft log is enough for linearizability.
    ///  2. support batch apply, async apply...
    ///  3. support command streaming...
    fn apply(&self, msg: ApplyMsg) -> Result<Command> {
        (**self).apply(msg)
    }
}
