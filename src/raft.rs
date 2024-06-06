use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

use crate::error::Result;

pub type Index = u64;
pub type Term = u64;
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Command(Option<Vec<u8>>);

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

mod message;
mod node;
mod persister;
mod server;
mod transport;
