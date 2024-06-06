use crate::error::Result;

pub type Index = u64;
pub type Term = u64;
pub type Command = Option<Vec<u8>>;
#[derive(Clone, PartialEq)]
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
