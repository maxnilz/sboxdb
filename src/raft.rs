pub type Index = u64;
pub type Term = u64;
pub type Command = Option<Vec<u8>>;
pub enum CommandResult {
    Dropped,
    Ongoing(Index),
    Applied { index: Index, res: crate::error::Result<Command> },
}

mod message;
mod node;
mod persister;
mod server;
mod transport;
