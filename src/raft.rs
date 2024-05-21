pub type Index = u64;
pub type Term = u64;
pub type NodeId = u64;
pub type RequestId = Vec<u8>;
mod message;
mod node;
mod persister;
mod server;
mod transport;
