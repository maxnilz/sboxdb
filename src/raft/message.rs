use serde::{Deserialize, Serialize};

use crate::error::Error;
use crate::error::Result;
use crate::raft::persister::Entry;
use crate::raft::{Index, NodeId, Term};

type ProposalId = Vec<u8>;

/// A message address.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum Address {
    /// Broadcast to all peers. Only valid as an outbound recipient (to).
    Broadcast,
    /// A node with the specified node ID (local or remote). Valid both as
    /// sender and recipient.
    Node(NodeId),
    /// A local dummy address. Can only send ProposeCommand messages, and
    /// receive ProposalDropped/ProposalApplied messages.
    Localhost,
}

/// A message that passed between raft peers
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Message {
    /// The sender address.
    pub from: Address,
    /// The recipient address.
    pub to: Address,
    /// The message payload.
    pub event: Event,
}

impl Address {
    pub fn get_node_id(&self) -> Result<NodeId> {
        match self {
            Self::Node(id) => Ok(*id),
            _ => Err(Error::internal(format!("unwrap called on non-Node address {:?}", self))),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Event {
    AppendEntries {
        // current term
        term: Term,
        // follower can redirect clients
        leader_id: NodeId,
        // leader's commit index
        leader_commit: Index,

        // index of log entry immediately preceding new ones
        prev_log_index: Index,
        // term of prev_log_index entry
        prev_log_term: Term,
        // log entries to store (empty for heartbeat;
        // may send more than one for efficiency)
        entries: Vec<Entry>,

        // for debug purpose
        seq: u64,
    },

    EntriesAccepted,

    EntriesRejected {
        // current term for stall
        // leader to updater itself.
        term: Term,
        // for fast rollback
        xterm: Term,
        xindex: Index,
        xlen: u64,
    },

    RequestVote {
        term: Term,
        // candidate requesting vote.
        candidate: NodeId,
        // index of candidate's last log entry.
        last_log_index: Index,
        // term of candidate's last log entry.
        last_log_term: Term,
    },

    VoteGranted,

    VoteRejected {
        // current term for stall
        // leader to updater itself.
        term: Term,
    },

    /// A command proposal. This can be submitted to the leader, or to a follower
    /// which will forward it to its leader. If there is no leader, or the
    /// leader or term changes, the proposal is dropped and the client must retry.
    /// If a request is accepted, the state machine would receive the accepted
    /// command from the ApplyMsg.
    ProposeCommand {
        id: ProposalId,
        command: Vec<u8>,
    },

    /// A proposal is dropped if there is no leader, or the leader or term changes.
    ProposalDropped {
        /// The proposal id. This matches the id of the ProposeCommand.
        id: ProposalId,
    },

    /// A proposal is applied with response.
    ProposalApplied {
        /// The proposal id. This matches the id of the ProposeCommand.
        id: ProposalId,
        /// The response
        response: Vec<u8>,
    },
}
