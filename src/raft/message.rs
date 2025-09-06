use std::fmt::Display;
use std::fmt::Formatter;
use std::time::Duration;

use serde::Deserialize;
use serde::Serialize;

use super::log::Entry;
use super::node::NodeId;
use super::node::ProposalId;
use super::Command;
use super::CommandResult;
use super::Index;
use super::Term;
use crate::error::Result;

/// A message that passed between raft peers
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Message {
    /// The current term of the sender. Must be set, unless the sender is
    /// Address::Localhost, in which case it must be 0.
    pub term: Term,
    /// The sender address.
    pub from: Address,
    /// The recipient address.
    pub to: Address,
    /// The message payload.
    pub event: Event,
}

impl Display for Message {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{{} -> {}, term: {}, {}}}", self.from, self.to, self.term, self.event)
    }
}

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

impl Address {
    pub fn unwrap_node_id(&self) -> NodeId {
        match self {
            Self::Node(id) => *id,
            _ => panic!("unwrap called on non-Node address {:?}", self),
        }
    }
}

impl Display for Address {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Address::Broadcast => {
                write!(f, "broadcast")
            }
            Address::Node(id) => {
                write!(f, "{}", id)
            }
            Address::Localhost => {
                write!(f, "localhost")
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RequestVote {
    // candidate requesting vote.
    pub candidate: NodeId,
    // index of candidate's last log entry.
    pub last_index: Index,
    // term of candidate's last log entry.
    pub last_term: Term,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AppendEntries {
    // follower can redirect clients
    pub leader_id: NodeId,
    // leader's commit index
    pub leader_commit: Index,

    // index of log entry immediately preceding new ones
    pub prev_log_index: Index,
    // term of prev_log_index entry
    pub prev_log_term: Term,
    // log entries to store (empty for heartbeat;
    // may send more than one for efficiency)
    pub entries: Vec<Entry>,

    // for debug purpose
    pub seq: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Event {
    AppendEntries(AppendEntries),

    EntriesAccepted(Index),

    EntriesRejected {
        xindex: Index,
    },

    RequestVote(RequestVote),

    VoteGranted,

    VoteRejected,

    /// A command proposal. This can be submitted to the leader, or to a follower
    /// which will forward it to its leader. If there is no leader, or the
    /// leader or term changes, the proposal is dropped and the client must retry.
    /// If a request is accepted, the state machine would receive the accepted
    /// command from the ApplyMsg.
    ProposalRequest {
        id: ProposalId,
        command: Command,
        /// the amount time to wait before returning to the client, None means
        /// no timeout at all, otherwise respect the given duration.
        /// If the command is not applied yet before the timeout duration, we
        /// will return the log index to the client, otherwise return the ApplyResp.
        timeout: Option<Duration>,
    },

    /// A proposal response.
    ProposalResponse {
        /// The proposal id. This matches the id of the ProposeCommand.
        id: ProposalId,
        /// The result return to the client, it may carry the application result
        /// generated from the state machine if the command is commited and applied
        /// to the state machine before timeout, otherwise return the log index, or
        /// the proposal get dropped if there is no leader or the leader/term changed.
        result: ProposalResult,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ProposalResult {
    /// A proposal is dropped if there is no leader, or the leader or term changes.
    Dropped,
    /// A proposal is in progress if it is not applied yet before timeout.
    Ongoing(Index),
    /// A proposal is applied to the state machine, the state machine may raise error
    /// or generate a result command with the given request command.
    Applied { index: Index, result: Result<Command> },
}

impl Display for ProposalResult {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ProposalResult::Dropped => {
                write!(f, "Dropped")
            }
            ProposalResult::Ongoing(index) => {
                write!(f, "Ongoing({})", index)
            }
            ProposalResult::Applied { index, result } => {
                write!(f, "Applied{{index:{}", index)?;
                match result {
                    Ok(cmd) => {
                        write!(f, ", Ok:{}}}", cmd)
                    }
                    Err(err) => {
                        write! {f, ", Err:{}}}", err}
                    }
                }
            }
        }
    }
}

impl From<ProposalResult> for CommandResult {
    fn from(value: ProposalResult) -> Self {
        match value {
            ProposalResult::Dropped => CommandResult::Dropped,
            ProposalResult::Ongoing(index) => CommandResult::Ongoing(index),
            ProposalResult::Applied { index, result } => CommandResult::Applied { index, result },
        }
    }
}

impl Display for Event {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Event::AppendEntries(ae) => {
                #[rustfmt::skip]
                write!(f, "AppendEntries: {} prev:{}/{} len:{} c:{}",
                       ae.seq, ae.prev_log_index, ae.prev_log_term, ae.entries.len(), ae.leader_commit)
            }
            Event::EntriesAccepted(index) => {
                write!(f, "EntriesAccepted: match_index:{}", index)
            }
            Event::EntriesRejected { xindex } => {
                write!(f, "EntriesRejected: xindex:{}", xindex)
            }
            Event::RequestVote(r) => {
                write!(f, "RequestVote: last:{}/{}", r.last_index, r.last_term)
            }
            Event::VoteGranted => {
                write!(f, "VoteGranted")
            }
            Event::VoteRejected => {
                write!(f, "VoteRejected")
            }
            Event::ProposalRequest { id, command, .. } => {
                write!(f, "PropReq: {} {}", id, command)
            }
            Event::ProposalResponse { id, result } => {
                write!(f, "PropRsp: {} {}", id, result)
            }
        }
    }
}
