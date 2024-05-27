use std::collections::HashSet;

use crate::error::{Error, Result};
use crate::raft::message::{Address, Event, Message, RequestVote};
use crate::raft::node::leader::Leader;
use crate::raft::node::{rand_election_timeout, RawNode, Ticks};
use crate::raft::node::{Node, NodeState};
use crate::raft::NodeId;

macro_rules! log {
    ($rn:expr, $lvl:expr, $($arg:tt)+) => {
        ::log::log!($lvl, "[c{}-{}] {}", $rn.id, $rn.term, format_args!($($arg)+))
    };
}

macro_rules! debug {
    ($rn:expr, $($arg:tt)+) => {
        log!($rn, ::log::Level::Debug, $($arg)+)
    };
}

macro_rules! info {
    ($rn:expr, $($arg:tt)+) => {
        log!($rn, ::log::Level::Info, $($arg)+)
    };
}

macro_rules! error {
    ($rn:expr, $($arg:tt)+) => {
        log!($rn, ::log::Level::Error, $($arg)+)
    };
}

pub struct Candidate {
    rn: RawNode,

    // accumulate tick the clock ticked.
    tick: Ticks,

    // maximum number of tick before triggering an election.
    // i.e., if tick >= timeout, fire a re-election.
    timeout: Ticks,

    votes: HashSet<NodeId>,
}

impl Candidate {
    pub fn new(mut rn: RawNode) -> Self {
        rn.leader = None;
        Self { rn, tick: 0, timeout: rand_election_timeout(), votes: HashSet::new() }
    }

    fn campaign(&mut self) -> Result<()> {
        let (term, voted_for) = (self.rn.term + 1, Some(self.rn.id));
        self.rn.save_hard_state(term, voted_for)?;

        self.votes.insert(voted_for.unwrap());

        let (log_index, log_term) = self.rn.persister.last();
        let message = Message {
            term,
            from: Address::Node(self.rn.id),
            to: Address::Broadcast,
            event: Event::RequestVote(RequestVote {
                candidate: voted_for.unwrap(),
                last_log_index: log_index,
                last_log_term: log_term,
            }),
        };
        self.rn.node_tx.send(message)?;

        Ok(())
    }
}

impl Node for Candidate {
    fn tick(mut self: Box<Self>) -> Result<Box<dyn Node>> {
        self.tick += 1;
        if self.tick >= self.timeout {
            let candidate: Candidate = self.rn.try_into()?;
            return Ok(Box::new(candidate));
        };
        Ok(self)
    }

    fn step(mut self: Box<Self>, msg: Message) -> Result<Box<dyn Node>> {
        // receive a stale message, drop it.
        if msg.term < self.rn.term {
            debug!(self.rn, "dropping stale message {:?}", msg);
            match msg.event {
                // drop any command proposal explicitly
                Event::ProposeCommand { id, .. } => {
                    let message = Message {
                        term: self.rn.term,
                        from: Address::Node(self.rn.id),
                        to: msg.from,
                        event: Event::ProposalDropped { id },
                    };
                    self.rn.node_tx.send(message)?;
                }
                // drop any other message quietly.
                _ => {}
            }
            return Ok(self);
        }

        // whenever we receive a message from higher term, transit
        // to follower, and process the message with follower.
        if msg.term > self.rn.term {
            debug!(self.rn, "found higher term message {:?}", msg);
            return self.rn.into_leaderless_follower(msg.term, msg);
        }

        match msg.event {
            // received a granted vote. record it, and if we
            // have votes from majority, transit to leader and
            // send an empty append entry as a heartbeat to
            // declare the leadership immediately.
            Event::VoteGranted => {
                debug!(self.rn, "{:?} granted vote", msg.from);
                self.votes.insert(msg.from.unwrap_node_id());

                let granted_votes = self.votes.len() as u8;
                if granted_votes >= self.rn.quorum_size() {
                    // save hard state before the transition.
                    let term = self.rn.term;
                    let voted_for = None;
                    self.rn.save_hard_state(term, voted_for)?;
                    // transit to leader
                    info!(self.rn, "elected as leader");
                    let leader: Leader = self.rn.try_into()?;
                    return Ok(Box::new(leader));
                }
            }

            // received a vote rejection, log it and do nothing.
            Event::VoteRejected { .. } => {
                info!(self.rn, "{:?} rejected vote", msg.from);
            }

            // reject any vote request while we are
            // requesting vote as a candidate.
            Event::RequestVote(_) => {
                let message = Message {
                    term: self.rn.term,
                    from: Address::Node(self.rn.id),
                    to: msg.from,
                    event: Event::VoteRejected,
                };
                self.rn.node_tx.send(message)?;
            }

            // an append entries MUST send by leader,
            // whenever we receive an append entry, it means
            // there is a leader we can talk to, so, we should
            // step down as a follower.
            Event::AppendEntries(_) => {
                assert_eq!(self.rn.term, msg.term);
                return self.rn.into_leaderless_follower(msg.term, msg);
            }

            // drop any command proposal while we are candidate.
            Event::ProposeCommand { id, .. } => {
                let message = Message {
                    term: self.rn.term,
                    from: Address::Node(self.rn.id),
                    to: msg.to,
                    event: Event::ProposalDropped { id },
                };
                self.rn.node_tx.send(message)?;
            }

            // As a candidate, we should not receive any of
            // the following messages.
            Event::EntriesAccepted
            | Event::EntriesRejected { .. }
            | Event::ProposalDropped { .. }
            | Event::ProposalApplied { .. } => {
                error!(self.rn, "received unexpected message {:?}", msg)
            }
        };

        Ok(self)
    }

    fn get_state(&self) -> NodeState {
        NodeState::from(&self.rn)
    }
}

impl TryFrom<RawNode> for Candidate {
    type Error = Error;

    fn try_from(rn: RawNode) -> Result<Candidate> {
        let mut candidate = Candidate::new(rn);
        candidate.campaign()?;
        Ok(candidate)
    }
}
