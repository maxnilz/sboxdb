use std::collections::HashSet;

use crate::error::{Error, Result};
use crate::raft::message::{Address, Event, Message, ProposalResult, RequestVote};
use crate::raft::node::leader::Leader;
use crate::raft::node::NodeId;
use crate::raft::node::{rand_election_timeout, RawNode, Ticks};
use crate::raft::node::{Node, NodeState};

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

        info!(self.rn, "requesting vote as candidate");

        let (last_index, last_term) = self.rn.persister.last();
        let req = RequestVote { candidate: voted_for.unwrap(), last_index, last_term };
        self.rn.send_message(Address::Broadcast, Event::RequestVote(req))?;

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
        debug!(self.rn, "receive message: {}", msg);

        // receive a stale message, drop it.
        if msg.term < self.rn.term {
            debug!(self.rn, "drop stale msg");
            match msg.event {
                // drop any command proposal explicitly
                Event::ProposalRequest { id, .. } => {
                    let event = Event::ProposalResponse { id, result: ProposalResult::Dropped };
                    self.rn.send_message(msg.from, event)?;
                }
                // drop any other message quietly.
                _ => {}
            }
            return Ok(self);
        }

        // whenever we receive a message from higher term, transit
        // to follower, and process the message with follower.
        if msg.term > self.rn.term {
            info!(self.rn, "become leaderless follower, caused by higher term");
            return self.rn.into_follower(msg.term, None)?.step(msg);
        }

        match msg.event {
            // received a granted vote. record it, and if we
            // have votes from majority, transit to leader and
            // send an empty append entry as a heartbeat to
            // declare the leadership immediately.
            Event::VoteGranted => {
                self.votes.insert(msg.from.unwrap_node_id());

                let votes: Vec<NodeId> = self.votes.iter().map(|&x| x).collect();
                info!(self.rn, "receive yes vote from {}, votes: {:?}", msg.from, votes);

                let granted_votes = self.votes.len() as u8;
                if granted_votes >= self.rn.quorum_size() {
                    // save hard state before the transition.
                    let term = self.rn.term;
                    let voted_for = None;
                    self.rn.save_hard_state(term, voted_for)?;
                    // transit to leader
                    let leader: Leader = self.rn.try_into()?;
                    return Ok(Box::new(leader));
                }
            }

            // received a vote rejection, log it and do nothing.
            Event::VoteRejected { .. } => {
                debug!(self.rn, "receive reject vote from {}", msg.from);
            }

            // reject any vote request while we are
            // requesting vote as a candidate.
            Event::RequestVote(_) => {
                self.rn.send_message(msg.from, Event::VoteRejected)?;
            }

            // an append entries MUST send by leader,
            // whenever we receive an append entry, it means
            // there is a leader we can talk to, so, we should
            // step down as a follower.
            Event::AppendEntries(ref ae) => {
                assert_eq!(self.rn.term, msg.term);
                info!(self.rn, "become follower, leader: {}, caused by AE", ae.leader_id);
                return self.rn.into_follower(msg.term, Some(ae.leader_id))?.step(msg);
            }

            // drop any command proposal while we are candidate.
            Event::ProposalRequest { id, .. } => {
                let event = Event::ProposalResponse { id, result: ProposalResult::Dropped };
                self.rn.send_message(msg.from, event)?;
            }

            // As a candidate, we should not receive any of
            // the following messages.
            Event::EntriesAccepted(_)
            | Event::EntriesRejected { .. }
            | Event::ProposalResponse { .. } => {
                error!(self.rn, "received unexpected message {}", msg)
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
