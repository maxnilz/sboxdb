use crate::error::Result;
use crate::raft::message::{Address, Event, Message};
use crate::raft::node::candidate::Candidate;
use crate::raft::node::{rand_election_timeout, Node, NodeState};
use crate::raft::node::{RawNode, Ticks};

macro_rules! log {
    ($rn:expr, $lvl:expr, $($arg:tt)+) => {
        ::log::log!($lvl, "[f{}-{}] {}", $rn.id, $rn.term, format_args!($($arg)+))
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

pub struct Follower {
    rn: RawNode,
    // accumulate tick the clock ticked, every time we receive
    // a message from a valid leader, reset the tick to 0.
    tick: Ticks,
    // maximum number of tick before triggering an election.
    // i.e., if tick >= timeout, transit to a candidate then
    // fire an election.
    timeout: Ticks,
}

impl Follower {
    pub fn new(rn: RawNode) -> Self {
        Self { rn, tick: 0, timeout: rand_election_timeout() }
    }

    fn into_follower(self: Box<Self>) -> Box<Follower> {
        return Box::new(self.rn.into());
    }
}

impl Node for Follower {
    fn tick(mut self: Box<Self>) -> Result<Box<dyn Node>> {
        self.tick += 1;
        if self.tick >= self.timeout {
            let candidate: Candidate = self.rn.try_into()?;
            return Ok(Box::new(candidate));
        }
        Ok(self)
    }

    fn step(mut self: Box<Self>, msg: Message) -> Result<Box<dyn Node>> {
        // receive a stale message, drop it.
        if msg.term > 0 && msg.term < self.rn.term {
            debug!(self.rn, "dropping stale message {:?}", msg);
            return Ok(self);
        }

        // found a higher term, could be the following cases:
        //  1. a new leader if the message is AppendEntries(empty
        //     AppendEntries implies heartbeat).
        //  2. or a vote request with higher term,
        //  3. otherwise, ignore it.
        // Here we yield ourselves to a leaderless follower to catch
        // up the new term and step the message with the new term-ed
        // follower to follow the potential new leader or deal with
        // the vote request.
        if msg.term > self.rn.term {
            debug!(self.rn, "found higher term message {:?}", msg);
            return self.rn.into_leaderless_follower(msg.term, msg);
        }

        match msg.event {
            Event::AppendEntries(ae) => {
                self.rn.leader = Some(ae.leader_id);

                // stay as follower by reset the election timer
                return Ok(self.into_follower());
            }
            // As a follower, ignore any resp of AppendEntries
            // which should only valid if we are still leader.
            Event::EntriesAccepted => {}
            Event::EntriesRejected { .. } => {}

            // Found a vote request indicate that a candidate
            // is requesting vote for the give term in msg.
            Event::RequestVote(req) => {
                assert_eq!(self.rn.term, msg.term);
                let candidate = req.candidate;

                // if we've voted for someone else, reject it.
                if let Some(voted_for) = self.rn.voted_for {
                    if voted_for != candidate {
                        debug!(self.rn, "vote rejected for {}, voted for {}", candidate, voted_for);
                        let message = Message {
                            term: self.rn.term,
                            from: Address::Node(self.rn.id),
                            to: msg.from,
                            event: Event::VoteRejected,
                        };
                        self.rn.node_tx.send(message)?;
                        return Ok(self);
                    }
                }

                // check if the candidate is up-to-date.
                let (last_index, last_term) = self.rn.persister.last();
                let is_up_to_date = req.last_log_term > last_term
                    || (req.last_log_term == last_term && req.last_log_index >= last_index);
                if !is_up_to_date {
                    debug!(self.rn, "vote rejected for {}, not up-to-date", candidate);
                    let message = Message {
                        term: self.rn.term,
                        from: Address::Node(self.rn.id),
                        to: msg.from,
                        event: Event::VoteRejected,
                    };
                    self.rn.node_tx.send(message)?;
                    return Ok(self);
                }

                // vote for the up-to-date candidate
                info!(self.rn, "vote granted for {}", candidate);
                self.rn.save_hard_state(msg.term, Some(candidate))?;
                let message = Message {
                    term: self.rn.term,
                    from: Address::Node(self.rn.id),
                    to: msg.from,
                    event: Event::VoteGranted,
                };
                self.rn.node_tx.send(message)?;

                // stay as follower by reset the election timer
                return Ok(self.into_follower());
            }

            // As a follower, ignore any resp of RequestVote
            // which should valid if we are still candidate.
            Event::VoteGranted => {}
            Event::VoteRejected { .. } => {}

            // follower should forward the proposed command to
            // leader if there is one, otherwise reply with
            // ProposalDropped.
            Event::ProposeCommand { .. } => {
                todo!()
            }

            // receive ProposalDropped from leader, forward it
            // to the command sender.
            Event::ProposalDropped { .. } => {
                todo!()
            }

            // receive ProposalApplied from leader, forward it
            // to the command sender.
            Event::ProposalApplied { .. } => {
                todo!()
            }
        }
        Ok(self)
    }

    fn get_state(&self) -> NodeState {
        NodeState::from(&self.rn)
    }
}

impl From<RawNode> for Follower {
    fn from(rn: RawNode) -> Self {
        Follower::new(rn)
    }
}
