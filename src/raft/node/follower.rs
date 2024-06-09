use std::cmp::min;
use std::collections::HashMap;

use crate::error::Result;
use crate::raft::message::{Address, AppendEntries, Event, Message, ProposalResult, RequestVote};
use crate::raft::node::candidate::Candidate;
use crate::raft::node::leader::Leader;
use crate::raft::node::{rand_election_timeout, Node, NodeState, ProposalId};
use crate::raft::node::{RawNode, Ticks};
use crate::raft::Index;
use crate::storage::state::ApplyMsg;

pub struct Follower {
    rn: RawNode,
    // accumulate tick the clock ticked, every time we receive
    // a message from a valid leader, reset the tick to 0.
    tick: Ticks,
    // maximum number of tick before triggering an election.
    // i.e., if tick >= timeout, transit to a candidate then
    // fire an election.
    timeout: Ticks,
    // proposals that have been forwarded to the leader and waiting for
    // response, these should be dropped when leader/term changes.
    forwarded: HashMap<ProposalId, Address>,
}

impl Follower {
    pub fn new(rn: RawNode) -> Self {
        let forwarded = HashMap::new();
        Self { rn, tick: 0, timeout: rand_election_timeout(), forwarded }
    }

    fn reset(mut self: Box<Self>) -> Box<Self> {
        debug!(self.rn, "reset election timer");

        self.tick = 0;
        self.timeout = rand_election_timeout();

        return self;
    }

    fn drop_forwarded(&mut self) -> Result<()> {
        for (id, peer) in std::mem::take(&mut self.forwarded) {
            let result = ProposalResult::Dropped;
            self.rn.send_message(peer, Event::ProposalResponse { id, result })?;
        }
        Ok(())
    }

    fn is_appendable(&self, ae: &AppendEntries) -> Result<(bool, Option<Index>)> {
        if ae.prev_log_index == 0 {
            return Ok((true, None));
        }
        let (last_index, _) = self.rn.persister.last();

        // follower's log is too short
        if ae.prev_log_index > last_index {
            return Ok((false, Some(last_index + 1)));
        }
        let entry = self.rn.persister.get_entry(ae.prev_log_index)?.unwrap();
        let prev_term = entry.term;
        if prev_term != ae.prev_log_term {
            let mut xindex = ae.prev_log_index;
            for index in (1..ae.prev_log_index).rev() {
                let entry = self.rn.persister.get_entry(index)?.unwrap();
                if entry.term == ae.prev_log_term {
                    // the last entry of the predecessor
                    // term of the conflicting term
                    break;
                }
                xindex = index;
            }
            return Ok((false, Some(xindex)));
        }
        return Ok((true, None));
    }

    fn is_votable(&self, req: &RequestVote) -> Result<bool> {
        let candidate = req.candidate;

        // if we've voted for someone else, reject it.
        if let Some(voted_for) = self.rn.voted_for {
            if voted_for != candidate {
                info!(self.rn, "reject vote for {}, voted for {}", candidate, voted_for);
                return Ok(false);
            }
        }

        // check if the candidate is up-to-date.
        let (last_index, last_term) = self.rn.persister.last();
        let is_upto_date = req.last_term > last_term
            || (req.last_term == last_term && req.last_index >= last_index);
        if !is_upto_date {
            info!(self.rn, "reject vote for {}, last:{}/{}", candidate, last_index, last_term);
            return Ok(false);
        }

        Ok(true)
    }

    fn maybe_apply_entries(&mut self) -> Result<()> {
        let from = self.rn.last_applied + 1;
        let to = self.rn.commit_index + 1;
        assert!(from < to);
        let entries = self.rn.persister.scan_entries(from, to)?;
        info!(self.rn, "applying entries [{}, {}), {}", from, to, entries.len());
        for entry in entries {
            let msg = ApplyMsg { index: entry.index, command: entry.command };
            self.rn.state.apply(msg)?;
            self.rn.last_applied += 1;
        }
        Ok(())
    }
}

impl Node for Follower {
    fn tick(mut self: Box<Self>) -> Result<Box<dyn Node>> {
        if self.rn.peers.is_empty() {
            // single node cluster, transit to leader directly.
            //
            // save hard state before the transition.
            let (term, voted_for) = (self.rn.term + 1, None);
            self.rn.save_hard_state(term, voted_for)?;
            // transit to leader
            let leader: Leader = self.rn.try_into()?;
            return Ok(Box::new(leader));
        }
        self.tick += 1;
        if self.tick >= self.timeout {
            self.drop_forwarded()?;
            let candidate: Candidate = self.rn.try_into()?;
            return Ok(Box::new(candidate));
        }
        Ok(self)
    }

    fn step(mut self: Box<Self>, msg: Message) -> Result<Box<dyn Node>> {
        debug!(self.rn, "recv msg: {}", msg);

        // receive a stale message, drop it.
        if msg.term > 0 && msg.term < self.rn.term {
            debug!(self.rn, "drop stale msg");
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
            info!(self.rn, "become leaderless follower, caused by higher term");
            self.drop_forwarded()?;
            return self.rn.into_follower(msg.term, None)?.step(msg);
        }

        match msg.event {
            Event::AppendEntries(ae) => {
                self.rn.leader = Some(ae.leader_id);

                let (ok, xindex) = self.is_appendable(&ae)?;
                if !ok {
                    // xindex is guaranteed to be valid in case of !ok.
                    let xindex = xindex.unwrap();

                    debug!(self.rn, "reject entries from {}, xindex: {}", msg.from, xindex);
                    self.rn.send_message(msg.from, Event::EntriesRejected { xindex })?;

                    // although we are reject to append entries,
                    // but it is a message from valid leader. So,
                    // stay as follower by reset the election timer.
                    return Ok(self.reset());
                }

                // we have a valid append entry request onwards.

                let (last_index0, _) = self.rn.persister.last();
                // keep the log entries upto prev_log_index(inclusive),
                // discard any entries from prev_log_index+1(inclusive).
                //
                // e.g., if the follower was a leader previously, it had
                // some uncommitted commands were append to the log(because
                // of network partition, it can't assemble majority), later,
                // A new leader with a shorter index shows up after the network
                // recover, we should discard those entries.
                let discard = self.rn.persister.remove_from(ae.prev_log_index + 1)?;

                // append entries if any
                let num = ae.entries.len();
                for entry in ae.entries {
                    self.rn.persister.append(entry.term, entry.command)?;
                }

                let (last_index, _) = self.rn.persister.last();

                #[rustfmt::skip]
                info!(self.rn, "accept {} entries from {}, discard: {}/{}, last_index: {}, c: {}/{}",
                    num, msg.from, discard, last_index0, last_index, ae.leader_commit, self.rn.commit_index);

                // check if we have entries need to apply to state machine.
                assert_eq!(ae.leader_commit >= self.rn.commit_index, true);
                if ae.leader_commit > self.rn.commit_index {
                    // update commit index
                    let commit_index = min(ae.leader_commit, last_index);
                    self.rn.commit_index = commit_index;

                    // since the commit index is changed, we may have entries
                    // need to apply to state machine.
                    self.maybe_apply_entries()?;
                }

                self.rn.send_message(msg.from, Event::EntriesAccepted(last_index))?;

                // stay as follower by reset the election timer
                return Ok(self.reset());
            }

            // As a follower, ignore any resp of AppendEntries
            // which should only valid if we are still leader.
            Event::EntriesAccepted(_) => {}
            Event::EntriesRejected { .. } => {}

            // Found a vote request indicate that a candidate
            // is requesting vote for the give term in msg.
            Event::RequestVote(req) => {
                // we should catch up the voting term by step into a
                // leaderless follower, so, assert the term must be
                // equals to the voting term in message.
                assert_eq!(self.rn.term, msg.term);

                let ok = self.is_votable(&req)?;
                if !ok {
                    self.rn.send_message(msg.from, Event::VoteRejected)?;
                    // stay as follower and leave the timer running.
                    return Ok(self);
                }

                // vote for the up-to-date candidate
                let candidate = req.candidate;
                info!(self.rn, "grant vote to {}", candidate);
                self.rn.save_hard_state(msg.term, Some(candidate))?;
                self.rn.send_message(msg.from, Event::VoteGranted)?;

                // stay as follower by reset the election timer
                return Ok(self.reset());
            }

            // As a follower, ignore any resp of RequestVote
            // which should valid if we are still candidate.
            Event::VoteGranted => {}
            Event::VoteRejected { .. } => {}

            // follower should forward the proposed command to
            // leader if there is one, otherwise reply with
            // ProposalDropped.
            Event::ProposalRequest { id, command, timeout } => {
                let leader = self.rn.leader;
                // no leader yet, drop the proposal.
                if leader.is_none() {
                    let (id, result) = (id.clone(), ProposalResult::Dropped);
                    self.rn.send_message(msg.from, Event::ProposalResponse { id, result })?;

                    // stay as follower and leave the timer running.
                    return Ok(self);
                }

                // we have a leader, forward the proposal
                let peer = Address::Node(leader.unwrap());
                let event = Event::ProposalRequest { id: id.clone(), command, timeout };
                self.rn.send_message(peer, event)?;
                // save the origin info for sending response back.
                self.forwarded.insert(id, msg.from);

                // stay as follower and leave the timer running.
                return Ok(self);
            }

            // receive ProposalResponse from leader, forward it
            // to the command sender.
            Event::ProposalResponse { id, result } => {
                if let Some(peer) = self.forwarded.remove(&id) {
                    self.rn.send_message(peer, Event::ProposalResponse { id, result })?;
                }
                // stay as follower by reset the election timer
                return Ok(self.reset());
            }
        }
        Ok(self)
    }

    fn get_state(&self) -> NodeState {
        NodeState::from(&self.rn)
    }
}
