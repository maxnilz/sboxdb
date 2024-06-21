use std::collections::HashMap;
use std::ops::Add;
use std::time::{Duration, SystemTime};

use crate::error::{Error, Result};
use crate::raft::message::{Address, AppendEntries, Event, Message, ProposalResult};
use crate::raft::node::{Node, NodeId, NodeState, ProposalId, MAX_NODE_ID};
use crate::raft::node::{RawNode, Ticks, HEARTBEAT_INTERVAL};
use crate::raft::ApplyMsg;
use crate::raft::{Index, Term};

struct Ticket {
    from: Address,
    id: ProposalId,
    expire_at: SystemTime,
}

pub struct Leader {
    rn: RawNode,

    // accumulate tick the clock ticked, every time we
    // send a message, reset to zero.
    tick: Ticks,

    // maximum number of tick before triggering a heartbeat.
    // i.e., if tick >= timeout, send a heartbeat message, i.e.,
    // an empty AppendEntries message.
    timeout: Ticks,

    // volatile state on leader
    // reinitialized after election
    //
    // for each peer, index of next log entry to send to that
    // peer, initialized to leader last log index + 1.
    // peer NodeId as array index.
    next_index: Vec<Index>,
    // for each peer, index of highest log entry known to be replicated
    // on the peer, initialized to 0, increases monotonically.
    // peer NodeId as array index.
    match_index: Vec<Index>,
    // tickets for the proposals that are in progress, once the proposal
    // at the given index is considered as commited & applied to state
    // machine, we use the indexed ticket to send the command apply result
    // back to the client.
    // these should be dropped when leadership get lost.
    tickets: HashMap<Index, Ticket>,
}

impl Leader {
    pub fn new(mut rn: RawNode) -> Self {
        // init index array
        let (last_index, _) = rn.persister.last();
        let next_index = vec![last_index + 1; MAX_NODE_ID as usize];
        let match_index = vec![0; MAX_NODE_ID as usize];
        // set leader to myself
        rn.leader = Some(rn.id);
        let proposals = HashMap::new();
        Self {
            rn,
            tick: 0,
            timeout: HEARTBEAT_INTERVAL,
            next_index,
            match_index,
            tickets: proposals,
        }
    }

    fn heartbeat(&self) -> Result<()> {
        let seq = self.rn.seq.get() + 1;

        let (last_index, last_term) = self.rn.persister.last();
        let req = AppendEntries {
            leader_id: self.rn.id,
            leader_commit: self.rn.commit_index,
            prev_log_index: last_index,
            prev_log_term: last_term,
            entries: vec![],
            seq,
        };
        self.rn.send_message(Address::Broadcast, Event::AppendEntries(req))?;

        debug!(self.rn, "send heartbeat, seq: {}", seq);

        self.rn.seq.set(seq);

        Ok(())
    }

    pub fn into_follower(self, term: Term, leader: Option<NodeId>) -> Result<Box<dyn Node>> {
        // drop any pending proposals that is waiting response
        for (_, ticket) in self.tickets {
            let event = Event::ProposalResponse { id: ticket.id, result: ProposalResult::Dropped };
            self.rn.send_message(ticket.from, event)?;
        }
        // transit to follower.
        self.rn.into_follower(term, leader)
    }

    fn may_send_append_entries(&self, peer: NodeId) -> Result<()> {
        let seq = self.rn.seq.get() + 1;

        // TODO: we are reading/sending the same data again and again
        //  in case of log synchronization, this might be too much overhead.

        // gather entries to be appended to peer
        let from = self.next_index[peer as usize];
        let entries = self.rn.persister.scan_from(from)?;
        let num_entries = entries.len();
        if num_entries == 0 {
            // if there is no entries to be sent to
            // the peer, issue a heartbeat instead.
            self.heartbeat()?;
            return Ok(());
        }

        // get the prev index and term
        let entry = self.rn.persister.get_entry(from - 1)?;
        let (prev_log_index, prev_log_term) =
            if let Some(entry) = entry { (entry.index, entry.term) } else { (0, 0) };

        let req = AppendEntries {
            leader_id: self.rn.id,
            leader_commit: self.rn.commit_index,
            prev_log_index,
            prev_log_term,
            entries,
            seq,
        };
        self.rn.send_message(Address::Node(peer), Event::AppendEntries(req))?;

        debug!(self.rn, "send append entries to {}, seq: {}, #entries: {}", peer, seq, num_entries);

        self.rn.seq.set(seq);

        Ok(())
    }

    fn maybe_commit_and_apply(&mut self) -> Result<()> {
        // quorum index is the index of highest log entry
        // known to be replicated to majority peers, i.e.,
        // the new commit index.
        let (quorum_index, match_index) = if self.rn.peers.is_empty() {
            // we are a single node cluster, set quorum_index
            // to the last index.
            let (last_index, _) = self.rn.persister.last();
            (last_index, vec![])
        } else {
            let mut match_index = vec![];
            for &peer in &self.rn.peers {
                match_index.push(self.match_index[peer as usize]);
            }
            let n = self.rn.quorum_size() - 1;
            // the k index in the array, from left to right in desc order.
            let k = n - 1;
            match_index.select_nth_unstable_by(k, |a, b| b.cmp(a));
            (match_index[k], match_index)
        };

        #[rustfmt::skip]
        debug!(self.rn, "match_index: {:?}, c/q: {}/{}", match_index, self.rn.commit_index, quorum_index);

        // if there is no entries are considered as committed
        // by comparing with the latest commit index, do nothing.
        // quorum_index maybe less than commit_index, because
        // match_index can be reset if the role of a node transit
        // as: leader -> follower -> leader, in which case, the
        // match_index will need wait at least majority peers
        // echo back to be in sync properly.
        if quorum_index <= self.rn.commit_index {
            return Ok(());
        }

        // update the commit index(implies the entries are committed)
        self.rn.commit_index = quorum_index;

        // apply committed entries
        let from = self.rn.last_applied + 1;
        let to = self.rn.commit_index + 1;
        let entries = self.rn.persister.scan_entries(from, to)?;

        info!(self.rn, "applying entries [{}, {}), {}", from, to, entries.len());

        for entry in entries {
            let (index, command) = (entry.index, entry.command);
            let msg = ApplyMsg { index, command };
            let result = self.rn.state.apply(msg);
            self.rn.last_applied += 1;
            if let Some(ticket) = self.tickets.remove(&index) {
                let (id, result) = (ticket.id, ProposalResult::Applied { index, result });
                self.rn.send_message(ticket.from, Event::ProposalResponse { id, result })?;
            }
        }

        Ok(())
    }

    fn check_expiring_proposal(&mut self) -> Result<()> {
        let now = SystemTime::now();

        // TODO:  speed up the expiration check.
        let indexes: Vec<_> = self
            .tickets
            .iter()
            .filter_map(
                |(&index, ticket)| {
                    if ticket.expire_at.le(&now) {
                        Some(index)
                    } else {
                        None
                    }
                },
            )
            .collect();

        for index in indexes {
            if let Some(ticket) = self.tickets.remove(&index) {
                let (id, result) = (ticket.id, ProposalResult::Ongoing(index));
                self.rn.send_message(ticket.from, Event::ProposalResponse { id, result })?;
            }
        }
        Ok(())
    }
}

impl Node for Leader {
    fn tick(mut self: Box<Self>) -> Result<Box<dyn Node>> {
        self.tick += 1;
        if self.tick > self.timeout {
            self.heartbeat()?;
            self.tick = 0;
        }
        self.check_expiring_proposal()?;
        Ok(self)
    }

    fn step(mut self: Box<Self>, msg: Message) -> Result<Box<dyn Node>> {
        debug!(self.rn, "recv msg: {}", msg);

        // receive a stale message, drop it.
        if msg.term > 0 && msg.term < self.rn.term {
            debug!(self.rn, "drop stale msg");
            return Ok(self);
        }

        // found a higher term, yield to follower and process
        // the message there.
        if msg.term > self.rn.term {
            info!(self.rn, "become leaderless follower, caused by higher term");
            return self.into_follower(msg.term, None)?.step(msg);
        }

        match msg.event {
            Event::EntriesAccepted(index) => {
                let peer = msg.from.unwrap_node_id();

                let ind = peer as usize;
                self.next_index[ind] = index + 1;
                self.match_index[ind] = index;
                self.maybe_commit_and_apply()?;

                return Ok(self);
            }

            Event::EntriesRejected { xindex } => {
                let peer = msg.from.unwrap_node_id();

                let ind = peer as usize;
                self.next_index[ind] = xindex;
                self.may_send_append_entries(peer)?;

                return Ok(self);
            }

            // receive a stale vote request where the voting term is same as
            // the current leader term(this might because there are two candidate
            // are trying to use the same term for election, but by the time the
            // slower candidate's vote request hit the faster one, the faster one
            // got the majority votes already and become leader). although ignore
            // it is okay, but we reject it explicitly here for debug purpose.
            Event::RequestVote(_) => {
                self.rn.send_message(msg.from, Event::VoteRejected)?;
            }

            // receive a slow vote grant response, however we've
            // already get majority vote and become leader, ignore it.
            Event::VoteGranted => {}

            // receive a slow vote rejection response, however we've
            // already get majority vote and become leader, ignore it.
            Event::VoteRejected => {}

            Event::ProposalRequest { id, command, timeout } => {
                let index = self.rn.persister.append(self.rn.term, command)?;
                if self.rn.peers.is_empty() {
                    // we are a single node cluster, no
                    // need to do any replication.
                    self.maybe_commit_and_apply()?;
                    return Ok(self);
                }

                // send append entries to the peers
                for &peer in &self.rn.peers {
                    self.may_send_append_entries(peer)?;
                }

                // check the timeout duration, start from the infinity(say 1000 days from now)
                let now = SystemTime::now();
                let mut expire_at = now.add(Duration::from_days(1000));
                if let Some(timeout) = timeout {
                    if timeout.is_zero() {
                        let result = ProposalResult::Ongoing(index);
                        self.rn.send_message(msg.from, Event::ProposalResponse { id, result })?;
                        return Ok(self);
                    }
                    expire_at = now.add(timeout);
                }

                // save the ongoing proposal as ticket so that we can send
                // the apply result back to the client.
                self.tickets.insert(index, Ticket { from: msg.from, id, expire_at });

                return Ok(self);
            }

            // As a leader, we should not receive any of
            // the following messages.
            Event::AppendEntries(_) => {}
            Event::ProposalResponse { .. } => {}
        }
        Ok(self)
    }

    fn get_state(&self) -> NodeState {
        NodeState::from(&self.rn)
    }
}

impl TryFrom<RawNode> for Leader {
    type Error = Error;

    fn try_from(rn: RawNode) -> Result<Leader> {
        let leader = Leader::new(rn);
        info!(leader.rn, "become leader");
        leader.heartbeat()?;
        Ok(leader)
    }
}
