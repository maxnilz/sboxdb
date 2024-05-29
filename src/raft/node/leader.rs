use crate::error::{Error, Result};
use crate::raft::message::{Address, AppendEntries, Event, Message};
use crate::raft::node::{Node, NodeState};
use crate::raft::node::{RawNode, Ticks, HEARTBEAT_INTERVAL};
use crate::raft::Index;

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
    next_index: Vec<Index>,
    // for each peer, index of highest log entry known to be replicated
    // on the peer, initialized to 0, increases monotonically
    match_index: Vec<Index>,
}

impl Leader {
    pub fn new(mut rn: RawNode) -> Self {
        // init index array
        let (log_index, _) = rn.persister.last();
        let mut next_index = vec![];
        let mut match_index = vec![];
        for _ in 0..rn.peers.len() {
            next_index.push(log_index);
            match_index.push(0);
        }
        // set leader to myself
        rn.leader = Some(rn.id);
        Self { rn, tick: 0, timeout: HEARTBEAT_INTERVAL, next_index, match_index }
    }

    pub fn heartbeat(&mut self) -> Result<()> {
        let seq = self.rn.seq + 1;

        let (log_index, log_term) = self.rn.persister.last();
        let message = Message {
            term: self.rn.term,
            from: Address::Node(self.rn.id),
            to: Address::Broadcast,
            event: Event::AppendEntries(AppendEntries {
                leader_id: self.rn.id,
                leader_commit: self.rn.commit_index,
                prev_log_index: log_index,
                prev_log_term: log_term,
                entries: vec![],
                seq,
            }),
        };
        self.rn.node_tx.send(message)?;

        debug!(self.rn, "send heartbeat, seq: {}", seq);

        self.rn.seq = seq;

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
        Ok(self)
    }

    fn step(self: Box<Self>, msg: Message) -> Result<Box<dyn Node>> {
        debug!(self.rn, "receive message: {}", msg);

        // receive a stale message, drop it.
        if msg.term > 0 && msg.term < self.rn.term {
            debug!(self.rn, "drop stale msg");
            return Ok(self);
        }

        // found a higher term, yield to follower and process
        // the message there.
        if msg.term > self.rn.term {
            info!(self.rn, "become leaderless follower, caused by higher term");
            return self.rn.into_follower(msg.term, None)?.step(msg);
        }

        match msg.event {
            Event::EntriesAccepted => {
                todo!()
            }
            Event::EntriesRejected { .. } => {
                todo!()
            }

            // receive a stale vote request where the voting term is same as
            // the current leader term(this might because there are two candidate
            // are trying to use the same term for election, but by the time the
            // slower candidate's vote request hit the faster one, the faster one
            // got the majority votes already and become leader). although ignore
            // it is okay, but we reject it explicitly here for debug purpose.
            Event::RequestVote(_) => {
                let message = Message {
                    term: self.rn.term,
                    from: Address::Node(self.rn.id),
                    to: msg.from,
                    event: Event::VoteRejected,
                };
                self.rn.node_tx.send(message)?;
            }

            // receive a slow vote grant response, however we've
            // already get majority vote and become leader, ignore it.
            Event::VoteGranted => {}

            // receive a slow vote rejection response, however we've
            // already get majority vote and become leader, ignore it.
            Event::VoteRejected => {}

            Event::ProposeCommand { .. } => {
                todo!()
            }

            // As a leader, we should not receive any of
            // the following messages.
            Event::AppendEntries(_) => {}
            Event::ProposalDropped { .. } => {}
            Event::ProposalApplied { .. } => {}
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
        let mut leader = Leader::new(rn);
        debug!(leader.rn, "become leader");
        leader.heartbeat()?;
        Ok(leader)
    }
}
