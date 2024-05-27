use std::time::Duration;

use rand::Rng;
use tokio::sync::mpsc;

use crate::error::Result;
use crate::raft::message::Message;
use crate::raft::node::follower::Follower;
use crate::raft::persister::{HardState, Persister};
use crate::raft::{Index, NodeId, Term};
use crate::storage::state::State;

pub mod candidate;
pub mod follower;
pub mod leader;

// A logical clock interval as number of ticks.
pub type Ticks = u8;

// The interval between Raft ticks, the unit of time for e.g. heartbeats and
// elections. consider it as a round trip between two peers.
pub const TICK_INTERVAL: Duration = Duration::from_millis(100);

// The interval between leader heartbeats, in ticks. i.e., 300ms if TICK_INTERVAL
// is 100ms.
pub const HEARTBEAT_INTERVAL: Ticks = 3;

// The randomized election timeout range (min-max), in ticks. This is
// randomized per node to avoid ties.
pub const ELECTION_TIMEOUT_RANGE: std::ops::Range<u8> = 10..20;

// Generates a randomized election timeout, range from TICK_INTERVAL * 10 to
// TICK_INTERVAL * 20, e.g., 1s to 2s if TICK_INTERVAL is 100ms.
fn rand_election_timeout() -> Ticks {
    rand::thread_rng().gen_range(ELECTION_TIMEOUT_RANGE)
}

pub trait Node: Send {
    fn tick(self: Box<Self>) -> Result<Box<dyn Node>>;
    // step advances the state machine using the given message.
    // the role transitions of a node are driven here.
    fn step(self: Box<Self>, msg: Message) -> Result<Box<dyn Node>>;
    fn get_state(&self) -> NodeState;
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct NodeState {
    pub me: NodeId,
    pub leader: Option<NodeId>,

    pub term: Term,
}

impl NodeState {
    pub fn is_leader(&self) -> bool {
        self.leader == Some(self.me)
    }
}

impl<'a> From<&'a RawNode> for NodeState {
    fn from(rn: &'a RawNode) -> Self {
        NodeState { me: rn.id, leader: rn.leader, term: rn.term }
    }
}

#[derive(Debug)]
pub struct RawNode {
    id: NodeId,
    peers: Vec<NodeId>,
    // persist facility for log entries and any raft
    // hard state, i.e., current_term, voted_for.
    persister: Persister,
    // a blocking channel for sending message
    // to raft peers.
    node_tx: mpsc::UnboundedSender<Message>,
    // state represents the state machine.
    state: Box<dyn State>,

    // persistent state
    //
    // latest term server has seen(initialized to 0
    // on first boot, increases monotonically)
    term: Term,
    voted_for: Option<NodeId>,

    // volatile state on all servers
    //
    // current leader
    leader: Option<NodeId>,
    // index of highest log entry known to be committed
    // initialized to 0, increases monotonically.
    // TODO: persist commit index to improve performance,
    //  although the commit index is not required to be
    //  persistent from the correctness point of view,
    //  have it been persistent would make the replication
    //  more performant, i.e., if the node recover from
    //  crash, if the commit_index is not persistent, the
    //  raft leader node would need to replicate all the
    //  entries to peers from scratch. so by having commit
    //  index persisted, we can avoid this expensive op.
    commit_index: Index,
    // index of highest log entry applied to state machine
    // initialized to 0, increases monotonically.
    // it is required the state machine to manage duplicated
    // apply message, e.g., if the raft node recover from
    // crash, it may replay the applied msg again upto the
    // latest commit_index from scratch.
    // TODO: this can be improved by asking the state machine
    //  about the persisted last_applied index, i.e., if the
    //  state machine persisted the state and last_applied index
    //  then as raft node here, we can start the replay from
    //  there upto the commit_index.
    last_applied: Index,

    seq: u64,
}

impl RawNode {
    pub fn new(
        id: NodeId,
        peers: Vec<NodeId>,
        persister: Persister,
        node_tx: mpsc::UnboundedSender<Message>,
        state: Box<dyn State>,
    ) -> Result<RawNode> {
        let hs = persister.get_hard_state()?;
        let (term, voted_for) = if let Some(x) = hs { (x.term, x.voted_for) } else { (0, None) };
        let (leader, commit_index, last_applied) = (None, 0, 0);
        let rn = RawNode {
            id,
            peers,
            persister,
            node_tx,
            state,
            term,
            voted_for,
            leader,
            commit_index,
            last_applied,
            seq: 0,
        };
        Ok(rn)
    }

    pub fn quorum_size(&self) -> u8 {
        let total = self.peers.len() + 1;
        total as u8 / 2 + 1
    }

    pub fn save_hard_state(&mut self, term: Term, voted_for: Option<NodeId>) -> Result<()> {
        let hs = HardState { term, voted_for };
        self.persister.save_hard_state(hs)?;
        self.term = term;
        self.voted_for = voted_for;
        Ok(())
    }

    pub fn into_leaderless_follower(mut self, term: Term, msg: Message) -> Result<Box<dyn Node>> {
        // save hard state before the transition.
        self.save_hard_state(term, None)?;
        // transit to leaderless follower
        self.leader = None;
        let follower: Follower = self.into();
        let follower = Box::new(follower);
        return follower.step(msg);
    }
}
