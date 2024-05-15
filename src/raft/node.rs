use std::time::Duration;

use rand::Rng;
use tokio::sync::mpsc;

use crate::error::Result;
use crate::raft::message::Message;
use crate::raft::persister::Persister;
use crate::raft::{Index, NodeId, Term};
use crate::storage::state::State;
use crate::storage::Storage;

pub mod candidate;
pub mod follower;
pub mod leader;

// A logical clock interval as number of ticks.
pub type Ticks = u8;

// The interval between Raft ticks, the unit of time for e.g. heartbeats and
// elections.
pub const TICK_INTERVAL: Duration = Duration::from_millis(100);

// The interval between leader heartbeats, in ticks.
const HEARTBEAT_INTERVAL: Ticks = 3;

// The randomized election timeout range (min-max), in ticks. This is
// randomized per node to avoid ties.
const ELECTION_TIMEOUT_RANGE: std::ops::Range<u8> = 10..20;

// Generates a randomized election timeout.
fn rand_election_timeout() -> Ticks {
    rand::thread_rng().gen_range(ELECTION_TIMEOUT_RANGE)
}

pub trait Node: Send + Sync {
    fn tick(self: Box<Self>) -> Result<Box<dyn Node + Send + Sync>>;
    // step advances the state machine using the given message.
    // the role transitions of a node are driven here.
    fn step(self: Box<Self>, msg: Message) -> Result<Box<dyn Node + Send + Sync>>;
}

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
    state: Box<dyn State + Send + Sync>,

    // persistent state
    //
    // latest term server has seen(initialized to 0
    // on first boot, increases monotonically)
    term: Term,
    voted_for: Option<NodeId>,

    // volatile state
    //
    // current leader
    leader: NodeId,
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
        let (leader, commit_index, last_applied) = (0, 0, 0);
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
        };
        Ok(rn)
    }
}
