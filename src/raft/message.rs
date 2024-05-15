use crate::raft::persister::Entry;
use crate::raft::{Index, NodeId, Term};

// A message that passed between raft peers
pub struct Message {
    from: NodeId,
    to: NodeId,
    event: Event,
}

enum Event {
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

    // Propose command be appended to the log.
    // Note that proposals can be lost without
    // notice, therefore it is user's job to
    // ensure proposal retries. if a proposal
    // is accepted, the state machine would
    // receive the accepted proposal's command
    // from the ApplyMsg over the apply channel.
    ProposeCommand {
        command: Vec<u8>,
    },

    // Acknowledge Proposal if the command is
    // proposed to a leader, indicating that
    // the proposed command has been acknowledged
    // by the leader, but not yet commited and
    // applied.
    ProposalAcknowledged {
        index: Index,
        term: Term,
    },

    // Drop proposal if the command is proposed to
    // either a follower or a candidate.
    ProposalDropped,

    ProposalApplied {
        index: Index,
        term: Term,
        res: Vec<u8>,
    },
}
