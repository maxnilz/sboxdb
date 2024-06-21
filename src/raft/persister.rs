use std::ops::Bound;

use serde::{Deserialize, Serialize};

use crate::codec::{bincodec, keycodec};
use crate::error::Result;
use crate::raft::node::NodeId;
use crate::raft::{Command, Index, Term};
use crate::storage::Storage;

#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
pub struct Entry {
    pub index: Index,
    pub term: Term,
    pub command: Command,
}

#[derive(Copy, Clone, PartialEq, Debug, Serialize, Deserialize)]
pub struct HardState {
    pub term: Term,
    pub voted_for: Option<NodeId>,
}

#[derive(Copy, Clone, Serialize, Deserialize)]
enum Key {
    State,
    Entry(Index),
}

impl Key {
    fn encode(&self, id: NodeId) -> Result<Vec<u8>> {
        let bytes = keycodec::serialize(self)?;
        let mut ans = id.to_be_bytes().to_vec();
        ans.extend(bytes);
        Ok(ans)
    }
}

// persister persist the raft state like log entries, term, and
// vote info, i.e., the persister here is only for raft states
// not the state of the state machine that live upper above the
// raft log.
#[derive(Debug)]
pub struct Persister {
    id: NodeId,
    storage: Box<dyn Storage>,

    last_index: Index,
    last_term: Term,
}

impl Persister {
    pub fn new(id: NodeId, storage: Box<dyn Storage>) -> Result<Persister> {
        let prefix = Key::Entry(0).encode(id)?;
        let last = storage.scan_prefix(&prefix).last();
        let (last_index, last_term) = if let Some(x) = last {
            let (_, v) = x?;
            let entry: Entry = bincodec::deserialize(&v)?;
            (entry.index, entry.term)
        } else {
            (0, 0)
        };
        Ok(Persister { id, storage, last_index, last_term })
    }

    pub fn last(&self) -> (Index, Term) {
        (self.last_index, self.last_term)
    }

    pub fn save_hard_state(&mut self, state: HardState) -> Result<()> {
        let key = Key::State.encode(self.id)?;
        let value = bincodec::serialize(&state)?;
        self.storage.set(&key, value)?;
        Ok(())
    }

    pub fn get_hard_state(&self) -> Result<Option<HardState>> {
        let key = Key::State.encode(self.id)?;
        let value = self.storage.get(&key)?;
        let ans = match value {
            None => None,
            Some(bs) => {
                let m: HardState = bincodec::deserialize(&bs)?;
                Some(m)
            }
        };
        Ok(ans)
    }

    pub fn append(&mut self, term: Term, command: Command) -> Result<Index> {
        let index = self.last_index + 1;

        let entry = Entry { index, term, command };
        self.append_entry(entry)?;

        Ok(index)
    }

    pub fn append_entry(&mut self, entry: Entry) -> Result<()> {
        if entry.index < self.last_index {
            // truncate entries
            let from = Key::Entry(entry.index).encode(self.id)?;
            self.storage.remove_prefix(&from)?;
        }
        let key = Key::Entry(entry.index).encode(self.id)?;
        let value = bincodec::serialize(&entry)?;
        self.storage.set(&key, value)?;

        self.last_index = entry.index;
        self.last_term = entry.term;

        Ok(())
    }

    pub fn get_entry(&self, index: Index) -> Result<Option<Entry>> {
        let key = Key::Entry(index).encode(self.id)?;
        let result = self.storage.get(&key)?;
        match result {
            None => Ok(None),
            Some(value) => {
                let entry: Entry = bincodec::deserialize(&value)?;
                Ok(Some(entry))
            }
        }
    }

    // scan entries by range [from, to).
    pub fn scan_entries(&self, from: Index, to: Index) -> Result<Vec<Entry>> {
        let from = Key::Entry(from).encode(self.id)?;
        let to = Key::Entry(to).encode(self.id)?;
        let range = (Bound::Included(from), Bound::Excluded(to));
        let result = self.storage.scan(range);
        // map results Vec<Result<Entry, Error>, the collect do the
        // transformation to Result<Vec<Entry, Error>(provided by std).
        // type infer take place by leverage the return type.
        result
            .map(|x| {
                let (_, value) = x?;
                let entry: Entry = bincodec::deserialize(&value)?;
                Ok(entry)
            })
            .collect()
    }

    pub fn scan_from(&self, from: Index) -> Result<Vec<Entry>> {
        let from = Key::Entry(from).encode(self.id)?;
        let result = self.storage.scan_prefix(&from);
        result
            .map(|x| {
                let (_, value) = x?;
                let entry: Entry = bincodec::deserialize(&value)?;
                Ok(entry)
            })
            .collect()
    }

    pub fn remove_from(&mut self, from: Index) -> Result<usize> {
        let prev = from - 1;

        // remove entries from the given index.
        let from = Key::Entry(from).encode(self.id)?;
        let values = self.storage.remove_prefix(&from)?;
        if values.is_empty() {
            return Ok(0);
        }

        // update the last index & term
        let last = self.get_entry(prev)?;
        (self.last_index, self.last_term) =
            if let Some(last) = last { (last.index, last.term) } else { (0, 0) };

        Ok(values.len())
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::{new_storage, StorageType};

    use super::*;

    #[test]
    fn test_persister_simple() -> Result<()> {
        let id = 0;
        let s = new_storage(StorageType::Memory)?;
        let mut p = Persister { id, storage: s, last_index: 0, last_term: 0 };

        // state ops

        // no state at the beginning
        let state = p.get_hard_state()?;
        assert_eq!(None, state);

        // set state.term
        let state = HardState { term: 1, voted_for: None };
        p.save_hard_state(state)?;
        let got = p.get_hard_state()?;
        assert_eq!(Some(HardState { term: 1, voted_for: None }), got);

        // set state.voted_for
        let state = HardState { term: 1, voted_for: Some(1) };
        p.save_hard_state(state)?;
        let got = p.get_hard_state()?;
        assert_eq!(Some(HardState { term: 1, voted_for: Some(1) }), got);

        // entries ops

        // append entries
        let entries = vec![
            Entry { index: 0, term: 0, command: vec![0].into() },
            Entry { index: 1, term: 1, command: vec![1].into() },
            Entry { index: 2, term: 2, command: vec![2].into() },
            Entry { index: 3, term: 3, command: vec![3].into() },
            Entry { index: 4, term: 4, command: vec![4].into() },
            Entry { index: 5, term: 5, command: vec![5].into() },
        ];
        for entry in &entries {
            p.append_entry(entry.clone())?;
        }

        // scan from beginning
        let got = p.scan_from(0)?;
        assert_eq!(got, entries);

        // scan [2, 2)
        let got = p.scan_entries(2, 2)?;
        assert_eq!(got, vec![]);

        // scan [2, 4)
        let got = p.scan_entries(2, 4)?;
        assert_eq!(got, entries[2..4]);

        // scan [2, ..)
        let got = p.scan_entries(2, 100)?;
        assert_eq!(got, entries[2..]);

        // append more with append
        let entries = vec![
            Entry { index: 6, term: 6, command: vec![6].into() },
            Entry { index: 7, term: 7, command: vec![7].into() },
            Entry { index: 8, term: 8, command: vec![8].into() },
            Entry { index: 9, term: 9, command: vec![9].into() },
        ];
        for entry in &entries {
            p.append(entry.term, entry.command.clone())?;
        }

        // scan [6, ..)
        let got = p.scan_entries(6, 100)?;
        assert_eq!(got, entries[..]);

        // assert last index and last term
        assert_eq!(9, p.last_index);
        assert_eq!(9, p.last_term);

        // truncate append
        let entry = Entry { index: 6, term: 6, command: vec![6, 6, 6].into() };
        p.append_entry(entry.clone())?;

        // get entry by index
        let got = p.get_entry(6)?;
        assert_eq!(Some(entry.clone()), got);

        // scan [6, ..)
        let got = p.scan_entries(6, 100)?;
        assert_eq!(1, got.len());
        assert_eq!(entry.clone(), got[0]);

        // assert last index and last term
        assert_eq!(6, p.last_index);
        assert_eq!(6, p.last_term);

        // test persister init

        // test persister init with non-empty storage
        let mut p = Persister::new(id, p.storage)?;
        assert_eq!(6, p.last_index);
        assert_eq!(6, p.last_term);

        // test remove entries by index
        //
        // remove from noexist index
        let got = p.remove_from(7)?;
        assert_eq!(got, 0);
        assert_eq!(6, p.last_index);
        assert_eq!(6, p.last_term);
        // remove last item
        let got = p.remove_from(6)?;
        assert_eq!(got, 1);
        assert_eq!(5, p.last_index);
        assert_eq!(5, p.last_term);
        // remove from 3
        let got = p.remove_from(3)?;
        assert_eq!(got, 3);
        assert_eq!(2, p.last_index);
        assert_eq!(2, p.last_term);

        // test persister init with empty storage
        let s = new_storage(StorageType::Memory)?;
        let p = Persister::new(id, s)?;
        assert_eq!(0, p.last_index);
        assert_eq!(0, p.last_term);

        Ok(())
    }
}
