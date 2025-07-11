use std::cmp::Ordering;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Mutex;

use super::page::FrameId;

///  Replacer tracks page usage for replacement in case of buffer pool is full.
pub trait Replacer {
    /// Record the event that the given frame id is accessed at current timestamp.
    /// Create a new entry for access history if frame id has not been seen before.
    fn record_access(&self, frame_id: FrameId);

    /// Find the frame to evict with replace policy(e.g. backward k-distance). Only frames that
    /// are marked as evictable are candidates for eviction.
    ///
    /// Successful eviction of a frame should decrement the size of replacer and remove the frame's
    /// access history.
    ///
    /// Return the frame id if a frame is evicted successfully, None if no frames can be evicted.
    fn evict(&self) -> Option<FrameId>;

    /// Toggle whether a frame is evictable or non-evictable. this function also control replacer
    /// size. Note that size is equal to number of evictable entries.
    ///
    /// If a frame was previously evictable and is to be set to non-evictable, then size should
    /// decrement. If a frame was previously non-evictable and is to be set evictable, then size
    /// should increment.
    fn set_evictable(&self, frame_id: FrameId, evictable: bool);

    /// Check if a frame is evictable. if the frame is not found, return true.
    fn is_evictable(&self, frame_id: FrameId) -> bool;

    /// Remove an evictable frame from replacer, along with its access history. This function
    /// should also decrement the replacer size if removal is successful.
    ///
    /// Note that this is different from evicting a frame without check replacer policy.
    ///
    /// If remove is called on a non-evictable frame, return an error. If the specified frame is
    /// not found, do nothing without return any error.
    fn remove(&self, frame_id: FrameId);

    /// replace size.
    fn size(&self) -> usize;
}

#[derive(Debug, Eq, PartialEq)]
struct KDistance {
    frame_id: FrameId,
    distance: usize,
    last_access_at: usize,
}

impl PartialOrd for KDistance {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for KDistance {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.distance.cmp(&other.distance).reverse() {
            Ordering::Equal => self.last_access_at.cmp(&other.last_access_at),
            other => other,
        }
    }
}

struct LRUKNode {
    k: usize,
    frame_id: FrameId,
    is_evictable: bool,
    /// history of last seen K timestamp of the given page.
    /// Least recent timestamp stored in front.
    history: VecDeque<usize>,
}

impl LRUKNode {
    fn new(frame_id: FrameId, k: usize) -> Self {
        assert!(k > 0, "replace k should be larger than zero");
        LRUKNode { history: VecDeque::with_capacity(k), k, frame_id, is_evictable: true }
    }

    fn record_access(&mut self, timestamp: usize) {
        if self.history.len() == self.k {
            self.history.pop_front();
        }
        self.history.push_back(timestamp)
    }

    fn k_distance(&self) -> KDistance {
        if self.history.len() < self.k {
            let last = self.history.back().unwrap_or(&0usize);
            return KDistance {
                frame_id: self.frame_id,
                distance: usize::MAX,
                last_access_at: *last,
            };
        }
        // since the size of k is guaranteed to be larger than zero
        // the history will have at lease one element, unwrap here is
        // guaranteed to be not panic.
        let last = self.history.back().unwrap();
        let least = self.history.front().unwrap();
        KDistance { frame_id: self.frame_id, distance: last - least, last_access_at: *last }
    }
}

/// LRUKReplacer implements the LRU-k replacement policy.
///
/// The LRU-k algorithm evicts a frame whose backward k-distance is maximum of
/// all frames. Backward k-distance is computed as the difference in time between
/// the current timestamp and the timestamp of k-th previous access.
///
/// A frame with less than k history references is given +inf as its backward k-distance.
/// when multiple frames have +inf backward k-distance, classical LRU algorithm is used
/// to choose victim.
pub struct LRUKReplacer {
    nodes: HashMap<FrameId, LRUKNode>,
    current_timestamp: usize,
    current_size: usize,
    replacer_size: usize,
    k: usize,
}

impl LRUKReplacer {
    pub fn new(k: usize, size: usize) -> Self {
        let nodes = HashMap::new();
        LRUKReplacer { nodes, current_timestamp: 0, current_size: 0, replacer_size: size, k }
    }

    /// Record the event that the given frame id is accessed at current timestamp.
    /// Create a new entry for access history if frame id has not been seen before.
    fn record_access(&mut self, frame_id: FrameId) {
        if frame_id >= self.replacer_size {
            return;
        }
        let node = self.nodes.entry(frame_id).or_insert_with(|| {
            self.current_size += 1;
            LRUKNode::new(frame_id, self.k)
        });
        node.record_access(self.current_timestamp);
        self.current_timestamp += 1;
    }

    /// Find the frame with the largest backward k-distance and evict that frame. Only frames
    /// that are marked as evictable are candidates for eviction.
    ///
    /// A Frame with less than k historical reference is given +inf as its backward k-distance.
    /// If multiple frames have inf backward k-distance, then evict the frame with the earliest
    /// timestamp overall.
    ///
    /// Successful eviction of a frame should decrement the size of replacer and remove the frame's
    /// access history.
    ///
    /// Return the frame id if a frame is evicted successfully, None if no frames can be evicted.
    fn evict(&mut self) -> Option<FrameId> {
        let mut distances = vec![];
        for node in self.nodes.values() {
            if !node.is_evictable {
                continue;
            }
            distances.push(node.k_distance());
        }
        if distances.is_empty() {
            return None;
        }

        distances.sort();

        // we will always have at least one element here, it is
        // okay to use unwrap.
        let dist = distances.first().unwrap();
        let evicted = self.nodes.remove(&dist.frame_id).unwrap();
        self.current_size -= 1;

        Some(evicted.frame_id)
    }

    /// Toggle whether a frame is evictable or non-evictable. this function also control replacer
    /// size. Note that size is equal to number of evictable entries.
    ///
    /// If a frame was previously evictable and is to be set to non-evictable, then size should
    /// decrement. If a frame was previously non-evictable and is to be set evictable, then size
    /// should increment.
    ///
    /// For other scenarios, this function should terminate without modifying anything.
    fn set_evictable(&mut self, frame_id: FrameId, evictable: bool) {
        let node = self.nodes.get_mut(&frame_id);
        if node.is_none() {
            return;
        }
        let node = node.unwrap();
        let is_evictable = node.is_evictable;
        if is_evictable == evictable {
            return;
        }

        node.is_evictable = evictable;
        if !is_evictable && evictable {
            self.current_size += 1;
        }
        if is_evictable && !evictable {
            self.current_size -= 1;
        }
    }

    /// Check if a frame is evictable. if the frame is not found, return true.
    fn is_evictable(&self, frame_id: FrameId) -> bool {
        let node = self.nodes.get(&frame_id);
        if node.is_none() {
            return true;
        }
        node.unwrap().is_evictable
    }

    /// Remove an evictable frame from replacer, along with its access history. This function
    /// should also decrement the replacer size if removal is successful.
    ///
    /// Note that this is different from evicting a frame, which always remove the frame with the
    /// largest backward k-distance. This function removes specified frame id, no matter what its
    /// backward k-distance is.
    ///
    /// If remove is called on a non-evictable frame, return an error. If the specified frame is
    /// not found, do nothing without return any error.
    fn remove(&mut self, frame_id: FrameId) {
        let node = self.nodes.get_mut(&frame_id);
        if node.is_none() {
            return;
        }
        let node = node.unwrap();
        if !node.is_evictable {
            return;
        }
        self.nodes.remove(&frame_id);
        self.current_size -= 1;
    }

    fn size(&self) -> usize {
        self.current_size
    }
}

/// SyncLRUKReplacer implements the thread-safe version of LRU-k replacement policy,
/// basically all the heavy lifting are happens in the LRUKReplacer.
pub struct SyncLRUKReplacer {
    inner: Mutex<LRUKReplacer>,
}

impl SyncLRUKReplacer {
    pub fn new(k: usize, size: usize) -> Self {
        let inner = Mutex::new(LRUKReplacer::new(k, size));
        SyncLRUKReplacer { inner }
    }
}

impl Replacer for SyncLRUKReplacer {
    /// Record the event that the given frame id is accessed at current timestamp.
    /// Create a new entry for access history if frame id has not been seen before.
    fn record_access(&self, frame_id: FrameId) {
        let mut guard = self.inner.lock().unwrap();
        guard.record_access(frame_id)
    }

    /// Find the frame with the largest backward k-distance and evict that frame. Only frames
    /// that are marked as evictable are candidates for eviction.
    fn evict(&self) -> Option<FrameId> {
        let mut guard = self.inner.lock().unwrap();
        guard.evict()
    }

    /// Toggle whether a frame is evictable or non-evictable. this function also control replacer
    /// size. Note that size is equal to number of evictable entries.
    fn set_evictable(&self, frame_id: FrameId, evictable: bool) {
        let mut guard = self.inner.lock().unwrap();
        guard.set_evictable(frame_id, evictable)
    }

    /// Check if a frame is evictable. if the frame is not found, return true.
    fn is_evictable(&self, frame_id: FrameId) -> bool {
        let guard = self.inner.lock().unwrap();
        guard.is_evictable(frame_id)
    }

    /// Remove an evictable frame from replacer, along with its access history. This function
    /// should also decrement the replacer size if removal is successful.
    fn remove(&self, frame_id: FrameId) {
        let mut guard = self.inner.lock().unwrap();
        guard.remove(frame_id)
    }

    /// Remove an evictable frame from replacer, along with its access history. This function
    /// should also decrement the replacer size if removal is successful.
    fn size(&self) -> usize {
        let guard = self.inner.lock().unwrap();
        guard.size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;

    #[test]
    fn test_kdistance_sort() -> Result<()> {
        let mut arr = [
            KDistance { frame_id: 1, distance: 5, last_access_at: 10 },
            KDistance { frame_id: 2, distance: 5, last_access_at: 5 },
            KDistance { frame_id: 3, distance: 3, last_access_at: 7 },
            KDistance { frame_id: 4, distance: 8, last_access_at: 3 },
            KDistance { frame_id: 5, distance: usize::MAX, last_access_at: 5 },
            KDistance { frame_id: 6, distance: usize::MAX, last_access_at: 3 },
        ];

        // Sort the array using the implemented Ord trait
        arr.sort();

        let mut ids = vec![];
        for kd in &arr {
            println!("{:?}", kd);
            ids.push(kd.frame_id);
        }
        assert_eq!(vec![6, 5, 4, 2, 1, 3], ids);

        Ok(())
    }

    #[test]
    fn test_lruk_node() -> Result<()> {
        // node with k = 1
        let mut node = LRUKNode::new(1, 1);

        node.record_access(1);
        let dist = node.k_distance();
        assert_eq!(dist.distance, 0);
        assert_eq!(dist.last_access_at, 1);

        // node with k = 2
        let mut node = LRUKNode::new(1, 2);

        node.record_access(1);
        let dist = node.k_distance();
        assert_eq!(dist.distance, usize::MAX);
        assert_eq!(dist.last_access_at, 1);

        node.record_access(2);
        let dist = node.k_distance();
        assert_eq!(dist.distance, 1);
        assert_eq!(dist.last_access_at, 2);

        node.record_access(5);
        let dist = node.k_distance();
        assert_eq!(dist.distance, 3);
        assert_eq!(dist.last_access_at, 5);

        Ok(())
    }

    #[test]
    fn test_lruk_replacer() -> Result<()> {
        let mut lru_replacer = LRUKReplacer::new(2, 7);

        // Scenario: add six elements to the replacer. We have [1,2,3,4,5]. Frame 6 is non-evictable.
        lru_replacer.record_access(1);
        lru_replacer.record_access(2);
        lru_replacer.record_access(3);
        lru_replacer.record_access(4);
        lru_replacer.record_access(5);
        lru_replacer.record_access(6);
        lru_replacer.set_evictable(1, true);
        lru_replacer.set_evictable(2, true);
        lru_replacer.set_evictable(3, true);
        lru_replacer.set_evictable(4, true);
        lru_replacer.set_evictable(5, true);
        lru_replacer.set_evictable(6, false);
        assert_eq!(5, lru_replacer.size());

        // Scenario: Insert access history for frame 1. Now frame 1 has two access histories.
        // All other frames have max backward k-dist. The order of eviction is [2,3,4,5,1].
        lru_replacer.record_access(1);

        // Scenario: Evict three pages from the replacer. Elements with max k-distance should be popped
        // first based on LRU.
        let frame_id = lru_replacer.evict();
        assert_eq!(Some(2), frame_id);
        let frame_id = lru_replacer.evict();
        assert_eq!(Some(3), frame_id);
        let frame_id = lru_replacer.evict();
        assert_eq!(Some(4), frame_id);
        assert_eq!(2, lru_replacer.size());

        // Scenario: Now replacer has frames [5,1].
        // Insert new frames 3, 4, and update access history for 5. We should end with [3,1,5,4]
        lru_replacer.record_access(3);
        lru_replacer.record_access(4);
        lru_replacer.record_access(5);
        lru_replacer.record_access(4);
        lru_replacer.set_evictable(3, true);
        lru_replacer.set_evictable(4, true);
        assert_eq!(4, lru_replacer.size());

        // Scenario: continue looking for victims. We expect 3 to be evicted next.
        let frame_id = lru_replacer.evict();
        assert_eq!(Some(3), frame_id);
        assert_eq!(3, lru_replacer.size());

        // Set 6 to be evictable. 6 Should be evicted next since it has max backward k-dist.
        lru_replacer.set_evictable(6, true);
        assert_eq!(4, lru_replacer.size());
        let frame_id = lru_replacer.evict();
        assert_eq!(Some(6), frame_id);
        assert_eq!(3, lru_replacer.size());

        // Now we have [1,5,4]. Continue looking for victims.
        lru_replacer.set_evictable(1, false);
        assert_eq!(2, lru_replacer.size());
        let frame_id = lru_replacer.evict();
        assert_eq!(Some(5), frame_id);
        assert_eq!(1, lru_replacer.size());

        // Update access history for 1. Now we have [4,1]. Next victim is 4.
        lru_replacer.record_access(1);
        lru_replacer.record_access(1);
        lru_replacer.set_evictable(1, true);
        assert_eq!(2, lru_replacer.size());
        let frame_id = lru_replacer.evict();
        assert_eq!(Some(4), frame_id);

        assert_eq!(1, lru_replacer.size());
        let frame_id = lru_replacer.evict();
        assert_eq!(Some(1), frame_id);
        assert_eq!(0, lru_replacer.size());

        // These operations should not modify size
        let frame_id = lru_replacer.evict();
        assert_eq!(None, frame_id);
        assert_eq!(0, lru_replacer.size());
        lru_replacer.remove(1);
        assert_eq!(0, lru_replacer.size());

        Ok(())
    }
}
