use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::ops::Bound;
use std::sync::Arc;
use std::sync::Mutex;

use super::ScanIterator;
use super::Storage;
use crate::error::Result;
use crate::storage;

#[derive(Debug)]
pub struct Memory {
    bm: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
}

impl Memory {
    pub fn new() -> Memory {
        Memory { bm: Arc::new(Mutex::new(BTreeMap::new())) }
    }
}

impl Storage for Memory {
    fn flush(&self) -> Result<()> {
        Ok(())
    }

    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        let key = Vec::from(key);
        let mut bm = self.bm.lock()?;
        bm.insert(key, value);
        Ok(())
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let bm = self.bm.lock()?;
        let value = bm.get(key);
        Ok(value.cloned())
    }

    fn scan(&self, range: (Bound<Vec<u8>>, Bound<Vec<u8>>)) -> Box<dyn ScanIterator<'_> + '_> {
        let bm = self.bm.lock().unwrap();
        let deque: VecDeque<(Vec<u8>, Vec<u8>)> =
            bm.range(range).map(|(k, v)| (k.clone(), v.clone())).collect();
        Box::new(VecDequeIterator { deque })
    }

    fn scan_prefix(&self, prefix: &[u8]) -> Box<dyn ScanIterator<'_> + '_> {
        self.scan(storage::prefix_range(prefix))
    }

    fn remove(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut bm = self.bm.lock()?;
        Ok(bm.remove(key))
    }

    fn remove_range(
        &mut self,
        range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
    ) -> Box<dyn ScanIterator<'_> + '_> {
        let mut bm = self.bm.lock().unwrap();
        let keys = bm.range(range).map(|(k, _)| k.clone()).collect::<Vec<_>>();
        let mut result: VecDeque<(Vec<u8>, Vec<u8>)> = VecDeque::new();
        for key in keys {
            let value = bm.remove(&key);
            if let Some(val) = value {
                result.push_back((key, val));
            }
        }
        Box::new(VecDequeIterator { deque: result })
    }

    fn remove_prefix(&mut self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut bm = self.bm.lock()?;
        let r = storage::prefix_range(prefix);
        let keys: Vec<Vec<u8>> = bm.range(r).map(|(k, _)| k.clone()).collect();
        let mut result = Vec::new();
        for key in keys {
            let res = bm.remove(&key);
            if let Some(val) = res {
                result.push((key.to_vec(), val));
            }
        }
        Ok(result)
    }
}

#[derive(Debug)]
pub struct VecDequeIterator {
    deque: VecDeque<(Vec<u8>, Vec<u8>)>,
}

impl VecDequeIterator {
    pub fn new(deque: VecDeque<(Vec<u8>, Vec<u8>)>) -> VecDequeIterator {
        VecDequeIterator { deque }
    }
}

impl Iterator for VecDequeIterator {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.deque.pop_front().map(Ok)
    }
}

impl DoubleEndedIterator for VecDequeIterator {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.deque.pop_back().map(Ok)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_point_ops() -> Result<()> {
        let mut m = Memory::new();

        // get missing key
        assert_eq!(None, m.get(b"a")?);

        // set & get key
        m.set(b"a", vec![1])?;
        assert_eq!(Some(vec![1]), m.get(b"a")?);

        // delete
        let got = m.remove(b"a")?;
        assert_eq!(Some(vec![1]), got);

        // get again
        assert_eq!(None, m.get(b"a")?);

        Ok(())
    }

    #[test]
    fn text_prefix_scan() -> Result<()> {
        let mut m = Memory::new();

        // set values
        m.set(b"apple", vec![1])?;
        m.set(b"app", vec![2])?;
        m.set(b"banala", vec![3])?;
        m.set(b"ba", vec![4])?;
        m.set(b"cat\x1F", vec![5])?;
        m.set(b"cat\x1E", vec![6])?;
        m.set(b"cat\x20", vec![7])?;

        // scan prefix
        let iter = m.scan_prefix(b"app");
        let values = iter.map(|it| it.unwrap().1).collect::<Vec<_>>();
        assert_eq!(vec![vec![2], vec![1]], values);

        // scan prefix
        let iter = m.scan_prefix(b"cat");
        let values = iter.map(|it| it.unwrap().1).collect::<Vec<_>>();
        assert_eq!(vec![vec![6], vec![5], vec![7]], values);

        // scan prefix
        m.set(&vec![1, 1, 1], vec![8])?;
        m.set(&vec![1, 1, 2], vec![9])?;
        m.set(&vec![1, 2, 1], vec![10])?;
        m.set(&vec![1, 2, 2], vec![11])?;

        let iter = m.scan_prefix(&vec![1, 1]);
        let values = iter.map(|it| it.unwrap().1).collect::<Vec<_>>();
        assert_eq!(vec![vec![8], vec![9]], values);

        Ok(())
    }

    #[test]
    fn test_range_scan() -> Result<()> {
        let mut m = Memory::new();

        // set values
        m.set(b"a", vec![1])?;
        m.set(b"b", vec![2])?;
        m.set(b"c", vec![3])?;
        m.set(b"d", vec![4])?;

        // range scan
        let start = Bound::Included(b"a".to_vec());
        let end = Bound::Excluded(b"d".to_vec());
        let iter = m.scan((start, end));
        let values = iter.map(|it| it.unwrap().1).collect::<Vec<_>>();
        assert_eq!(vec![vec![1], vec![2], vec![3]], values);

        // set more values
        m.set(&vec![1, 1, 1], vec![8])?;
        m.set(&vec![1, 1, 2], vec![9])?;
        m.set(&vec![1, 2, 1], vec![10])?;
        m.set(&vec![1, 2, 2], vec![11])?;

        // range scan with half open range
        let from = Bound::Included(vec![1, 1]);
        let to = Bound::Excluded(vec![1, 255]);
        let iter = m.scan((from, to));
        let values = iter.map(|it| it.unwrap().1).collect::<Vec<_>>();
        assert_eq!(vec![vec![8], vec![9], vec![10], vec![11]], values);

        // range scan on [from, to] with the exactly matched key,
        // since the key `vec![1, 2, 2]` is included in the range,
        // the result should contain the value of `vec![1, 2, 2]`.
        let from = Bound::Included(vec![1, 1, 1]);
        let to = Bound::Included(vec![1, 2, 2]);
        let iter = m.scan((from, to));
        let values = iter.map(|it| it.unwrap().1).collect::<Vec<_>>();
        assert_eq!(vec![vec![8], vec![9], vec![10], vec![11]], values);

        // range scan on [from, to] with the exactly matched key,
        // since the key `vec![1, 2]` is not an exact key in the storage,
        // the result should not contain any value greater that `vec![1, 2]`,
        // and vec[1, 2] itself.
        let from = Bound::Included(vec![1, 1]);
        let to = Bound::Included(vec![1, 2]);
        let iter = m.scan((from, to));
        let values = iter.map(|it| it.unwrap().1).collect::<Vec<_>>();
        assert_eq!(vec![vec![8], vec![9]], values);

        Ok(())
    }

    #[test]
    fn test_range_scan_invariants() -> Result<()> {
        let mut m = Memory::new();
        m.set(b"a", vec![1])?;
        m.set(b"b", vec![2])?;
        m.set(b"c", vec![3])?;
        m.set(b"d", vec![4])?;

        let from = Bound::Included(b"a".to_vec());
        let to = Bound::Included(b"d".to_vec());

        let iter = m.scan((from.clone(), to.clone()));
        let mut forward = iter.collect::<Result<Vec<_>>>()?;
        forward.reverse();

        let rev_iter = m.scan((from.clone(), to.clone())).rev();
        let reverse = rev_iter.collect::<Result<Vec<_>>>()?;
        assert_eq!(reverse, forward);

        Ok(())
    }

    #[test]
    fn test_prefix_scan_invariants() -> Result<()> {
        let mut m = Memory::new();
        m.set(b"a", vec![1])?;
        m.set(b"b", vec![2])?;
        m.set(b"c", vec![3])?;
        m.set(b"d", vec![4])?;

        let iter = m.scan_prefix(b"");
        let mut forward = iter.collect::<Result<Vec<_>>>()?;
        forward.reverse();

        let rev_iter = m.scan_prefix(b"").rev();
        let reverse = rev_iter.collect::<Result<Vec<_>>>()?;
        assert_eq!(reverse, forward);

        Ok(())
    }
}
