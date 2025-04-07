use std::collections::btree_map::Range;
use std::collections::{BTreeMap, VecDeque};
use std::ops::Bound;

use super::{ScanIterator, Storage};
use crate::error::Result;

#[derive(Debug)]
pub struct Memory {
    data: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl Memory {
    pub fn new() -> Memory {
        Memory { data: BTreeMap::new() }
    }
}

impl Storage for Memory {
    fn flush(&self) -> Result<()> {
        Ok(())
    }

    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        let key = Vec::from(key);
        self.data.insert(key, value);
        Ok(())
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let value = self.data.get(key);
        Ok(value.cloned())
    }

    fn scan(&self, range: (Bound<Vec<u8>>, Bound<Vec<u8>>)) -> Box<dyn ScanIterator<'_> + '_> {
        let result = self.data.range(range);
        Box::new(RangeIterator { it: result })
    }

    fn remove(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.data.remove(key))
    }

    fn remove_range(
        &mut self,
        range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
    ) -> Box<dyn ScanIterator<'_> + '_> {
        let keys = self.data.range(range).map(|(k, _)| k.clone()).collect::<Vec<_>>();
        let mut result: VecDeque<(Vec<u8>, Vec<u8>)> = VecDeque::new();
        for key in keys {
            let value = self.data.remove(&key);
            if let Some(val) = value {
                result.push_back((key, val));
            }
        }
        Box::new(OwnedRangeIterator { it: result })
    }

    fn remove_prefix(&mut self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let iter = self.scan_prefix(prefix);
        let keys = iter
            .map(|x| {
                let (k, _) = x?;
                Ok(k)
            })
            .collect::<Result<Vec<Vec<u8>>>>()?;

        let mut result = Vec::new();
        for key in &keys {
            let res = self.data.remove(key);
            if let Some(val) = res {
                result.push((key.to_vec(), val));
            }
        }
        Ok(result)
    }
}

pub struct OwnedRangeIterator {
    pub it: VecDeque<(Vec<u8>, Vec<u8>)>,
}

impl Iterator for OwnedRangeIterator {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.it.pop_front().map(Ok)
    }
}

impl DoubleEndedIterator for OwnedRangeIterator {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.it.pop_back().map(Ok)
    }
}

pub struct RangeIterator<'a> {
    it: Range<'a, Vec<u8>, Vec<u8>>,
}

impl Iterator for RangeIterator<'_> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.it.next().map(|item| {
            let (k, v) = item;
            Ok((k.clone(), v.clone()))
        })
    }
}
impl DoubleEndedIterator for RangeIterator<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.it.next_back().map(|item| {
            let (k, v) = item;
            Ok((k.clone(), v.clone()))
        })
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
