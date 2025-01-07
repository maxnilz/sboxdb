use std::collections::btree_map::Range;
use std::collections::{BTreeMap, VecDeque};
use std::ops::Bound;

use super::{Key, ScanIterator, Storage};
use crate::error::Result;

#[derive(Debug, Clone)]
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

    fn set(&mut self, key: Key, value: Vec<u8>) -> Result<()> {
        self.data.insert(key.encode()?, value);
        Ok(())
    }

    fn get(&self, key: Key) -> Result<Option<Vec<u8>>> {
        let key = key.encode()?;
        let value = self.data.get(&key);
        Ok(value.cloned())
    }

    fn scan(&self, range: (Bound<Key>, Bound<Key>)) -> Box<dyn ScanIterator<'_> + '_> {
        let r = (range.0.map(|k| k.encode().unwrap()), range.1.map(|k| k.encode().unwrap()));
        let result = self.data.range(r);
        Box::new(RangeIterator { it: result })
    }

    fn remove(&mut self, key: Key) -> Result<Option<Vec<u8>>> {
        let key = key.encode()?;
        Ok(self.data.remove(&key))
    }

    fn remove_range(&mut self, range: (Bound<Key>, Bound<Key>)) -> Box<dyn ScanIterator<'_> + '_> {
        let r = (range.0.map(|k| k.encode().unwrap()), range.1.map(|k| k.encode().unwrap()));
        let keys = self.data.range(r).map(|(k, _)| k.clone()).collect::<Vec<_>>();
        let mut result: VecDeque<(Vec<u8>, Vec<u8>)> = VecDeque::new();
        for key in keys {
            let value = self.data.remove(&key);
            if let Some(val) = value {
                result.push_back((key, val));
            }
        }
        Box::new(OwnedRangeIterator { it: result })
    }

    fn remove_prefix(&mut self, prefix: &[u8]) -> Result<Vec<Vec<u8>>> {
        let iter = self.scan_prefix(prefix);
        let keys = iter
            .map(|x| {
                let (k, _) = x?;
                Ok(k)
            })
            .collect::<Result<Vec<Vec<u8>>>>()?;

        let mut values = vec![];
        for key in &keys {
            let res = self.data.remove(key);
            if let Some(val) = res {
                values.push(val);
            }
        }
        Ok(values)
    }
}

struct OwnedRangeIterator {
    it: VecDeque<(Vec<u8>, Vec<u8>)>,
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
        assert_eq!(None, m.get(key!(b"a"))?);

        // set & get key
        m.set(key!(b"a"), vec![1])?;
        assert_eq!(Some(vec![1]), m.get(key!(b"a"))?);

        // delete
        let got = m.remove(key!(b"a"))?;
        assert_eq!(Some(vec![1]), got);

        // get again
        assert_eq!(None, m.get(key!(b"a"))?);

        Ok(())
    }

    #[test]
    fn text_prefix_scan() -> Result<()> {
        let mut m = Memory::new();

        // set values
        m.set(key!(b"apple"), vec![1])?;
        m.set(key!(b"app"), vec![2])?;
        m.set(key!(b"banala"), vec![3])?;
        m.set(key!(b"ba"), vec![4])?;
        m.set(key!(b"cat\x1F"), vec![5])?;
        m.set(key!(b"cat\x1E"), vec![6])?;
        m.set(key!(b"cat\x20"), vec![7])?;

        // scan prefix
        let iter = m.scan_prefix(b"app");
        let values = iter.map(|it| it.unwrap().1).collect::<Vec<_>>();
        assert_eq!(vec![vec![2], vec![1]], values);

        // scan prefix
        let iter = m.scan_prefix(b"cat");
        let values = iter.map(|it| it.unwrap().1).collect::<Vec<_>>();
        assert_eq!(vec![vec![6], vec![5], vec![7]], values);

        // scan prefix
        m.set(key!(vec![1, 1, 1]), vec![8])?;
        m.set(key!(vec![1, 1, 2]), vec![9])?;
        m.set(key!(vec![1, 2, 1]), vec![10])?;
        m.set(key!(vec![1, 2, 2]), vec![11])?;

        let iter = m.scan_prefix(&vec![1, 1]);
        let values = iter.map(|it| it.unwrap().1).collect::<Vec<_>>();
        assert_eq!(vec![vec![8], vec![9]], values);

        Ok(())
    }

    #[test]
    fn test_range_scan() -> Result<()> {
        let mut m = Memory::new();

        // set values
        m.set(key!(b"a"), vec![1])?;
        m.set(key!(b"b"), vec![2])?;
        m.set(key!(b"c"), vec![3])?;
        m.set(key!(b"d"), vec![4])?;

        // range scan
        let start = Bound::Included(key!(b"a"));
        let end = Bound::Excluded(key!(b"d"));
        let iter = m.scan((start, end));
        let values = iter.map(|it| it.unwrap().1).collect::<Vec<_>>();
        assert_eq!(vec![vec![1], vec![2], vec![3]], values);

        // set more values
        m.set(key!(vec![1, 1, 1]), vec![8])?;
        m.set(key!(vec![1, 1, 2]), vec![9])?;
        m.set(key!(vec![1, 2, 1]), vec![10])?;
        m.set(key!(vec![1, 2, 2]), vec![11])?;

        // range scan with half open range
        let from = Bound::Included(key!(vec![1, 1]));
        let to = Bound::Excluded(key!(vec![1, 255]));
        let iter = m.scan((from, to));
        let values = iter.map(|it| it.unwrap().1).collect::<Vec<_>>();
        assert_eq!(vec![vec![8], vec![9], vec![10], vec![11]], values);

        // range scan on [from, to] with the exactly matched key,
        // since the key `vec![1, 2, 2]` is included in the range,
        // the result should contain the value of `vec![1, 2, 2]`.
        let from = Bound::Included(key!(vec![1, 1, 1]));
        let to = Bound::Included(key!(vec![1, 2, 2]));
        let iter = m.scan((from, to));
        let values = iter.map(|it| it.unwrap().1).collect::<Vec<_>>();
        assert_eq!(vec![vec![8], vec![9], vec![10], vec![11]], values);

        // range scan on [from, to] with the exactly matched key,
        // since the key `vec![1, 2]` is not an exact key in the storage,
        // the result should not contain any value greater that `vec![1, 2]`,
        // and vec[1, 2] itself.
        let from = Bound::Included(key!(vec![1, 1]));
        let to = Bound::Included(key!(vec![1, 2]));
        let iter = m.scan((from, to));
        let values = iter.map(|it| it.unwrap().1).collect::<Vec<_>>();
        assert_eq!(vec![vec![8], vec![9]], values);

        Ok(())
    }
}
