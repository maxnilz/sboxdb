use std::collections::btree_map::Range;
use std::collections::BTreeMap;
use std::ops::Bound;

use crate::error::Result;
use crate::storage::{ScanIterator, Storage};

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
        Box::new(MemScanIterator { it: result })
    }

    fn delete(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let res = self.data.remove(key);
        Ok(res)
    }
}

pub struct MemScanIterator<'a> {
    it: Range<'a, Vec<u8>, Vec<u8>>,
}
impl<'a> DoubleEndedIterator for MemScanIterator<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.it.next_back().map(|item| {
            let (k, v) = item;
            Ok((k.clone(), v.clone()))
        })
    }
}

impl Iterator for MemScanIterator<'_> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.it.next().map(|item| {
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
        let got = m.delete(b"a")?;
        assert_eq!(Some(vec![1]), got);

        // get again
        assert_eq!(None, m.get(b"a")?);

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
        m.set(b"e", vec![5])?;

        // range scan
        let start = Bound::Included(b"a".to_vec());
        let end = Bound::Excluded(b"d".to_vec());
        let iter = m.scan((start, end));
        let values = iter.map(|it| it.unwrap().1).collect::<Vec<_>>();
        assert_eq!(vec![vec![1], vec![2], vec![3]], values);

        Ok(())
    }
}
