use std::collections::Bound;

use crate::storage::{ScanIterator, Storage};

pub mod bufferpool;
pub mod page;
pub mod replacer;

#[derive(Debug)]
pub struct Heap {}

impl Storage for Heap {
    fn flush(&self) -> crate::error::Result<()> {
        todo!()
    }

    fn set(&mut self, _key: &[u8], _value: Vec<u8>) -> crate::error::Result<()> {
        todo!()
    }

    fn get(&self, _key: &[u8]) -> crate::error::Result<Option<Vec<u8>>> {
        todo!()
    }

    fn scan(&self, _range: (Bound<Vec<u8>>, Bound<Vec<u8>>)) -> Box<dyn ScanIterator<'_> + '_> {
        todo!()
    }
    fn scan_prefix(&self, _prefix: &[u8]) -> Box<dyn ScanIterator<'_> + '_> {
        todo!()
    }

    fn remove(&mut self, _key: &[u8]) -> crate::error::Result<Option<Vec<u8>>> {
        todo!()
    }

    fn remove_range(
        &mut self,
        _range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
    ) -> Box<dyn ScanIterator<'_> + '_> {
        todo!()
    }

    fn remove_prefix(&mut self, _prefix: &[u8]) -> crate::error::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        todo!()
    }
}
