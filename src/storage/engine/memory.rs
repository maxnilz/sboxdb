use std::collections::BTreeMap;

use crate::error::Result;
use crate::storage::engine::Engine;

struct Memory {
    data: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl Memory {
    fn new() -> Memory {
        Memory { data: BTreeMap::new() }
    }
}

impl Engine for Memory {
    fn flush(&self) -> Result<()> {
        Ok(())
    }

    fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let key = Vec::from(key);
        let value = Vec::from(value);
        self.data.insert(key, value);
        Ok(())
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let value = self.data.get(key);
        Ok(value.cloned())
    }
}

#[cfg(test)]
mod tests {}
