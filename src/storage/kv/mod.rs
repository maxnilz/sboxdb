use std::collections::Bound;
use std::fmt::Debug;
use std::iter::once;

use serde::{Deserialize, Serialize};

use codec::keycodec;

use crate::error::Result;

pub type Version = u64;

const MAX_VERSION: Version = u64::MAX;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Key {
    #[serde(with = "serde_bytes")]
    pub key: Vec<u8>,
    pub version: Option<Version>,
}

impl Key {
    fn encode(&self) -> Result<Vec<u8>> {
        keycodec::serialize(self)
    }
}

#[macro_export]
macro_rules! key {
    ($k:literal, $v:expr) => {
        $crate::storage::kv::Key { key: $k.to_vec(), version: Some($v) }
    };
    ($k:literal) => {
        $crate::storage::kv::Key { key: $k.to_vec(), version: None }
    };
    ($k:expr, $v:expr) => {
        $crate::storage::kv::Key { key: $k, version: Some($v) }
    };
    ($k:expr) => {
        $crate::storage::kv::Key { key: $k, version: None }
    };
}

/// A key/value storage engine, where both keys and values are arbitrary byte
/// strings. stored in lexicographical key order. Writes are only guaranteed
/// durable after calling flush().
///
/// The storage is designed as a version-awareness storage, e.g., setv, getv, removev
/// methods to support the versioned key-value pairs. The version is used to distinguish
/// the different versions of the same key, and it is appended to the tail of the actual
/// key, i.e., the same key with different version would have same prefix but different
/// suffix.
///
/// The Storage trait is designed as `trait object` compatible, i.e., follow
/// the [object safety rules](https://doc.rust-lang.org/reference/items/traits.html#object-safety)
/// e.g., the method `scan` is using `(Bound<Vec<u8>>, Bound<Vec<u8>>)` as the range parameter type
/// instead of a generic involved type `std::ops::RangeBounds`.
pub trait Storage: Debug + Send + Sync {
    /// Flushes any buffered data to underlying storage medium.
    fn flush(&self) -> Result<()>;

    /// Sets a value for a key with a given version, overwrite the existing value if any.
    /// The version is used to distinguish the different versions of the same key.
    fn set(&mut self, key: Key, value: Vec<u8>) -> Result<()>;

    /// Gets the value with a given key and version.
    fn get(&self, key: Key) -> Result<Option<Vec<u8>>>;

    /// Iterates over the key/values pares with the given key range
    /// by returning a trait object of `ScanIterator` we are using the dynamic
    /// dispatch, which has a minor performance penalty(compare with using generic trait)
    fn scan(&self, range: (Bound<Key>, Bound<Key>)) -> Box<dyn ScanIterator<'_> + '_>;

    /// Iterates over all key/value pairs starting with prefix.
    /// Since we assume the key is always lexicographic order,
    /// scan_prefix can simply use the prefix as the range.start
    /// e.g., `app` is smaller than `apple`, meanwhile, `app` is
    /// also the prefix of `apple`.
    fn scan_prefix(&self, prefix: &[u8]) -> Box<dyn ScanIterator<'_> + '_> {
        let start = Bound::Included(Key { key: prefix.to_vec(), version: None });
        let breaker = prefix.iter().rposition(|&b| b != 0xff);
        let end = match breaker {
            None => Bound::Unbounded,
            Some(i) => Bound::Excluded(key!(
                prefix.iter().take(i).copied().chain(once(prefix[i] + 1)).collect(),
                MAX_VERSION
            )),
        };
        self.scan((start, end))
    }

    /// Removes a key from the storage, returning the value at the key if the key
    /// was previously in the storage.
    /// The key may be any borrowed form of the map's key type, but the ordering
    /// on the borrowed form must match the ordering on the key type.
    fn remove(&mut self, key: Key) -> Result<Option<Vec<u8>>>;

    fn remove_range(&mut self, range: (Bound<Key>, Bound<Key>)) -> Box<dyn ScanIterator<'_> + '_>;

    /// Remove all keys starting with prefix, returning the values that are removed from.
    fn remove_prefix(&mut self, prefix: &[u8]) -> Result<Vec<Vec<u8>>>;
}

pub trait ScanIterator<'a>: DoubleEndedIterator<Item=Result<(Vec<u8>, Vec<u8>)>> + 'a {}

// A blanket implementation to ensure that any type T that satisfies the
// constraints of being a DoubleEndedIterator with an item type of
// Result<(Vec<u8>, Vec<u8>)> and having a lifetime 'a, automatically
// qualifies as an implementation of ScanIterator<'a>.
// Without this blanket implementation, anyone defining a new type that
// they want to use as a ScanIterator would need to manually implement the
// ScanIterator trait for each type. The blanket implementation simplifies
// this by automatically making many potential iterator types available as
// ScanIterator instances without additional code.
impl<'a, T> ScanIterator<'a> for T
where
    T: DoubleEndedIterator<Item=Result<(Vec<u8>, Vec<u8>)>> + 'a,
{}

pub mod codec;
pub mod memory;

#[derive(Debug, PartialEq, Deserialize)]
pub enum StorageType {
    Memory,
}

pub fn new_storage(typ: StorageType) -> Result<Box<dyn Storage>> {
    match typ {
        StorageType::Memory => Ok(Box::new(memory::Memory::new())),
    }
}
