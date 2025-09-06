use std::collections::Bound;
use std::fmt::Debug;
use std::iter::once;

use serde::Deserialize;
use serde::Serialize;

use crate::error::Result;

pub mod codec;
pub mod heap;

pub mod memory;

/// A key/value storage engine, where both keys and values are arbitrary byte
/// strings. stored in lexicographical key order. Writes are only guaranteed
/// durable after calling flush().
/// The Storage trait is designed as `trait object` compatible, i.e., follow
/// the [object safety rules](https://doc.rust-lang.org/reference/items/traits.html#object-safety)
/// e.g., the method `scan` is using `(Bound<Vec<u8>>, Bound<Vec<u8>>)` as the range parameter type
/// instead of a generic involved type `std::ops::RangeBounds`.
pub trait Storage: Debug + Send + Sync {
    /// Flushes any buffered data to underlying storage medium.
    fn flush(&self) -> Result<()>;

    /// Sets a value for a key with a given version, overwrite the existing value if any.
    /// The version is used to distinguish the different versions of the same key.
    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()>;

    /// Gets the value with a given key and version.
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Iterates over the key/values pares with the given key range
    /// by returning a trait object of `ScanIterator` we are using the dynamic
    /// dispatch, which has a minor performance penalty(compare with using generic trait)
    fn scan(&self, range: (Bound<Vec<u8>>, Bound<Vec<u8>>)) -> Box<dyn ScanIterator<'_> + '_>;

    /// Iterates over all key/value pairs starting with prefix.
    /// Since we assume the key is always lexicographic order,
    /// scan_prefix can simply use the prefix as the range.start
    /// e.g., `app` is smaller than `apple`, meanwhile, `app` is
    /// also the prefix of `apple`.
    fn scan_prefix(&self, prefix: &[u8]) -> Box<dyn ScanIterator<'_> + '_>;

    /// Removes a key from the storage, returning the value at the key if the key
    /// was previously in the storage.
    /// The key may be any borrowed form of the map's key type, but the ordering
    /// on the borrowed form must match the ordering on the key type.
    fn remove(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    fn remove_range(
        &mut self,
        range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
    ) -> Box<dyn ScanIterator<'_> + '_>;

    /// Remove all keys starting with prefix, returning the values that are removed from.
    fn remove_prefix(&mut self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;
}

pub fn prefix_range(prefix: &[u8]) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
    let start = Bound::Included(prefix.to_vec());
    let breaker = prefix.iter().rposition(|&b| b != 0xff);
    let end = match breaker {
        None => Bound::Unbounded,
        Some(i) => {
            let a: Vec<u8> = prefix.iter().take(i).copied().chain(once(prefix[i] + 1)).collect();
            Bound::Excluded(a)
        }
    };
    (start, end)
}

pub trait ScanIterator<'a>:
    DoubleEndedIterator<Item = Result<(Vec<u8>, Vec<u8>)>> + Debug + 'a
{
}

// A blanket implementation to ensure that any type T that satisfies the
// constraints of being a DoubleEndedIterator with an item type of
// Result<(Vec<u8>, Vec<u8>)> and having a lifetime 'a, automatically
// qualifies as an implementation of ScanIterator<'a>.
// Without this blanket implementation, anyone defining a new type that
// they want to use as a ScanIterator would need to manually implement the
// ScanIterator trait for each type. The blanket implementation simplifies
// this by automatically making many potential iterator types available as
// ScanIterator instances without additional code.
impl<'a, T: Debug> ScanIterator<'a> for T where
    T: DoubleEndedIterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a
{
}

#[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum StorageType {
    Memory,
}

pub fn new_storage(typ: StorageType) -> Result<Box<dyn Storage>> {
    match typ {
        StorageType::Memory => Ok(Box::new(memory::Memory::new())),
    }
}
