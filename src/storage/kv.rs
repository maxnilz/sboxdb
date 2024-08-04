use crate::error::Result;
use serde::Deserialize;
use std::collections::Bound;
use std::fmt::Debug;
use std::iter::once;

mod memory;

pub trait ScanIterator<'a>: DoubleEndedIterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a {}

// A blanket implementation to ensure that any type T that satisfies the
// constraints of being a DoubleEndedIterator with an item type of
// Result<(Vec<u8>, Vec<u8>)> and having a lifetime 'a, automatically
// qualifies as an implementation of ScanIterator<'a>.
// Without this blanket implementation, anyone defining a new type that
// they want to use as a ScanIterator would need to manually implement the
// ScanIterator trait for each type. The blanket implementation simplifies
// this by automatically making many potential iterator types available as
// ScanIterator instances without additional code.
impl<'a, T> ScanIterator<'a> for T where
    T: DoubleEndedIterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a
{
}

/// A key/value storage engine, where both keys and values are arbitrary byte
/// strings. stored in lexicographical key order. Writes are only guaranteed
/// durable after calling flush().
///
/// The Storage trait is designed as `trait object` compatible, i.e., follow
/// the [object safety rules](https://doc.rust-lang.org/reference/items/traits.html#object-safety)
/// e.g., the method `scan` is using `(Bound<Vec<u8>>, Bound<Vec<u8>>)` as the range parameter type
/// instead of a generic involved type `std::ops::RangeBounds`.
pub trait KvStorage: Debug + Send + Sync {
    /// Flushes any buffered data to underlying storage medium.
    fn flush(&self) -> Result<()>;

    /// Sets a value for a key, overwrite the existing value if any.
    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()>;

    /// Gets the value with a given key.
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
    fn scan_prefix(&self, prefix: &[u8]) -> Box<dyn ScanIterator<'_> + '_> {
        let start = Bound::Included(prefix.to_vec());
        let breaker = prefix.iter().rposition(|b| *b == 0xff);
        let end = match breaker {
            None => Bound::Unbounded,
            Some(i) => {
                Bound::Excluded(prefix.iter().take(i).copied().chain(once(prefix[i] + 1)).collect())
            }
        };
        self.scan((start, end))
    }

    /// Removes a key from the storage, returning the value at the key if the key
    /// was previously in the storage.
    /// The key may be any borrowed form of the map's key type, but the ordering
    /// on the borrowed form must match the ordering on the key type.
    fn remove(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Remove all keys starting with prefix, returning the values that are removed from.
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
            let res = self.remove(&key)?;
            if let Some(val) = res {
                values.push(val);
            }
        }
        Ok(values)
    }
}

#[derive(Debug, PartialEq, Deserialize)]
pub enum StorageType {
    Memory,
}

pub fn new_storage(typ: StorageType) -> Result<Box<dyn KvStorage>> {
    match typ {
        StorageType::Memory => Ok(Box::new(memory::Memory::new())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fn1<E: KvStorage>(s: E) -> Result<()> {
        s.flush()
    }

    struct Struct1<E: KvStorage> {
        storage: E,
    }

    // test generic trait
    #[test]
    fn test_new_v1() -> Result<()> {
        let m = memory::Memory::new();
        fn1(m)?;

        let m = memory::Memory::new();
        let _s1: Struct1<memory::Memory> = Struct1 { storage: m };

        Ok(())
    }

    // test `impl Trait`
    fn new_storage_v2(_typ: StorageType) -> impl KvStorage {
        let m = memory::Memory::new();
        m
    }

    #[test]
    fn test_new_v2() -> Result<()> {
        let m = memory::Memory::new();
        fn1(m)?;

        let m = new_storage_v2(StorageType::Memory);
        let s1 = Struct1 { storage: m };
        s1.storage.flush()?;

        Ok(())
    }

    // test `impl Trait` with incompatible type
    //
    // implement noop types for ScanIterator, Storage for
    // `impl Trait` test.
    struct ScanIteratorNoop {}

    impl DoubleEndedIterator for ScanIteratorNoop {
        fn next_back(&mut self) -> Option<Self::Item> {
            None
        }
    }

    impl Iterator for ScanIteratorNoop {
        type Item = Result<(Vec<u8>, Vec<u8>)>;

        fn next(&mut self) -> Option<Self::Item> {
            None
        }
    }

    #[derive(Debug)]
    struct StorageNoop {}
    impl KvStorage for StorageNoop {
        fn flush(&self) -> Result<()> {
            Ok(())
        }

        fn set(&mut self, _key: &[u8], _value: Vec<u8>) -> Result<()> {
            Ok(())
        }

        fn get(&self, _key: &[u8]) -> Result<Option<Vec<u8>>> {
            Ok(None)
        }

        fn scan(&self, _range: (Bound<Vec<u8>>, Bound<Vec<u8>>)) -> Box<dyn ScanIterator<'_> + '_> {
            Box::new(ScanIteratorNoop {})
        }

        fn remove(&mut self, _key: &[u8]) -> Result<Option<Vec<u8>>> {
            Ok(None)
        }
    }

    fn new_storage_v3(typ: &str) -> impl KvStorage {
        // impl Trait allows us to “erase”
        // the type of a return value, specifying only the trait or traits
        // it implements, without dynamic dispatch or a heap
        // allocation.
        //
        // impl Trait is a form of static
        // dispatch, so the compiler has to know the type being
        // returned from the function at compile time in order to
        // allocate the right amount of space on the stack and
        // correctly access ﬁelds and methods on that type.
        //
        // error: `match` arms have incompatible types
        match typ {
            // "memory" => memory::Memory::new(),
            _ => StorageNoop {},
        }
    }

    #[test]
    fn test_new_v3() -> Result<()> {
        let m = new_storage_v3("memory");
        let s1 = Struct1 { storage: m };
        s1.storage.flush()?;

        Ok(())
    }

    // test trait object for factory pattern that’s
    // commonly used in object-oriented languages.
    fn new_storage_v4() -> Box<dyn KvStorage> {
        Box::new(StorageNoop {})
    }

    #[test]
    fn test_new_v4() -> Result<()> {
        let s = new_storage_v4();
        s.flush()?;
        Ok(())
    }
}
