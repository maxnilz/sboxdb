use crate::error::Result;

/// A key/value storage engine, where both keys and values are arbitrary byte
/// strings. stored in lexicographical key order. Writes are only guaranteed
/// durable after calling flush().
trait Engine {
    /// Flushes any buffered data to underlying storage medium.
    fn flush(&self) -> Result<()>;

    /// Sets a value for a key, overwrite the existing value if any.
    fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()>;

    /// Gets the value with a given key.
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
}

mod memory;
