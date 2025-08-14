use crate::access::predicate::Predicate;
use crate::access::value::IndexKey;
use crate::access::value::PrimaryKey;
use crate::access::value::Tuple;
use crate::catalog::catalog::Catalog;
use crate::error::Result;

/// The Transactional access engine interface
pub trait Engine {
    type Transaction: Transaction;

    /// Begins a read-write transaction.
    fn begin(&self) -> Result<Self::Transaction>;

    /// Begins a read-only transaction.
    fn begin_read_only(&self) -> Result<Self::Transaction>;

    /// Begins a read-only transaction as of a historical version.
    fn begin_as_of(&self, version: u64) -> Result<Self::Transaction>;
}

pub type Scan = Box<dyn DoubleEndedIterator<Item = Result<Tuple>>>;

pub type IndexScan = Box<dyn DoubleEndedIterator<Item = (IndexKey, Vec<Tuple>)>>;

/// Relation oriented transaction.
pub trait Transaction: Catalog {
    /// The transaction's version
    fn version(&self) -> u64;

    /// Whether the transaction is read-only
    fn read_only(&self) -> bool;

    /// Commits the transaction
    fn commit(&self) -> Result<()>;
    /// Rolls back the transaction
    fn rollback(&self) -> Result<()>;

    /// Inserts a new table row
    fn insert(&self, table: &str, tuple: Tuple) -> Result<PrimaryKey>;
    /// Deletes a table row
    fn delete(&self, table: &str, pk: &PrimaryKey) -> Result<()>;
    /// Reads a table row, if it exists
    fn read(&self, table: &str, pk: &PrimaryKey) -> Result<Option<Tuple>>;
    /// Scan a table with optional pushdown eligible predicate
    fn scan(&self, table: &str, predicate: Option<Predicate>) -> Result<Scan>;
    /// drop table data
    fn drop(&self, table: &str) -> Result<()>;

    /// Reads an index entry from index, if it exists
    fn read_index_entry(
        &self,
        table: &str,
        index: &str,
        index_key: IndexKey,
    ) -> Result<Option<Vec<Tuple>>>;
    /// Scan index entries
    fn scan_index_entries(&self, table: &str, index: &str) -> Result<IndexScan>;
}
