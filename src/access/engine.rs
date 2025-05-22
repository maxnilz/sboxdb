use crate::access::expression::Expression;
use crate::access::value::{IndexKey, PrimaryKey, Tuple};
use crate::catalog::catalog::Catalog;
use crate::catalog::index::Index;
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
    fn commit(self) -> Result<()>;
    /// Rolls back the transaction
    fn rollback(self) -> Result<()>;

    /// Inserts a new table row
    fn insert(&mut self, tblname: &str, tuple: Tuple) -> Result<PrimaryKey>;
    /// Deletes a table row
    fn delete(&mut self, tblname: &str, pk: &PrimaryKey) -> Result<()>;
    /// Reads a table row, if it exists
    fn get(&self, tblname: &str, pk: &PrimaryKey) -> Result<Option<Tuple>>;
    /// Scan a table
    fn scan(&self, tblname: &str, predicate: Option<Expression>) -> Result<Scan>;
    /// drop table data
    fn drop(&self, tblname: &str) -> Result<()>;

    /// Insert an index entry to the index
    fn insert_index_entry(&mut self, index: Index, tuple: &Tuple) -> Result<()>;
    /// Delete an index entry from index
    fn delete_index_entry(
        &mut self,
        index: Index,
        index_key: IndexKey,
        pk: &PrimaryKey,
    ) -> Result<()>;
    /// Gets an index entry from index, if it exists
    fn get_index_entry(
        &self,
        tblname: &str,
        indname: &str,
        index_key: IndexKey,
    ) -> Result<Option<Vec<Tuple>>>;
    /// Scan index entries
    fn scan_index_entries(&self, tblname: &str, indname: &str) -> Result<IndexScan>;

    /// Delete index entries
    fn delete_index_entries(&self, tblname: &str, indname: &str) -> Result<()>;
}
