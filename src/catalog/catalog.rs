use crate::catalog::index::{Index, Indexes};
use crate::catalog::table::{Table, Tables};
use crate::error::{Error, Result};

/// The catalog stores schema information. It handles table
/// creation, table lookup, index creation, and index lookup.
pub trait Catalog {
    /// Reads a table, if it exists
    fn read_table(&self, table_name: &str) -> Result<Option<Table>>;

    /// Reads a table, and errors if it does not exist
    fn must_read_table(&self, table: &str) -> Result<Table> {
        self.read_table(table)?
            .ok_or_else(|| Error::value(format!("Table {} does not exist", table)))
    }

    /// Creates a new table
    fn create_table(&self, table: Table) -> Result<()>;

    /// Deletes a table with the given table name, or errors
    /// if it does not exist.
    fn delete_table(&self, table_name: &str) -> Result<()>;

    /// Scan all tables
    fn scan_tables(&self) -> Result<Tables>;

    /// Gets an index with the given index for given table
    fn read_index(&self, index_name: &str, table_name: &str) -> Result<Option<Index>>;

    /// Creates an index
    fn create_index(&self, index: Index) -> Result<()>;

    /// Deletes an index with the given index & table, or errors
    /// if it does not exist
    fn delete_index(&self, index_name: &str, table_name: &str) -> Result<()>;

    /// Scan all the indexes for the table identified by table_name
    fn scan_table_indexes(&self, table_name: &str) -> Result<Indexes>;
}
