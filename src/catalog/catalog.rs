use crate::catalog::index::{Index, Indexes};
use crate::catalog::schema::{Schema, Schemas};
use crate::error::{Error, Result};

/// The catalog stores schema information. It handles table
/// creation, table lookup, index creation, and index lookup.
pub trait Catalog {
    /// Reads a table, if it exists
    fn read_table(&self, table: &str) -> Result<Option<Schema>>;

    /// Reads a table, and errors if it does not exist
    fn must_read_table(&self, table: &str) -> Result<Schema> {
        self.read_table(table)?
            .ok_or_else(|| Error::value(format!("Table {} does not exist", table)))
    }

    /// Creates a new table
    fn create_table(&self, schema: Schema) -> Result<()>;

    /// Deletes a table with the given table name, or errors
    /// if it does not exist.
    fn delete_table(&self, table: &str) -> Result<()>;

    /// Scan all tables
    fn scan_tables(&self) -> Result<Schemas>;

    /// Gets an index with the given index for given table
    fn read_index(&self, index: &str, table: &str) -> Result<Option<Index>>;

    /// Creates an index
    fn create_index(&self, index: Index) -> Result<()>;

    /// Deletes an index with the given index & table, or errors
    /// if it does not exist
    fn delete_index(&self, index: &str, table: &str) -> Result<()>;

    /// Scan all the indexes for the table identified by table_name
    fn scan_table_indexes(&self, table: &str) -> Result<Indexes>;
}
