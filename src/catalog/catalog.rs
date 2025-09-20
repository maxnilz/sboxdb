use crate::catalog::index::Index;
use crate::catalog::index::Indexes;
use crate::catalog::schema::Schema;
use crate::catalog::schema::Schemas;
use crate::error::Result;
use crate::value_err;

/// The catalog stores schema information. It handles table
/// creation, table lookup, index creation, and index lookup.
pub trait Catalog: Sync + Send {
    /// Reads a table, if it exists
    fn read_table(&self, table: &str) -> Result<Option<Schema>>;

    /// Reads a table, and errors if it does not exist
    fn must_read_table(&self, table: &str) -> Result<Schema> {
        self.read_table(table)?.ok_or_else(|| value_err!("Table {} does not exist", table))
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

pub struct TodoCatalog {}
impl Catalog for TodoCatalog {
    fn read_table(&self, _table: &str) -> Result<Option<Schema>> {
        todo!()
    }

    fn create_table(&self, _schema: Schema) -> Result<()> {
        todo!()
    }

    fn delete_table(&self, _table: &str) -> Result<()> {
        todo!()
    }

    fn scan_tables(&self) -> Result<Schemas> {
        todo!()
    }

    fn read_index(&self, _index: &str, _table: &str) -> Result<Option<Index>> {
        todo!()
    }

    fn create_index(&self, _index: Index) -> Result<()> {
        todo!()
    }

    fn delete_index(&self, _index: &str, _table: &str) -> Result<()> {
        todo!()
    }

    fn scan_table_indexes(&self, _table: &str) -> Result<Indexes> {
        todo!()
    }
}
