use serde::{Deserialize, Serialize};

use crate::catalog::column::{Column, Columns};
use crate::error::Error;
use crate::error::Result;

/// Table holds metadata about table
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Table {
    /// Table name
    pub name: String,
    /// User-orientated Table columns
    pub columns: Columns,
}

/// A table scan iterator
pub type Tables = Box<dyn DoubleEndedIterator<Item = Table>>;

impl Table {
    pub fn new(name: String, columns: Vec<Column>) -> Table {
        Table { name, columns: Columns::new(columns) }
    }
    pub fn validate(&self) -> Result<()> {
        if self.name.is_empty() {
            return Err(Error::value("Table name can't be empty"));
        }
        if self.columns.is_empty() {
            return Err(Error::value(format!("Table {} have no columns", self.name)));
        }
        match self.columns.iter().filter(|it| it.primary_key).count() {
            1 => {}
            0 => return Err(Error::value(format!("No primary key in table {}", self.name))),
            _ => return Err(Error::value(format!("Multiple primary keys in table {}", self.name))),
        };
        self.columns.validate()?;
        Ok(())
    }
}
