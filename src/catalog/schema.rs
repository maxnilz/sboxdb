use std::sync::Arc;

use serde::Deserialize;
use serde::Serialize;

use crate::catalog::column::Columns;
use crate::error::Error;
use crate::error::Result;

/// A reference-counted reference to a [`Schema`].
pub type SchemaRef = Arc<Schema>;

/// Describes the meta-data of an ordered sequence of relative
/// types, e.g. table schema etc.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Schema {
    /// Schema name, a qualifier name the columns belongs.
    pub name: String,
    /// A sequence of columns that describe the schema.
    pub columns: Columns,
}

/// A table scan iterator
pub type Schemas = Box<dyn DoubleEndedIterator<Item = Schema>>;

impl Schema {
    pub fn new(name: String, columns: Columns) -> Schema {
        Schema { name, columns }
    }

    pub fn empty() -> Schema {
        Schema { name: "".to_string(), columns: Columns::empty() }
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
