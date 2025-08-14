use std::fmt::Display;
use std::fmt::Formatter;
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

impl Default for Schema {
    fn default() -> Self {
        Schema::empty()
    }
}

impl Display for Schema {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let sep = if f.alternate() { "\n" } else { "" };
        write!(f, "TABLE {}(", self.name)?;
        write!(f, "{}", sep)?;
        for (i, col) in self.columns.iter().enumerate() {
            write!(f, "{}", col)?;
            if i < self.columns.len() - 1 {
                write!(f, ", {}", sep)?;
            }
        }
        write!(f, ")")?;
        Ok(())
    }
}

/// A table scan iterator
pub type Schemas = Box<dyn DoubleEndedIterator<Item = Schema>>;

impl Schema {
    pub fn new(name: impl Into<String>, columns: impl Into<Columns>) -> Schema {
        Schema { name: name.into(), columns: columns.into() }
    }

    pub fn try_new(name: impl Into<String>, columns: impl Into<Columns>) -> Result<Schema> {
        let schema = Schema { name: name.into(), columns: columns.into() };
        schema.validate()?;
        Ok(schema)
    }

    pub fn empty() -> Schema {
        Schema::new("", Columns::empty())
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
