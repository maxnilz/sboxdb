use std::fmt::Display;
use std::fmt::Formatter;
use std::ops::Deref;
use std::sync::Arc;
use std::vec::IntoIter;

use serde::Deserialize;
use serde::Serialize;

use crate::catalog::column::Columns;
use crate::error::Result;
use crate::value_err;

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
    /// A list of schema constraints
    pub constraints: Constraints,
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
    pub fn new(
        name: impl Into<String>,
        columns: impl Into<Columns>,
        constraints: impl Into<Constraints>,
    ) -> Schema {
        Schema { name: name.into(), columns: columns.into(), constraints: constraints.into() }
    }

    pub fn try_new(
        name: impl Into<String>,
        columns: impl Into<Columns>,
        constraints: impl Into<Constraints>,
    ) -> Result<Schema> {
        let schema =
            Schema { name: name.into(), columns: columns.into(), constraints: constraints.into() };
        schema.validate()?;
        Ok(schema)
    }

    pub fn empty() -> Schema {
        Schema::new("", Columns::empty(), Constraints::empty())
    }

    pub fn validate(&self) -> Result<()> {
        if self.name.is_empty() {
            return Err(value_err!("Table name can't be empty"));
        }
        if self.columns.is_empty() {
            return Err(value_err!("Table {} have no columns", self.name));
        }
        self.columns.validate()?;
        // check schema primary key
        if self.columns.primary_key().is_empty() {
            return Err(value_err!("Primary key not found"));
        }
        // check constraints
        for c in self.constraints.iter() {
            match c {
                Constraint::Unique(cols) => {
                    for &i in cols {
                        self.columns
                            .get(i)
                            .ok_or_else(|| value_err!("No column at {} as unique key column", i))?;
                    }
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum Constraint {
    Unique(Vec<usize>),
}

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Constraints {
    inner: Vec<Constraint>,
}

impl Constraints {
    pub fn empty() -> Self {
        Self { inner: vec![] }
    }
}

impl IntoIterator for Constraints {
    type Item = Constraint;
    type IntoIter = IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.into_iter()
    }
}

impl Deref for Constraints {
    type Target = [Constraint];

    fn deref(&self) -> &Self::Target {
        self.inner.as_slice()
    }
}

impl From<Vec<Constraint>> for Constraints {
    fn from(value: Vec<Constraint>) -> Self {
        Self { inner: value }
    }
}
