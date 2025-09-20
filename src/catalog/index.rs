use serde::Deserialize;
use serde::Serialize;

use crate::catalog::column::Columns;
use crate::error::Result;
use crate::value_err;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum IndexType {
    HashIndex,
}

impl Default for IndexType {
    fn default() -> Self {
        IndexType::HashIndex
    }
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct Index {
    /// Index name
    pub name: String,
    /// Index type
    pub index_type: IndexType,
    /// unique index, we support only the unique index for now.
    pub uniqueness: bool,
    /// The name of the table on which the index is created
    pub tblname: String,
    /// The columns for the index key.
    pub columns: Columns,
}

impl Index {
    pub fn new(
        name: impl Into<String>,
        tblname: impl Into<String>,
        columns: impl Into<Columns>,
        uniqueness: bool,
    ) -> Index {
        Index {
            name: name.into(),
            index_type: IndexType::HashIndex,
            uniqueness,
            tblname: tblname.into(),
            columns: columns.into(),
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.name.is_empty() {
            return Err(value_err!("Index name can't be empty"));
        }
        if self.tblname.is_empty() {
            return Err(value_err!("Index {} have no table name", self.name));
        }
        if self.columns.is_empty() {
            return Err(value_err!("Index {} have no key columns", self.name));
        }
        self.columns.validate()?;
        Ok(())
    }
}

pub type Indexes = Vec<Index>;
