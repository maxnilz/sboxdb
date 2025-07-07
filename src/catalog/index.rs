use serde::{Deserialize, Serialize};

use crate::catalog::column::{Column, ColumnRef, Columns};
use crate::error::Error;
use crate::error::Result;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum IndexType {
    HashIndex,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
    pub fn new(name: String, tblname: String, columns: Vec<Column>, uniqueness: bool) -> Index {
        Index {
            name,
            index_type: IndexType::HashIndex,
            uniqueness,
            tblname,
            columns: Columns::from(columns),
        }
    }

    pub fn from(name: &str, tblname: &str, columns: Columns, uniqueness: bool) -> Index {
        Index {
            name: name.to_string(),
            tblname: tblname.to_string(),
            index_type: IndexType::HashIndex,
            columns,
            uniqueness,
        }
    }
    pub fn validate(&self) -> Result<()> {
        if self.name.is_empty() {
            return Err(Error::value("Index name can't be empty"));
        }
        if self.tblname.is_empty() {
            return Err(Error::value(format!("Index {} have no table name", self.name)));
        }
        if self.columns.is_empty() {
            return Err(Error::value(format!("Index {} have no key columns", self.name)));
        }
        self.columns.validate()?;
        Ok(())
    }
}

pub type Indexes = Vec<Index>;
