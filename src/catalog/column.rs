use std::ops::Deref;

use serde::{Deserialize, Serialize};

use crate::catalog::r#type::{DataType, Value};
use crate::error::{Error, Result};

/// A table column schema
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Column {
    /// Column name
    pub name: String,
    /// Column data type
    pub datatype: DataType,
    /// Whether a column is a primary key
    pub primary_key: bool,
    /// Whether a column is nullable
    pub nullable: bool,
    /// The default value of the column
    pub default: Option<Value>,
    /// Whether the column should only take unique values
    pub unique: bool,
}

impl Column {
    pub fn new(
        name: String,
        datatype: DataType,
        primary_key: bool,
        nullable: bool,
        unique: bool,
        default: Option<Value>,
    ) -> Column {
        Column { name, datatype, primary_key, nullable, unique, default }
    }

    pub fn validate(&self) -> Result<()> {
        if self.name.is_empty() {
            return Err(Error::value("Column name can't be empty"));
        }
        // Validate primary key
        if self.primary_key && self.nullable {
            return Err(Error::value(format!("Primary key {} cannot be nullable", self.name)));
        }
        if self.primary_key && !self.unique {
            return Err(Error::value(format!("Primary key {} must be unique", self.name)));
        }
        // Validate default data type
        if let Some(default) = &self.default {
            if let Some(datatype) = default.datatype() {
                if datatype != self.datatype {
                    return Err(Error::value(format!(
                        "Default value for column {} has datatype {}, expect {}",
                        self.name, datatype, self.datatype
                    )));
                }
            }
            if !self.nullable {
                return Err(Error::value(format!(
                    "Can't use NULL as default for non-nullable column {}",
                    self.name
                )));
            }
            return Ok(());
        }
        if self.nullable {
            return Err(Error::value(format!(
                "Nullable column {} must have a default value",
                self.name
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Columns {
    columns: Vec<Column>,
}

impl Columns {
    pub fn new(columns: Vec<Column>) -> Columns {
        Columns { columns }
    }

    pub fn from(columns: Vec<&Column>) -> Columns {
        let mut inner = vec![];
        for col in columns {
            inner.push(col.clone())
        }
        Columns { columns: inner }
    }

    pub fn from_ref_vec(columns: &[Column]) -> Columns {
        let mut inner = vec![];
        for col in columns {
            inner.push(col.clone())
        }
        Columns { columns: inner }
    }

    pub fn get_column<P>(&self, col_name: &str) -> Result<&'_ Column> {
        self.columns
            .get(self.get_column_idx(col_name)?)
            .ok_or_else(|| Error::value(format!("Column {} not found", col_name)))
    }

    pub fn get_column_idx(&self, col_name: &str) -> Result<usize> {
        self.columns
            .iter()
            .position(|it| it.name == col_name)
            .ok_or_else(|| Error::value(format!("Column {} not found", col_name)))
    }

    pub fn get_pk_column_idx(&self) -> Result<usize> {
        self.columns
            .iter()
            .position(|it| it.primary_key)
            .ok_or_else(|| Error::value("Primary key column not found"))
    }

    pub fn validate(&self) -> Result<()> {
        for column in &self.columns {
            column.validate()?
        }
        Ok(())
    }
}

impl Deref for Columns {
    type Target = [Column];

    fn deref(&self) -> &[Column] {
        &self.columns
    }
}
