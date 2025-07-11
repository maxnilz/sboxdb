use std::ops::Deref;
use std::sync::Arc;

use serde::Deserialize;
use serde::Serialize;

use crate::catalog::r#type::DataType;
use crate::catalog::r#type::Value;
use crate::error::Error;
use crate::error::Result;

/// A reference counted [`Column`]
pub type ColumnRef = Arc<Column>;

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
    fn validate(&self) -> Result<()> {
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

/// Builder for creating [`Column`] instances with a fluent interface
#[derive(Debug, Clone)]
pub struct ColumnBuilder {
    name: String,
    datatype: DataType,
    primary_key: bool,
    nullable: bool,
    unique: bool,
    default: Option<Value>,
}

impl ColumnBuilder {
    /// Create a new column builder with the required name and data type
    pub fn new(name: impl Into<String>, datatype: DataType) -> Self {
        Self {
            name: name.into(),
            datatype,
            primary_key: false,
            nullable: true,
            unique: false,
            default: None,
        }
    }

    /// Mark this column as a primary key
    pub fn primary_key(mut self) -> Self {
        self.primary_key = true;
        self.nullable = false; // Primary keys are automatically not nullable
        self.unique = true; // Primary keys are automatically unique
        self
    }

    /// Set whether this column is nullable
    pub fn nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }

    /// Mark this column as not nullable
    pub fn not_null(mut self) -> Self {
        self.nullable = false;
        self
    }

    /// Set whether this column is unique
    pub fn uniqueness(mut self, unique: bool) -> Self {
        self.unique = unique;
        self
    }

    /// Mark this column as unique
    pub fn unique(mut self) -> Self {
        self.unique = true;
        self
    }

    /// Set the default value for this column
    pub fn default_value(mut self, value: Value) -> Self {
        self.default = Some(value);
        self
    }

    /// Build the column, validating it in the process
    pub fn build(self) -> Result<Column> {
        let column = Column {
            name: self.name,
            datatype: self.datatype,
            primary_key: self.primary_key,
            nullable: self.nullable,
            unique: self.unique,
            default: self.default,
        };
        column.validate()?;
        Ok(column)
    }

    /// Build the column without validation
    pub fn build_unchecked(self) -> Column {
        Column {
            name: self.name,
            datatype: self.datatype,
            primary_key: self.primary_key,
            nullable: self.nullable,
            unique: self.unique,
            default: self.default,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Columns(Arc<[ColumnRef]>);

impl Columns {
    pub fn empty() -> Self {
        Self(Arc::new([]))
    }
}

impl Default for Columns {
    fn default() -> Self {
        Self::empty()
    }
}
impl FromIterator<Column> for Columns {
    fn from_iter<T: IntoIterator<Item = Column>>(iter: T) -> Self {
        iter.into_iter().map(Arc::new).collect()
    }
}

impl FromIterator<ColumnRef> for Columns {
    fn from_iter<T: IntoIterator<Item = ColumnRef>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}

impl From<Vec<Column>> for Columns {
    fn from(value: Vec<Column>) -> Self {
        value.into_iter().collect()
    }
}

impl From<Vec<ColumnRef>> for Columns {
    fn from(value: Vec<ColumnRef>) -> Self {
        value.into_iter().collect()
    }
}

impl From<&[ColumnRef]> for Columns {
    fn from(value: &[ColumnRef]) -> Self {
        Self(Arc::from(value))
    }
}

impl<const N: usize> From<[ColumnRef; N]> for Columns {
    fn from(value: [ColumnRef; N]) -> Self {
        Self(Arc::new(value))
    }
}

impl Deref for Columns {
    type Target = [ColumnRef];

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

impl<'a> IntoIterator for &'a Columns {
    type Item = &'a ColumnRef;
    type IntoIter = std::slice::Iter<'a, ColumnRef>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl Columns {
    /// Searches for a column by name, returning it along with its index if found
    pub fn find(&self, name: &str) -> Option<(usize, &ColumnRef)> {
        self.0.iter().enumerate().find(|(_, c)| c.name == name)
    }

    pub fn get_pk_column_idx(&self) -> Result<usize> {
        self.0
            .iter()
            .position(|it| it.primary_key)
            .ok_or_else(|| Error::value("Primary key column not found"))
    }

    pub fn validate(&self) -> Result<()> {
        for column in self {
            column.validate()?
        }
        Ok(())
    }
}
