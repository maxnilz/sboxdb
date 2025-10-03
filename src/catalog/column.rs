use std::fmt::Display;
use std::fmt::Formatter;
use std::ops::Deref;
use std::sync::Arc;

use serde::Deserialize;
use serde::Serialize;

use crate::catalog::r#type::DataType;
use crate::catalog::r#type::Value;
use crate::error::Result;
use crate::value_err;

/// A reference counted [`Column`]
pub type ColumnRef = Arc<Column>;

/// A table column schema
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Column {
    /// Column name
    pub name: String,
    /// Column data type
    pub datatype: DataType,
    /// A single column as primary key
    pub primary_key: bool,
    /// Whether a column is part of composite primary key
    pub part_of_key: bool,
    /// Whether a column is nullable
    pub nullable: bool,
    /// The default value of the column
    pub default: Option<Value>,
}

impl Display for Column {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.name, self.datatype)?;
        if self.primary_key {
            write!(f, " PRIMARY KEY")?;
        }
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        if let Some(default) = &self.default {
            write!(f, " DEFAULT {}", default)?;
        }
        Ok(())
    }
}

impl Column {
    fn validate(&self) -> Result<()> {
        if self.name.is_empty() {
            return Err(value_err!("Column name can't be empty"));
        }
        if self.primary_key && self.part_of_key {
            return Err(value_err!(
                "Singular primary key and part of key can't be set at the same time",
            ));
        }
        if self.default.is_none() {
            return Ok(());
        }

        // Validate default data type
        let default = self.default.as_ref().unwrap();
        let datatype = default.datatype();
        if datatype == DataType::Null && !self.nullable {
            return Err(value_err!(
                "Can't use NULL as default for non-nullable column {}",
                self.name
            ));
        }
        if datatype == DataType::Null && self.nullable {
            // nullable column can have null as default
            return Ok(());
        }
        if datatype != self.datatype {
            return Err(value_err!(
                "Default value for column {} has datatype {}, expect {}",
                self.name,
                datatype,
                self.datatype
            ));
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
    part_of_key: bool,
    nullable: bool,
    default: Option<Value>,
}

impl ColumnBuilder {
    /// Create a new column builder with the required name and data type
    pub fn new(name: impl Into<String>, datatype: DataType) -> Self {
        Self {
            name: name.into(),
            datatype,
            primary_key: false,
            part_of_key: false,
            nullable: true,
            default: None,
        }
    }

    /// Mark this column as a primary key
    pub fn primary(mut self) -> Self {
        self.primary_key = true;
        self.nullable = false; // Primary keys are automatically not nullable
        self
    }

    pub fn primary_key(self, primary_key: bool) -> Self {
        if !primary_key {
            return self;
        }
        self.primary()
    }

    /// Set whether this column is primary key
    pub fn part_of_key(mut self, part_of_key: bool) -> Self {
        if !part_of_key {
            return self;
        }

        self.part_of_key = true;
        self.nullable = false; // Primary keys are automatically not nullable
        self
    }

    /// Set whether this column is nullable
    pub fn nullable(mut self, nullable: bool) -> Self {
        if !self.part_of_key || self.primary_key {
            self.nullable = nullable;
        }
        self
    }

    /// Mark this column as not nullable
    pub fn not_null(self) -> Self {
        self.nullable(false)
    }

    /// Set the default value for this column
    pub fn default_value(self, value: Value) -> Self {
        self.default(Some(value))
    }

    pub fn default(mut self, default: Option<Value>) -> Self {
        self.default = default;
        self
    }

    /// Build the column, validating it in the process
    pub fn build(self) -> Result<Column> {
        let column = Column {
            name: self.name,
            datatype: self.datatype,
            primary_key: self.primary_key,
            part_of_key: self.part_of_key,
            nullable: self.nullable,
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
            part_of_key: self.part_of_key,
            nullable: self.nullable,
            default: self.default,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Columns(Arc<Vec<ColumnRef>>);

impl Columns {
    pub fn empty() -> Self {
        Self(Arc::new(vec![]))
    }

    pub fn primary_key(&self) -> Vec<usize> {
        self.0
            .iter()
            .enumerate()
            .filter_map(|(i, c)| if c.part_of_key || c.primary_key { Some(i) } else { None })
            .collect()
    }

    /// Searches for a column by name, returning it along with its index if found
    pub fn find(&self, name: &str) -> Option<(usize, &ColumnRef)> {
        self.0.iter().enumerate().find(|(_, c)| c.name == name)
    }

    pub fn get_column_idx(&self, name: &str) -> Result<usize> {
        self.0
            .iter()
            .position(|it| it.name == name)
            .ok_or_else(|| value_err!("Column {} not found", name))
    }

    pub fn validate(&self) -> Result<()> {
        for column in self {
            column.validate()?
        }

        Ok(())
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
        Self(Arc::new(iter.into_iter().collect()))
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
        Self(Arc::from(value.to_vec()))
    }
}

impl<const N: usize> From<[ColumnRef; N]> for Columns {
    fn from(value: [ColumnRef; N]) -> Self {
        Self(Arc::new(value.to_vec()))
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
