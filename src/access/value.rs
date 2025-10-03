use std::collections::HashSet;
use std::fmt::Display;
use std::fmt::Formatter;
use std::ops::Deref;
use std::ops::DerefMut;
use std::sync::Arc;

use serde::Deserialize;
use serde::Serialize;

use crate::catalog::column::ColumnRef;
use crate::catalog::column::Columns;
use crate::catalog::r#type::DataType;
use crate::catalog::r#type::Value;
use crate::error::Result;
use crate::internal_err;
use crate::value_err;

pub type TupleRef = Arc<Tuple>;

/// Tabular values, i.e., tuple values. Depends on context, it can be
/// interpreted as row-wise tuple, i.e., heterogeneous values across
/// columns, or column-wise tuple, i.e., homogeneous values for same
/// column.
#[derive(Debug, Clone, Eq, PartialEq, Hash, Default, Serialize, Deserialize)]
pub struct Tuple(Vec<Value>);

impl Tuple {
    pub fn into_vec(self) -> Vec<Value> {
        self.0
    }

    pub fn scalar(mut self) -> Result<Value> {
        let sz = self.len();
        if sz != 1 {
            return Err(internal_err!("Expect single scalar value, got {} values", sz));
        }
        Ok(self.0.remove(0))
    }

    pub fn bool(self) -> Result<bool> {
        let scalar = self.scalar()?;
        match scalar {
            Value::Null => Ok(false),
            Value::Boolean(b) => Ok(b),
            _ => Err(internal_err!("Expect boolean value got {}", scalar.datatype())),
        }
    }

    pub fn extend(&mut self, other: Tuple) {
        self.0.extend(other.0)
    }

    pub fn project(mut self, indices: &[usize]) -> Self {
        let mut values = vec![];
        for &i in indices {
            values.push(std::mem::take(&mut self.0[i]))
        }
        Self(values)
    }
}

impl From<Vec<Value>> for Tuple {
    fn from(values: Vec<Value>) -> Self {
        Self(values)
    }
}

impl From<Tuple> for HashSet<Value> {
    fn from(values: Tuple) -> Self {
        let mut out = HashSet::new();
        for value in values.into_iter() {
            out.insert(value);
        }
        out
    }
}

impl Deref for Tuple {
    type Target = [Value];

    fn deref(&self) -> &[Value] {
        &self.0
    }
}

impl DerefMut for Tuple {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl IntoIterator for Tuple {
    type Item = Value;
    type IntoIter = std::vec::IntoIter<Value>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a> IntoIterator for &'a Tuple {
    type Item = &'a Value;
    type IntoIter = std::slice::Iter<'a, Value>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PrimaryKey {
    inner: Vec<Value>,
}

impl Display for PrimaryKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let sz = self.inner.len();
        match sz {
            0 => write!(f, "!BADKEY"),
            1 => write!(f, "{}", self.inner[0]),
            _ => {
                write!(f, "(")?;
                for (i, it) in self.inner.iter().enumerate() {
                    write!(f, "{}", it)?;
                    if i < sz - 1 {
                        write!(f, ", ")?;
                    }
                }
                write!(f, ")")
            }
        }
    }
}

impl From<Vec<Value>> for PrimaryKey {
    fn from(value: Vec<Value>) -> Self {
        PrimaryKey { inner: value }
    }
}

/// Index key composed of multiple column values.
pub type IndexKey = Tuple;

/// Schema-aware row-wise values, i.e., heterogeneous values across
/// columns
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Row {
    pub tuple: Tuple,
    columns: Columns,
}

impl Row {
    pub fn new(tuple: Tuple, columns: &[ColumnRef]) -> Result<Row> {
        let tuple = Row { tuple, columns: Columns::from(columns) };
        tuple.validate()?;
        Ok(tuple)
    }

    pub fn primary_key(&self) -> Result<PrimaryKey> {
        let inds = self.columns.primary_key();
        let mut out = vec![];
        for i in inds {
            out.push(self.tuple[i].clone())
        }
        Ok(out.into())
    }

    pub fn get_value(&self, i: usize) -> Option<&'_ Value> {
        self.tuple.get(i)
    }

    pub fn get_values(&self, columns: &Columns) -> Result<Tuple> {
        let mut out = Vec::new();
        for column in columns.iter() {
            let idx = self.columns.get_column_idx(&column.name)?;
            let val = self
                .tuple
                .get(idx)
                .ok_or_else(|| value_err!("Column {} is not found", column.name))?;
            out.push(val.clone());
        }
        Ok(Tuple::from(out))
    }

    pub fn check_columns(&self, columns: &Columns) -> Result<()> {
        if self.columns != *columns {
            return Err(value_err!("Invalid columns definitions"));
        }
        Ok(())
    }

    fn validate(&self) -> Result<()> {
        if self.tuple.len() != self.columns.len() {
            return Err(value_err!("Invalid values size"));
        }
        for (column, value) in self.columns.iter().zip(&self.tuple) {
            match value.datatype() {
                DataType::Null if column.nullable => Ok(()),
                DataType::Null => {
                    Err(value_err!("NULL value is not allowed for column {}", column.name))
                }
                datatype if datatype != column.datatype => {
                    Err(value_err!("Invalid datatype {} for column {}", datatype, column.name))
                }
                _ => Ok(()),
            }?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::vec;

    use super::*;
    use crate::catalog::column::ColumnBuilder;
    use crate::catalog::r#type::DataType;
    use crate::storage::codec::bincodec;

    #[test]
    fn test_values_codec() -> Result<()> {
        let columns = Columns::from(vec![ColumnBuilder::new("", DataType::String)
            .primary()
            .build_unchecked()]);
        let tuple = Tuple::from(vec![Value::String("hello".to_string())]);

        // encode with &values.as_ref() as input arg, decode with Values
        let enc = bincodec::serialize(&tuple.as_ref())?;
        let row = Row::new(bincodec::deserialize(&enc)?, &columns)?;
        assert_eq!(row.tuple, tuple);

        // encode with &values as input arg, decode with Values
        let enc = bincodec::serialize(&tuple)?;
        let row = Row::new(bincodec::deserialize(&enc)?, &columns)?;
        assert_eq!(row.tuple, tuple);

        Ok(())
    }
}
