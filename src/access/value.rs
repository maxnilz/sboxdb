use std::ops::Deref;

use serde::{Deserialize, Serialize};

use crate::catalog::column::{Column, Columns};
use crate::catalog::r#type::Value;
use crate::error::{Error, Result};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Values {
    values: Vec<Value>,
}

impl Values {
    pub fn new(values: Vec<Value>) -> Values {
        Values { values }
    }
    pub fn into_vec(self) -> Vec<Value> {
        self.values
    }
}

impl Deref for Values {
    type Target = [Value];

    fn deref(&self) -> &[Value] {
        &self.values
    }
}

pub type PrimaryKey = Value;

pub type IndexKey = Values;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Tuple {
    pub values: Values,
    columns: Columns,
}

impl Tuple {
    pub fn new(values: Values, columns: Columns) -> Result<Tuple> {
        let tuple = Tuple { values, columns };
        tuple.validate()?;
        Ok(tuple)
    }

    pub fn from(values: Vec<Value>, columns: &[Column]) -> Result<Tuple> {
        let tuple = Tuple { values: Values::new(values), columns: Columns::from_ref_vec(columns) };
        tuple.validate()?;
        Ok(tuple)
    }

    pub fn primary_key(&self) -> Result<&'_ Value> {
        let idx = self.columns.get_pk_column_idx()?;
        self.values.get(idx).ok_or_else(|| Error::value("Primary key not found"))
    }

    pub fn get_value(&self, i: usize) -> Option<&'_ Value> {
        self.values.get(i)
    }

    pub fn get_values(&self, columns: &Columns) -> Result<Values> {
        let mut out = Vec::new();
        for column in columns.iter() {
            let idx = self.columns.get_pk_column_idx()?;
            let val = self
                .values
                .get(idx)
                .ok_or_else(|| Error::value(format!("Column {} is not found", column.name)))?;
            out.push(val.clone());
        }
        Ok(Values::new(out))
    }

    pub fn check_columns(&self, columns: &Columns) -> Result<()> {
        if self.columns != *columns {
            return Err(Error::value("Invalid columns definitions"));
        }
        Ok(())
    }

    fn validate(&self) -> Result<()> {
        if self.values.len() != self.columns.len() {
            return Err(Error::value("Invalid values size"));
        }
        for (column, value) in self.columns.iter().zip(&self.values.values) {
            match value.datatype() {
                None if column.nullable => Ok(()),
                None => Err(Error::value(format!(
                    "NULL value is not allowed for column {}",
                    column.name
                ))),
                Some(ref datatype) if datatype != &column.datatype => Err(Error::value(format!(
                    "Invalid datatype {} for column {}",
                    datatype, column.name
                ))),
                _ => Ok(()),
            }?;
        }
        Ok(())
    }
}
