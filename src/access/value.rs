use std::ops::Deref;

use serde::Deserialize;
use serde::Serialize;

use crate::catalog::column::Column;
use crate::catalog::column::ColumnRef;
use crate::catalog::column::Columns;
use crate::catalog::r#type::Value;
use crate::error::Error;
use crate::error::Result;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Values {
    values: Vec<Value>,
}

impl Values {
    pub fn into_vec(self) -> Vec<Value> {
        self.values
    }
}

impl From<Vec<Value>> for Values {
    fn from(values: Vec<Value>) -> Self {
        Self { values }
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
    pub fn new(values: Values, columns: &[ColumnRef]) -> Result<Tuple> {
        let tuple = Tuple { values, columns: Columns::from(columns) };
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
        Ok(Values::from(out))
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
            .primary_key()
            .build_unchecked()]);
        let values = Values::from(vec![Value::String("hello".to_string())]);

        // encode with &values.as_ref() as input arg, decode with Values
        let enc = bincodec::serialize(&values.as_ref())?;
        let tuple = Tuple::new(bincodec::deserialize(&enc)?, &columns)?;
        assert_eq!(tuple.values, values);

        // encode with &values as input arg, decode with Values
        let enc = bincodec::serialize(&values)?;
        let tuple = Tuple::new(bincodec::deserialize(&enc)?, &columns)?;
        assert_eq!(tuple.values, values);

        Ok(())
    }
}
