use std::cmp::Ordering;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;

use chrono::DateTime;
use chrono::NaiveDateTime;
use serde::Deserialize;
use serde::Serialize;

use crate::error::Result;
use crate::parse_err;

/// A datatype
#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum DataType {
    Boolean,
    Integer,
    Float,
    String,
    Null,
    Timestamp,
}

impl DataType {
    pub fn is_numeric(&self) -> bool {
        match self {
            DataType::Integer | DataType::Float => true,
            _ => false,
        }
    }

    pub fn can_cast_to(&self, to: DataType) -> bool {
        if *self == to {
            return true;
        }
        match (self, to) {
            (DataType::Null, _) => true,
            (DataType::Boolean, DataType::Integer | DataType::Float | DataType::String) => true,
            (DataType::Integer, DataType::Boolean | DataType::Float | DataType::String) => true,
            (DataType::Float, DataType::Boolean | DataType::String) => true,
            (DataType::String, DataType::Boolean) => true,
            (DataType::String, DataType::Timestamp) => true,
            _ => false,
        }
    }
}

impl Default for DataType {
    fn default() -> Self {
        Self::String
    }
}

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(match self {
            Self::Boolean => "BOOLEAN",
            Self::Integer => "INTEGER",
            Self::Float => "FLOAT",
            Self::String => "TEXT",
            Self::Null => "NULL",
            Self::Timestamp => "TIMESTAMP",
        })
    }
}

pub type ValueRef = Arc<Value>;

/// A specific value of a data type
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum Value {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
    Timestamp(i64),
}

impl Value {
    pub fn datatype(&self) -> DataType {
        match self {
            Value::Null => DataType::Null,
            Value::Boolean(_) => DataType::Boolean,
            Value::Integer(_) => DataType::Integer,
            Value::Float(_) => DataType::Float,
            Value::String(_) => DataType::String,
            Value::Timestamp(_) => DataType::Timestamp,
        }
    }

    pub fn cast_to(&self, to: &DataType) -> Result<Value> {
        let value = match (self, to) {
            (Value::Null, _) => Value::Null,
            (Value::Boolean(b), DataType::Integer) => {
                if *b {
                    Value::Integer(1)
                } else {
                    Value::Integer(0)
                }
            }
            (Value::Boolean(b), DataType::Float) => {
                if *b {
                    Value::Float(1.0)
                } else {
                    Value::Float(0.0)
                }
            }
            (Value::Boolean(b), DataType::String) => {
                if *b {
                    Value::String("true".to_string())
                } else {
                    Value::String("false".to_string())
                }
            }
            (Value::Integer(i), DataType::Boolean) => {
                if *i == 0 {
                    Value::Boolean(false)
                } else {
                    Value::Boolean(false)
                }
            }
            (Value::Integer(i), DataType::Float) => Value::Float(*i as f64),
            (Value::Integer(i), DataType::String) => Value::String(i.to_string()),
            (Value::Float(f), DataType::Boolean) => {
                if f.eq(&0.0) {
                    Value::Boolean(false)
                } else {
                    Value::Boolean(true)
                }
            }
            (Value::Float(f), DataType::String) => Value::String(f.to_string()),
            (Value::String(s), DataType::Boolean) => {
                if s.is_empty() {
                    Value::Boolean(false)
                } else {
                    Value::Boolean(true)
                }
            }
            (Value::String(s), DataType::Timestamp) => {
                let dt = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                    .map_err(|err| parse_err!("Can't cast '{}' as timestamp: {}", s, err))?;
                Value::Timestamp(dt.and_utc().timestamp())
            }
            (_, typ) => return Err(parse_err!("Can't cast {} to type {}", self, typ)),
        };
        Ok(value)
    }
}

impl Eq for Value {}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Null, Value::Null) => true,
            (Value::Boolean(a), Value::Boolean(b)) => a == b,
            (Value::Integer(a), Value::Integer(b)) => a == b,
            (Value::Float(a), Value::Float(b)) => {
                // Handle NaN equality - treat NaN as equal to NaN
                if a.is_nan() && b.is_nan() {
                    true
                } else {
                    a == b
                }
            }
            (Value::String(a), Value::String(b)) => a == b,

            // Cross-type numeric equality
            (Value::Integer(a), Value::Float(b)) => *a as f64 == *b,
            (Value::Float(a), Value::Integer(b)) => *a == *b as f64,

            // Different variants types are never equal
            _ => false,
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        use std::cmp::Ordering;

        // First check equality for efficiency
        if self == other {
            return Some(Ordering::Equal);
        }

        match (self, other) {
            // Same types - direct comparison
            (Value::Boolean(a), Value::Boolean(b)) => a.partial_cmp(b),
            (Value::Integer(a), Value::Integer(b)) => a.partial_cmp(b),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
            (Value::String(a), Value::String(b)) => a.partial_cmp(b),
            (Value::Null, Value::Null) => Some(Ordering::Equal),

            // Cross-type numeric comparisons
            (Value::Integer(a), Value::Float(b)) => (*a as f64).partial_cmp(b),
            (Value::Float(a), Value::Integer(b)) => a.partial_cmp(&(*b as f64)),

            // Null comparisons
            (Value::Null, _) => Some(Ordering::Less),
            (_, Value::Null) => Some(Ordering::Greater),

            // Different types that can't be compared
            _ => None,
        }
    }
}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Value::Null => {
                0u8.hash(state);
            }
            Value::Boolean(b) => {
                1u8.hash(state);
                b.hash(state);
            }
            Value::Integer(i) => {
                2u8.hash(state);
                i.hash(state);
            }
            Value::Float(f) => {
                3u8.hash(state);
                // For floats, convert to bits to handle NaN and -0.0 consistently
                f.to_bits().hash(state);
            }
            Value::String(s) => {
                4u8.hash(state);
                s.hash(state);
            }
            Value::Timestamp(i64) => {
                5u8.hash(state);
                i64.hash(state);
            }
        }
    }
}

impl Default for Value {
    fn default() -> Self {
        Value::Null
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let ans = match self {
            Value::Null => "NULL".to_string(),
            Value::Boolean(b) if *b => "TRUE".to_string(),
            Value::Boolean(_) => "FALSE".to_string(),
            Value::Integer(i) => i.to_string(),
            Value::Float(f) => format!("{:.2}", f),
            Value::String(s) => format!("'{}'", s),
            Value::Timestamp(i64) => {
                let dt = DateTime::from_timestamp(*i64, 0)
                    .ok_or_else(|| parse_err!("Invalid timestamp {}", *i64))?;
                format!("'{}'", dt.format("%Y-%m-%d %H:%M:%S").to_string())
            }
        };
        // Use pad to work with formatting flags.
        f.pad(&ans)
    }
}
