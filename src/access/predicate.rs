use serde::Deserialize;
use serde::Serialize;

use crate::access::value::Tuple;
use crate::catalog::r#type::Value;
use crate::error::Result;

/// Access layer [`Predicate`], subset of Expr that returns 3-valued boolean value
/// and pushdown eligible.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Predicate {}

impl Predicate {
    pub fn evaluate(&self, _tuple: Option<&Tuple>) -> Result<Value> {
        todo!()
    }
}
