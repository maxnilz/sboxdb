use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;

use crate::access::value::Tuple;
use crate::catalog::r#type::DataType;
use crate::catalog::r#type::Value;
use crate::error::Result;
use crate::sql::plan::expr::Expr;
use crate::sql::plan::schema::FieldBuilder;
use crate::sql::plan::schema::FieldRef;
use crate::sql::plan::schema::FieldReference;
use crate::sql::plan::schema::LogicalSchema;
use crate::sql::udf::signature::Signature;

pub type GroupId = u64;

/// Trait for implementing user defined aggregate functions.
pub trait AggregateUDF: Debug + Sync + Send {
    fn as_any(&self) -> &dyn Any;

    /// Returns this function's name.
    fn name(&self) -> &str;

    /// Expand asterisk to concrete exprs
    fn expand_asterisk(&self, schema: &LogicalSchema) -> Result<Vec<Expr>> {
        let exprs = schema
            .iter()
            .map(|(q, f)| Expr::FieldReference(FieldReference::new(&f.name, q.cloned())))
            .collect();
        Ok(exprs)
    }

    /// Returns the name of the column this expression would create
    fn schema_name(&self, args: &[Expr]) -> String {
        format!(
            "{}({})",
            self.name(),
            args.iter().map(|it| it.schema_name().to_string()).collect::<Vec<_>>().join(", ")
        )
    }

    /// Return the datatype this function returns given the input argument types.
    fn return_field(&self, arg_fields: &[FieldRef]) -> Result<FieldRef>;

    /// Return an accumulator that aggregates for the udf.
    fn accumulator(&self) -> Box<dyn Accumulator>;
}

/// Accumulator track the aggregation state
pub trait Accumulator: Debug {
    /// Push the args paired values to the accumulator for state update.
    fn push(&mut self, gid: GroupId, args_values: Vec<Tuple>) -> Result<()>;

    /// Returns the final aggregate value, consuming the internal state.
    fn evaluate(&mut self) -> Result<Vec<(GroupId, Value)>>;
}

macro_rules! make_udfa_function {
    ($UDF:ty, $NAME:ident) => {
        pub fn $NAME() -> std::sync::Arc<dyn AggregateUDF> {
            static INSTANCE: std::sync::LazyLock<std::sync::Arc<dyn AggregateUDF>> =
                std::sync::LazyLock::new(|| std::sync::Arc::new(<$UDF>::new()));
            std::sync::Arc::clone(&INSTANCE)
        }
    };
}

make_udfa_function!(Count, count);

#[derive(Debug)]
pub struct Count {
    signature: Signature,
}

impl Count {
    pub fn new() -> Self {
        Self { signature: Signature::Nullary }
    }
}

impl AggregateUDF for Count {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "count"
    }

    /// Expand asterisk to concrete exprs by ignore it, i.e.,
    /// COUNT(*) would result no expr as arg.
    fn expand_asterisk(&self, _schema: &LogicalSchema) -> Result<Vec<Expr>> {
        Ok(vec![])
    }

    fn schema_name(&self, _args: &[Expr]) -> String {
        // we support count(*) only for now
        format!("{}(*)", self.name())
    }

    fn return_field(&self, _arg_fields: &[FieldRef]) -> Result<FieldRef> {
        Ok(FieldRef::new(FieldBuilder::new("count", DataType::Integer).not_null().build()))
    }

    fn accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(CountAccumulator { counts: HashMap::new() })
    }
}

#[derive(Debug)]
struct CountAccumulator {
    // group id to count
    counts: HashMap<GroupId, i64>,
}

impl Accumulator for CountAccumulator {
    fn push(&mut self, gid: GroupId, args_values: Vec<Tuple>) -> Result<()> {
        let entry = self.counts.entry(gid).or_insert(0);
        *entry += args_values.len() as i64;
        Ok(())
    }

    fn evaluate(&mut self) -> Result<Vec<(GroupId, Value)>> {
        let mut out = vec![];
        let counts = std::mem::take(&mut self.counts);
        for (k, v) in counts {
            out.push((k, Value::Integer(v)))
        }
        Ok(out)
    }
}
