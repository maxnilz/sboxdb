use std::any::Any;
use std::collections::HashSet;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::Arc;

use crate::access::value::Tuple;
use crate::catalog::r#type::DataType;
use crate::catalog::r#type::Value;
use crate::error::Error;
use crate::error::Result;
use crate::sql::execution::compiler::RecordBatch;
use crate::sql::execution::compiler::RecordBatchBuilder;
use crate::sql::execution::context::ConstEvalCtx;
use crate::sql::execution::Context;
use crate::sql::execution::ExecutionPlan;
use crate::sql::execution::Scheduler;
use crate::sql::plan::expr::BinaryTypeCoercer;
use crate::sql::plan::expr::Operator;
use crate::sql::plan::schema::FieldBuilder;
use crate::sql::plan::schema::FieldRef;
use crate::sql::plan::schema::FieldReference;
use crate::sql::plan::schema::LogicalSchema;
use crate::sql::plan::visitor::DynTreeNode;
use crate::sql::udf::scalar::ScalarUDF;
use crate::sql::udf::signature::ScalarFunctionArgs;

/// Physical expr executor
pub trait PhysicalExpr: Debug + Display {
    /// Returns the physical expression as [`Any`] so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// Get the data type of the expr, given the schema of the input.
    fn data_type(&self, schema: &LogicalSchema) -> Result<DataType>;
    /// Evaluate an expression against a RecordBatch, returns
    /// paired columnar-values with the input records.
    fn evaluate(&self, ctx: &mut dyn Context, batch: &RecordBatch) -> Result<Tuple>;

    /// Get a list of child PhysicalExpr that provide the input for this expr.
    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![]
    }

    /// Get a list of child subquery executor for this expr.
    fn subqueries(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
}

impl DynTreeNode for dyn PhysicalExpr {
    fn arc_children(&self) -> Vec<&Arc<Self>> {
        self.children()
    }
}

/// Literal value physical expr
#[derive(Debug)]
pub struct ValueExec {
    value: Value,
}

impl ValueExec {
    pub fn new(value: Value) -> Self {
        ValueExec { value }
    }
}

impl PhysicalExpr for ValueExec {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn data_type(&self, _schema: &LogicalSchema) -> Result<DataType> {
        Ok(self.value.datatype())
    }

    fn evaluate(&self, _ctx: &mut dyn Context, batch: &RecordBatch) -> Result<Tuple> {
        let n = batch.num_rows();
        if n == 0 {
            // In case of it is a dumpy RecordBatch, we need to
            // return a single scalar value.
            return Ok(vec![self.value.clone()].into());
        }
        Ok(vec![self.value.clone(); n].into())
    }
}

impl Display for ValueExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.value)
    }
}

/// Field reference physical expr
#[derive(Debug)]
pub struct FieldReferenceExec {
    /// The name of the column (used for debugging and display purposes)
    name: String,
    /// index of schema
    index: usize,
}

impl FieldReferenceExec {
    pub fn try_new(f: FieldReference, schema: &LogicalSchema) -> Result<Self> {
        let index = schema.field_index_by_name(&f.relation, &f.name).ok_or_else(|| {
            Error::parse(format!(
                "Unexpected field reference {rel}{name}",
                rel = if let Some(table) = f.relation {
                    format!("{table}.").to_string()
                } else {
                    "".to_string()
                },
                name = f.name
            ))
        })?;
        Ok(Self { name: f.name, index })
    }
}

impl PhysicalExpr for FieldReferenceExec {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn data_type(&self, schema: &LogicalSchema) -> Result<DataType> {
        Ok(schema.field(self.index).datatype.clone())
    }

    fn evaluate(&self, _ctx: &mut dyn Context, batch: &RecordBatch) -> Result<Tuple> {
        Ok(batch.rows.iter().map(|row| row[self.index].clone()).collect::<Vec<_>>().into())
    }
}

impl Display for FieldReferenceExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}@{}", self.name, self.index)
    }
}

/// Outer field reference physical expr.
#[derive(Debug)]
pub struct OuterFieldReferenceExec {
    datatype: DataType,
    fr: FieldReference,
}

impl OuterFieldReferenceExec {
    pub fn new(datatype: DataType, fr: FieldReference) -> Self {
        Self { datatype, fr }
    }
}

impl PhysicalExpr for OuterFieldReferenceExec {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn data_type(&self, _schema: &LogicalSchema) -> Result<DataType> {
        Ok(self.datatype.clone())
    }

    fn evaluate(&self, ctx: &mut dyn Context, _batch: &RecordBatch) -> Result<Tuple> {
        let outer_query_batches = ctx.outer_query_batches();
        match outer_query_batches {
            None => Ok(Tuple::from(vec![])),
            Some(it) => it.columnar_values_at(&self.fr),
        }
    }
}

impl Display for OuterFieldReferenceExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.fr)
    }
}

/// Binary physical expr
#[derive(Debug)]
pub struct BinaryExprExec {
    left: Arc<dyn PhysicalExpr>,
    op: Operator,
    right: Arc<dyn PhysicalExpr>,
}

impl BinaryExprExec {
    pub fn new(left: Arc<dyn PhysicalExpr>, op: Operator, right: Arc<dyn PhysicalExpr>) -> Self {
        Self { left, op, right }
    }
}

// TODO: wrap arithmetic overflow
macro_rules! arithmetic_op {
    ($lhs:expr,$op:tt,$rhs:expr) => {{
        match ($lhs, $rhs) {
            (Value::Integer(lhs), Value::Integer(rhs)) => Ok(Value::Integer(lhs $op rhs)),
            (Value::Integer(lhs), Value::Float(rhs)) => Ok(Value::Float(lhs as f64 $op rhs)),
            (Value::Integer(_), Value::Null) => Ok(Value::Null),
            (Value::Float(lhs), Value::Integer(rhs)) => Ok(Value::Float(lhs $op rhs as f64)),
            (Value::Float(lhs), Value::Float(rhs)) => Ok(Value::Float(lhs $op rhs)),
            (Value::Float(_), Value::Null) => Ok(Value::Null),
            (Value::Null, Value::Float(_)) => Ok(Value::Null),
            (Value::Null, Value::Integer(_)) => Ok(Value::Null),
            (Value::Null, Value::Null) => Ok(Value::Null),
            (lhs, rhs) => {
                Err(crate::error::Error::Value(format!("Can't {} {} and {}", stringify!($op), lhs, rhs)))
            }
        }
    }};
}

macro_rules! compare_op {
    ($lhs:expr,$op:tt,$rhs:expr) => {{
        match ($lhs, $rhs) {
            (Value::Boolean(lhs), Value::Boolean(rhs)) => Ok(Value::Boolean(lhs $op rhs)),
            (Value::Integer(lhs), Value::Integer(rhs)) => Ok(Value::Boolean(lhs $op rhs)),
            (Value::Integer(lhs), Value::Float(rhs)) => Ok(Value::Boolean((lhs as f64) $op rhs)),
            (Value::Float(lhs), Value::Integer(rhs)) => Ok(Value::Boolean(lhs $op rhs as f64)),
            (Value::Float(lhs), Value::Float(rhs)) => Ok(Value::Boolean(lhs $op rhs)),
            (Value::String(lhs), Value::String(rhs)) => Ok(Value::Boolean(lhs $op rhs)),
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (lhs, rhs) => {
                Err(crate::error::Error::Value(format!("Can't compare {} and {}", lhs, rhs)))
            }
        }
    }};
}

impl PhysicalExpr for BinaryExprExec {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn data_type(&self, schema: &LogicalSchema) -> Result<DataType> {
        let ld = self.left.data_type(schema)?;
        let rd = self.right.data_type(schema)?;
        let type_coercer = BinaryTypeCoercer::new(&ld, &self.op, &rd);
        type_coercer.get_result_type()
    }

    fn evaluate(&self, ctx: &mut dyn Context, batch: &RecordBatch) -> Result<Tuple> {
        // Evaluate left-hand side expression.
        let lhs = self.left.evaluate(ctx, batch)?;
        // Evaluate right-hand side expression.
        let rhs = self.right.evaluate(ctx, batch)?;
        if lhs.len() != rhs.len() {
            return Err(Error::internal(format!(
                "Cannot evaluate arrays of different length, got {} vs {}",
                lhs.len(),
                rhs.len()
            )));
        }
        let zip = lhs.into_iter().zip(rhs.into_iter());
        let values: Result<Vec<Value>> = match self.op {
            Operator::Plus => zip.map(|(l, r)| arithmetic_op!(l, +, r)).collect(),
            Operator::Minus => zip.map(|(l, r)| arithmetic_op!(l, -, r)).collect(),
            Operator::Multiply => zip.map(|(l, r)| arithmetic_op!(l, *, r)).collect(),
            Operator::Divide => zip.map(|(l, r)| arithmetic_op!(l, /, r)).collect(),
            Operator::Modulo => zip.map(|(l, r)| arithmetic_op!(l, %, r)).collect(),
            Operator::Eq => zip.map(|(l, r)| compare_op!(l, ==, r)).collect(),
            Operator::NotEq => zip.map(|(l, r)| compare_op!(l, !=, r)).collect(),
            Operator::Gt => zip.map(|(l, r)| compare_op!(l, >, r)).collect(),
            Operator::GtEq => zip.map(|(l, r)| compare_op!(l, >=, r)).collect(),
            Operator::Lt => zip.map(|(l, r)| compare_op!(l, <, r)).collect(),
            Operator::LtEq => zip.map(|(l, r)| compare_op!(l, <=, r)).collect(),
            Operator::And => zip
                .map(|(l, r)| match (l, r) {
                    (Value::Boolean(lhs), Value::Boolean(rhs)) => Ok(Value::Boolean(lhs && rhs)),
                    (Value::Boolean(lhs), Value::Null) if !lhs => Ok(Value::Boolean(false)),
                    (Value::Boolean(_), Value::Null) => Ok(Value::Null),
                    (Value::Null, Value::Boolean(rhs)) if !rhs => Ok(Value::Boolean(false)),
                    (Value::Null, Value::Boolean(_)) => Ok(Value::Null),
                    (Value::Null, Value::Null) => Ok(Value::Null),
                    (lhs, rhs) => Err(Error::Value(format!("Can't and {} and {}", lhs, rhs))),
                })
                .collect(),
            Operator::Or => zip
                .map(|(l, r)| match (l, r) {
                    (Value::Boolean(lhs), Value::Boolean(rhs)) => Ok(Value::Boolean(lhs || rhs)),
                    (Value::Boolean(lhs), Value::Null) if lhs => Ok(Value::Boolean(true)),
                    (Value::Boolean(_), Value::Null) => Ok(Value::Null),
                    (Value::Null, Value::Boolean(rhs)) if rhs => Ok(Value::Boolean(true)),
                    (Value::Null, Value::Boolean(_)) => Ok(Value::Null),
                    (Value::Null, Value::Null) => Ok(Value::Null),
                    (lhs, rhs) => Err(Error::Value(format!("Can't or {} and {}", lhs, rhs))),
                })
                .collect(),
        };
        values.map(Tuple::from)
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.left, &self.right]
    }
}

impl Display for BinaryExprExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        fn write_child(
            f: &mut Formatter<'_>,
            expr: &Arc<dyn PhysicalExpr>,
            prec: u8,
        ) -> std::fmt::Result {
            if let Some(child) = expr.as_any().downcast_ref::<BinaryExprExec>() {
                if child.op.prec_value() < prec {
                    write!(f, "({child})")
                } else {
                    write!(f, "{child}")
                }
            } else {
                write!(f, "{expr}")
            }
        }

        let prec = self.op.prec_value();
        write_child(f, &self.left, prec)?;
        write!(f, " {} ", self.op)?;
        write_child(f, &self.right, prec)?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct CastExec {
    datatype: DataType,
    expr: Arc<dyn PhysicalExpr>,
}

impl CastExec {
    pub fn new(datatype: DataType, expr: Arc<dyn PhysicalExpr>) -> Self {
        Self { datatype, expr }
    }
}

impl PhysicalExpr for CastExec {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn data_type(&self, _schema: &LogicalSchema) -> Result<DataType> {
        Ok(self.datatype.clone())
    }

    fn evaluate(&self, ctx: &mut dyn Context, batch: &RecordBatch) -> Result<Tuple> {
        let tuple = self.expr.evaluate(ctx, batch)?;
        tuple
            .into_iter()
            .map(|it| it.cast_to(&self.datatype))
            .collect::<Result<Vec<_>>>()
            .and_then(|it| Ok(Tuple::from(it)))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.expr]
    }
}

impl Display for CastExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CAST({} AS {:?})", self.expr, self.datatype)
    }
}

/// Negative physical expr
#[derive(Debug)]
pub struct NegativeExec {
    expr: Arc<dyn PhysicalExpr>,
}

impl NegativeExec {
    pub fn try_new(expr: Arc<dyn PhysicalExpr>, schema: &LogicalSchema) -> Result<Self> {
        let datatype = expr.data_type(schema)?;
        if !datatype.is_numeric() {
            return Err(Error::parse(format!("Unexpected negative op on {} type", datatype)));
        }
        Ok(Self { expr })
    }
}

impl PhysicalExpr for NegativeExec {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn data_type(&self, schema: &LogicalSchema) -> Result<DataType> {
        self.expr.data_type(schema)
    }

    fn evaluate(&self, ctx: &mut dyn Context, batch: &RecordBatch) -> Result<Tuple> {
        let tuple = self.expr.evaluate(ctx, batch)?;
        let values = tuple
            .into_iter()
            .map(|it| match it {
                Value::Null => Value::Null,
                Value::Integer(i) => Value::Integer(-i),
                Value::Float(f) => Value::Float(-f),
                _ => unreachable!(),
            })
            .collect::<Vec<_>>();
        Ok(Tuple::from(values))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.expr]
    }
}

impl Display for NegativeExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "(- {})", self.expr)
    }
}

/// Like physical expr
#[derive(Debug)]
pub struct LikeExec {
    negated: bool,
    case_insensitive: bool,
    expr: Arc<dyn PhysicalExpr>,
    pattern: Arc<dyn PhysicalExpr>,
}

impl LikeExec {
    pub fn try_new(
        expr: Arc<dyn PhysicalExpr>,
        pattern: Arc<dyn PhysicalExpr>,
        negated: bool,
        case_insensitive: bool,
        schema: &LogicalSchema,
    ) -> Result<Self> {
        let expr_type = expr.data_type(schema)?;
        let pattern_type = pattern.data_type(schema)?;
        if expr_type != pattern_type {
            return Err(Error::parse(format!(
                "Like expr expect same type, found {} and {}",
                expr_type, pattern_type
            )));
        }

        // Support like on string type
        if expr_type != DataType::String {
            return Err(Error::parse(format!("Like expr does not support on {} type", expr_type)));
        }

        Ok(Self { expr, pattern, negated, case_insensitive })
    }
}

macro_rules! like_op {
    ($text:expr, $pattern:expr, $negated:expr, $case_insensitive:expr) => {{
        let text = if $case_insensitive { $text.to_lowercase() } else { $text.clone() };
        let pattern = if $case_insensitive { $pattern.to_lowercase() } else { $pattern.clone() };

        // Convert SQL LIKE pattern to regex
        let regex_pattern = pattern
            .replace("\\%", "\x00") // Temporarily replace escaped %
            .replace("\\_ ", "\x01") // Temporarily replace escaped _
            .replace("%", ".*") // % matches any sequence
            .replace("_", ".") // _ matches any single char
            .replace("\x00", "%") // Restore escaped %
            .replace("\x01", "_"); // Restore escaped _

        let regex_pattern = format!("^{}$", regex_pattern);

        match regex::Regex::new(&regex_pattern) {
            Ok(regex) => {
                let matches = regex.is_match(&text);
                Ok(Value::Boolean(if $negated { !matches } else { matches }))
            }
            Err(_) => Err(Error::Value(format!("Invalid LIKE pattern: {}", $pattern))),
        }
    }};
}
impl PhysicalExpr for LikeExec {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn data_type(&self, _schema: &LogicalSchema) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn evaluate(&self, ctx: &mut dyn Context, batch: &RecordBatch) -> Result<Tuple> {
        let lhs = self.expr.evaluate(ctx, batch)?;
        let rhs = self.pattern.evaluate(ctx, batch)?;
        if lhs.len() != rhs.len() {
            return Err(Error::internal(format!(
                "Cannot compare arrays of different length, got {} vs {}",
                lhs.len(),
                rhs.len()
            )));
        }
        let values = lhs
            .into_iter()
            .zip(rhs.into_iter())
            .map(|(txt, pat)| match (txt, pat) {
                (Value::String(t), Value::String(p)) => {
                    like_op!(t, p, self.negated, self.case_insensitive)
                }
                (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                (t, p) => {
                    Err(Error::value(format!("Like expects string value, got {} and {}", t, p)))
                }
            })
            .collect::<Result<Vec<Value>>>()?;
        Ok(Tuple::from(values))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.expr, &self.pattern]
    }
}

impl Display for LikeExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let op_name = match (self.negated, self.case_insensitive) {
            (false, false) => "LIKE",
            (true, false) => "NOT LIKE",
            (false, true) => "ILIKE",
            (true, true) => "NOT ILIKE",
        };
        write!(f, "{} {} {}", self.expr, op_name, self.pattern)
    }
}

/// InList physical expr, e.g., `[ NOT ] IN (val1, val2, ...)`
#[derive(Debug)]
pub struct InListExec {
    expr: Arc<dyn PhysicalExpr>,
    negated: bool,
    static_list: HashSet<Value>,
}

impl InListExec {
    pub fn try_new(
        expr: Arc<dyn PhysicalExpr>,
        list: Vec<Arc<dyn PhysicalExpr>>,
        negated: bool,
        schema: &LogicalSchema,
    ) -> Result<Self> {
        let expr_type = expr.data_type(schema)?;
        let batch = RecordBatchBuilder::new(schema).build();
        let mut static_list = HashSet::new();
        for it in list.iter() {
            let typ = it.data_type(schema)?;
            if expr_type != typ {
                return Err(Error::parse(format!(
                    "The data type inlist should be same, got {} and {}",
                    expr_type, typ
                )));
            }

            // we support only const list expr for now.
            let ctx = &mut ConstEvalCtx::new();
            match it.evaluate(ctx, &batch) {
                Ok(tuple) if tuple.len() == 1 => {
                    static_list.insert(tuple.into_iter().next().unwrap());
                }
                Ok(tuple) => {
                    return Err(Error::unimplemented(format!(
                        "Support only single static value as element of list expr, got {} values",
                        tuple.len()
                    )))
                }
                Err(err) => {
                    return Err(Error::unimplemented(format!(
                        "Support only static value as element of list expr, got eval err: {}",
                        err
                    )))
                }
            }
        }
        Ok(Self { expr, static_list, negated })
    }
}

impl PhysicalExpr for InListExec {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn data_type(&self, _schema: &LogicalSchema) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn evaluate(&self, ctx: &mut dyn Context, batch: &RecordBatch) -> Result<Tuple> {
        let tuple = self.expr.evaluate(ctx, batch)?;
        let values = tuple
            .into_iter()
            .map(|value| {
                let contained = self.static_list.contains(&value);
                if !self.negated {
                    Value::Boolean(contained)
                } else {
                    Value::Boolean(!contained)
                }
            })
            .collect::<Vec<Value>>();
        Ok(Tuple::from(values))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.expr]
    }
}

impl Display for InListExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let op = if self.negated { "NOT IN" } else { "IN" };
        write!(f, "{} {} SET({:?})", self.expr, op, self.static_list)
    }
}

/// Exists physical expr. Returns a Boolean (true if any row exists).
///
/// In case of evaluation here, we execute once then broadcast to all, and support
/// non-correlated subquery only.
#[derive(Debug)]
pub struct ExistsExec {
    /// The execution plan physical executor, wrap it with Mutex for interior
    /// mutability, so that we can get the mutable reference for poll_executor.
    executor: Arc<dyn ExecutionPlan>,
    negated: bool,
    correlated: bool,
}

impl ExistsExec {
    pub fn new(executor: Arc<dyn ExecutionPlan>, negated: bool, correlated: bool) -> Self {
        Self { executor, negated, correlated }
    }
}

impl ExistsExec {
    /// evaluate non-correlated subquery once and broadcast to all.
    fn evaluate_non_correlated(&self, ctx: &mut dyn Context, batch: &RecordBatch) -> Result<Tuple> {
        let rs = Scheduler::poll_executor(ctx, Arc::clone(&self.executor))?;
        let exists = !rs.is_empty();
        let value = if !self.negated { Value::Boolean(exists) } else { Value::Boolean(!exists) };
        let values = vec![value; batch.num_rows()];
        Ok(Tuple::from(values))
    }

    fn evaluate_one_row(
        &self,
        ctx: &mut dyn Context,
        schema: &LogicalSchema,
        row: &Tuple,
    ) -> Result<bool> {
        let batch = RecordBatchBuilder::new(schema).rows_ref(vec![row]).build();
        let prev_outer_query_batches = ctx.extend_outer_query_batches(batch)?;
        let rs = Scheduler::poll_executor(ctx, Arc::clone(&self.executor))?;
        // restore the previous outer query batches so that we won't
        // pollute the query context by the subquery above.
        ctx.set_outer_query_batches(prev_outer_query_batches)?;
        let exists = !rs.is_empty();
        Ok(if !self.negated { exists } else { exists })
    }
}

impl PhysicalExpr for ExistsExec {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn data_type(&self, _schema: &LogicalSchema) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn evaluate(&self, ctx: &mut dyn Context, batch: &RecordBatch) -> Result<Tuple> {
        if !self.correlated {
            return self.evaluate_non_correlated(ctx, batch);
        }
        // evaluate correlated subquery row by row.
        let values = batch
            .rows
            .iter()
            .map(|row| self.evaluate_one_row(ctx, &batch.schema, row))
            .map(|b| b.and_then(|b| Ok(Value::Boolean(b))))
            .collect::<Result<Vec<_>>>()?;
        Ok(Tuple::from(values))
    }

    fn subqueries(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.executor]
    }
}

impl Display for ExistsExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let op = if self.negated { "NOT EXISTS" } else { "EXISTS" };
        let sub = if !f.alternate() {
            format!("Subquery(correlated: {}, ...)", self.correlated)
        } else {
            format!("{:#?}", self.executor)
        };
        write!(f, "{} ({})", op, sub)
    }
}

/// Scalar subquery physical expr, produce exactly one column and at most one row.
///
/// In case of evaluation here, we execute once then broadcast to all, and support
/// non-correlated subquery only.
#[derive(Debug)]
pub struct ScalarSubqueryExec {
    executor: Arc<dyn ExecutionPlan>,
    correlated: bool,
}

impl ScalarSubqueryExec {
    pub fn new(executor: Arc<dyn ExecutionPlan>, correlated: bool) -> Self {
        Self { executor, correlated }
    }

    /// evaluate non-correlated subquery once and broadcast to all.
    fn evaluate_non_correlated(&self, ctx: &mut dyn Context, batch: &RecordBatch) -> Result<Tuple> {
        let mut rs = Scheduler::poll_executor(ctx, Arc::clone(&self.executor))?;
        if rs.rows.len() != 1 && rs.rows[0].len() != 1 {
            return Err(Error::internal("Expect single value from the scalar subquery"));
        }
        let value = rs.rows.remove(0).scalar()?;
        Ok(Tuple::from(vec![value; batch.num_rows()]))
    }

    fn evaluate_one_row(
        &self,
        ctx: &mut dyn Context,
        schema: &LogicalSchema,
        row: &Tuple,
    ) -> Result<Value> {
        let batch = RecordBatchBuilder::new(schema).rows_ref(vec![row]).build();
        let prev_outer_query_batches = ctx.extend_outer_query_batches(batch)?;
        let mut rs = Scheduler::poll_executor(ctx, Arc::clone(&self.executor))?;
        // restore the previous outer query batches so that we won't
        // pollute the query context by the subquery above.
        ctx.set_outer_query_batches(prev_outer_query_batches)?;

        if rs.rows.len() != 1 && rs.rows[0].len() != 1 {
            return Err(Error::internal("Expect single value from the scalar subquery"));
        }
        let value = rs.rows.remove(0).scalar()?;
        Ok(value)
    }
}

impl PhysicalExpr for ScalarSubqueryExec {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn data_type(&self, schema: &LogicalSchema) -> Result<DataType> {
        Ok(schema.field(0).datatype.clone())
    }

    fn evaluate(&self, ctx: &mut dyn Context, batch: &RecordBatch) -> Result<Tuple> {
        if !self.correlated {
            return self.evaluate_non_correlated(ctx, batch);
        }
        // evaluate correlated subquery row by row.
        let values = batch
            .rows
            .iter()
            .map(|row| self.evaluate_one_row(ctx, &batch.schema, row))
            .collect::<Result<Vec<_>>>()?;
        Ok(Tuple::from(values))
    }

    fn subqueries(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.executor]
    }
}

impl Display for ScalarSubqueryExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let sub = if !f.alternate() {
            format!("Subquery(correlated: {}, ...)", self.correlated)
        } else {
            format!("{:#?}", self.executor)
        };
        write!(f, "({})", sub)
    }
}

/// InSubquery physical expr, returns boolean, Left-hand side compared against a
/// set of values from the subquery (which must return 1 column).
///
/// In case of evaluation here, we execute once then broadcast to all, and support
/// non-correlated subquery only.
#[derive(Debug)]
pub struct InSubqueryExec {
    executor: Arc<dyn ExecutionPlan>,
    expr: Arc<dyn PhysicalExpr>,
    negated: bool,
    correlated: bool,
}

impl InSubqueryExec {
    pub fn new(
        executor: Arc<dyn ExecutionPlan>,
        expr: Arc<dyn PhysicalExpr>,
        negated: bool,
        correlated: bool,
    ) -> Self {
        Self { executor, expr, negated, correlated }
    }

    /// evaluate non-correlated subquery once and broadcast to all.
    fn evaluate_non_correlated(&self, ctx: &mut dyn Context, batch: &RecordBatch) -> Result<Tuple> {
        let rs = Scheduler::poll_executor(ctx, Arc::clone(&self.executor))?;
        if rs.num_cols() != 1 {
            return Err(Error::internal(format!(
                "InSubquery is expected to produce one column only, got {}",
                rs.num_cols()
            )));
        }
        let hs: HashSet<Value> = rs.columnar_values_at(0)?.into();
        let values = self.expr.evaluate(ctx, batch)?;
        let out = values
            .into_iter()
            .map(|it| {
                let contained = hs.contains(&it);
                if !self.negated {
                    Value::Boolean(contained)
                } else {
                    Value::Boolean(!contained)
                }
            })
            .collect::<Vec<_>>();
        Ok(Tuple::from(out))
    }

    fn evaluate_one_row(
        &self,
        ctx: &mut dyn Context,
        schema: &LogicalSchema,
        row: &Tuple,
    ) -> Result<bool> {
        let batch = RecordBatchBuilder::new(schema).rows_ref(vec![row]).build();
        let prev_outer_query_batches = ctx.extend_outer_query_batches(batch.clone())?;
        let rs = Scheduler::poll_executor(ctx, Arc::clone(&self.executor))?;
        // restore the previous outer query batches so that we won't
        // pollute the query context by the subquery above.
        ctx.set_outer_query_batches(prev_outer_query_batches)?;
        if rs.num_cols() != 1 {
            return Err(Error::internal(format!(
                "InSubquery is expected to produce one column only, got {}",
                rs.num_cols()
            )));
        }
        let hs: HashSet<Value> = rs.columnar_values_at(0)?.into();
        let values = self.expr.evaluate(ctx, &batch)?;
        let value = values.into_iter().next().ok_or_else(|| {
            Error::internal("Expect single value output from the InSubquery expr eval")
        })?;
        let contained = hs.contains(&value);
        Ok(if !self.negated { contained } else { !contained })
    }
}

impl PhysicalExpr for InSubqueryExec {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn data_type(&self, _schema: &LogicalSchema) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn evaluate(&self, ctx: &mut dyn Context, batch: &RecordBatch) -> Result<Tuple> {
        if !self.correlated {
            return self.evaluate_non_correlated(ctx, batch);
        }
        // evaluate correlated subquery row by row.
        let values = batch
            .rows
            .iter()
            .map(|row| self.evaluate_one_row(ctx, &batch.schema, row))
            .map(|b| b.and_then(|b| Ok(Value::Boolean(b))))
            .collect::<Result<Vec<_>>>()?;
        Ok(Tuple::from(values))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.expr]
    }

    fn subqueries(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.executor]
    }
}

impl Display for InSubqueryExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let op = if self.negated { "NOT IN" } else { "IN" };
        let sub = if !f.alternate() {
            format!("Subquery(correlated: {}, ...)", self.correlated)
        } else {
            format!("{:#?}", self.executor)
        };
        write!(f, "{} ({})", op, sub)
    }
}

/// Scalar function physical expr
#[derive(Debug)]
pub struct ScalarFunctionExec {
    func: Arc<dyn ScalarUDF>,
    args: Vec<Arc<dyn PhysicalExpr>>,

    arg_fields: Vec<FieldRef>,
}

impl ScalarFunctionExec {
    pub fn try_new(
        func: Arc<dyn ScalarUDF>,
        args: Vec<Arc<dyn PhysicalExpr>>,
        schema: &LogicalSchema,
    ) -> Result<Self> {
        let arg_fields = args
            .iter()
            .map(|expr| {
                // TODO: field nullability
                let field = FieldBuilder::new(format!("{}", expr), expr.data_type(schema)?).build();
                Ok(FieldRef::new(field))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { func, args, arg_fields })
    }
}

impl PhysicalExpr for ScalarFunctionExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _schema: &LogicalSchema) -> Result<DataType> {
        let f = self.func.return_field(&self.arg_fields)?;
        Ok(f.datatype.clone())
    }

    fn evaluate(&self, ctx: &mut dyn Context, batch: &RecordBatch) -> Result<Tuple> {
        let mut columnar_arg_values = vec![];
        for arg_expr in self.args.iter() {
            let tuple = arg_expr.evaluate(ctx, batch)?;
            columnar_arg_values.push(tuple.into_vec());
        }
        let num_rows = batch.num_rows();
        let mut rows = Vec::with_capacity(num_rows);
        for i in 0..num_rows {
            let mut args = Vec::with_capacity(columnar_arg_values.len());
            for arg_values in &mut columnar_arg_values {
                args.push(std::mem::take(&mut arg_values[i]));
            }
            rows.push(Tuple::from(args))
        }
        let args = ScalarFunctionArgs::new(rows, num_rows);
        self.func.invoke_with_args(args)
    }
}

impl Display for ScalarFunctionExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let args = self.args.iter().map(|it| format!("{}", it)).collect::<Vec<_>>();
        write!(f, "{}({})", self.func.name(), args.join(","))
    }
}
