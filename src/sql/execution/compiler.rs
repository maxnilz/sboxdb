use std::any::Any;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::ops::Deref;
use std::sync::Arc;

use crate::access::value::Values;
use crate::catalog::column::Columns;
use crate::catalog::r#type::Value;
use crate::catalog::schema::Schema;
use crate::error::Error;
use crate::error::Result;
use crate::sql::execution::context::Context;
use crate::sql::execution::context::PlanContext;
use crate::sql::execution::ddl::CreateIndexExec;
use crate::sql::execution::ddl::CreateTableExec;
use crate::sql::execution::ddl::DropIndexExec;
use crate::sql::execution::ddl::DropTableExec;
use crate::sql::execution::display::TabularDisplay;
use crate::sql::execution::dml::DeleteExec;
use crate::sql::execution::dml::InsertExec;
use crate::sql::execution::dml::UpdateExec;
use crate::sql::execution::expr::BinaryExprExec;
use crate::sql::execution::expr::CastExec;
use crate::sql::execution::expr::ExistsExec;
use crate::sql::execution::expr::FieldReferenceExec;
use crate::sql::execution::expr::InListExec;
use crate::sql::execution::expr::InSubqueryExec;
use crate::sql::execution::expr::LikeExec;
use crate::sql::execution::expr::NegativeExec;
use crate::sql::execution::expr::OuterFieldReferenceExec;
use crate::sql::execution::expr::PhysicalExpr;
use crate::sql::execution::expr::ScalarSubqueryExec;
use crate::sql::execution::expr::ValueExec;
use crate::sql::execution::query::ExplainExec;
use crate::sql::execution::query::FilterExec;
use crate::sql::execution::query::LimitExec;
use crate::sql::execution::query::ProjectionExec;
use crate::sql::execution::query::SeqScanExec;
use crate::sql::execution::query::SortExec;
use crate::sql::execution::query::SortExprExec;
use crate::sql::execution::query::SubqueryAliasExec;
use crate::sql::execution::query::ValuesExec;
use crate::sql::plan::expr::Alias;
use crate::sql::plan::expr::BinaryExpr;
use crate::sql::plan::expr::Cast;
use crate::sql::plan::expr::Exists;
use crate::sql::plan::expr::Expr;
use crate::sql::plan::expr::InList;
use crate::sql::plan::expr::InSubquery;
use crate::sql::plan::expr::Like;
use crate::sql::plan::expr::Operator;
use crate::sql::plan::expr::Subquery;
use crate::sql::plan::plan::CreateTable;
use crate::sql::plan::plan::Delete;
use crate::sql::plan::plan::Explain;
use crate::sql::plan::plan::Filter;
use crate::sql::plan::plan::Insert;
use crate::sql::plan::plan::Join;
use crate::sql::plan::plan::Limit;
use crate::sql::plan::plan::Plan;
use crate::sql::plan::plan::Projection;
use crate::sql::plan::plan::Sort;
use crate::sql::plan::plan::SubqueryAlias;
use crate::sql::plan::plan::Update;
use crate::sql::plan::schema::FieldReference;
use crate::sql::plan::schema::Fields;
use crate::sql::plan::schema::LogicalSchema;
use crate::sql::plan::schema::EMPTY_SCHEMA;
use crate::sql::plan::visitor::DynTreeNode;

/// A physical executable node in the query plan.
///
/// Unlike the Volcano model, which uses a tuple-at-a-time iterator (`next()`),
/// this follows a **vectorized iterator model**, yielding a batch of tuples
/// (e.g., a `RecordBatch`) at a time.
pub trait ExecutionPlan: Debug + Display {
    /// Returns the physical expression as [`Any`] so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    fn schema(&self) -> LogicalSchema;

    /// Initialize the executor.
    fn init(&self, ctx: &mut dyn Context) -> Result<()>;

    /// Yields the next batch of tuples from this executor.
    fn execute(&self, ctx: &mut dyn Context) -> Result<Option<RecordBatch>>;

    /// Get a list of children `ExecutionPlan`s that act as inputs to this plan.
    /// The returned list will be empty for leaf nodes such as scans, will contain
    /// a single value for unary nodes, or two values for binary nodes (such as
    /// joins).
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
}

impl DynTreeNode for dyn ExecutionPlan {
    fn arc_children(&self) -> Vec<&Arc<Self>> {
        self.children()
    }
}

/// Compiles logical plans into executable physical plans.
///
/// The Compiler transforms logical query plans into physical execution plans
/// by selecting concrete implementations for each operation. It handles the
/// conversion of logical expressions into physical expressions and builds
/// the execution tree that can be run against actual data.
///
/// This is also known as physical planning in query processing terminology.
pub struct Compiler {}

impl Compiler {
    pub fn build_execution_plan(&self, plan: Plan) -> Result<Arc<dyn ExecutionPlan>> {
        match plan {
            Plan::CreateTable(CreateTable { relation, schema, if_not_exists }) => {
                let columns = self.columns_from_fields(schema.fields())?;
                let table_schema = Schema::try_new(relation, columns)?;
                Ok(Arc::new(CreateTableExec::new(table_schema, if_not_exists)))
            }
            Plan::CreateIndex(node) => Ok(Arc::new(CreateIndexExec::try_new(node)?)),
            Plan::DropTable(node) => Ok(Arc::new(DropTableExec::new(node))),
            Plan::DropIndex(node) => Ok(Arc::new(DropIndexExec::new(node))),
            Plan::Insert(Insert { table, input, output_schema }) => {
                let input = self.build_execution_plan(*input)?;
                Ok(Arc::new(InsertExec::try_new(table, input, output_schema)?))
            }
            Plan::Update(Update { table, input, output_schema }) => {
                let input = self.build_execution_plan(*input)?;
                Ok(Arc::new(UpdateExec::try_new(table, input, output_schema)?))
            }
            Plan::Delete(Delete { table, input, output_schema }) => {
                let input = self.build_execution_plan(*input)?;
                Ok(Arc::new(DeleteExec::try_new(table, input, output_schema)?))
            }
            Plan::Values(crate::sql::plan::plan::Values { schema, values }) => {
                let exprs = values
                    .into_iter()
                    .map(|row| {
                        row.into_iter()
                            .map(|it| self.build_physical_expr(it, &schema))
                            .collect::<Result<Vec<_>>>()
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(Arc::new(ValuesExec::try_new(schema, exprs)?))
            }
            Plan::Projection(Projection { exprs, input, output_schema }) => {
                let exprs = exprs
                    .into_iter()
                    .map(|it| self.build_physical_expr(it, input.schema()))
                    .collect::<Result<Vec<_>>>()?;
                let input = self.build_execution_plan(*input)?;
                Ok(Arc::new(ProjectionExec::new(input, exprs, output_schema)))
            }
            Plan::TableScan(t) => {
                // By default, the Logical TableScan node is executed by SeqScan.
                // TODO: Have the physical optimizer rewrite it to other types of scan if
                //  applicable. In case of physical optimizer kicks in, it is expected to
                //  see new physical plan node like, IndexScan, etc.
                Ok(Arc::new(SeqScanExec::try_new(t)?))
            }
            Plan::Subquery(Subquery { subquery, correlated }) => {
                // TODO: The Subquery is a node for subquery in Expr here.
                //  Although subquery is generated in the Explain stmt, During
                //  the logical planning we are not transforming the subquery
                //  in expr into this node yet.
                unreachable!()
            }
            Plan::SubqueryAlias(SubqueryAlias { input, schema, alias }) => {
                // Note: Except the subquery in the Expr, we have derived table
                // in the FROM/JOIN clause also using this node for alias.
                let input = self.build_execution_plan(*input)?;
                Ok(Arc::new(SubqueryAliasExec::new(input, schema, alias)))
            }
            Plan::Join(Join { left, right, join_type, constraint, schema }) => {
                // By default, the Logical Join node is execute by HashJoin.
                // TODO: Have the physical optimizer rewrite it to other types of join if
                //  applicable. In case of physical optimizer kicks in, it is expected to
                //  see new physical node like MergeSortJoin, NestedLoopJoin, etc.
                let constraint = self.build_physical_expr(constraint, &schema)?;
                let left = self.build_execution_plan(*left)?;
                let right = self.build_execution_plan(*right)?;
                todo!()
            }
            Plan::Filter(Filter { predicate, input }) => {
                let pred = self.build_physical_expr(predicate, input.schema())?;
                let input = self.build_execution_plan(*input)?;
                Ok(Arc::new(FilterExec::new(input, pred)))
            }
            Plan::Aggregate(_) => todo!(),
            Plan::Sort(Sort { input, expr }) => {
                let order = expr
                    .into_iter()
                    .map(|it| {
                        let expr = self.build_physical_expr(it.expr, input.schema())?;
                        Ok(SortExprExec::new(expr, it.asc))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let input = self.build_execution_plan(*input)?;
                Ok(Arc::new(SortExec::new(input, order)))
            }
            Plan::Limit(Limit { input, skip, fetch }) => {
                let input = self.build_execution_plan(*input)?;
                Ok(Arc::new(LimitExec::new(input, skip, fetch)))
            }
            Plan::Explain(Explain { physical, verbose, plan, output_schema }) => {
                let executor = self.build_execution_plan(*plan.clone())?;
                Ok(Arc::new(ExplainExec::new(*plan, executor, verbose, physical, output_schema)))
            }
            _ => Err(Error::internal(format!("Unexpected logical plan: {}", plan))),
        }
    }

    fn build_physical_expr(
        &self,
        expr: Expr,
        input_schema: &LogicalSchema,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        match expr {
            Expr::Alias(Alias { expr, .. }) => self.build_physical_expr(*expr, input_schema),
            Expr::Value(value) => Ok(Arc::new(ValueExec::new(value))),
            Expr::FieldReference(f) => Ok(Arc::new(FieldReferenceExec::try_new(f, input_schema)?)),
            Expr::OuterFieldReference(datatype, f) => {
                Ok(Arc::new(OuterFieldReferenceExec::new(datatype, f)))
            }
            Expr::Not(expr) => {
                let left = self.build_physical_expr(*expr, input_schema)?;
                let right =
                    self.build_physical_expr(Expr::Value(Value::Boolean(false)), &EMPTY_SCHEMA)?;
                Ok(Arc::new(BinaryExprExec::new(left, Operator::Eq, right)))
            }
            Expr::IsNull(expr) => {
                let left = self.build_physical_expr(*expr, input_schema)?;
                let right =
                    self.build_physical_expr(Expr::Value(Value::Null), &LogicalSchema::empty())?;
                Ok(Arc::new(BinaryExprExec::new(left, Operator::Eq, right)))
            }
            Expr::IsNotNull(expr) => {
                let left = self.build_physical_expr(*expr, input_schema)?;
                let right =
                    self.build_physical_expr(Expr::Value(Value::Null), &LogicalSchema::empty())?;
                Ok(Arc::new(BinaryExprExec::new(left, Operator::NotEq, right)))
            }
            Expr::IsTrue(expr) => {
                let left = self.build_physical_expr(*expr, input_schema)?;
                let right =
                    self.build_physical_expr(Expr::Value(Value::Boolean(true)), &EMPTY_SCHEMA)?;
                Ok(Arc::new(BinaryExprExec::new(left, Operator::Eq, right)))
            }
            Expr::IsNotTrue(expr) => {
                let left = self.build_physical_expr(*expr, input_schema)?;
                let right =
                    self.build_physical_expr(Expr::Value(Value::Boolean(true)), &EMPTY_SCHEMA)?;
                Ok(Arc::new(BinaryExprExec::new(left, Operator::NotEq, right)))
            }
            Expr::IsFalse(expr) => {
                let left = self.build_physical_expr(*expr, input_schema)?;
                let right =
                    self.build_physical_expr(Expr::Value(Value::Boolean(false)), &EMPTY_SCHEMA)?;
                Ok(Arc::new(BinaryExprExec::new(left, Operator::Eq, right)))
            }
            Expr::IsNotFalse(expr) => {
                let left = self.build_physical_expr(*expr, input_schema)?;
                let right =
                    self.build_physical_expr(Expr::Value(Value::Boolean(false)), &EMPTY_SCHEMA)?;
                Ok(Arc::new(BinaryExprExec::new(left, Operator::NotEq, right)))
            }
            Expr::Negative(expr) => {
                let expr = self.build_physical_expr(*expr, input_schema)?;
                Ok(Arc::new(NegativeExec::try_new(expr, input_schema)?))
            }
            Expr::Like(Like { expr, pattern, negated, case_insensitive }) => {
                let expr = self.build_physical_expr(*expr, input_schema)?;
                let pattern = self.build_physical_expr(*pattern, input_schema)?;
                Ok(Arc::new(LikeExec::try_new(
                    expr,
                    pattern,
                    negated,
                    case_insensitive,
                    input_schema,
                )?))
            }
            Expr::InList(InList { expr, list, negated }) => {
                let expr = self.build_physical_expr(*expr, input_schema)?;
                let list = list
                    .into_iter()
                    .map(|expr| self.build_physical_expr(*expr, input_schema))
                    .collect::<Result<Vec<_>>>()?;
                Ok(Arc::new(InListExec::try_new(expr, list, negated, input_schema)?))
            }
            Expr::Exists(Exists { subquery, negated }) => {
                let executor = self.build_execution_plan(*subquery.subquery)?;
                Ok(Arc::new(ExistsExec::new(executor, negated, subquery.correlated)))
            }
            Expr::ScalarSubquery(Subquery { subquery, correlated }) => {
                let executor = self.build_execution_plan(*subquery)?;
                Ok(Arc::new(ScalarSubqueryExec::new(executor, correlated)))
            }
            Expr::InSubquery(InSubquery { subquery, expr, negated }) => {
                let executor = self.build_execution_plan(*subquery.subquery)?;
                let expr = self.build_physical_expr(*expr, input_schema)?;
                Ok(Arc::new(InSubqueryExec::new(executor, expr, negated, subquery.correlated)))
            }
            Expr::Cast(Cast { data_type, expr }) => {
                let expr = self.build_physical_expr(*expr, input_schema)?;
                Ok(Arc::new(CastExec::new(data_type, expr)))
            }
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                let left = self.build_physical_expr(*left, input_schema)?;
                let right = self.build_physical_expr(*right, input_schema)?;
                Ok(Arc::new(BinaryExprExec::new(left, op, right)))
            }
        }
    }

    fn columns_from_fields(&self, fields: &Fields) -> Result<Columns> {
        let ctx = &mut PlanContext {};
        fields.to_columns(|col_name, default| {
            default
                .map(|expr| {
                    let expr = self.build_physical_expr(expr, &EMPTY_SCHEMA)?;
                    let values = expr.evaluate(ctx, &RecordBatchBuilder::empty_schema().build())?;
                    values.into_iter().next().ok_or_else(|| {
                        Error::value(format!("Invalid default expr on column {}", col_name))
                    })
                })
                .transpose()
        })
    }
}

#[derive(Clone)]
pub struct RecordBatchInner {
    pub schema: LogicalSchema,
    pub tuples: Vec<Values>,
    // If we know there is no more batch in the pipe,
    // set it explicitly to allow the scheduler do
    // short-circuit.
    pub has_next: bool,
}

/// A dataset with multiple row-wise values
#[derive(Clone)]
pub struct RecordBatch(Arc<RecordBatchInner>);

impl RecordBatch {
    pub fn new(schema: &LogicalSchema, tuples: Vec<Values>) -> Self {
        Self(Arc::new(RecordBatchInner { schema: schema.clone(), tuples, has_next: true }))
    }

    pub fn into_inner(self) -> Result<RecordBatchInner> {
        Arc::into_inner(self.0)
            .ok_or(Error::internal("Cannot convert RecordBatchRef into RecordBatch"))
    }

    pub fn num_tuples(&self) -> usize {
        self.tuples.len()
    }
}

impl Deref for RecordBatch {
    type Target = RecordBatchInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Display for RecordBatch {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        TabularDisplay::new(&self.schema, &self.tuples).fmt(f)
    }
}

pub struct RecordBatchBuilder {
    schema: LogicalSchema,
    tuples: Vec<Values>,
    has_next: bool,
}

impl RecordBatchBuilder {
    pub fn new(schema: &LogicalSchema) -> Self {
        Self { schema: schema.clone(), tuples: Vec::new(), has_next: true }
    }

    pub fn empty_schema() -> Self {
        Self { schema: EMPTY_SCHEMA.clone(), tuples: Vec::new(), has_next: true }
    }

    pub fn extend(mut self, tuples: Vec<Values>) -> Self {
        self.tuples.extend(tuples);
        self
    }

    pub fn nomore(mut self) -> Self {
        self.has_next = false;
        self
    }

    pub fn tuples_ref(self, tuples: Vec<&Values>) -> Self {
        // TODO: tuples' clone is expensive here!!!
        let tuples =
            tuples.into_iter().map(|row| Values::from(row.to_vec())).collect::<Vec<Values>>();
        self.extend(tuples)
    }

    pub fn build(self) -> RecordBatch {
        let inner =
            RecordBatchInner { schema: self.schema, tuples: self.tuples, has_next: self.has_next };
        RecordBatch(Arc::new(inner))
    }
}

/// Record batches provide a unified view by virtually merge the tuples
/// index-wise without clone the tuple values and merge them physically.
///
/// The virtual view layout is a combined table like the following:
///
/// Batch1.schema[0],    Batch1.schema[1],    Batch2.schema[0], ...
/// Batch1.tuples[0][0], Batch1.tuples[0][1], Batch2.tuples[0][0], ...
///                      Batch1.tuples[1][1], Batch2.tuples[1][0], ...
///
/// Different batches can have different num of tuples.
#[derive(Clone)]
pub struct RecordBatchesRef(Vec<RecordBatch>);

impl RecordBatchesRef {
    pub fn new(rb: RecordBatch) -> Self {
        Self(vec![rb])
    }
    pub fn push(&mut self, rb: RecordBatch) -> Result<()> {
        if self.0.is_empty() {
            self.0.push(rb);
            return Ok(());
        }
        self.0.push(rb);
        Ok(())
    }

    pub fn columnar_values_at(&self, f: &FieldReference) -> Result<Values> {
        for rb in self.iter() {
            let idx = rb.schema.field_index_by_name(&f.relation, &f.name);
            if idx.is_none() {
                continue;
            }
            let idx = idx.unwrap();
            let values = rb
                .tuples
                .iter()
                .map(|row| {
                    row.get(idx)
                        .cloned()
                        .ok_or(Error::internal(format!("value at column {} is out of bound", idx)))
                })
                .collect::<Result<Vec<_>>>()?;
            return Ok(Values::from(values));
        }
        Err(Error::internal(format!("No reference value found for field {}", f)))
    }
}

impl Deref for RecordBatchesRef {
    type Target = [RecordBatch];

    fn deref(&self) -> &[RecordBatch] {
        &self.0
    }
}
