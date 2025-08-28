use std::any::Any;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::Arc;

use crate::access::value::Tuple;
use crate::catalog::r#type::Value;
use crate::error::Result;
use crate::sql::execution::compiler::ExecutionPlan;
use crate::sql::execution::compiler::RecordBatch;
use crate::sql::execution::context::Context;
use crate::sql::execution::expr::PhysicalExpr;
use crate::sql::plan::schema::LogicalSchema;
use crate::sql::udf::aggregate::Accumulator;
use crate::sql::udf::aggregate::AggregateUDF;
use crate::sql::udf::aggregate::GroupId;

#[derive(Debug)]
pub struct AggregateFuncExpr {
    func: Arc<dyn AggregateUDF>,
    args_expr: Vec<Arc<dyn PhysicalExpr>>,
    display: String,
}

impl AggregateFuncExpr {
    pub fn new(
        func: Arc<dyn AggregateUDF>,
        args_expr: Vec<Arc<dyn PhysicalExpr>>,
        display: String,
    ) -> Self {
        Self { func, args_expr, display }
    }
}

impl Display for AggregateFuncExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.display)
    }
}

#[derive(Debug)]
struct GroupValues {
    m: HashMap<Tuple, GroupId>,
}

impl GroupValues {
    fn new() -> Self {
        Self { m: HashMap::new() }
    }

    fn push(&mut self, key: impl Into<Tuple>) -> GroupId {
        let sz = self.m.len();
        let entry = self.m.entry(key.into()).or_insert(sz as GroupId);
        *entry
    }

    /// consume the value entries
    #[allow(clippy::wrong_self_convention)]
    fn into_entries(&mut self) -> Vec<(Tuple, GroupId)> {
        let m = std::mem::take(&mut self.m);
        m.into_iter().collect()
    }
}

struct BatchGroups<'a> {
    batch: &'a RecordBatch,
    gids: Vec<GroupId>,
    row_indices: HashMap<GroupId, Vec<usize>>,
}

impl<'a> BatchGroups<'a> {
    fn new(batch: &'a RecordBatch) -> Self {
        Self { batch, gids: vec![], row_indices: HashMap::new() }
    }

    /// Insert a row reference to gid, one row would have only one group
    /// and one group may have multiple rows.
    fn insert(&mut self, gid: GroupId, row_index: usize) {
        let entry = self.row_indices.entry(gid).or_insert_with(|| {
            self.gids.push(gid);
            vec![row_index]
        });
        entry.push(row_index)
    }

    fn entries(&self) -> Vec<(GroupId, Vec<usize>)> {
        let mut out = vec![];
        for gid in &self.gids {
            let rows = self.row_indices.get(&gid);
            if rows.is_none() {
                out.push((*gid, vec![]))
            }
            out.push((*gid, rows.unwrap().clone()))
        }
        out
    }
}

/// Aggregate physical executor.
#[derive(Debug)]
pub struct AggregateExec {
    input: Arc<dyn ExecutionPlan>,

    group_exprs: Vec<Arc<dyn PhysicalExpr>>,
    aggr_funcs: Vec<AggregateFuncExpr>,
    output_schema: LogicalSchema,

    // mutable states
    //
    // group values to track the group ids
    group_values: RefCell<GroupValues>,
    // accumulators per func
    accumulators: RefCell<Vec<Box<dyn Accumulator>>>,
    // result
    result: RefCell<Option<Vec<Tuple>>>,
}

impl AggregateExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        group_exprs: Vec<Arc<dyn PhysicalExpr>>,
        aggr_funcs: Vec<AggregateFuncExpr>,
        output_schema: LogicalSchema,
    ) -> Result<Self> {
        let group_values = GroupValues::new();
        let accumulators = aggr_funcs.iter().map(|it| it.func.accumulator()).collect::<Vec<_>>();
        Ok(Self {
            input,
            aggr_funcs,
            output_schema,
            group_exprs,
            group_values: RefCell::new(group_values),
            accumulators: RefCell::new(accumulators),
            result: RefCell::new(None),
        })
    }

    /// aggregate implemented as a pipeline breaker operator.
    fn aggregate(&self, ctx: &mut dyn Context) -> Result<()> {
        while let Some(rb) = self.input.execute(ctx)? {
            self.aggregate_batch(ctx, rb)?;
        }

        // Gather each aggregate function results by group
        let mut aggr_results = vec![];
        let mut accumulators = self.accumulators.borrow_mut();
        for a in accumulators.iter_mut() {
            let result = a.evaluate()?.into_iter().collect::<HashMap<GroupId, Value>>();
            aggr_results.push(result)
        }

        // Gather the final aggregated results by consume the group values
        // and append the aggregate function result.
        let mut results = vec![];
        for (tuple, gid) in self.group_values.borrow_mut().into_entries() {
            let mut values = tuple.into_vec();
            for am in &mut aggr_results {
                let value = am.remove(&gid).map_or(Value::Null, |it| it);
                values.push(value);
            }
            results.push(Tuple::from(values));
        }

        // Materialize the finale results
        self.result.swap(&RefCell::new(Some(results)));

        Ok(())
    }

    fn aggregate_batch(&self, ctx: &mut dyn Context, batch: RecordBatch) -> Result<()> {
        let num_rows = batch.num_rows();
        // Evaluate the grouping exprs
        let mut columnar_group_by_values = vec![];
        for expr in self.group_exprs.iter() {
            let tuple = expr.evaluate(ctx, &batch)?;
            columnar_group_by_values.push(tuple.into_vec());
        }
        let mut groups = BatchGroups::new(&batch);
        for i in 0..num_rows {
            let mut values = vec![];
            for it in &mut columnar_group_by_values {
                values.push(std::mem::take(&mut it[i]))
            }
            let gid = self.group_values.borrow_mut().push(values);
            groups.insert(gid, i);
        }
        self.accumulate(ctx, groups)?;
        Ok(())
    }

    fn accumulate(&self, ctx: &mut dyn Context, g: BatchGroups) -> Result<()> {
        let batch = g.batch;
        for (i, func) in self.aggr_funcs.iter().enumerate() {
            let mut columnar_args_value = vec![];
            for expr in func.args_expr.iter() {
                let tuple = expr.evaluate(ctx, batch)?;
                columnar_args_value.push(tuple)
            }
            let num_rows = batch.num_rows();
            let mut row_aggr_args_value = Vec::with_capacity(num_rows);
            for i in 0..num_rows {
                let mut values = vec![];
                for it in &mut columnar_args_value {
                    values.push(std::mem::take(&mut it[i]))
                }
                row_aggr_args_value.push(Tuple::from(values));
            }
            for (gid, rows) in g.entries() {
                let accumulator = &mut self.accumulators.borrow_mut()[i];
                let args_value = row_aggr_args_value
                    .iter()
                    .enumerate()
                    .filter_map(
                        |(i, value)| if rows.contains(&i) { Some(value.clone()) } else { None },
                    )
                    .collect::<Vec<_>>();
                accumulator.push(gid, args_value)?;
            }
        }
        Ok(())
    }
}

impl ExecutionPlan for AggregateExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> LogicalSchema {
        self.output_schema.clone()
    }

    fn init(&self, ctx: &mut dyn Context) -> Result<()> {
        self.input.init(ctx)
    }

    fn execute(&self, ctx: &mut dyn Context) -> Result<Option<RecordBatch>> {
        if self.result.borrow().is_none() {
            self.aggregate(ctx)?;
        }
        let mut result_borrow = self.result.borrow_mut();
        let result = result_borrow.as_mut().unwrap();
        if result.is_empty() {
            return Ok(None);
        }
        let n = ctx.vector_size().min(result.len());
        let rows = result.drain(0..n).collect::<Vec<_>>();
        Ok(Some(RecordBatch::new(&self.output_schema, rows)))
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
}

impl Display for AggregateExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let group_by =
            self.group_exprs.iter().map(|it| it.to_string()).collect::<Vec<_>>().join(", ");
        let aggr = self.aggr_funcs.iter().map(|it| it.to_string()).collect::<Vec<_>>().join(", ");
        write!(f, "AggregateExec: groupBy[{}], aggr=[{}]", group_by, aggr)
    }
}
