use std::any::Any;
use std::cell::Cell;
use std::cell::RefCell;
use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::Arc;

use crate::access::engine::Scan;
use crate::access::predicate::Predicate;
use crate::access::value::Values;
use crate::catalog::r#type::Value;
use crate::error::Error;
use crate::error::Result;
use crate::sql::execution::compiler::RecordBatch;
use crate::sql::execution::compiler::RecordBatchBuilder;
use crate::sql::execution::display::DisplayableExecutionPlan;
use crate::sql::execution::expr::PhysicalExpr;
use crate::sql::execution::Context;
use crate::sql::execution::ExecutionPlan;
use crate::sql::plan::plan::JoinType;
use crate::sql::plan::plan::Plan;
use crate::sql::plan::plan::TableScan;
use crate::sql::plan::schema::LogicalSchema;
use crate::sql::plan::schema::TableReference;

#[derive(Debug)]
pub struct ValuesExec {
    schema: LogicalSchema,
    values: Vec<Vec<Arc<dyn PhysicalExpr>>>,
    cursor: Cell<usize>,
}

impl ValuesExec {
    pub fn try_new(schema: LogicalSchema, values: Vec<Vec<Arc<dyn PhysicalExpr>>>) -> Result<Self> {
        Ok(Self { schema, values, cursor: Cell::new(0) })
    }
}

impl ExecutionPlan for ValuesExec {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> LogicalSchema {
        self.schema.clone()
    }

    fn init(&self, _ctx: &mut dyn Context) -> Result<()> {
        self.cursor.set(0);
        Ok(())
    }
    fn execute(&self, ctx: &mut dyn Context) -> Result<Option<RecordBatch>> {
        let cursor = self.cursor.get();
        if cursor >= self.values.len() {
            return Ok(None);
        }
        let to = self.values.len().min(cursor + ctx.vector_size());

        // Dummy batch for cell expr evaluation
        let batch = RecordBatchBuilder::new(&self.schema).build();
        let result = self.values[cursor..to]
            .iter()
            .map(|tuple_exprs| {
                let res = tuple_exprs
                    .iter()
                    .map(|cell| {
                        let values = cell.evaluate(ctx, &batch)?;
                        Ok(values.scalar()?)
                    })
                    .collect::<Result<Vec<Value>>>();
                res
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .map(|tuple| Values::from(tuple))
            .collect();

        self.cursor.set(to);
        Ok(Some(RecordBatch::new(&self.schema, result)))
    }
}

impl Display for ValuesExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let values = self
            .values
            .iter()
            .take(3)
            .map(|row| {
                let item = row.iter().map(|expr| expr.to_string()).collect::<Vec<_>>().join(", ");
                format!("({})", item)
            })
            .collect::<Vec<_>>();
        let eclipse = if values.len() > 3 { "..." } else { "" };
        write!(f, "ValuesExec: {}{}", values.join(", "), eclipse)
    }
}

#[derive(Debug)]
pub struct ProjectionExec {
    input: Arc<dyn ExecutionPlan>,
    exprs: Vec<Arc<dyn PhysicalExpr>>,
    output_schema: LogicalSchema,
}

impl ProjectionExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        exprs: Vec<Arc<dyn PhysicalExpr>>,
        output_schema: LogicalSchema,
    ) -> Self {
        Self { input, exprs, output_schema }
    }
}

impl ExecutionPlan for ProjectionExec {
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
        let rb = self.input.execute(ctx)?;
        if rb.is_none() {
            return Ok(None);
        }
        let rb = rb.unwrap().into_inner()?;
        let mut output_tuples = vec![];
        for tuple in rb.tuples {
            let mut output_tuple = vec![];
            let batch = RecordBatch::new(&rb.schema, vec![tuple]);
            for expr in &self.exprs {
                let values = expr.evaluate(ctx, &batch)?;
                output_tuple.push(values.scalar()?)
            }
            output_tuples.push(Values::from(output_tuple));
        }
        Ok(Some(RecordBatch::new(&rb.schema, output_tuples)))
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        let mut children =
            self.exprs.iter().map(|it| it.subqueries()).flatten().collect::<Vec<_>>();
        children.push(&self.input);
        children
    }
}

impl Display for ProjectionExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ProjectionExec: ")?;
        for (i, expr) in self.exprs.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{expr}")?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct SeqScanExec {
    table: TableReference,
    schema: LogicalSchema,
    projection: Option<Vec<usize>>,
    predicate: Option<Predicate>,
    output_schema: LogicalSchema,
    scan: RefCell<Option<Scan>>,
}

impl SeqScanExec {
    pub fn try_new(ts: TableScan) -> Result<Self> {
        if let Some(proj) = &ts.projection {
            if !proj.is_empty() {
                return Err(Error::unimplemented("projection push down is unimplemented yet"));
            }
        }

        // TODO: build predicate from filter.
        let predicate = None;

        Ok(SeqScanExec {
            table: ts.relation,
            schema: ts.schema,
            projection: ts.projection,
            predicate,
            output_schema: ts.output_schema,
            scan: RefCell::new(None),
        })
    }
}

impl ExecutionPlan for SeqScanExec {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> LogicalSchema {
        self.output_schema.clone()
    }

    fn init(&self, ctx: &mut dyn Context) -> Result<()> {
        let txn = ctx.txn();
        let scan = txn.scan(&self.table, self.predicate.clone())?;
        self.scan.borrow_mut().replace(scan);
        Ok(())
    }

    fn execute(&self, ctx: &mut dyn Context) -> Result<Option<RecordBatch>> {
        let mut scan_borrow = self.scan.borrow_mut();
        let scan =
            scan_borrow.as_mut().ok_or_else(|| Error::internal("SeqScanExec not initialized"))?;
        let mut tuples = vec![];
        let mut num_tuples = 0;
        while let Some(tuple) = scan.next().transpose()? {
            tuples.push(tuple.values);
            num_tuples += 1;
            if num_tuples == ctx.vector_size() {
                break;
            }
        }
        if tuples.is_empty() {
            return Ok(None);
        }
        Ok(Some(RecordBatch::new(&self.output_schema, tuples)))
    }
}

impl Display for SeqScanExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let projection = match &self.projection {
            Some(indices) => {
                let names =
                    indices.iter().map(|i| self.schema.field(*i).name.as_str()).collect::<Vec<_>>();
                format!(" projection=[{}]", names.join(", "))
            }
            _ => "".to_string(),
        };
        let predicate = match &self.predicate {
            Some(predicate) => {
                format!(" predicates=[{:?}]", predicate)
            }
            _ => "".to_string(),
        };
        write!(f, "SeqScanExec: {}{}{}", self.table, projection, predicate)
    }
}

#[derive(Debug)]
pub struct SubqueryAliasExec {
    input: Arc<dyn ExecutionPlan>,
    alias_schema: LogicalSchema,
    alias: TableReference,
}

impl SubqueryAliasExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        alias_schema: LogicalSchema,
        alias: TableReference,
    ) -> Self {
        Self { input, alias_schema, alias }
    }
}

impl ExecutionPlan for SubqueryAliasExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> LogicalSchema {
        self.alias_schema.clone()
    }

    fn init(&self, ctx: &mut dyn Context) -> Result<()> {
        self.input.init(ctx)
    }
    fn execute(&self, ctx: &mut dyn Context) -> Result<Option<RecordBatch>> {
        let rs = self.input.execute(ctx)?;
        if rs.is_none() {
            return Ok(None);
        }
        let rs = rs.unwrap();
        if rs.schema.len() != self.alias_schema.len() {
            return Err(Error::internal(format!(
                "Unexpected alias schema fields size, expect {}, got {}",
                rs.schema.len(),
                self.alias_schema.len()
            )));
        }
        for i in 0..rs.schema.len() {
            let a = &rs.schema.field(i).datatype;
            let b = &self.alias_schema.field(i).datatype;
            if a != b {
                return Err(Error::internal(format!(
                    "Unexpected alias schema field type at {}, expect: {}, got {}",
                    i, a, b
                )));
            }
        }
        let tuples = rs.into_inner()?.tuples;
        Ok(Some(RecordBatch::new(&self.alias_schema, tuples)))
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
}

impl Display for SubqueryAliasExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SubqueryAlias: {}", self.alias)
    }
}

/// Filter physical executor
#[derive(Debug)]
pub struct FilterExec {
    input: Arc<dyn ExecutionPlan>,
    predicate: Arc<dyn PhysicalExpr>,

    buffer: RefCell<Vec<Values>>,
}

impl FilterExec {
    fn filter_source(&self, ctx: &mut dyn Context) -> Result<Option<Vec<Values>>> {
        let rb = self.input.execute(ctx)?;
        if rb.is_none() {
            return Ok(None);
        }

        let rb = rb.unwrap();
        let indices = self
            .predicate
            .evaluate(ctx, &rb)?
            .into_iter()
            .enumerate()
            .filter_map(|(i, value)| match value {
                Value::Boolean(b) => {
                    if b {
                        Some(i)
                    } else {
                        None
                    }
                }
                _ => None,
            })
            .collect::<Vec<_>>();

        let mut matches = vec![];
        let mut rb = rb.into_inner()?;
        for i in indices {
            matches.push(std::mem::take(&mut rb.tuples[i]));
        }
        Ok(Some(matches))
    }
}

impl FilterExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, predicate: Arc<dyn PhysicalExpr>) -> Self {
        Self { input, predicate, buffer: RefCell::new(vec![]) }
    }
}

impl ExecutionPlan for FilterExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> LogicalSchema {
        self.input.schema()
    }

    fn init(&self, ctx: &mut dyn Context) -> Result<()> {
        self.input.init(ctx)
    }

    fn execute(&self, ctx: &mut dyn Context) -> Result<Option<RecordBatch>> {
        let mut n = ctx.vector_size();
        let mut tuples = vec![];

        // Consume residual first if any
        let residual = self.buffer.replace(vec![]);
        if !residual.is_empty() {
            n -= residual.len();
            tuples.extend(residual);
        }

        // Filter source
        loop {
            let output = self.filter_source(ctx)?;
            if output.is_none() {
                break;
            }
            let mut output = output.unwrap();
            if output.is_empty() {
                continue;
            }
            let k = n.min(output.len());
            let residual = output.split_off(k);
            tuples.extend(output);
            n -= k;
            if n == 0 {
                self.buffer.borrow_mut().extend(residual);
                break;
            }
        }

        if tuples.is_empty() {
            return Ok(None);
        }

        Ok(Some(RecordBatch::new(&self.input.schema(), tuples)))
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
}

impl Display for FilterExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "FilterExec: {}", self.predicate)
    }
}

#[derive(Debug)]
pub struct LimitExec {
    input: Arc<dyn ExecutionPlan>,
    skip: Option<u64>,
    fetch: Option<u64>,

    // mutable states across multiple exec
    skipped: Cell<bool>,
    fetched: Cell<u64>,
    buffer: RefCell<Vec<Values>>,
}

impl LimitExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, skip: Option<u64>, fetch: Option<u64>) -> Self {
        let skipped = if skip.is_none() { true } else { false };
        Self {
            input,
            skip,
            fetch,
            skipped: Cell::new(skipped),
            fetched: Cell::new(0),
            buffer: RefCell::new(vec![]),
        }
    }

    // TODO: It is able to skip without poll from table and discard
    //  here just for counting in some cases, e.g., order by pk, etc.
    //  The optimizer needs to rewrite the plan somehow.
    fn skip(&self, ctx: &mut dyn Context) -> Result<Option<()>> {
        if self.skipped.get() {
            return Ok(Some(()));
        }
        let mut skip = if let Some(k) = self.skip { k } else { 0 } as usize;
        if skip == 0 {
            return Ok(Some(()));
        }
        // Skipped tuples
        let mut tuples = vec![];
        loop {
            let output = self.poll(ctx)?;
            if output.is_none() {
                break;
            }
            let mut output = output.unwrap();
            if output.is_empty() {
                break;
            }
            let k = skip.min(output.len());
            let remaining = output.split_off(k);
            tuples.extend(output);
            skip -= k;
            if skip == 0 {
                self.skipped.set(true);
                self.buffer.borrow_mut().extend(remaining);
                break;
            }
        }
        if tuples.is_empty() {
            return Ok(None);
        }
        Ok(Some(()))
    }

    fn poll(&self, ctx: &mut dyn Context) -> Result<Option<Vec<Values>>> {
        let rb = self.input.execute(ctx)?;
        if rb.is_none() {
            return Ok(None);
        }

        let rb = rb.unwrap().into_inner()?;
        Ok(Some(rb.tuples))
    }
}

impl ExecutionPlan for LimitExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> LogicalSchema {
        self.input.schema()
    }

    fn init(&self, ctx: &mut dyn Context) -> Result<()> {
        self.input.init(ctx)
    }

    fn execute(&self, ctx: &mut dyn Context) -> Result<Option<RecordBatch>> {
        if self.skip(ctx)?.is_none() {
            return Ok(None);
        }
        let fetch = if let Some(k) = self.fetch { k } else { u64::MAX };
        let num_remain = (fetch - self.fetched.get()) as usize;
        if num_remain == 0 {
            return Ok(None);
        }

        // Number of tuples this batch would need return
        let mut n = ctx.vector_size().min(num_remain);
        if n == 0 {
            return Ok(None);
        }

        let mut tuples = vec![];

        // Consume buffered remaining first if any
        let mut buffered = self.buffer.replace(vec![]);
        if !buffered.is_empty() {
            if buffered.len() > n {
                // buffered tuples is enough, return directly.
                _ = buffered.split_off(n);
                tuples.extend(buffered);
                return Ok(Some(RecordBatch::new(&self.input.schema(), tuples)));
            }
            n -= buffered.len();
            tuples.extend(buffered);
        }

        loop {
            let output = self.poll(ctx)?;
            if output.is_none() {
                break;
            }
            let mut output = output.unwrap();
            if output.is_empty() {
                break;
            }
            let k = n.min(output.len());
            let remaining = output.split_off(k);
            tuples.extend(output);
            n -= k;
            if n == 0 {
                self.buffer.borrow_mut().extend(remaining);
                break;
            }
        }

        if tuples.is_empty() {
            return Ok(None);
        }

        Ok(Some(RecordBatch::new(&self.input.schema(), tuples)))
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
}

impl Display for LimitExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let skip = match self.skip {
            None => "".to_string(),
            Some(n) => format!(" skip={n}"),
        };
        let fetch = match self.fetch {
            None => "".to_string(),
            Some(n) => format!(" fetch={n}"),
        };
        write!(f, "LimitExec: {skip}{fetch}")
    }
}

#[derive(Debug)]
pub struct SortExprExec {
    expr: Arc<dyn PhysicalExpr>,
    asc: bool,
}

impl SortExprExec {
    pub fn new(expr: Arc<dyn PhysicalExpr>, asc: bool) -> Self {
        Self { expr, asc }
    }
}

impl Display for SortExprExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.expr)?;
        if self.asc {
            write!(f, " ASC")?;
        } else {
            write!(f, " DESC")?;
        }
        Ok(())
    }
}

/// Sort physical executor, it is implemented as a pipeline breaker
/// that needs all tuples before sorting.
/// TODO: support spill to disk.
#[derive(Debug)]
pub struct SortExec {
    input: Arc<dyn ExecutionPlan>,
    order: Vec<SortExprExec>,
    // mutable states across multiple exec
    sorted: Cell<bool>,
    buffer: RefCell<Vec<Values>>,
}

impl SortExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, order: Vec<SortExprExec>) -> Self {
        Self { input, order, sorted: Cell::new(false), buffer: RefCell::new(vec![]) }
    }

    pub fn materialized_sort(&self, ctx: &mut dyn Context) -> Result<()> {
        if self.sorted.get() {
            return Ok(());
        }

        let mut sortby = vec![];
        let mut tuples = vec![];
        while let Some(rb) = self.input.execute(ctx)? {
            // Evaluate the expr per batch, each element in the values
            // are evaluated values for each tuple for one order item.
            let mut expr_values = self
                .order
                .iter()
                .map(|it| it.expr.evaluate(ctx, &rb))
                .collect::<Result<Vec<_>>>()?;
            // Parse the values to sort criteria tuple-wise by pivot the values.
            for i in 0..rb.num_tuples() {
                let sort_keys = self
                    .order
                    .iter()
                    .enumerate()
                    .map(|(j, se)| {
                        let value = std::mem::take(&mut expr_values[j][i]);
                        (value, se.asc)
                    })
                    .collect::<Vec<_>>();
                sortby.push(sort_keys);
            }
            // Materialize the polled tuples
            let num_tuples = rb.num_tuples();
            let rb = rb.into_inner()?;
            tuples.extend(rb.tuples);
            if !rb.has_next || num_tuples < ctx.vector_size() {
                break;
            }
        }

        // Create paired data for sorting
        let mut entries: Vec<(usize, &Vec<(Value, bool)>)> =
            sortby.iter().enumerate().map(|(i, values)| (i, values)).collect();

        // Sort the indices based on the sort criteria
        entries.sort_by(|a, b| {
            for ((value_a, asc_a), (value_b, asc_b)) in a.1.iter().zip(b.1.iter()) {
                if asc_a != asc_b {
                    unreachable!()
                }
                match value_a.partial_cmp(value_b) {
                    None => {}
                    Some(std::cmp::Ordering::Equal) => {}
                    Some(o) => return if *asc_a { o } else { o.reverse() },
                }
            }
            std::cmp::Ordering::Equal
        });

        // Reorder tuples based on sorted indices
        let sorted_tuples =
            entries.into_iter().map(|(i, _)| std::mem::take(&mut tuples[i])).collect::<Vec<_>>();

        // Materialize the sorted tuples
        self.buffer.replace(sorted_tuples);
        self.sorted.set(true);

        Ok(())
    }
}

impl ExecutionPlan for SortExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> LogicalSchema {
        self.input.schema()
    }

    fn init(&self, ctx: &mut dyn Context) -> Result<()> {
        self.input.init(ctx)?;
        Ok(())
    }

    fn execute(&self, ctx: &mut dyn Context) -> Result<Option<RecordBatch>> {
        if !self.sorted.get() {
            self.materialized_sort(ctx)?;
        }
        let mut buffer = self.buffer.borrow_mut();
        if buffer.is_empty() {
            return Ok(None);
        }
        let n = ctx.vector_size().min(buffer.len());
        let tuples = buffer.drain(0..n).collect::<Vec<_>>();
        Ok(Some(RecordBatch::new(&self.input.schema(), tuples)))
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
}

impl Display for SortExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SortExec: ")?;
        for (i, it) in self.order.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{it}")?;
        }
        Ok(())
    }
}

/// Hash join physical executor, it is implemented as a pipeline breaker
/// that needs all tuples before joining.
/// TODO: support spill to disk.
#[derive(Debug)]
pub struct HashJoinExec {
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    join_type: JoinType,
    schema: LogicalSchema,
}

impl HashJoinExec {
    pub fn new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        join_type: JoinType,
        schema: LogicalSchema,
    ) -> Self {
        Self { left, right, join_type, schema }
    }
}

impl ExecutionPlan for HashJoinExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> LogicalSchema {
        self.schema.clone()
    }

    fn init(&self, ctx: &mut dyn Context) -> Result<()> {
        self.left.init(ctx)?;
        self.right.init(ctx)?;
        Ok(())
    }

    fn execute(&self, ctx: &mut dyn Context) -> Result<Option<RecordBatch>> {
        todo!()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.left, &self.right]
    }
}

impl Display for HashJoinExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

#[derive(Debug)]
pub struct ExplainExec {
    plan: Plan,
    executor: Arc<dyn ExecutionPlan>,
    verbose: bool,
    physical: bool,
    output_schema: LogicalSchema,
}

impl ExplainExec {
    pub fn new(
        plan: Plan,
        executor: Arc<dyn ExecutionPlan>,
        verbose: bool,
        physical: bool,
        output_schema: LogicalSchema,
    ) -> Self {
        Self { plan, executor, verbose, physical, output_schema }
    }
}

impl ExecutionPlan for ExplainExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> LogicalSchema {
        self.output_schema.clone()
    }

    fn init(&self, _ctx: &mut dyn Context) -> Result<()> {
        Ok(())
    }

    fn execute(&self, _ctx: &mut dyn Context) -> Result<Option<RecordBatch>> {
        let mut tuples = vec![];

        let logical_str =
            if self.verbose { format!("{:#}", self.plan) } else { format!("{}", self.plan) };
        tuples.push(Values::from(vec![
            Value::String("logical plan".to_string()),
            Value::String(logical_str),
        ]));

        if self.physical {
            tuples.push(Values::from(vec![
                Value::String("physical plan".to_string()),
                Value::String(format!("{}", DisplayableExecutionPlan::new(&self.executor))),
            ]))
        }

        let rb = RecordBatchBuilder::new(&self.output_schema).extend(tuples).nomore().build();
        Ok(Some(rb))
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.executor]
    }
}

impl Display for ExplainExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ExplainExec:")
    }
}
