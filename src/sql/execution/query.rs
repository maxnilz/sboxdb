use std::any::Any;
use std::cell::Cell;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::Arc;

use crate::access::engine::Scan;
use crate::access::predicate::Predicate;
use crate::access::value::Tuple;
use crate::catalog::r#type::Value;
use crate::error::Error;
use crate::error::Result;
use crate::sql::execution::compiler::RecordBatch;
use crate::sql::execution::compiler::RecordBatchBuilder;
use crate::sql::execution::display::DisplayableExecutionPlan;
use crate::sql::execution::expr::PhysicalExpr;
use crate::sql::execution::Context;
use crate::sql::execution::ExecutionPlan;
use crate::sql::execution::ExecutionEngine;
use crate::sql::plan::plan::JoinType;
use crate::sql::plan::plan::Plan;
use crate::sql::plan::plan::TableScan;
use crate::sql::plan::schema::FieldReference;
use crate::sql::plan::schema::LogicalSchema;
use crate::sql::plan::schema::TableReference;

#[derive(Debug)]
pub struct ValuesExec {
    schema: LogicalSchema,
    rows: Vec<Vec<Arc<dyn PhysicalExpr>>>,
    cursor: Cell<usize>,
}

impl ValuesExec {
    pub fn try_new(schema: LogicalSchema, values: Vec<Vec<Arc<dyn PhysicalExpr>>>) -> Result<Self> {
        Ok(Self { schema, rows: values, cursor: Cell::new(0) })
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
        if cursor >= self.rows.len() {
            return Ok(None);
        }
        let to = self.rows.len().min(cursor + ctx.vector_size());

        // Dummy batch for cell expr evaluation
        let batch = RecordBatchBuilder::new(&self.schema).build();
        let result = self.rows[cursor..to]
            .iter()
            .map(|tuple_exprs| {
                tuple_exprs
                    .iter()
                    .map(|cell| {
                        let values = cell.evaluate(ctx, &batch)?;
                        Ok(values.scalar()?)
                    })
                    .collect::<Result<Vec<Value>>>()
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .map(|values| Tuple::from(values))
            .collect();

        self.cursor.set(to);
        Ok(Some(RecordBatch::new(&self.schema, result)))
    }
}

impl Display for ValuesExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let values = self
            .rows
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
        let mut output_rows = vec![];
        for row in rb.rows {
            let mut output_row = vec![];
            let batch = RecordBatch::new(&rb.schema, vec![row]);
            for expr in &self.exprs {
                let values = expr.evaluate(ctx, &batch)?;
                output_row.push(values.scalar()?)
            }
            output_rows.push(Tuple::from(output_row));
        }
        Ok(Some(RecordBatch::new(&rb.schema, output_rows)))
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
        let mut rows = vec![];
        let mut num_rows = 0;
        while let Some(row) = scan.next().transpose()? {
            rows.push(row.tuple);
            num_rows += 1;
            if num_rows == ctx.vector_size() {
                break;
            }
        }
        if rows.is_empty() {
            return Ok(None);
        }
        Ok(Some(RecordBatch::new(&self.output_schema, rows)))
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
        let rows = rs.into_inner()?.rows;
        Ok(Some(RecordBatch::new(&self.alias_schema, rows)))
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

    buffer: RefCell<Vec<Tuple>>,
}

impl FilterExec {
    fn filter_source(&self, ctx: &mut dyn Context) -> Result<Option<Vec<Tuple>>> {
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
            matches.push(std::mem::take(&mut rb.rows[i]));
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
        let mut rows = vec![];

        // Consume residual first if any
        let residual = self.buffer.replace(vec![]);
        if !residual.is_empty() {
            n -= residual.len();
            rows.extend(residual);
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
            rows.extend(output);
            n -= k;
            if n == 0 {
                self.buffer.borrow_mut().extend(residual);
                break;
            }
        }

        if rows.is_empty() {
            return Ok(None);
        }

        Ok(Some(RecordBatch::new(&self.input.schema(), rows)))
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
    buffer: RefCell<Vec<Tuple>>,
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
        // Skipped rows
        let mut rows = vec![];
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
            rows.extend(output);
            skip -= k;
            if skip == 0 {
                self.skipped.set(true);
                self.buffer.borrow_mut().extend(remaining);
                break;
            }
        }
        if rows.is_empty() {
            return Ok(None);
        }
        Ok(Some(()))
    }

    fn poll(&self, ctx: &mut dyn Context) -> Result<Option<Vec<Tuple>>> {
        let rb = self.input.execute(ctx)?;
        if rb.is_none() {
            return Ok(None);
        }

        let rb = rb.unwrap().into_inner()?;
        Ok(Some(rb.rows))
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

        // Number of rows this batch would need return
        let mut n = ctx.vector_size().min(num_remain);
        if n == 0 {
            return Ok(None);
        }

        let mut rows = vec![];

        // Consume buffered remaining first if any
        let mut buffered = self.buffer.replace(vec![]);
        if !buffered.is_empty() {
            if buffered.len() > n {
                // buffered rows is enough, return directly.
                _ = buffered.split_off(n);
                rows.extend(buffered);
                return Ok(Some(RecordBatch::new(&self.input.schema(), rows)));
            }
            n -= buffered.len();
            rows.extend(buffered);
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
            rows.extend(output);
            n -= k;
            if n == 0 {
                self.buffer.borrow_mut().extend(remaining);
                break;
            }
        }

        if rows.is_empty() {
            return Ok(None);
        }

        Ok(Some(RecordBatch::new(&self.input.schema(), rows)))
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
/// that needs all rows before sorting.
/// TODO: support spill to disk.
#[derive(Debug)]
pub struct SortExec {
    input: Arc<dyn ExecutionPlan>,
    order: Vec<SortExprExec>,
    // mutable states across multiple exec
    sorted: Cell<bool>,
    buffer: RefCell<Vec<Tuple>>,
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
        let mut rows = vec![];
        while let Some(rb) = self.input.execute(ctx)? {
            // Evaluate the expr per batch, each element in the values
            // are evaluated values for each row for one order item.
            let mut expr_values = self
                .order
                .iter()
                .map(|it| it.expr.evaluate(ctx, &rb))
                .collect::<Result<Vec<_>>>()?;
            // Parse the values to sort criteria row-wise by pivot the values.
            for i in 0..rb.num_rows() {
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
            // Materialize the polled rows
            let num_rows = rb.num_rows();
            let rb = rb.into_inner()?;
            rows.extend(rb.rows);
            if !rb.has_next || num_rows < ctx.vector_size() {
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

        // Reorder rows based on sorted indices
        let sorted_rows =
            entries.into_iter().map(|(i, _)| std::mem::take(&mut rows[i])).collect::<Vec<_>>();

        // Materialize the sorted rows
        self.buffer.replace(sorted_rows);
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
        let rows = buffer.drain(0..n).collect::<Vec<_>>();
        Ok(Some(RecordBatch::new(&self.input.schema(), rows)))
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

#[derive(Debug, Default)]
pub struct HashJoinExecBuilder {
    left: Option<Arc<dyn ExecutionPlan>>,
    right: Option<Arc<dyn ExecutionPlan>>,
    join_type: Option<JoinType>,
    keys: Vec<(FieldReference, FieldReference)>,
    constraint: Option<Arc<dyn PhysicalExpr>>,
    schema: Option<LogicalSchema>,
}

impl HashJoinExecBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn left(mut self, left: Arc<dyn ExecutionPlan>) -> Self {
        self.left = Some(left);
        self
    }

    pub fn right(mut self, right: Arc<dyn ExecutionPlan>) -> Self {
        self.right = Some(right);
        self
    }

    pub fn join_type(mut self, join_type: JoinType) -> Self {
        self.join_type = Some(join_type);
        self
    }

    pub fn keys(mut self, keys: Vec<(FieldReference, FieldReference)>) -> Self {
        self.keys = keys;
        self
    }

    pub fn constraint(mut self, constraint: Arc<dyn PhysicalExpr>) -> Self {
        self.constraint = Some(constraint);
        self
    }

    pub fn schema(mut self, schema: LogicalSchema) -> Self {
        self.schema = Some(schema);
        self
    }

    pub fn build(mut self) -> Result<HashJoinExec> {
        let left = self.left.ok_or_else(|| Error::internal("left input is required"))?;
        let right = self.right.ok_or_else(|| Error::internal("right input is required"))?;
        let join_type = self.join_type.ok_or_else(|| Error::internal("join type is required"))?;
        if join_type != JoinType::Inner {
            return Err(Error::internal("Support inner join only"));
        }
        let constraint =
            self.constraint.ok_or_else(|| Error::internal("constraint is required"))?;
        let schema = self.schema.ok_or_else(|| Error::internal("schema is required"))?;

        if self.keys.is_empty() {
            return Err(Error::unimplemented(
                "Hash join without equijoin keys is not supported yet",
            ));
        }
        if self.keys.len() > 1 {
            return Err(Error::unimplemented(
                "Hash join multiple equijoin keys is not supported yet",
            ));
        }
        let (l, r) = self.keys.remove(0);
        let il = left
            .schema()
            .field_index_by_name(&l.relation, &l.name)
            .ok_or_else(|| Error::internal(format!("unknown left field {}", l)))?;
        let ir = right
            .schema()
            .field_index_by_name(&r.relation, &r.name)
            .ok_or_else(|| Error::internal(format!("unknown right field {}", r)))?;

        Ok(HashJoinExec {
            left,
            right,
            join_type: join_type,
            il,
            ir,
            constraint,
            schema,
            hashtable: RefCell::new(HashMap::new()),
            result: RefCell::new(None),
        })
    }
}

/// A straw man hash join physical executor, it is implemented as a pipeline breaker
/// that needs all rows before joining.
/// TODO: in-memory hash join for now, support spill to disk.
#[derive(Debug)]
pub struct HashJoinExec {
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    join_type: JoinType,
    // index for left schema field.
    il: usize,
    // index for right schema field.
    ir: usize,
    constraint: Arc<dyn PhysicalExpr>,
    schema: LogicalSchema,

    // mutable states
    //
    // hash table for in-memory probe.
    hashtable: RefCell<HashMap<Value, Vec<Tuple>>>,
    // for simplicity, join the result in memory in advance
    // then emit batch by batch, maybe do the prob on the
    // fly later.
    result: RefCell<Option<Vec<Tuple>>>,
}

impl HashJoinExec {
    /// Build the left side plan into a hash table for probe later by
    /// poll the whole data from the left executor.
    fn build_left(&self, ctx: &mut dyn Context) -> Result<()> {
        let rs = ExecutionEngine::poll_executor(ctx, Arc::clone(&self.left))?;
        let i = self.il;
        let mut hashtable = self.hashtable.borrow_mut();
        for row in rs.rows {
            let key = row[i].clone();
            match hashtable.get_mut(&key) {
                Some(entry) => entry.push(row),
                None => {
                    hashtable.insert(key, vec![row]);
                }
            }
        }
        Ok(())
    }

    fn prob(&self, row: Tuple) -> Vec<Tuple> {
        let key = &row[self.ir];
        let hashtable = self.hashtable.borrow();
        let entries = hashtable.get(key);
        if entries.is_none() {
            return vec![];
        }
        let entries = entries.unwrap();
        let mut output = vec![];
        for row in entries {
            let mut ans = row.clone();
            ans.extend(row.clone());
            output.push(ans);
        }
        output
    }

    fn strawman_join(&self, ctx: &mut dyn Context) -> Result<()> {
        let mut results = vec![];
        let rs = ExecutionEngine::poll_executor(ctx, Arc::clone(&self.right))?;
        for row in rs.rows {
            let output = self.prob(row);
            results.extend(output)
        }
        self.result.swap(&RefCell::new(Some(results)));
        Ok(())
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
        if self.result.borrow().is_none() {
            // build the hashtable from left input.
            self.build_left(ctx)?;
            // join
            self.strawman_join(ctx)?;
        }

        let mut result_borrow = self.result.borrow_mut();
        let result = result_borrow.as_mut().unwrap();
        if result.is_empty() {
            return Ok(None);
        }
        let mut n = ctx.vector_size().min(result.len());
        let mut rows = vec![];
        loop {
            let row = result.remove(0);
            let ok = self
                .constraint
                .evaluate(ctx, &RecordBatch::new(&self.schema, vec![row.clone()]))?
                .bool()?;
            if !ok {
                continue;
            }
            rows.push(row);
            n -= 1;
            if n == 0 {
                break;
            }
        }
        Ok(Some(RecordBatch::new(&self.schema, rows)))
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.left, &self.right]
    }
}

impl Display for HashJoinExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} HashJoinExec:", self.join_type)?;
        let fl = self.left.schema().field(self.il).clone();
        let fr = self.right.schema().field(self.ir).clone();
        write!(f, " {}@{} = {}@{}", &fl.name, self.il, &fr.name, self.ir)?;
        write!(f, ", Constraint: {}", self.constraint)?;
        Ok(())
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
        let mut rows = vec![];

        let logical_str =
            if self.verbose { format!("{:#}", self.plan) } else { format!("{}", self.plan) };
        rows.push(Tuple::from(vec![
            Value::String("logical plan".to_string()),
            Value::String(logical_str),
        ]));

        if self.physical {
            rows.push(Tuple::from(vec![
                Value::String("physical plan".to_string()),
                Value::String(format!("{}", DisplayableExecutionPlan::new(&self.executor))),
            ]))
        }

        let rb = RecordBatchBuilder::new(&self.output_schema).extend(rows).nomore().build();
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
