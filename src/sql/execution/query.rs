use std::any::Any;
use std::cell::Cell;
use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::Arc;

use crate::access::predicate::Predicate;
use crate::access::value::Values;
use crate::catalog::r#type::Value;
use crate::error::Error;
use crate::error::Result;
use crate::sql::execution::expr::PhysicalExpr;
use crate::sql::execution::compiler::RecordBatch;
use crate::sql::execution::Context;
use crate::sql::execution::ExecutionPlan;
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

    fn init(&self) -> Result<()> {
        self.cursor.set(0);
        Ok(())
    }
    fn execute(&self, ctx: &mut dyn Context) -> Result<Option<RecordBatch>> {
        let batch = RecordBatch::new_empty_with_schema(&self.schema);
        let cursor = self.cursor.get();
        if cursor >= self.values.len() {
            return Ok(None);
        }
        let to = self.values.len().min(cursor + ctx.vector_size());
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

    fn init(&self) -> Result<()> {
        self.input.init()
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

    fn init(&self) -> Result<()> {
        Ok(())
    }

    fn execute(&self, ctx: &mut dyn Context) -> Result<Option<RecordBatch>> {
        let txn = ctx.txn();
        let mut scan = txn.scan(&self.table, self.predicate.clone())?;
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

    fn init(&self) -> Result<()> {
        self.input.init()
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
