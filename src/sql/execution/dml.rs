use std::any::Any;
use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::Arc;

use crate::access::value::Row;
use crate::access::value::Tuple;
use crate::catalog::r#type::Value;
use crate::error::Result;
use crate::sql::execution::compiler::RecordBatch;
use crate::sql::execution::compiler::RecordBatchBuilder;
use crate::sql::execution::Context;
use crate::sql::execution::ExecutionPlan;
use crate::sql::execution::ExecutionEngine;
use crate::sql::plan::schema::LogicalSchema;
use crate::sql::plan::schema::TableReference;

/// Insert rows execution plan
#[derive(Debug)]
pub struct InsertExec {
    pub table: TableReference,
    pub input: Arc<dyn ExecutionPlan>,
    pub output_schema: LogicalSchema,
}

impl InsertExec {
    pub fn try_new(
        table: TableReference,
        input: Arc<dyn ExecutionPlan>,
        output_schema: LogicalSchema,
    ) -> Result<Self> {
        output_schema.is_affected_rows_count_schema()?;
        Ok(Self { table, input, output_schema })
    }
}

impl ExecutionPlan for InsertExec {
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
        let rs = ExecutionEngine::poll_executor(ctx, Arc::clone(&self.input))?;
        let columns = rs.schema.fields().to_columns_with_value_as_default()?;
        let txn = ctx.txn();
        let mut rows_affected = 0;
        for tv in rs.rows.into_iter() {
            txn.insert(&self.table, Row::new(tv, &columns)?)?;
            rows_affected += 1;
        }
        let output_row: Tuple = vec![Value::Integer(rows_affected)].into();
        let rb =
            RecordBatchBuilder::new(&self.output_schema).extend(vec![output_row]).nomore().build();
        Ok(Some(rb))
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
}

impl Display for InsertExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DML: op=[Insert] table=[{}]", self.table)
    }
}

/// Update execution plan
#[derive(Debug)]
pub struct UpdateExec {
    pub table: TableReference,
    pub input: Arc<dyn ExecutionPlan>,
    pub output_schema: LogicalSchema,
}

impl UpdateExec {
    pub fn try_new(
        table: TableReference,
        input: Arc<dyn ExecutionPlan>,
        output_schema: LogicalSchema,
    ) -> Result<Self> {
        output_schema.is_affected_rows_count_schema()?;
        Ok(Self { table, input, output_schema })
    }
}

impl ExecutionPlan for UpdateExec {
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
        let rs = ExecutionEngine::poll_executor(ctx, Arc::clone(&self.input))?;
        let columns = rs.schema.fields().to_columns_with_value_as_default()?;
        let txn = ctx.txn();
        let mut rows_affected = 0;
        for tv in rs.rows.into_iter() {
            let row = Row::new(tv, &columns)?;
            let pk = row.primary_key()?;
            txn.delete(&self.table, pk)?;
            txn.insert(&self.table, row)?;
            rows_affected += 1;
        }
        let output_row: Tuple = vec![Value::Integer(rows_affected)].into();
        let rb =
            RecordBatchBuilder::new(&self.output_schema).extend(vec![output_row]).nomore().build();
        Ok(Some(rb))
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
}

impl Display for UpdateExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DML: op=[Update] table=[{}]", self.table)
    }
}

/// Delete execution plan
#[derive(Debug)]
pub struct DeleteExec {
    pub table: TableReference,
    pub input: Arc<dyn ExecutionPlan>,
    pub output_schema: LogicalSchema,
}

impl DeleteExec {
    pub fn try_new(
        table: TableReference,
        input: Arc<dyn ExecutionPlan>,
        output_schema: LogicalSchema,
    ) -> Result<Self> {
        output_schema.is_affected_rows_count_schema()?;
        Ok(Self { table, input, output_schema })
    }
}

impl ExecutionPlan for DeleteExec {
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
        let rs = ExecutionEngine::poll_executor(ctx, Arc::clone(&self.input))?;
        let columns = rs.schema.fields().to_columns_with_value_as_default()?;
        let txn = ctx.txn();
        let mut rows_affected = 0;
        for tv in rs.rows.into_iter() {
            let row = Row::new(tv, &columns)?;
            let pk = row.primary_key()?;
            txn.delete(&self.table, pk)?;
            rows_affected += 1;
        }
        let output_row: Tuple = vec![Value::Integer(rows_affected)].into();
        let rb =
            RecordBatchBuilder::new(&self.output_schema).extend(vec![output_row]).nomore().build();
        Ok(Some(rb))
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
}

impl Display for DeleteExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DML: op=[Delete] table=[{}]", self.table)
    }
}
