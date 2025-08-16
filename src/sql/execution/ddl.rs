use std::any::Any;
use std::cell::RefCell;
use std::fmt::Display;
use std::fmt::Formatter;

use crate::catalog::index::Index;
use crate::catalog::schema::Schema;
use crate::error::Error;
use crate::error::Result;
use crate::sql::execution::compiler::RecordBatch;
use crate::sql::execution::Context;
use crate::sql::execution::ExecutionPlan;
use crate::sql::plan::plan::CreateIndex;
use crate::sql::plan::plan::DropIndex;
use crate::sql::plan::plan::DropTable;
use crate::sql::plan::schema::LogicalSchema;

/// Create table execution plan
#[derive(Debug)]
pub struct CreateTableExec {
    table_name: String,
    pub table_schema: RefCell<Schema>,
    pub if_not_exists: bool,
}

impl CreateTableExec {
    /// Create a table creation execution plan from
    /// create table logical plan node.
    pub fn new(table_schema: Schema, if_not_exists: bool) -> Self {
        Self {
            table_name: table_schema.name.clone(),
            table_schema: RefCell::new(table_schema),
            if_not_exists,
        }
    }
}

impl ExecutionPlan for CreateTableExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> LogicalSchema {
        LogicalSchema::empty()
    }

    fn init(&self, _ctx: &mut dyn Context) -> Result<()> {
        Ok(())
    }
    fn execute(&self, ctx: &mut dyn Context) -> Result<Option<RecordBatch>> {
        let txn = &mut ctx.txn();
        let schema = self.table_schema.take();
        match txn.create_table(schema) {
            Ok(_) => Ok(None),
            Err(Error::AlreadyExists(_)) if self.if_not_exists => Ok(None),
            Err(err) => Err(err),
        }
    }
}

impl Display for CreateTableExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CREATE TABLE {}, if not exists: {}", self.table_name, self.if_not_exists)
    }
}

/// Create index execution plan
#[derive(Debug)]
pub struct CreateIndexExec {
    table_name: String,
    index_name: String,
    pub index: RefCell<Index>,
    pub if_not_exists: bool,
}

impl CreateIndexExec {
    pub fn try_new(node: CreateIndex) -> Result<Self> {
        let table_name = node.relation.to_string();
        let index_name = node.name.clone();
        let columns = node.columns.to_columns(|_, _| Ok(None))?;
        let index = Index::new(node.name, node.relation, columns, node.unique);
        Ok(Self {
            table_name,
            index_name,
            index: RefCell::new(index),
            if_not_exists: node.if_not_exists,
        })
    }
}

impl ExecutionPlan for CreateIndexExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> LogicalSchema {
        LogicalSchema::empty()
    }

    fn init(&self, _ctx: &mut dyn Context) -> Result<()> {
        Ok(())
    }
    fn execute(&self, ctx: &mut dyn Context) -> Result<Option<RecordBatch>> {
        let index = self.index.take();
        match ctx.txn().create_index(index) {
            Ok(_) => Ok(None),
            Err(Error::AlreadyExists(_)) if self.if_not_exists => Ok(None),
            Err(err) => Err(err),
        }
    }
}

impl Display for CreateIndexExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CREATE INDEX {} on {}, if not exists: {}",
            self.index_name, self.table_name, self.if_not_exists
        )
    }
}

/// Drop table execution plan
#[derive(Debug)]
pub struct DropTableExec {
    pub table_name: String,
    // TODO: support if_exists.
    pub _if_exists: bool,
}

impl DropTableExec {
    pub fn new(node: DropTable) -> Self {
        Self { table_name: node.relation.into(), _if_exists: node.if_exists }
    }
}

impl ExecutionPlan for DropTableExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> LogicalSchema {
        LogicalSchema::empty()
    }
    fn init(&self, _ctx: &mut dyn Context) -> Result<()> {
        Ok(())
    }
    fn execute(&self, ctx: &mut dyn Context) -> Result<Option<RecordBatch>> {
        ctx.txn().delete_table(&self.table_name)?;
        Ok(None)
    }
}

impl Display for DropTableExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DROP TABLE {}", self.table_name)
    }
}

/// Drop index execution plan
#[derive(Debug)]
pub struct DropIndexExec {
    pub index_name: String,
    pub table_name: String,
    // TODO: support if_exists.
    pub _if_exists: bool,
}

impl DropIndexExec {
    pub fn new(node: DropIndex) -> Self {
        Self {
            index_name: node.name,
            table_name: node.table_reference.into(),
            _if_exists: node.if_exists,
        }
    }
}

impl ExecutionPlan for DropIndexExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> LogicalSchema {
        LogicalSchema::empty()
    }
    fn init(&self, _ctx: &mut dyn Context) -> Result<()> {
        Ok(())
    }
    fn execute(&self, ctx: &mut dyn Context) -> Result<Option<RecordBatch>> {
        ctx.txn().delete_index(&self.index_name, &self.table_name)?;
        Ok(None)
    }
}

impl Display for DropIndexExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DROP INDEX {} ON {}", self.index_name, self.table_name)
    }
}
