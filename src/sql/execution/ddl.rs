use std::any::Any;
use std::cell::RefCell;
use std::fmt::Display;
use std::fmt::Formatter;

use log::debug;

use crate::catalog::index::Index;
use crate::catalog::schema::Schema;
use crate::error::Error;
use crate::error::Result;
use crate::sql::execution::compiler::Compiler;
use crate::sql::execution::compiler::RecordBatch;
use crate::sql::execution::context::ExecContext;
use crate::sql::execution::Context;
use crate::sql::execution::ExecutionEngine;
use crate::sql::execution::ExecutionPlan;
use crate::sql::parser::Parser;
use crate::sql::plan::plan::CreateIndex;
use crate::sql::plan::plan::DropIndex;
use crate::sql::plan::plan::DropTable;
use crate::sql::plan::planner::BindContext;
use crate::sql::plan::planner::Planner;
use crate::sql::plan::schema::LogicalSchema;
use crate::tpcc;
use crate::tpcc::TpccGenerator;
use crate::unimplemented_err;

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

#[derive(Debug)]
pub struct CreateDatasetExec {
    pub dataset_name: String,
    pub if_not_exists: bool,
}

impl CreateDatasetExec {
    pub fn try_new(dataset_name: String, if_not_exists: bool) -> Result<Self> {
        if !dataset_name.eq_ignore_ascii_case("tpcc") {
            return Err(unimplemented_err!("Unknown dataset {}", dataset_name));
        }
        Ok(Self { dataset_name, if_not_exists })
    }
}

impl ExecutionPlan for CreateDatasetExec {
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
        match self.dataset_name.as_str() {
            "tpcc" => {
                // create tables
                debug!("tpcc creating tables");
                let mut parser = Parser::new(tpcc::TPCC_TABLES)?;
                let stmts = parser.parse_statements()?;
                let planner = Planner::new();
                let compiler = Compiler {};
                for stmt in stmts.into_iter() {
                    let mut bc = BindContext::new(ctx.txn());
                    let plan = planner.sql_statement_to_plan(&mut bc, stmt)?;
                    let executor = compiler.build_execution_plan(plan)?;
                    let mut ec = ExecContext::new(ctx.txn(), 10);
                    ExecutionEngine::execute(&mut ec, executor)?;
                }
                // load data
                debug!("tpcc init generator");
                let mut generator = TpccGenerator::new(10);

                debug!("tpcc generating data");
                let data = generator.generate()?;

                debug!("tpcc loading generated data");
                let queries = data.to_queries()?;
                for it in queries.into_iter() {
                    let mut parser = Parser::new(&it)?;
                    let stmts = parser.parse_statements()?;
                    for stmt in stmts.into_iter() {
                        let mut bc = BindContext::new(ctx.txn());
                        let plan = planner.sql_statement_to_plan(&mut bc, stmt)?;
                        let executor = compiler.build_execution_plan(plan)?;
                        let mut ec = ExecContext::new(ctx.txn(), 10);
                        ExecutionEngine::execute(&mut ec, executor)?;
                    }
                }
                Ok(None)
            }
            _ => Ok(None),
        }
    }
}

impl Display for CreateDatasetExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CreateDataset {}, if not exists: {}", self.dataset_name, self.if_not_exists)
    }
}
