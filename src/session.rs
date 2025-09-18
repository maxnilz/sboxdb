use std::sync::Arc;

use crate::access::engine::Engine;
use crate::access::engine::Transaction;
use crate::error::Error;
use crate::error::Result;
use crate::sql::execution::compiler::Compiler;
use crate::sql::execution::context::Context;
use crate::sql::execution::context::ExecContext;
use crate::sql::execution::ExecutionEngine;
use crate::sql::execution::ResultSet;
use crate::sql::parser::ast::Statement;
use crate::sql::parser::Parser;
use crate::sql::plan::planner::BindContext;
use crate::sql::plan::planner::Planner;

/// A source agnostic session for query
pub struct Session<E: Engine> {
    engine: E,

    planner: Planner,
    compiler: Compiler,
    vector_size: usize,

    txn: Option<Arc<dyn Transaction>>,
}

impl<E: Engine + 'static> Session<E> {
    pub fn new(engine: E) -> Self {
        Self {
            engine,
            planner: Planner::new(),
            compiler: Compiler::new(),
            vector_size: 10,
            txn: None,
        }
    }

    /// Process query
    pub fn process_query(&mut self, query: String) -> Result<ResultSet> {
        let stmt = self.parse_query(query)?;
        match stmt {
            Statement::Begin { .. } if self.txn.is_some() => {
                Err(Error::value("Already in a transaction"))
            }
            Statement::Begin { read_only: true, as_of: None } => {
                let txn = self.engine.begin_read_only()?;
                self.txn = Some(Arc::new(txn));
                Ok(ResultSet::from(self.must_txn().as_ref()))
            }
            Statement::Begin { read_only: true, as_of: Some(version) } => {
                let txn = self.engine.begin_as_of(version)?;
                self.txn = Some(Arc::new(txn));
                Ok(ResultSet::from(self.must_txn().as_ref()))
            }
            Statement::Begin { read_only: false, as_of: Some(_) } => {
                Err(Error::value("Can't start read-write transaction in a given version"))
            }
            Statement::Begin { read_only: false, as_of: None } => {
                let txn = self.engine.begin()?;
                self.txn = Some(Arc::new(txn));
                Ok(ResultSet::from(self.must_txn().as_ref()))
            }
            Statement::Commit | Statement::Rollback if self.txn.is_none() => {
                Err(Error::value("Not in a transaction"))
            }
            Statement::Commit => {
                let txn = self.must_txn();
                txn.commit()?;
                let rs = ResultSet::from(txn.as_ref());
                self.txn = None;
                Ok(rs)
            }
            Statement::Rollback => {
                let txn = self.must_txn();
                txn.rollback()?;
                let rs = ResultSet::from(txn.as_ref());
                self.txn = None;
                Ok(rs)
            }
            stmt if self.txn.is_some() => self.execute_stmt(stmt),
            stmt => self.execute_auto_stmt(stmt),
        }
    }

    /// Execute the statement that have no explicit transaction wrapped
    /// around by start & attach an implicit txn then detach it afterward.
    fn execute_auto_stmt(&mut self, stmt: Statement) -> Result<ResultSet> {
        let txn = match stmt {
            Statement::Select { .. } => self.engine.begin_read_only(),
            _ => self.engine.begin(),
        }?;
        self.txn = Some(Arc::new(txn));
        let res = self.execute_stmt(stmt);
        let result = match res {
            Ok(rs) => {
                self.must_txn().commit()?;
                Ok(rs)
            }
            Err(err) => {
                self.must_txn().rollback()?;
                Err(err)
            }
        };
        self.txn = None;
        result
    }

    /// Execute a statement inside a transaction.
    fn execute_stmt(&mut self, stmt: Statement) -> Result<ResultSet> {
        let txn = self.must_txn();
        let catalog = Arc::clone(txn);
        let mut ctx = BindContext::new(catalog);
        let plan = self.planner.sql_statement_to_plan(&mut ctx, stmt)?;
        let executor = self.compiler.build_execution_plan(plan)?;
        let ctx: &mut dyn Context = &mut ExecContext::new(Arc::clone(txn), self.vector_size);
        ExecutionEngine::execute(ctx, executor)
    }

    fn must_txn(&self) -> &Arc<dyn Transaction> {
        self.txn.as_ref().unwrap()
    }

    fn parse_query(&self, query: String) -> Result<Statement> {
        let mut parser = Parser::new(&query)?;
        parser.parse_statement()
    }
}
