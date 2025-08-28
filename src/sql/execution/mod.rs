use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::Arc;

use crate::access::value::Tuple;
use crate::error::Error;
use crate::error::Result;
use crate::sql::execution::compiler::ExecutionPlan;
use crate::sql::execution::context::Context;
use crate::sql::execution::display::TabularDisplay;
use crate::sql::plan::schema::LogicalSchema;

mod aggregate;
mod compiler;
mod context;
mod ddl;
mod display;
mod dml;
mod expr;
mod query;

#[derive(Clone)]
pub struct Scheduler {}

impl Scheduler {
    /// Execute a physical query plan.
    pub fn execute(ctx: &mut dyn Context, executor: Arc<dyn ExecutionPlan>) -> Result<ResultSet> {
        executor.init(ctx)?;
        Self::poll_executor(ctx, executor)
    }

    /// Poll the executor until exhausted.
    fn poll_executor(ctx: &mut dyn Context, executor: Arc<dyn ExecutionPlan>) -> Result<ResultSet> {
        let mut rs = ResultSet { schema: executor.schema(), rows: vec![] };
        while let Some(rb) = executor.execute(ctx)? {
            let num_rows = rb.num_rows();

            let rb = rb.into_inner()?;
            rs.rows.extend(rb.rows);

            if !rb.has_next || num_rows < ctx.vector_size() {
                break;
            }
        }
        Ok(rs)
    }
}

pub struct ResultSet {
    schema: LogicalSchema,
    rows: Vec<Tuple>,
}

impl ResultSet {
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn num_cols(&self) -> usize {
        self.schema.fields().len()
    }

    pub fn columnar_values_at(&self, col_idx: usize) -> Result<Tuple> {
        let values = self
            .rows
            .iter()
            .map(|row| {
                row.get(col_idx)
                    .cloned()
                    .ok_or(Error::internal(format!("value at column {} is out of bound", col_idx)))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Tuple::from(values))
    }
}

impl Display for ResultSet {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        TabularDisplay::new(&self.schema, &self.rows).fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use goldenfile::Mint;

    use super::*;
    use crate::access::engine::Engine;
    use crate::access::engine::Transaction;
    use crate::access::kv::Kv;
    use crate::catalog::catalog::Catalog;
    use crate::sql::execution::compiler::Compiler;
    use crate::sql::execution::context::ExecContext;
    use crate::sql::execution::display::DisplayableExecutionPlan;
    use crate::sql::parser::display_utils;
    use crate::sql::parser::Parser;
    use crate::sql::plan::plan::Plan;
    use crate::sql::plan::planner::BindContext;
    use crate::sql::plan::planner::Planner;
    use crate::storage::memory::Memory;

    struct TxnExecutor {
        planner: Planner,
        compiler: Compiler,

        txn: Arc<dyn Transaction>,
    }

    impl TxnExecutor {
        fn try_new(kv: &Kv<Memory>) -> Result<Self> {
            let txn: Arc<dyn Transaction> = Arc::new(kv.begin()?);
            let planner = Planner::new();
            let compiler = Compiler {};
            Ok(Self { planner, compiler, txn })
        }
        fn execute_query(&self, query: &str) -> Result<ResultSet> {
            let mut parser = Parser::new(query)?;
            let stmt = parser.parse_statement()?;
            let mut ctx = BindContext::new(Arc::clone(&self.txn) as Arc<dyn Catalog>);
            let plan = self.planner.sql_statement_to_plan(&mut ctx, stmt)?;
            let executor = self.compiler.build_execution_plan(plan)?;
            let ctx: &mut dyn Context = &mut ExecContext::new(Arc::clone(&self.txn), 10);
            Scheduler::execute(ctx, executor)
        }

        fn logical_plan(&self, query: &str) -> Result<Plan> {
            let mut parser = Parser::new(query)?;
            let stmt = parser.parse_statement()?;
            let mut ctx = BindContext::new(Arc::clone(&self.txn) as Arc<dyn Catalog>);
            self.planner.sql_statement_to_plan(&mut ctx, stmt)
        }

        fn physical_plan(&self, plan: Plan) -> Result<Arc<dyn ExecutionPlan>> {
            self.compiler.build_execution_plan(plan)
        }

        fn execute(&self, executor: Arc<dyn ExecutionPlan>) -> Result<ResultSet> {
            let ctx: &mut dyn Context = &mut ExecContext::new(Arc::clone(&self.txn), 2);
            Scheduler::execute(ctx, executor)
        }

        fn commit(&self) -> Result<()> {
            self.txn.commit()
        }

        fn rollback(&self) -> Result<()> {
            self.txn.rollback()
        }
    }

    fn setup(queries: &[&str]) -> Result<Kv<Memory>> {
        let kv = Kv::new(Memory::new());

        let txn = TxnExecutor::try_new(&kv)?;
        for q in queries.iter() {
            txn.execute_query(q)?;
        }
        txn.commit()?;

        Ok(kv)
    }

    const GOLDEN_DIR: &str = "src/sql/execution/golden";
    macro_rules! test_physical_planner {
        ($($name:ident: $stmt:expr, )*) => {
            $(
                #[test]
                fn $name() -> Result<()> {
                    let queries = vec![
                        "CREATE TABLE users (
                            id INTEGER PRIMARY KEY,
                            name VARCHAR(100) NOT NULL,
                            email VARCHAR(100) NOT NULL
                        );",
                        "INSERT INTO users (id, name, email) VALUES
                          (1, 'Alice', 'alice@example.com'),
                          (2, 'Bob', 'bob@example.com'),
                          (3, 'Charlie', 'charlie@example.com');",
                    ];
                    let kv = setup(&queries)?;
                    let txn = TxnExecutor::try_new(&kv)?;

                    let mut mint = Mint::new(GOLDEN_DIR);
                    let mut f = mint.new_goldenfile(format!("{}", stringify!($name)))?;

                    write!(f, "Stmt: \n")?;
                    write!(f, "-----\n")?;
                    write!(f, "{}\n\n", display_utils::dedent($stmt))?;

                    let plan = txn.logical_plan($stmt)?;

                    write!(f, "Logical Plan:\n")?;
                    write!(f, "--------------\n\n")?;
                    write!(f, "{}\n\n", &plan)?;

                    let executor = txn.physical_plan(plan)?;
                    let displayable = DisplayableExecutionPlan::new(&executor);

                    write!(f, "Physical Plan:\n")?;
                    write!(f, "---------------\n\n")?;
                    write!(f, "{}\n\n", displayable)?;

                    let rs = txn.execute(executor)?;
                    write!(f, "Result:\n")?;
                    write!(f, "-------\n\n")?;
                    write!(f, "{}\n\n", rs)?;

                    txn.rollback()?;

                    Ok(())
                }
            )*
        }
    }

    test_physical_planner! {
        simple_query: "SELECT *, 1+1 FROM users",
        alias: "SELECT a.* FROM (SELECT * FROM users) AS a",
        query_filer_simple: "SELECT *, 1+1 FROM users WHERE id = 1",
        query_filter_const: "SELECT *, 1+1 FROM users WHERE 1=1",
        query_filter_conj_or: "SELECT *, 1+1 FROM users WHERE id = 1 OR id = 2 OR id = 3",
        limit: "SELECT *, 1+1 FROM users WHERE id = 1 OR id = 2 OR id = 3 offset 2 limit 4",
        sort: "SELECT id, name, email FROM users ORDER BY name DESC, email ASC",
        explain: "EXPLAIN physical verbose SELECT * FROM users",
        join: "SELECT a.*, b.id AS b_id, b.name AS b_name, b.email AS b_email FROM users AS a JOIN users AS b ON a.id = b.id",
        simple_agg: "SELECT count(*) FROM users",
        simple_agg_with_groupby: "SELECT id, count(*) AS a, count(*) AS b FROM users GROUP BY id ORDER BY id",
    }
}
