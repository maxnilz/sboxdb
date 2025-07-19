use std::sync::Arc;

use crate::catalog::r#type::DataType;
use crate::error::Error;
use crate::error::Result;
use crate::sql::plan::expr::Expr;
use crate::sql::plan::schema::FieldBuilder;
use crate::sql::plan::schema::Fields;
use crate::sql::plan::schema::LogicalSchema;
use crate::sql::plan::schema::TableReference;

/// A `Plan` is a logical node in a tree of relational operators(such as
/// Projection or Filter). Also known as `Logical Plan`
#[derive(Clone, Debug)]
pub enum Plan {
    /// Transaction statements
    Transaction(Transaction),
    /// Create table
    CreateTable(CreateTable),
    /// Create index
    CreateIndex(CreateIndex),
    /// Drop table
    DropTable(DropTable),
    /// Drop index
    DropIndex(DropIndex),
    /// Insert data
    Insert(Insert),
    /// Update data
    Update(Update),
    /// Delete data
    Delete(Delete),
    /// Values expression. See
    /// [Postgres VALUES](https://www.postgresql.org/docs/current/queries-values.html)
    /// documentation for more details. This is used to implement SQL such as
    /// `VALUES (1, 2), (3, 4)`
    Values(Values),
    /// Evaluates an arbitrary list of expressions on its input.
    Projection(Projection),
    /// Scan rows from a table/relation.
    TableScan(TableScan),
    /// Aliased relation provides, or changes, the name of a relation.
    SubqueryAlias(SubqueryAlias),
    /// Join two logical plans on one or more join columns.
    /// This is used to implement SQL `JOIN`
    Join(Join),
    /// Filters rows from its input that do not match an
    /// expression (essentially a WHERE clause with a predicate
    /// expression).
    ///
    /// Semantically, `<predicate>` is evaluated for each row of the
    /// input; If the value of `<predicate>` is true, the input row is
    /// passed to the output. If the value of `<predicate>` is false
    /// (or null), the row is discarded.
    Filter(Filter),
    /// Aggregates its input based on a set of grouping and aggregate
    /// expressions (e.g. SUM). This is used to implement SQL aggregates
    /// and `GROUP BY`.
    /// TODO: aggregate functions does not supported yet
    Aggregate(Aggregate),
    /// Sorts its input according to a list of sort expressions. This
    /// is used to implement SQL `ORDER BY`
    Sort(Sort),
    /// Skip some number of rows, and then fetch some number of rows.
    Limit(Limit),
    /// Produces a relation with string representations of
    /// various parts of the plan. This is used to implement SQL `EXPLAIN`.
    Explain(Explain),
}

impl Plan {
    pub fn schema(&self) -> &LogicalSchema {
        todo!()
    }
}

#[derive(Clone, Debug)]
pub struct Explain {
    pub analyse: bool,
    pub verbose: bool,
    pub plan: Arc<Plan>,
    pub output_schema: LogicalSchema,
}

impl Explain {
    pub fn new(plan: Plan, verbose: bool, analyse: bool) -> Self {
        // TODO: set output_schema
        Self { plan: Arc::new(plan), analyse, verbose, output_schema: LogicalSchema::empty() }
    }
}

#[derive(Clone, Debug)]
pub struct Limit {
    /// The incoming logical plan
    pub input: Arc<Plan>,
    /// Number of rows to skip before fetch
    pub skip: Option<u64>,
    /// Maximum number of rows to fetch,
    /// None means fetching all rows
    pub fetch: Option<u64>,
}

impl Limit {
    pub fn new(input: Plan, skip: Option<u64>, fetch: Option<u64>) -> Self {
        Self { input: Arc::new(input), skip, fetch }
    }
}

#[derive(Clone, Debug)]
pub struct Sort {
    /// The sort expressions
    pub expr: Vec<SortExpr>,
    /// The incoming logical plan
    pub input: Arc<Plan>,
}

impl Sort {
    pub fn new(expr: Vec<SortExpr>, input: Plan) -> Self {
        Self { expr, input: Arc::new(input) }
    }
}

#[derive(Clone, Debug)]
pub struct SortExpr {
    /// The expression to sort on
    pub expr: Expr,
    /// The direction of the sort
    pub asc: bool,
}

#[derive(Clone, Debug)]
pub struct Aggregate {
    /// The incoming logical plan
    pub input: Arc<Plan>,
    /// Grouping expressions
    pub group_expr: Vec<Expr>,
    /// Aggregate expressions
    pub aggr_expr: Vec<Expr>,
    /// The output schema.
    pub output_schema: LogicalSchema,
}

impl Aggregate {
    pub fn try_new(input: Plan, group_expr: Vec<Expr>, aggr_expr: Vec<Expr>) -> Result<Self> {
        let fields = group_expr.iter().map(|it| it.to_field()).collect::<Result<Vec<_>>>()?;
        let output_schema = LogicalSchema::from_unqualified_fields(fields.into())?;
        Ok(Self { input: Arc::new(input), group_expr, aggr_expr, output_schema })
    }
}

/// Filters rows from its input that do not match an
/// expression (essentially a WHERE clause with a predicate
/// expression).
///
/// Semantically, `<predicate>` is evaluated for each row of the input;
/// If the value of `<predicate>` is true, the input row is passed to
/// the output. If the value of `<predicate>` is false, the row is
/// discarded.
///
/// Filter should not be created directly but instead use `try_new()`
/// and that these fields are only pub to support pattern matching
#[derive(Clone, Debug)]
pub struct Filter {
    /// The predicate expression, which must have Boolean type.
    pub predicate: Expr,
    /// The incoming logical plan
    pub input: Arc<Plan>,
}

impl Filter {
    pub fn try_new(expr: Expr, input: Plan) -> Result<Self> {
        let (datatype, _) = expr.datatype_and_nullable(input.schema())?;
        if datatype != DataType::Boolean {
            return Err(Error::parse(format!(
                "Invalid filter result type, expect boolean, got {datatype}"
            )));
        }
        Ok(Self { predicate: expr, input: Arc::new(input) })
    }
}

/// Join two logical plans on one or more join columns
#[derive(Clone, Debug)]
pub struct Join {
    /// Left input
    pub left: Arc<Plan>,
    /// Right input
    pub right: Arc<Plan>,
    /// Join type
    pub join_type: JoinType,
    /// Join condition
    pub filter: Expr,
    /// The output schema, containing fields from the left and right inputs
    pub schema: LogicalSchema,
}

impl Join {
    pub fn new(
        left: Plan,
        right: Plan,
        join_type: JoinType,
        filter: Expr,
        schema: LogicalSchema,
    ) -> Self {
        Self { left: Arc::new(left), right: Arc::new(right), join_type, filter, schema }
    }
}

#[derive(Clone, Debug)]
pub enum JoinType {
    /// Inner Join - Returns only rows where there is a matching value in both tables based on the join condition.
    /// For example, if joining table A and B on A.id = B.id, only rows where A.id equals B.id will be included.
    /// All columns from both tables are returned for the matching rows. Non-matching rows are excluded entirely.
    Inner,
    /// Left Join - Returns all rows from the left table and matching rows from the right table.
    /// If no match, NULL values are returned for columns from the right table.
    Left,
    /// Right Join - Returns all rows from the right table and matching rows from the left table.
    /// If no match, NULL values are returned for columns from the left table.
    Right,
    /// Full Join (also called Full Outer Join) - Returns all rows from both tables, matching rows where possible.
    /// When a row from either table has no match in the other table, the missing columns are filled with NULL values.
    /// For example, if table A has row X with no match in table B, the result will contain row X with NULL values for all of table B's columns.
    /// This join type preserves all records from both tables, making it useful when you need to see all data regardless of matches.
    Full,
}

#[derive(Clone, Debug)]
pub struct SubqueryAlias {
    /// The incoming logical plan
    pub input: Arc<Plan>,
    /// The alias for the input relation
    pub alias: TableReference,
    /// The schema with qualified field names
    pub schema: LogicalSchema,
}

impl SubqueryAlias {
    pub fn try_new(plan: Plan, alias: impl Into<TableReference>) -> Result<Self> {
        let alias = alias.into();
        let input_fields = plan.schema().fields();
        let schema = LogicalSchema::new(
            input_fields.clone(),
            vec![Some(alias.clone()); input_fields.len()],
        )?;
        Ok(Self { input: Arc::new(plan), alias, schema })
    }
}

/// Transaction statements
#[derive(Clone, Debug)]
pub enum Transaction {
    Begin { read_only: bool, as_of: Option<u64> },
    Commit,
    Abort,
}

/// Scan rows from a table/relation.
#[derive(Clone, Debug)]
pub struct TableScan {
    pub relation: TableReference,
    /// Optional column indices to use as a projection
    pub projection: Option<Vec<usize>>,
    /// Optional expressions to be used as filters
    pub filters: Vec<Expr>,
    /// The schema description of the output
    pub output_schema: LogicalSchema,
}

pub struct TableScanBuilder {
    relation: TableReference,
    projection: Option<Vec<usize>>,
    filters: Vec<Expr>,
    output_schema: LogicalSchema,
}

impl TableScanBuilder {
    /// Create a new TableScanBuilder.
    pub fn new(relation: impl Into<TableReference>, table_schema: &LogicalSchema) -> Self {
        let projection = (0..table_schema.fields().len()).map(|i| i).collect::<Vec<_>>();
        Self {
            relation: relation.into(),
            projection: Some(projection),
            filters: vec![],
            output_schema: table_schema.clone(),
        }
    }

    /// Set the column indices to use as a projection
    pub fn project(mut self, indices: Vec<usize>) -> Self {
        self.projection = Some(indices);
        self
    }

    /// Add a filter expression
    pub fn filter(mut self, expr: Expr) -> Self {
        self.filters.push(expr);
        self
    }

    /// Add multiple filter expressions
    pub fn filters(mut self, exprs: Vec<Expr>) -> Self {
        self.filters.extend(exprs);
        self
    }

    /// Set the output schema
    pub fn output_schema(mut self, schema: LogicalSchema) -> Self {
        self.output_schema = schema;
        self
    }

    /// Build the TableScan
    pub fn build(self) -> TableScan {
        TableScan {
            relation: self.relation,
            projection: self.projection,
            filters: self.filters,
            output_schema: self.output_schema,
        }
    }
}

/// Evaluates an arbitrary list of expressions on its input.
#[derive(Clone, Debug)]
pub struct Projection {
    /// The list of expressions
    pub exprs: Vec<Expr>,
    /// The incoming logical plan
    pub input: Arc<Plan>,
    /// The schema description of the output
    pub output_schema: LogicalSchema,
}

impl Projection {
    pub fn new(exprs: Vec<Expr>, input: Plan, output_schema: LogicalSchema) -> Self {
        Self { exprs, input: Arc::new(input), output_schema }
    }

    pub fn try_new(exprs: Vec<Expr>, input: Plan) -> Result<Self> {
        let fields = exprs.iter().map(|it| it.to_field()).collect::<Result<Vec<_>>>()?;
        let output_schema = LogicalSchema::from_unqualified_fields(fields.into())?;
        Ok(Self { exprs, input: Arc::new(input), output_schema })
    }
}

#[derive(Clone, Debug)]
pub struct Delete {
    /// The table
    pub table: TableReference,
    /// Input source for delete
    pub input: Arc<Plan>,
    /// The output schema is always a single column with the number of rows affected
    pub output_schema: LogicalSchema,
}

impl Delete {
    pub fn new(table: impl Into<TableReference>, input: Plan) -> Self {
        Self {
            table: table.into(),
            input: Arc::new(input),
            output_schema: schema_affected_rows_count(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Update {
    /// The table
    pub table: TableReference,
    /// Input source for update
    pub input: Arc<Plan>,
    /// The output schema is always a single column with the number of rows affected
    pub output_schema: LogicalSchema,
}

impl Update {
    pub fn new(table: impl Into<TableReference>, input: Plan) -> Self {
        Self {
            table: table.into(),
            input: Arc::new(input),
            output_schema: schema_affected_rows_count(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Insert {
    /// The table
    pub table: TableReference,
    /// Input source for insertion
    pub input: Arc<Plan>,
    /// The output schema is always a single column with the number of rows affected
    pub output_schema: LogicalSchema,
}

impl Insert {
    pub fn new(table: impl Into<TableReference>, input: Plan) -> Self {
        Self {
            table: table.into(),
            input: Arc::new(input),
            output_schema: schema_affected_rows_count(),
        }
    }
}

fn schema_affected_rows_count() -> LogicalSchema {
    LogicalSchema::from_unqualified_fields(Fields::from(vec![FieldBuilder::new(
        "count",
        DataType::Integer,
    )
    .build()]))
    .unwrap()
}

/// Values expression. See
/// [Postgres VALUES](https://www.postgresql.org/docs/current/queries-values.html)
/// documentation for more details.
#[derive(Clone, Debug)]
pub struct Values {
    /// The values schema
    pub schema: LogicalSchema,
    /// Values
    pub values: Vec<Vec<Expr>>,
}

impl Values {
    pub fn new(exprs: Vec<Vec<Expr>>, schema: LogicalSchema) -> Result<Self> {
        todo!()
    }
}

#[derive(Clone, Debug)]
pub struct DropIndex {
    /// The index name
    pub name: String,
    /// Option to not error if table not exists
    pub if_exists: bool,
}

#[derive(Clone, Debug)]
pub struct DropTable {
    /// The table relation
    pub relation: TableReference,
    /// Option to not error if table not exists
    pub if_exists: bool,
}

/// Create table logical plan.
#[derive(Clone, Debug)]
pub struct CreateTable {
    /// The table relation
    pub relation: TableReference,
    /// The schema description of the output.
    pub schema: LogicalSchema,
    /// Option to not error if table already exists
    pub if_not_exists: bool,
}

/// Create index logical plan.
#[derive(Clone, Debug)]
pub struct CreateIndex {
    /// The index name
    pub name: String,
    /// The table relation where index is created on
    pub relation: TableReference,
    /// The resolved index columns name
    pub columns: Fields,
    /// Unique index
    pub unique: bool,
    /// Option to not error if index already exists
    pub if_not_exists: bool,
}
