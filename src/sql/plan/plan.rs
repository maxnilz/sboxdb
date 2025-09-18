use std::fmt::Display;
use std::fmt::Formatter;

use crate::apply_each;
use crate::catalog::r#type::DataType;
use crate::error::Error;
use crate::error::Result;
use crate::format_expr_vec;
use crate::map_each_children;
use crate::sql::plan::expr::BinaryExpr;
use crate::sql::plan::expr::Exists;
use crate::sql::plan::expr::Expr;
use crate::sql::plan::expr::InSubquery;
use crate::sql::plan::expr::Operator;
use crate::sql::plan::expr::Subquery;
use crate::sql::plan::schema::Field;
use crate::sql::plan::schema::FieldBuilder;
use crate::sql::plan::schema::FieldReference;
use crate::sql::plan::schema::Fields;
use crate::sql::plan::schema::LogicalSchema;
use crate::sql::plan::schema::TableReference;
use crate::sql::plan::schema::EMPTY_SCHEMA;
use crate::sql::plan::visitor::Transformed;
use crate::sql::plan::visitor::TreeNode;
use crate::sql::plan::visitor::TreeNodeVisitor;
use crate::sql::plan::visitor::VisitRecursion;

/// A `Plan` is a logical node in a tree of relational operators(such as
/// Projection or Filter). Also known as `Logical Plan`
#[derive(Clone, Debug, PartialOrd, Eq, PartialEq, Hash)]
pub enum Plan {
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
    /// Produces the output of running another query. This is used to
    /// implement SQL subqueries
    Subquery(Subquery),
    /// Aliased relation provides, or changes, the name of a relation.
    SubqueryAlias(SubqueryAlias),
    /// Join two logical plans on one or more join columns.
    /// This is used to implement SQL `JOIN`.
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
    /// TODO: Support scalar function and aggregation function.
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
        match self {
            Plan::CreateIndex(_) | Plan::DropTable(_) | Plan::DropIndex(_) => &EMPTY_SCHEMA,
            Plan::CreateTable(CreateTable { schema, .. }) => schema,
            Plan::Insert(Insert { output_schema, .. }) => output_schema,
            Plan::Update(Update { output_schema, .. }) => output_schema,
            Plan::Delete(Delete { output_schema, .. }) => output_schema,
            Plan::Values(Values { schema, .. }) => schema,
            Plan::Projection(Projection { output_schema, .. }) => output_schema,
            Plan::TableScan(TableScan { output_schema, .. }) => output_schema,
            Plan::Subquery(Subquery { subquery, .. }) => subquery.schema(),
            Plan::SubqueryAlias(SubqueryAlias { schema, .. }) => schema,
            Plan::Join(Join { schema, .. }) => schema,
            Plan::Filter(Filter { input, .. }) => input.schema(),
            Plan::Aggregate(Aggregate { output_schema, .. }) => output_schema,
            Plan::Sort(Sort { input, .. }) => input.schema(),
            Plan::Limit(Limit { input, .. }) => input.schema(),
            Plan::Explain(Explain { output_schema, .. }) => output_schema,
        }
    }

    /// traverse all the exprs in given plan node recursively.
    pub fn visit_exprs<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(&Expr) -> Result<VisitRecursion>,
    {
        self.walk(|node| match node {
            Plan::Values(Values { values, .. }) => {
                let exprs: Vec<&Expr> = values.iter().flatten().collect();
                apply_each!(f; exprs)
            }
            Plan::Projection(Projection { exprs, .. }) => apply_each!(f; exprs),
            Plan::TableScan(TableScan { filters, .. }) => apply_each!(f; filters),
            Plan::Join(Join { constraint, .. }) => apply_each!(f, constraint),
            Plan::Filter(Filter { predicate, .. }) => apply_each!(f, predicate),
            Plan::Aggregate(Aggregate { group_exprs, aggr_exprs, .. }) => {
                apply_each!(f; group_exprs)?.when_sibling(|| apply_each!(f; aggr_exprs))
            }
            Plan::Sort(Sort { expr, .. }) => {
                let exprs: Vec<&Expr> = expr.iter().map(|it| &it.expr).collect();
                apply_each!(f; exprs)
            }
            _ => Ok(VisitRecursion::Continue),
        })?;
        Ok(())
    }
}

impl TreeNode for Plan {
    /// Apply `f` to visit node's children (but **NOT** the node itself).
    ///
    /// **NB**: subqueries in expr are considered as children for visiting,
    /// e.g., subquery in `Expr::Exists`, `Expr::InSubquery` or `Expr::ScalarSubquery`.
    fn visit_children<F>(&self, mut f: F) -> Result<VisitRecursion>
    where
        F: FnMut(&Self) -> Result<VisitRecursion>,
    {
        // visit any subquery in expr...
        let mut visit_expr = |e: &Expr| {
            e.walk(|expr| match expr {
                Expr::Exists(Exists { subquery, .. })
                | Expr::ScalarSubquery(subquery)
                | Expr::InSubquery(InSubquery { subquery, .. }) => {
                    // Apply the given visit function on a dummy logical
                    // subquery node.
                    f(&Plan::Subquery(subquery.clone()))
                }
                _ => Ok(VisitRecursion::Continue),
            })
        };
        match self {
            Plan::CreateTable(_)
            | Plan::CreateIndex(_)
            | Plan::DropTable(_)
            | Plan::DropIndex(_)
            | Plan::Values(_)
            | Plan::TableScan(_) => Ok(VisitRecursion::Continue),
            Plan::Insert(Insert { input, .. }) => apply_each!(f, input),
            Plan::Update(Update { input, .. }) => apply_each!(f, input),
            Plan::Delete(Delete { input, .. }) => apply_each!(f, input),
            Plan::Projection(Projection { input, exprs, .. }) => {
                apply_each!(visit_expr; exprs)?.when_sibling(|| apply_each!(f, input))
            }
            Plan::Subquery(Subquery { subquery, .. }) => apply_each!(f, subquery),
            Plan::SubqueryAlias(SubqueryAlias { input, .. }) => apply_each!(f, input),
            Plan::Filter(Filter { input, predicate, .. }) => {
                apply_each!(visit_expr, predicate)?.when_sibling(|| apply_each!(f, input))
            }
            Plan::Aggregate(Aggregate { input, group_exprs, aggr_exprs, .. }) => {
                apply_each!(visit_expr; group_exprs)?
                    .when_sibling(|| apply_each!(visit_expr; aggr_exprs))?
                    .when_sibling(|| apply_each!(f, input))
            }
            Plan::Sort(Sort { input, expr, .. }) => {
                let exprs = expr.iter().map(|it| &it.expr).collect::<Vec<_>>();
                apply_each!(visit_expr; exprs)?.when_sibling(|| apply_each!(f, input))
            }
            Plan::Limit(Limit { input, .. }) => apply_each!(f, input),
            Plan::Explain(Explain { plan, .. }) => apply_each!(f, plan),
            Plan::Join(Join { left, right, constraint, .. }) => {
                apply_each!(visit_expr, constraint)?.when_sibling(|| apply_each!(f, left, right))
            }
        }
    }

    /// Applies `f` to each child of this plan node, rewriting them *in place.*
    ///
    /// **NB**: subqueries in expr are **NOT** considered as children for rewriting,
    /// e.g., subquery in `Expr::Exists`, `Expr::InSubquery` or `Expr::ScalarSubquery`.
    fn map_children<F>(self, mut f: F) -> Result<Transformed<Self>>
    where
        F: FnMut(Self) -> Result<Transformed<Self>>,
    {
        match self {
            Plan::CreateTable(_)
            | Plan::CreateIndex(_)
            | Plan::DropTable(_)
            | Plan::DropIndex(_)
            | Plan::Values(_)
            | Plan::TableScan(_) => Ok(Transformed::no(self)),
            Plan::Insert(a) => map_each_children!(f, *a.input)?.map_data(|mut children| {
                Ok(Plan::Insert(Insert { input: Box::new(children.remove(0)), ..a }))
            }),
            Plan::Update(a) => map_each_children!(f, *a.input)?.map_data(|mut children| {
                Ok(Plan::Update(Update { input: Box::new(children.remove(0)), ..a }))
            }),
            Plan::Delete(a) => map_each_children!(f, *a.input)?.map_data(|mut children| {
                Ok(Plan::Delete(Delete { input: Box::new(children.remove(0)), ..a }))
            }),
            Plan::Projection(a) => map_each_children!(f, *a.input)?.map_data(|mut children| {
                Ok(Plan::Projection(Projection { input: Box::new(children.remove(0)), ..a }))
            }),
            Plan::Subquery(a) => map_each_children!(f, *a.subquery)?.map_data(|mut children| {
                Ok(Plan::Subquery(Subquery { subquery: Box::new(children.remove(0)), ..a }))
            }),
            Plan::SubqueryAlias(a) => map_each_children!(f, *a.input)?.map_data(|mut children| {
                Ok(Plan::SubqueryAlias(SubqueryAlias { input: Box::new(children.remove(0)), ..a }))
            }),
            Plan::Join(a) => map_each_children!(f, *a.left, *a.right)?.map_data(|mut children| {
                let left = Box::new(children.remove(0));
                let right = Box::new(children.remove(0));
                Ok(Plan::Join(Join { left, right, ..a }))
            }),
            Plan::Filter(a) => map_each_children!(f, *a.input)?.map_data(|mut children| {
                Ok(Plan::Filter(Filter { input: Box::new(children.remove(0)), ..a }))
            }),
            Plan::Aggregate(a) => map_each_children!(f, *a.input)?.map_data(|mut children| {
                Ok(Plan::Aggregate(Aggregate { input: Box::new(children.remove(0)), ..a }))
            }),
            Plan::Sort(a) => map_each_children!(f, *a.input)?.map_data(|mut children| {
                Ok(Plan::Sort(Sort { input: Box::new(children.remove(0)), ..a }))
            }),
            Plan::Limit(a) => map_each_children!(f, *a.input)?.map_data(|mut children| {
                Ok(Plan::Limit(Limit { input: Box::new(children.remove(0)), ..a }))
            }),
            Plan::Explain(a) => map_each_children!(f, *a.plan)?.map_data(|mut children| {
                Ok(Plan::Explain(Explain { plan: Box::new(children.remove(0)), ..a }))
            }),
        }
    }
}

impl Display for Plan {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let with_schema = f.alternate();
        let mut visitor = IndentVisitor::new(f, with_schema);
        match self.visit(&mut visitor) {
            Ok(_) => Ok(()),
            Err(_) => Err(std::fmt::Error),
        }
    }
}

struct IndentVisitor<'a, 'b> {
    f: &'a mut Formatter<'b>,
    /// If true, includes summarized schema information
    with_schema: bool,
    /// The current indent
    indent: usize,
}

impl<'a, 'b> IndentVisitor<'a, 'b> {
    fn new(f: &'a mut Formatter<'b>, with_schema: bool) -> Self {
        Self { f, with_schema, indent: 0 }
    }

    fn display_plan<'c>(&self, node: &'c Plan) -> impl Display + 'c {
        struct Wrapper<'a>(&'a Plan);
        impl Display for Wrapper<'_> {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                match self.0 {
                    Plan::CreateTable(CreateTable { relation, if_not_exists, .. }) => {
                        write!(f, "CreateTable {relation}, if not exists: {if_not_exists}")
                    }
                    Plan::CreateIndex(CreateIndex { name, relation, if_not_exists, .. }) => {
                        write!(
                            f,
                            "CreateIndex {name} on {relation}, if not exists: {if_not_exists}"
                        )
                    }
                    Plan::DropTable(DropTable { relation, if_exists, .. }) => {
                        write!(f, "DropTable {relation}, if exists: {if_exists}")
                    }
                    Plan::DropIndex(DropIndex { name, if_exists, .. }) => {
                        write!(f, "DropIndex {name}, if exists: {if_exists}")
                    }
                    Plan::Insert(Insert { table, .. }) => {
                        write!(f, "DML: op=[Insert] table=[{table}]")
                    }
                    Plan::Update(Update { table, .. }) => {
                        write!(f, "DML: op=[Update] table=[{table}]")
                    }
                    Plan::Delete(Delete { table, .. }) => {
                        write!(f, "DML: op=[Delete] table=[{table}]")
                    }
                    Plan::Values(values) => {
                        let values = values
                            .values
                            .iter()
                            .take(3)
                            .map(|row| {
                                let item = row
                                    .iter()
                                    .map(|expr| expr.to_string())
                                    .collect::<Vec<_>>()
                                    .join(", ");
                                format!("({})", item)
                            })
                            .collect::<Vec<_>>();
                        let eclipse = if values.len() > 3 { "..." } else { "" };
                        write!(f, "Values: {}{}", values.join(", "), eclipse)
                    }
                    Plan::Projection(Projection { exprs, .. }) => {
                        write!(f, "Projection: ")?;
                        for (i, expr) in exprs.iter().enumerate() {
                            if i > 0 {
                                write!(f, ", ")?;
                            }
                            write!(f, "{expr}")?;
                        }
                        Ok(())
                    }
                    Plan::TableScan(TableScan {
                        relation, schema, projection, filters, ..
                    }) => {
                        let projection = match projection {
                            Some(indices) => {
                                let names = indices
                                    .iter()
                                    .map(|i| schema.field(*i).name.as_str())
                                    .collect::<Vec<_>>();
                                format!(" projection=[{}]", names.join(", "))
                            }
                            _ => "".to_string(),
                        };
                        let filter = if filters.is_empty() {
                            "".to_string()
                        } else {
                            let filters =
                                filters.iter().map(|it| it.to_string()).collect::<Vec<_>>();
                            format!(" filters=[{}]", filters.join(", "))
                        };
                        write!(f, "TableScan: {relation}{projection}{filter}")
                    }
                    Plan::Subquery(Subquery { .. }) => {
                        write!(f, "Subquery:")
                    }
                    Plan::SubqueryAlias(SubqueryAlias { alias, .. }) => {
                        write!(f, "SubqueryAlias: {alias}")
                    }
                    Plan::Join(Join { join_type, constraint, .. }) => {
                        write!(f, "{join_type} Join: {constraint}")
                    }
                    Plan::Filter(Filter { predicate, .. }) => {
                        write!(f, "Filter: {predicate}")
                    }
                    Plan::Aggregate(Aggregate { group_exprs, aggr_exprs, .. }) => {
                        write!(
                            f,
                            "Aggregate: groupBy=[{}], aggr=[{}]",
                            format_expr_vec!(group_exprs),
                            format_expr_vec!(aggr_exprs)
                        )
                    }
                    Plan::Sort(Sort { expr, .. }) => {
                        write!(f, "Sort: ")?;
                        for (i, it) in expr.iter().enumerate() {
                            if i > 0 {
                                write!(f, ", ")?;
                            }
                            write!(f, "{it}")?;
                        }
                        Ok(())
                    }
                    Plan::Limit(Limit { skip, fetch, .. }) => {
                        let skip = match skip {
                            None => "".to_string(),
                            Some(n) => format!(" skip={n}"),
                        };
                        let fetch = match fetch {
                            None => "".to_string(),
                            Some(n) => format!(" fetch={n}"),
                        };
                        write!(f, "Limit:{skip}{fetch}")
                    }
                    Plan::Explain(Explain { physical: analyse, verbose, .. }) => {
                        write!(f, "Explain: analyse: {analyse}, verbose: {verbose}")
                    }
                }
            }
        }
        Wrapper(node)
    }

    fn display_schema<'c>(&self, schema: &'c LogicalSchema) -> impl Display + 'c {
        struct Wrapper<'a>(&'a LogicalSchema);
        impl Display for Wrapper<'_> {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "[")?;
                for (i, field) in self.0.fields().iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    let nullable_str = if field.nullable { ";N" } else { "" };
                    write!(f, "{}:{:?}{}", field.name, field.datatype, nullable_str)?;
                }
                write!(f, "]")
            }
        }
        Wrapper(schema)
    }
}

impl<'a, 'b, 'n> TreeNodeVisitor<'n> for IndentVisitor<'a, 'b> {
    type Node = Plan;

    fn f_down(&mut self, node: &'n Self::Node) -> Result<VisitRecursion> {
        if self.indent > 0 {
            writeln!(self.f)?;
        }
        write!(self.f, "{:indent$}", "", indent = self.indent * 2)?;
        write!(self.f, "{}", self.display_plan(node))?;
        if self.with_schema {
            write!(self.f, " {}", self.display_schema(node.schema()))?;
        }

        self.indent += 1;
        Ok(VisitRecursion::Continue)
    }

    fn f_up(&mut self, _node: &'n Self::Node) -> Result<VisitRecursion> {
        self.indent -= 1;
        Ok(VisitRecursion::Continue)
    }
}

#[derive(Clone, Debug, PartialOrd, Eq, PartialEq, Hash)]
pub struct Explain {
    pub physical: bool,
    pub verbose: bool,
    pub plan: Box<Plan>,
    pub output_schema: LogicalSchema,
}

impl Explain {
    pub fn new(plan: Plan, verbose: bool, physical: bool) -> Self {
        let fields: Fields = vec![
            FieldBuilder::new("type", DataType::String).build(),
            FieldBuilder::new("plan", DataType::String).build(),
        ]
        .into();
        let output_schema = LogicalSchema::from_unqualified_fields(fields).unwrap();
        Self { plan: Box::new(plan), physical, verbose, output_schema }
    }
}

#[derive(Clone, Debug, PartialOrd, Eq, PartialEq, Hash)]
pub struct Limit {
    /// The incoming logical plan
    pub input: Box<Plan>,
    /// Number of rows to skip before fetch
    pub skip: Option<u64>,
    /// Maximum number of rows to fetch,
    /// None means fetching all rows
    pub fetch: Option<u64>,
}

impl Limit {
    pub fn new(input: Plan, skip: Option<u64>, fetch: Option<u64>) -> Self {
        Self { input: Box::new(input), skip, fetch }
    }
}

#[derive(Clone, Debug, PartialOrd, Eq, PartialEq, Hash)]
pub struct Sort {
    /// The sort expressions
    pub expr: Vec<SortExpr>,
    /// The incoming logical plan
    pub input: Box<Plan>,
}

impl Sort {
    pub fn new(expr: Vec<SortExpr>, input: Plan) -> Self {
        Self { expr, input: Box::new(input) }
    }
}

#[derive(Clone, Debug, PartialOrd, Eq, PartialEq, Hash)]
pub struct SortExpr {
    /// The expression to sort on
    pub expr: Expr,
    /// The direction of the sort
    pub asc: bool,
}

impl Display for SortExpr {
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

#[derive(Clone, Debug, PartialOrd, Eq, PartialEq, Hash)]
pub struct Aggregate {
    /// The incoming logical plan
    pub input: Box<Plan>,
    /// Grouping expressions
    pub group_exprs: Vec<Expr>,
    /// Aggregate expressions
    pub aggr_exprs: Vec<Expr>,
    /// The output schema.
    pub output_schema: LogicalSchema,
}

impl Aggregate {
    pub fn try_new(input: Plan, group_exprs: Vec<Expr>, aggr_exprs: Vec<Expr>) -> Result<Self> {
        let schema = input.schema();
        // Build fields from fields that generated from the group_exprs.
        let mut qualified_fields =
            group_exprs.iter().map(|it| it.to_field(schema)).collect::<Result<Vec<_>>>()?;
        // Append the fields for aggr_exprs to the fields.
        qualified_fields
            .extend(aggr_exprs.iter().map(|it| it.to_field(schema)).collect::<Result<Vec<_>>>()?);

        let (qualifiers, fields): (Vec<Option<TableReference>>, Vec<Field>) =
            qualified_fields.into_iter().unzip();
        let output_schema = LogicalSchema::try_new(fields, qualifiers)?;
        Ok(Self { input: Box::new(input), group_exprs, aggr_exprs, output_schema })
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
#[derive(Clone, Debug, PartialOrd, Eq, PartialEq, Hash)]
pub struct Filter {
    /// The predicate expression, which must have Boolean type.
    pub predicate: Expr,
    /// The incoming logical plan
    pub input: Box<Plan>,
}

impl Filter {
    pub fn try_new(expr: Expr, input: Plan) -> Result<Self> {
        let (datatype, _) = expr.datatype_and_nullable(input.schema())?;
        if datatype != DataType::Boolean {
            return Err(Error::parse(format!(
                "Invalid filter result type, expect boolean, got {datatype}"
            )));
        }
        Ok(Self { predicate: expr, input: Box::new(input) })
    }
}

/// Join two logical plans on one or more join columns
#[derive(Clone, Debug, PartialOrd, Eq, PartialEq, Hash)]
pub struct Join {
    /// Left input
    pub left: Box<Plan>,
    /// Right input
    pub right: Box<Plan>,
    /// The original join constraint
    pub constraint: Expr,
    /// Normalized equijoin atoms, e.g., left.k = right.k, extracted from
    /// the original join constraint.
    pub on: Vec<(FieldReference, FieldReference)>,
    /// Join type
    pub join_type: JoinType,
    /// The output schema, containing fields from the left and right inputs
    pub schema: LogicalSchema,
}

impl Join {
    pub fn try_new(
        left: Plan,
        right: Plan,
        join_type: JoinType,
        constraint: Expr,
        schema: LogicalSchema,
    ) -> Result<Self> {
        // Split the join constraint into join keys and residual filter
        // by flatten the expr into a top-level CNF(k-ary AND) and check
        // if the expr can be treated as join key element-wise.
        let conjuncts = constraint.clone().flatten_top_and()?;
        let mut equijoin_keys = vec![];
        enum Side {
            None,
            Left,
            Right,
        }
        let side_of_fr = |fr: &FieldReference, ls: &LogicalSchema, rs: &LogicalSchema| -> Side {
            if let Some(_) = ls.field_reference_by_qname(&fr.relation, &fr.name) {
                return Side::Left;
            }
            if let Some(_) = rs.field_reference_by_qname(&fr.relation, &fr.name) {
                return Side::Right;
            }
            Side::None
        };
        for expr in conjuncts {
            match expr {
                Expr::BinaryExpr(BinaryExpr { left: ea, op, right: eb }) if op == Operator::Eq => {
                    if let (Some(fra), Some(frb)) = (ea.is_field_ref(), eb.is_field_ref()) {
                        let ls = left.schema();
                        let rs = right.schema();
                        match (side_of_fr(&fra, ls, rs), side_of_fr(&frb, ls, rs)) {
                            (Side::Left, Side::Right) => equijoin_keys.push((fra, frb)),
                            (Side::Right, Side::Left) => equijoin_keys.push((frb, fra)),
                            _ => {}
                        }
                    }
                }
                _ => {}
            }
        }
        Ok(Self {
            left: Box::new(left),
            right: Box::new(right),
            constraint,
            on: equijoin_keys,
            join_type,
            schema,
        })
    }
}

#[derive(Clone, Debug, PartialOrd, Eq, PartialEq, Hash)]
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

impl Display for JoinType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            JoinType::Inner => write!(f, "Inner"),
            JoinType::Left => write!(f, "Left"),
            JoinType::Right => write!(f, "Right"),
            JoinType::Full => write!(f, "Full"),
        }
    }
}

#[derive(Clone, Debug, PartialOrd, Eq, PartialEq, Hash)]
pub struct SubqueryAlias {
    /// The incoming logical plan
    pub input: Box<Plan>,
    /// The alias for the input relation
    pub alias: TableReference,
    /// The schema with qualified field names
    pub schema: LogicalSchema,
}

impl SubqueryAlias {
    pub fn try_new(plan: Plan, alias: impl Into<TableReference>) -> Result<Self> {
        let alias = alias.into();
        let input_fields = plan.schema().fields();
        let schema = LogicalSchema::try_new(
            input_fields.clone(),
            vec![Some(alias.clone()); input_fields.len()],
        )?;
        Ok(Self { input: Box::new(plan), alias, schema })
    }
}

/// Scan rows from a table/relation.
#[derive(Clone, Debug, PartialOrd, Eq, PartialEq, Hash)]
pub struct TableScan {
    pub relation: TableReference,
    /// relation schema
    pub schema: LogicalSchema,
    /// Optional column indices to use as a projection
    pub projection: Option<Vec<usize>>,
    /// Optional expressions to be used as filters
    pub filters: Vec<Expr>,
    /// The schema description of the output, if
    /// the projection is None, it should be same
    /// as the relation schema.
    pub output_schema: LogicalSchema,
}

pub struct TableScanBuilder {
    relation: TableReference,
    schema: LogicalSchema,
    projection: Option<Vec<usize>>,
    filters: Vec<Expr>,
    output_schema: LogicalSchema,
}

impl TableScanBuilder {
    /// Create a new TableScanBuilder.
    pub fn new(relation: impl Into<TableReference>, table_schema: &LogicalSchema) -> Self {
        Self {
            relation: relation.into(),
            schema: table_schema.clone(),
            projection: None,
            filters: vec![],
            output_schema: table_schema.clone(),
        }
    }

    /// Set the column indices to use as a projection
    #[allow(dead_code)]
    pub fn project(mut self, indices: Vec<usize>) -> Self {
        self.projection = Some(indices);
        self
    }

    /// Add a filter expression
    pub fn filter(mut self, expr: Expr) -> Self {
        self.filters.push(expr);
        self
    }

    /// Set the output schema
    #[allow(dead_code)]
    pub fn output_schema(mut self, schema: LogicalSchema) -> Self {
        self.output_schema = schema;
        self
    }

    /// Build the TableScan
    pub fn build(self) -> TableScan {
        TableScan {
            relation: self.relation,
            schema: self.schema,
            projection: self.projection,
            filters: self.filters,
            output_schema: self.output_schema,
        }
    }
}

/// Evaluates an arbitrary list of expressions on its input.
#[derive(Clone, Debug, PartialOrd, Eq, PartialEq, Hash)]
pub struct Projection {
    /// The list of expressions
    pub exprs: Vec<Expr>,
    /// The incoming logical plan
    pub input: Box<Plan>,
    /// The schema description of the output
    pub output_schema: LogicalSchema,
}

impl Projection {
    pub fn new(exprs: Vec<Expr>, input: Plan, output_schema: LogicalSchema) -> Self {
        Self { exprs, input: Box::new(input), output_schema }
    }

    pub fn try_new(exprs: Vec<Expr>, input: Plan) -> Result<Self> {
        let schema = input.schema();
        let qualified_fields =
            exprs.iter().map(|it| it.to_field(schema)).collect::<Result<Vec<_>>>()?;

        let (qualifiers, fields): (Vec<Option<TableReference>>, Vec<Field>) =
            qualified_fields.into_iter().unzip();
        let output_schema = LogicalSchema::try_new(fields, qualifiers)?;
        Ok(Self { exprs, input: Box::new(input), output_schema })
    }
}

#[derive(Clone, Debug, PartialOrd, Eq, PartialEq, Hash)]
pub struct Delete {
    /// The table
    pub table: TableReference,
    /// Input source for delete
    pub input: Box<Plan>,
    /// The output schema is always a single column with the number of rows affected
    pub output_schema: LogicalSchema,
}

impl Delete {
    pub fn new(table: impl Into<TableReference>, input: Plan) -> Self {
        Self {
            table: table.into(),
            input: Box::new(input),
            output_schema: LogicalSchema::schema_affected_rows_count(),
        }
    }
}

#[derive(Clone, Debug, PartialOrd, Eq, PartialEq, Hash)]
pub struct Update {
    /// The table
    pub table: TableReference,
    /// Input source for update
    pub input: Box<Plan>,
    /// The output schema is always a single column with the number of rows affected
    pub output_schema: LogicalSchema,
}

impl Update {
    pub fn new(table: impl Into<TableReference>, input: Plan) -> Self {
        Self {
            table: table.into(),
            input: Box::new(input),
            output_schema: LogicalSchema::schema_affected_rows_count(),
        }
    }
}

#[derive(Clone, Debug, PartialOrd, Eq, PartialEq, Hash)]
pub struct Insert {
    /// The table
    pub table: TableReference,
    /// Input source for insertion
    pub input: Box<Plan>,
    /// The output schema is always a single column with the number of rows affected
    pub output_schema: LogicalSchema,
}

impl Insert {
    pub fn new(table: impl Into<TableReference>, input: Plan) -> Self {
        Self {
            table: table.into(),
            input: Box::new(input),
            output_schema: LogicalSchema::schema_affected_rows_count(),
        }
    }
}

/// Values expression. See
/// [Postgres VALUES](https://www.postgresql.org/docs/current/queries-values.html)
/// documentation for more details.
#[derive(Clone, Debug, PartialOrd, Eq, PartialEq, Hash)]
pub struct Values {
    /// The values schema
    pub schema: LogicalSchema,
    /// Values
    pub values: Vec<Vec<Expr>>,
}

impl Values {
    pub fn try_new(values: Vec<Vec<Expr>>, schema: LogicalSchema) -> Result<Self> {
        let n = schema.len();
        if n == 0 {
            return Err(Error::parse("Values list cannot be zero length"));
        }
        for (i, row) in values.iter().enumerate() {
            if row.len() != n {
                return Err(Error::parse(format!(
                    "Invalid values length: got {} values at row {}, expected: {}",
                    row.len(),
                    i,
                    n
                )));
            }
            for j in 0..n {
                let field = schema.field(j);
                let cel = &row[j];
                let (data_type, _) = cel.datatype_and_nullable(&schema)?;
                if data_type != field.datatype {
                    return Err(Error::parse(format!(
                        "Type mismatch, cast use {} as {}",
                        data_type, field.datatype
                    )));
                }
            }
        }
        Ok(Self { values, schema })
    }
}

#[derive(Clone, Debug, PartialOrd, Eq, PartialEq, Hash)]
pub struct DropIndex {
    /// The index name
    pub name: String,
    pub table_reference: TableReference,
    /// Option to not error if table not exists
    pub if_exists: bool,
}

impl DropIndex {
    pub fn new(
        name: impl Into<String>,
        table_reference: impl Into<TableReference>,
        if_exists: bool,
    ) -> Self {
        Self { name: name.into(), table_reference: table_reference.into(), if_exists }
    }
}

#[derive(Clone, Debug, PartialOrd, Eq, PartialEq, Hash)]
pub struct DropTable {
    /// The table relation
    pub relation: TableReference,
    /// Option to not error if table not exists
    pub if_exists: bool,
}

impl DropTable {
    pub fn new(relation: impl Into<TableReference>, if_exists: bool) -> Self {
        Self { relation: relation.into(), if_exists }
    }
}

/// Create table logical plan.
#[derive(Clone, Debug, PartialOrd, Eq, PartialEq, Hash)]
pub struct CreateTable {
    /// The table relation
    pub relation: TableReference,
    /// The table schema
    pub schema: LogicalSchema,
    /// Option to not error if table already exists
    pub if_not_exists: bool,
}

impl CreateTable {
    pub fn new(
        relation: impl Into<TableReference>,
        schema: LogicalSchema,
        if_not_exists: bool,
    ) -> Self {
        Self { relation: relation.into(), schema, if_not_exists }
    }
}

/// Create index logical plan.
#[derive(Clone, Debug, PartialOrd, Eq, PartialEq, Hash)]
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

impl CreateIndex {
    pub fn new(
        name: impl Into<String>,
        relation: impl Into<TableReference>,
        columns: Fields,
        unique: bool,
        if_not_exists: bool,
    ) -> Self {
        Self { name: name.into(), relation: relation.into(), columns, unique, if_not_exists }
    }
}
