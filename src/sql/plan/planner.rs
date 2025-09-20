use std::borrow::Cow;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use log::debug;

use crate::catalog::catalog::Catalog;
use crate::catalog::catalog::TodoCatalog;
use crate::catalog::r#type::DataType;
use crate::catalog::r#type::Value;
use crate::catalog::schema::Constraint;
use crate::error::Error;
use crate::error::Result;
use crate::format_expr_vec;
use crate::internal_err;
use crate::parse_err;
use crate::sql::parser::ast::BinaryOperator;
use crate::sql::parser::ast::CreateIndex as SQLCreateIndex;
use crate::sql::parser::ast::CreateTable as SQLCreateTable;
use crate::sql::parser::ast::DataType as SQLDataType;
use crate::sql::parser::ast::Expr as SQLExpr;
use crate::sql::parser::ast::Function;
use crate::sql::parser::ast::FunctionArg;
use crate::sql::parser::ast::Ident;
use crate::sql::parser::ast::Insert as SQLInsert;
use crate::sql::parser::ast::InsertSource;
use crate::sql::parser::ast::Join as SQLJoin;
use crate::sql::parser::ast::JoinConstraint;
use crate::sql::parser::ast::JoinOperator;
use crate::sql::parser::ast::Query;
use crate::sql::parser::ast::SelectItem;
use crate::sql::parser::ast::Statement;
use crate::sql::parser::ast::TableConstraint;
use crate::sql::parser::ast::TableFactor;
use crate::sql::parser::ast::TableWithJoins;
use crate::sql::parser::ast::UnaryOperator;
use crate::sql::parser::ast::Update as SQLUpdate;
use crate::sql::parser::ast::Value as SQLValue;
use crate::sql::parser::ast::WildcardExpr;
use crate::sql::plan::expr::find_aggregate_exprs;
use crate::sql::plan::expr::AggregateFunction;
use crate::sql::plan::expr::Alias;
use crate::sql::plan::expr::BinaryExpr;
use crate::sql::plan::expr::Exists;
use crate::sql::plan::expr::Expr;
use crate::sql::plan::expr::InList;
use crate::sql::plan::expr::InSubquery;
use crate::sql::plan::expr::Like;
use crate::sql::plan::expr::Operator;
use crate::sql::plan::expr::ScalarFunction;
use crate::sql::plan::expr::Subquery;
use crate::sql::plan::plan::Aggregate;
use crate::sql::plan::plan::CreateIndex;
use crate::sql::plan::plan::CreateTable;
use crate::sql::plan::plan::Delete;
use crate::sql::plan::plan::DropIndex;
use crate::sql::plan::plan::DropTable;
use crate::sql::plan::plan::Explain;
use crate::sql::plan::plan::Filter;
use crate::sql::plan::plan::Insert;
use crate::sql::plan::plan::Join;
use crate::sql::plan::plan::JoinType;
use crate::sql::plan::plan::Limit;
use crate::sql::plan::plan::Plan;
use crate::sql::plan::plan::Projection;
use crate::sql::plan::plan::Sort;
use crate::sql::plan::plan::SortExpr;
use crate::sql::plan::plan::SubqueryAlias;
use crate::sql::plan::plan::TableScanBuilder;
use crate::sql::plan::plan::Update;
use crate::sql::plan::plan::Values;
use crate::sql::plan::schema::FieldBuilder;
use crate::sql::plan::schema::FieldReference;
use crate::sql::plan::schema::Fields;
use crate::sql::plan::schema::LogicalSchema;
use crate::sql::plan::schema::TableReference;
use crate::sql::plan::schema::EMPTY_SCHEMA;
use crate::sql::plan::visitor::Transformed;
use crate::sql::plan::visitor::TreeNode;
use crate::sql::plan::visitor::VisitRecursion;
use crate::sql::udf::new_func_registry;
use crate::sql::udf::FuncRegistry;
use crate::unimplemented_err;

pub struct BindContext<'a> {
    /// The transactional catalog for relation/column lookup.
    catalog: Arc<dyn Catalog + 'a>,
    /// The query schema of the outer query plan, used to resolve
    /// the fields in subquery.
    outer_query_schema: Option<LogicalSchema>,
    /// The joined schemas of all FROM clauses planned so far. When
    /// planning LATERAL in the FROM/JOIN clauses, this should become
    /// a suffix of the `outer_query_schema`(Unimplemented yet).
    outer_from_schema: Option<LogicalSchema>,
}

impl<'a> BindContext<'a> {
    pub fn new(catalog: Arc<dyn Catalog + 'a>) -> Self {
        Self { catalog, outer_from_schema: None, outer_query_schema: None }
    }

    /// Sets the outer query schema, returning the existing one, if
    /// any.
    pub fn set_outer_query_schema(
        &mut self,
        mut schema: Option<LogicalSchema>,
    ) -> Option<LogicalSchema> {
        std::mem::swap(&mut self.outer_query_schema, &mut schema);
        schema
    }

    /// Sets the outer FROM schema, returning the existing one, if any
    pub fn set_outer_from_schema(
        &mut self,
        mut schema: Option<LogicalSchema>,
    ) -> Option<LogicalSchema> {
        std::mem::swap(&mut self.outer_from_schema, &mut schema);
        schema
    }

    pub fn outer_from_schema(&self) -> Option<LogicalSchema> {
        self.outer_from_schema.clone()
    }

    /// Extends the FROM schema, returning the existing one, if any
    pub fn extend_outer_from_schema(
        &mut self,
        schema: &LogicalSchema,
    ) -> Result<Option<LogicalSchema>> {
        let prev = self.outer_from_schema.clone();
        match self.outer_from_schema.as_mut() {
            Some(from_schema) => from_schema.merge(schema),
            None => self.outer_from_schema = Some(schema.clone()),
        };
        Ok(prev)
    }
}

/// SQL query planner and binder
///
/// This struct is used to convert a SQL AST into a Logical plan node [`Plan`].
///
/// It performs the following tasks:
///
/// 1. Name and type resolution (called "binding" in other systems). This
///    phase looks up table and column names using the [`Catalog`].
/// 2. Mechanical translation of the AST into a Logical plan node [`Plan`].
///
/// It does not perform type coercion, or perform optimization, which are done
/// by subsequent passes.
#[derive(Clone)]
pub struct Planner {
    ident_normalizer: IdentNormalizer,
    func_registry: Arc<dyn FuncRegistry>,
}

impl Planner {
    pub fn new() -> Self {
        let ident_normalizer = IdentNormalizer::new(true);
        let func_registry = new_func_registry();
        Self { ident_normalizer, func_registry }
    }

    pub fn parse_scalar_expr(&self, sqlexpr: SQLExpr) -> Result<Expr> {
        let mut ctx = BindContext::new(Arc::new(TodoCatalog {}));
        self.sqlexpr_to_expr(&mut ctx, sqlexpr, &EMPTY_SCHEMA)
    }

    pub fn sql_statement_to_plan(
        &self,
        ctx: &mut BindContext,
        statement: Statement,
    ) -> Result<Plan> {
        match statement {
            Statement::CreateTable(sql_create_table) => {
                self.create_table_to_plan(ctx, sql_create_table)
            }
            Statement::CreateIndex(sql_create_index) => {
                self.create_index_to_plan(ctx, sql_create_index)
            }
            Statement::DropTable { table_name, if_exists } => {
                let relation = TableReference::new(&self.normalize_ident(&table_name));
                Ok(Plan::DropTable(DropTable::new(relation, if_exists)))
            }
            Statement::DropIndex { index_name, table_name, if_exists } => {
                let name = self.normalize_ident(&index_name);
                let table = self.normalize_ident(&table_name);
                Ok(Plan::DropIndex(DropIndex::new(name, table, if_exists)))
            }
            Statement::AlterTable { .. } => {
                self.unimplemented_err(parse_err!("ALTER TABLE not supported yet"))
            }
            Statement::Insert(insert) => self.insert_to_plan(ctx, insert),
            Statement::Update(update) => self.update_to_plan(ctx, update),
            Statement::Delete { table, selection } => self.delete_to_plan(ctx, table, selection),
            Statement::Select { query } => self.query_to_plan(ctx, *query),
            Statement::Explain { physical, verbose, statement } => {
                let plan = self.sql_statement_to_plan(ctx, *statement)?;
                Ok(Plan::Explain(Explain::new(plan, verbose, physical)))
            }
            _ => Err(internal_err!("Unexpected stmt {}", statement)),
        }
    }

    /// Generate a logical plan from an SQL query/subquery
    fn query_to_plan(&self, ctx: &mut BindContext, query: Query) -> Result<Plan> {
        // plan table with joins
        let mut plan = self.plan_table_with_joins(ctx, query.from)?;

        // plan the selection
        if let Some(sqlexpr) = query.selection {
            let schema = plan.schema();
            let expr = self.sqlexpr_to_expr(ctx, sqlexpr, schema)?;
            plan = Plan::Filter(Filter::try_new(expr, plan)?)
        }

        // Process group by, does NOT support reference aliases defined in select projection.
        let groupby_schema = plan.schema().clone();
        let groupby_exprs = query
            .group_by
            .into_iter()
            .map(|it| {
                let expr = self.sqlexpr_to_expr(ctx, it, &groupby_schema)?;
                if !expr.is_group_by_allowed_expr() {
                    self.semantic_err(format!("Expr {} is not allowed in GROUP BY clause", expr))
                } else {
                    Ok(expr)
                }
            })
            .collect::<Result<Vec<_>>>()?;
        debug!("groupby_exprs: {}", format_expr_vec!(groupby_exprs));

        // Build the select exprs from projection
        let select_exprs = query
            .projection
            .into_iter()
            .map(|it| self.parse_select_item(ctx, &plan, it))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        debug!("select_exprs: {}", format_expr_vec!(select_exprs));

        // Find all the aggregated exprs recursively in the select exprs
        let aggr_exprs = find_aggregate_exprs(&select_exprs)?;
        debug!("aggr_expr: {}", format_expr_vec!(aggr_exprs));

        // Build projection node directly if no aggregate needed, otherwise
        // aggregate then projection.
        plan = if groupby_exprs.is_empty() && aggr_exprs.is_empty() {
            Plan::Projection(Projection::try_new(select_exprs, plan)?)
        } else {
            // Build aggregation node
            let agg_plan =
                Plan::Aggregate(Aggregate::try_new(plan, groupby_exprs, aggr_exprs.to_vec())?);
            // Rewrite the projection recursively, in which the AggregateFunction get replaced
            // with the field reference that can be found from the `agg_plan.schema()`, e.g.,
            // the rewritten name is basically same as the field name in the agg_plan output
            // schema.
            let select_exprs = select_exprs
                .into_iter()
                .map(|expr| {
                    expr.transform_down(|it| {
                        if aggr_exprs.contains(&it) {
                            // transform
                            let fr = FieldReference::from_name(it.schema_name().to_string());
                            let transformed_expr = Expr::FieldReference(fr);
                            // Stop recursing down this expr once we find a match
                            return Ok(Transformed::new(
                                transformed_expr,
                                true,
                                VisitRecursion::Stop,
                            ));
                        }
                        return Ok(Transformed::no(it));
                    })
                })
                .map(|res| res.map(|it| it.data))
                .collect::<Result<Vec<_>>>()?;
            // Build the projection node with agg_plan as input, which would enforce the
            // selected item MUST either be in the group by clause or an udaf implicitly.
            Plan::Projection(Projection::try_new(select_exprs, agg_plan)?)
        };

        // Process order by
        let order_by_schema = plan.schema().clone();
        let order_by_exprs = query
            .order_by
            .into_iter()
            .map(|it| {
                let expr = self.sqlexpr_to_expr(ctx, it.expr, &order_by_schema)?;
                let asc = if let Some(desc) = it.desc { !desc } else { true };
                Ok(SortExpr { expr, asc })
            })
            .collect::<Result<Vec<_>>>()?;
        if !order_by_exprs.is_empty() {
            plan = Plan::Sort(Sort::new(order_by_exprs, plan))
        }

        // process limit clause
        if let Some(limit) = query.limit_clause {
            plan = Plan::Limit(Limit::new(plan, limit.offset, limit.limit))
        }

        Ok(plan)
    }

    fn parse_select_item(
        &self,
        ctx: &mut BindContext,
        input: &Plan,
        item: SelectItem,
    ) -> Result<Vec<Expr>> {
        let schema = input.schema();
        match item {
            SelectItem::UnnamedExpr(expr) => {
                let expr = self.sqlexpr_to_expr(ctx, expr, schema)?;
                Ok(vec![expr])
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let expr = self.sqlexpr_to_expr(ctx, expr, schema)?;
                let alias = self.normalize_ident(&alias);
                Ok(vec![Expr::Alias(Alias::new(expr, None::<&str>, alias))])
            }
            SelectItem::WildcardExpr(wildcard) => match wildcard {
                WildcardExpr::QualifiedWildcard(idents) => {
                    if idents.len() != 1 {
                        return self.unimplemented_err(
                            "Multiple qualified wildcard idents not supported yet",
                        );
                    }
                    let table = self.normalize_ident(&idents[0]);
                    let q = TableReference::from(table);
                    let exprs = schema
                        .iter()
                        .filter_map(|(t, f)| {
                            if t.is_none() || t.unwrap() != &q {
                                return None;
                            }
                            Some(Expr::FieldReference(FieldReference::new(&f.name, t.cloned())))
                        })
                        .collect();
                    Ok(exprs)
                }
                WildcardExpr::Wildcard => {
                    let exprs = schema
                        .iter()
                        .map(|(t, f)| {
                            Expr::FieldReference(FieldReference::new(&f.name, t.cloned()))
                        })
                        .collect();
                    Ok(exprs)
                }
            },
        }
    }

    fn plan_table_with_joins(&self, ctx: &mut BindContext, t: TableWithJoins) -> Result<Plan> {
        let mut left = self.plan_table_factor(ctx, t.relation)?;
        let prev_outer_from_schema = ctx.outer_from_schema();
        for join in t.joins {
            ctx.extend_outer_from_schema(left.schema())?;
            left = self.plan_relation_join(ctx, left, join)?;
        }
        ctx.set_outer_from_schema(prev_outer_from_schema);
        Ok(left)
    }

    fn plan_relation_join(&self, ctx: &mut BindContext, left: Plan, join: SQLJoin) -> Result<Plan> {
        let right = self.plan_table_factor(ctx, join.relation)?;
        match join.join_operator {
            JoinOperator::Join(jc) | JoinOperator::Inner(jc) => {
                self.parse_join_to_plan(ctx, left, right, jc, JoinType::Inner)
            }
            JoinOperator::Left(jc) | JoinOperator::LeftOuter(jc) => {
                self.parse_join_to_plan(ctx, left, right, jc, JoinType::Left)
            }
            JoinOperator::Right(jc) | JoinOperator::RightOuter(jc) => {
                self.parse_join_to_plan(ctx, left, right, jc, JoinType::Right)
            }
            JoinOperator::Full(jc) | JoinOperator::FullOuter(jc) => {
                self.parse_join_to_plan(ctx, left, right, jc, JoinType::Full)
            }
        }
    }

    fn parse_join_to_plan(
        &self,
        ctx: &mut BindContext,
        left: Plan,
        right: Plan,
        constraint: JoinConstraint,
        typ: JoinType,
    ) -> Result<Plan> {
        match constraint {
            JoinConstraint::On(expr) => {
                let schema = left.schema().join(right.schema())?;
                let join_constraint = self.sqlexpr_to_expr(ctx, expr, &schema)?;
                Ok(Plan::Join(Join::try_new(left, right, typ, join_constraint, schema)?))
            }
        }
    }

    fn plan_table_factor(&self, ctx: &mut BindContext, relation: TableFactor) -> Result<Plan> {
        let (plan, alias) = match relation {
            TableFactor::Table { name, alias } => {
                let table = self.normalize_ident(&name);
                let table_schema: LogicalSchema = ctx
                    .catalog
                    .read_table(&table)?
                    .map(LogicalSchema::from)
                    .ok_or(parse_err!("Table {} not exits", table))?;
                let scan = TableScanBuilder::new(table.as_str(), &table_schema).build();
                (Plan::TableScan(scan), alias)
            }
            TableFactor::Derived { subquery, alias } => {
                let subplan = self.query_to_plan(ctx, *subquery)?;
                (subplan, alias)
            }
        };

        if alias.is_none() {
            return Ok(plan);
        }

        // apply table alias
        let alias = alias.unwrap();
        Ok(Plan::SubqueryAlias(SubqueryAlias::try_new(plan, alias)?))
    }

    fn delete_to_plan(
        &self,
        ctx: &mut BindContext,
        table: Ident,
        selection: Option<SQLExpr>,
    ) -> Result<Plan> {
        let table = self.normalize_ident(&table);
        let table_schema: LogicalSchema = ctx
            .catalog
            .read_table(&table)?
            .map(LogicalSchema::from)
            .ok_or(parse_err!("Table {} not exits", table))?;

        // build a full table scan node
        let mut scan_builder = TableScanBuilder::new(table.as_str(), &table_schema);
        if let Some(expr) = selection {
            let expr = self.sqlexpr_to_expr(ctx, expr, &table_schema)?;
            scan_builder = scan_builder.filter(expr);
        }
        let scan = Plan::TableScan(scan_builder.build());
        Ok(Plan::Delete(Delete::new(table, scan)))
    }

    fn update_to_plan(&self, ctx: &mut BindContext, update: SQLUpdate) -> Result<Plan> {
        let table = self.normalize_ident(&update.table);
        let table_schema: LogicalSchema = ctx
            .catalog
            .read_table(&table)?
            .map(LogicalSchema::from)
            .ok_or(parse_err!("Table {} not exits", table))?;
        if update.assignments.is_empty() {
            return Err(parse_err!("No assignments found for update table {}", table));
        }

        // build a full table scan node
        let mut scan_builder = TableScanBuilder::new(table.as_str(), &table_schema);
        if let Some(expr) = update.selection {
            let expr = self.sqlexpr_to_expr(ctx, expr, &table_schema)?;
            scan_builder = scan_builder.filter(expr);
        }
        let scan = Plan::TableScan(scan_builder.build());

        let mut assign_map = update
            .assignments
            .into_iter()
            .map(|it| {
                let column_name = self.normalize_ident(&it.column);
                (column_name, it.value)
            })
            .collect::<HashMap<String, SQLExpr>>();

        // build value exprs for all table columns with either
        // the input source , or previous value.
        let exprs = table_schema
            .iter()
            .map(|(q, f)| {
                let sqlexpr = assign_map.remove(&f.name);
                let expr = match sqlexpr {
                    None => {
                        Ok(Expr::FieldReference(FieldReference::new(&f.name, q.map(|a| a.clone()))))
                    }
                    Some(sqlexpr) => {
                        let expr = self.sqlexpr_to_expr(ctx, sqlexpr, scan.schema())?;
                        expr.can_cast_to(&f.datatype, scan.schema())
                    }
                }?;
                Ok(Expr::Alias(Alias::new(expr, None::<&str>, &f.name)))
            })
            .collect::<Result<Vec<_>>>()?;

        // build a projection plan node with source node and exprs as reference
        // to ensure the insertion values align to the table schema.
        let schema = scan.schema().clone();
        let projection = Plan::Projection(Projection::new(exprs, scan, schema));
        Ok(Plan::Update(Update::new(table, projection)))
    }

    fn insert_to_plan(&self, ctx: &mut BindContext, insert: SQLInsert) -> Result<Plan> {
        let table = self.normalize_ident(&insert.table);
        let table_schema: LogicalSchema = ctx
            .catalog
            .read_table(&table)?
            .map(LogicalSchema::from)
            .ok_or(parse_err!("Table {} not exits", table))?;

        // Get the insertion schema from the given columns and the target
        // index in table schema.
        //
        // value_indices[i] = Some(j) means the value of the i-th table
        // column is map to the j-th value of the input source values.
        //
        // values_indices[i] = None, means there is no value for i-th table
        // column, and should use the default value as the value of i-th
        // table column.
        let (insertion_schema, value_indices) = if insert.columns.is_empty() {
            let value_indices = (0..table_schema.len()).map(Some).collect::<Vec<_>>();
            (table_schema.clone(), value_indices)
        } else {
            let mut value_indices = vec![None; table_schema.len()];
            let fields: Fields = insert
                .columns
                .into_iter()
                .enumerate()
                .map(|(i, id)| {
                    let name = self.normalize_ident(&id);
                    let idx = table_schema
                        .field_index_by_name(&None, &name)
                        .ok_or(parse_err!("Column {} not exists", name))?;
                    if value_indices[idx].is_some() {
                        return Err(parse_err!("Duplicated column {}", name));
                    }
                    value_indices[idx] = Some(i);
                    let field = table_schema.field(idx);
                    Ok(field.clone())
                })
                .collect::<Result<Vec<_>>>()?
                .into();
            let qualifiers = vec![Some(TableReference::new(&table)); fields.len()];
            (LogicalSchema::from_qualified_fields(fields, qualifiers)?, value_indices)
        };

        // parse the origin source plan with the input insertion schema.
        let source = match insert.source {
            InsertSource::Select(query) => {
                let plan = self.query_to_plan(ctx, *query)?;
                if plan.schema().len() != insertion_schema.len() {
                    return Err(parse_err!("Query output columns doesn't match the insert query",));
                }
                Ok::<Plan, Error>(plan)
            }
            InsertSource::Values(sqlvalues) => {
                let empty_schema = Arc::new(LogicalSchema::empty());
                let exprs = sqlvalues
                    .rows
                    .into_iter()
                    .map(|row| {
                        row.into_iter()
                            .map(|it| self.sqlexpr_to_expr(ctx, it, &empty_schema))
                            .collect::<Result<Vec<_>>>()
                    })
                    .collect::<Result<Vec<_>>>()?;
                let values = Values::try_new(exprs, insertion_schema)?;
                Ok(Plan::Values(values))
            }
        }?;

        // build value exprs for all table columns with either
        // the input source , or the default value.
        let exprs = value_indices
            .into_iter()
            .enumerate()
            .map(|(i, j)| {
                let target_field = table_schema.field(i);
                let expr = match j {
                    None => target_field
                        .default
                        .clone()
                        .unwrap_or(Expr::Value(Value::Null))
                        .can_cast_to(&target_field.datatype, &LogicalSchema::empty())?,
                    Some(j) => Expr::FieldReference(source.schema().field_reference(j))
                        .can_cast_to(&target_field.datatype, source.schema())?,
                };
                Ok(Expr::Alias(Alias::new(expr, None::<&str>, &target_field.name)))
            })
            .collect::<Result<Vec<_>>>()?;

        // build a projection plan node with source node and exprs as reference
        // to ensure the insertion values align to the table schema.
        let projection = Plan::Projection(Projection::new(exprs, source, table_schema));

        Ok(Plan::Insert(Insert::new(table, projection)))
    }

    fn create_index_to_plan(&self, ctx: &mut BindContext, a: SQLCreateIndex) -> Result<Plan> {
        let name = self.normalize_ident(&a.name);
        let table = self.normalize_ident(&a.table_name);
        let table_schema: LogicalSchema = ctx
            .catalog
            .read_table(&table)?
            .ok_or(parse_err!("Table {} not exists", &table))?
            .into();
        let columns: Fields = a
            .column_names
            .into_iter()
            .map(|it| {
                let name = self.normalize_ident(&it);
                let (_, field_ref) = table_schema.find(&name).ok_or(parse_err!(
                    "Column {} not exists on table {}",
                    name,
                    table
                ))?;
                Ok(field_ref)
            })
            .collect::<Result<Vec<_>>>()?
            .into();
        Ok(Plan::CreateIndex(CreateIndex::new(name, table, columns, a.unique, a.if_not_exists)))
    }

    fn create_table_to_plan(&self, ctx: &mut BindContext, a: SQLCreateTable) -> Result<Plan> {
        let relation = self.normalize_ident(&a.name);
        let mut constraints = vec![];
        let mut fields = vec![];
        for (i, it) in a.columns.into_iter().enumerate() {
            let empty_schema = Arc::new(LogicalSchema::empty());
            let default = it
                .default
                .map(|sqlexpr| self.sqlexpr_to_expr(ctx, sqlexpr, &empty_schema))
                .transpose()?;
            let datatype = self.sql_convert_data_type(&it.datatype)?;
            let field = FieldBuilder::new(self.normalize_ident(&it.name), datatype)
                .primary_key(it.primary_key)
                .nullable(it.nullable)
                .default(default);
            fields.push(field);
            if it.unique {
                constraints.push(Constraint::Unique(vec![i]));
            }
        }
        for tc in a.table_constraints {
            match tc {
                TableConstraint::PrimaryKey { columns } => {
                    let column_names: HashSet<String> =
                        columns.into_iter().map(|it| self.normalize_ident(&it)).collect();
                    fields = fields
                        .into_iter()
                        .map(|it| if column_names.contains(&it.name) { it.partkey() } else { it })
                        .collect();
                }
            }
        }
        let schema = LogicalSchema::from_fields_constraints(fields, constraints)?;
        Ok(Plan::CreateTable(CreateTable::new(relation, schema, a.if_not_exists)))
    }

    /// Generate a relational/logical expression from a SQL expression.
    fn sqlexpr_to_expr(
        &self,
        ctx: &mut BindContext,
        sqlexpr: SQLExpr,
        schema: &LogicalSchema,
    ) -> Result<Expr> {
        // According to issue at https://github.com/apache/datafusion/issues/4065,
        // it is very likely get stack overflow in case of complex query, and since
        // a complex query would normally include many binary expr, have binary op
        // get parsed separately and iteratively would fix the issue.
        //
        // Although we are a lab project, it is easy to implement an iterative expr
        // parse on binary expr with the following steps by employ a shunting yard
        // variation algo and do it on the fly:
        // 1. Have two stacks, one for simulates post-order traversal,
        //   and one for eval on the fly.
        // 2. We first push the whole expr to stack, then do the following in a loop
        // 3. Pop the stack, check the popped element
        // 4. If it is a binary expr, reduce it to the op, right, and left, and push
        //   to stack in the order of op, right and left to simulate the post-order
        //   traversal.
        // 5. If it is not a binary expr, convert it to logical expr and push to eval
        //   stack.
        // 6. If it is an op, since we are focus on binary expr, it must be binary op,
        //   pop two exprs from the eval stack, compose it into one logical binary expr
        //   and push it back to eval stack.
        // 7. repleat steps 3-6, until we find the stack is empty.
        enum StackEntry {
            SQLExpr(Box<SQLExpr>),
            BinaryOperator(BinaryOperator),
        }
        let mut stack = vec![StackEntry::SQLExpr(Box::new(sqlexpr))];
        let mut eval_stack = vec![];
        while let Some(entry) = stack.pop() {
            match entry {
                StackEntry::SQLExpr(sqlexpr) => match *sqlexpr {
                    SQLExpr::BinaryOp { left, op, right } => {
                        // simulate post-order traversal by push op, right and
                        // left in order, so that we can get left, right, op popped
                        // out sequence.
                        stack.push(StackEntry::BinaryOperator(op));
                        stack.push(StackEntry::SQLExpr(right));
                        stack.push(StackEntry::SQLExpr(left));
                    }
                    _ => eval_stack.push(self.sqlexpr_to_expr_internal(ctx, *sqlexpr, schema)?),
                },
                StackEntry::BinaryOperator(op) => {
                    let right = eval_stack.pop().unwrap();
                    let left = eval_stack.pop().unwrap();
                    let op = match op {
                        BinaryOperator::Plus => Operator::Plus,
                        BinaryOperator::Minus => Operator::Minus,
                        BinaryOperator::Multiply => Operator::Multiply,
                        BinaryOperator::Divide => Operator::Divide,
                        BinaryOperator::Modulo => Operator::Modulo,
                        BinaryOperator::Eq => Operator::Eq,
                        BinaryOperator::NotEq => Operator::NotEq,
                        BinaryOperator::Gt => Operator::Gt,
                        BinaryOperator::GtEq => Operator::GtEq,
                        BinaryOperator::Lt => Operator::Lt,
                        BinaryOperator::LtEq => Operator::LtEq,
                        BinaryOperator::And => Operator::And,
                        BinaryOperator::Or => Operator::Or,
                    };
                    eval_stack.push(Expr::BinaryExpr(BinaryExpr::new(left, op, right)))
                }
            }
        }
        assert_eq!(1, eval_stack.len());
        let expr = eval_stack.pop().unwrap();
        Ok(expr)
    }

    fn sqlexpr_to_expr_internal(
        &self,
        ctx: &mut BindContext,
        sqlexpr: SQLExpr,
        schema: &LogicalSchema,
    ) -> Result<Expr> {
        match sqlexpr {
            SQLExpr::Value(value) => Ok(Expr::Value(self.parse_value(value)?)),
            SQLExpr::Identifier(ident) => self.parse_identifier_to_expr(ctx, ident, schema),
            SQLExpr::CompoundIdentifier(idents) => {
                self.parse_compound_ident_to_expr(ctx, idents, schema)
            }
            SQLExpr::UnaryOp { op, expr } => self.parse_unaryop_to_expr(ctx, op, *expr, schema),
            SQLExpr::Nested(expr) => self.sqlexpr_to_expr(ctx, *expr, schema),
            SQLExpr::IsNull(expr) => {
                Ok(Expr::IsNull(Box::new(self.sqlexpr_to_expr(ctx, *expr, schema)?)))
            }
            SQLExpr::IsNotNull(expr) => {
                Ok(Expr::IsNotNull(Box::new(self.sqlexpr_to_expr(ctx, *expr, schema)?)))
            }
            SQLExpr::IsTrue(expr) => {
                Ok(Expr::IsTrue(Box::new(self.sqlexpr_to_expr(ctx, *expr, schema)?)))
            }
            SQLExpr::IsNotTrue(expr) => {
                Ok(Expr::IsNotTrue(Box::new(self.sqlexpr_to_expr(ctx, *expr, schema)?)))
            }
            SQLExpr::IsFalse(expr) => {
                Ok(Expr::IsFalse(Box::new(self.sqlexpr_to_expr(ctx, *expr, schema)?)))
            }
            SQLExpr::IsNotFalse(expr) => {
                Ok(Expr::IsNotFalse(Box::new(self.sqlexpr_to_expr(ctx, *expr, schema)?)))
            }
            SQLExpr::Like { negated, expr, pattern } => {
                self.parse_like_to_expr(ctx, *expr, *pattern, negated, schema, false)
            }
            SQLExpr::ILike { negated, expr, pattern } => {
                self.parse_like_to_expr(ctx, *expr, *pattern, negated, schema, true)
            }
            SQLExpr::InList { expr, list, negated } => {
                self.parse_inlist_to_expr(ctx, *expr, list, negated, schema)
            }
            SQLExpr::Tuple(_) => self.unimplemented_err("Expr tuple not supported yet"),
            SQLExpr::Exists { subquery, negated } => {
                self.parse_exists_subquery_to_expr(ctx, *subquery, negated, schema)
            }
            SQLExpr::ScalarSubquery(query) => {
                self.parse_scalar_subquery_to_expr(ctx, *query, schema)
            }
            SQLExpr::InSubquery { expr, subquery, negated } => {
                self.parse_in_subquery_to_expr(ctx, *expr, *subquery, negated, schema)
            }
            SQLExpr::Function(f) => {
                // There different types of functions in SQL, i.e.,
                //  1. scalar function: operate on single input value, return single
                //   output value, e.g., UPPER(str), ABS(x), etc.
                //  2. aggregate function: operate on set of rows, return one value per
                //   group or per entire dataset, e.g, SUM(x), COUNT(*).
                //  3. window function: like aggregate function, but use `over` clause
                //   to define a window frame, and return one value per row, e.g.,
                //   ROW_NUMBER(), RANK(), SUM() OVER(...), COUNT() OVER(...)
                // TODO: Support window function
                self.parse_function_to_expr(ctx, f, schema)
            }
            _ => self.semantic_err(format!("Unknown sqlexpr {}", sqlexpr)),
        }
    }

    fn parse_function_to_expr(
        &self,
        ctx: &mut BindContext,
        func: Function,
        schema: &LogicalSchema,
    ) -> Result<Expr> {
        let func_name = self.normalize_ident(&func.name);
        // Check scalar function first
        if let Some(udf) = self.func_registry.udf(&func_name) {
            let args = func
                .args
                .into_iter()
                .map(|it| match it {
                    FunctionArg::Expr(expr) => self.sqlexpr_to_expr(ctx, expr, schema),
                    FunctionArg::Asterisk => self.semantic_err(format!(
                        "scalar function {} does not support * as arg",
                        func_name
                    )),
                })
                .collect::<Result<Vec<_>>>()?;
            return Ok(Expr::ScalarFunction(ScalarFunction::new(udf, args)));
        }

        // Then the aggregate function
        if let Some(udaf) = self.func_registry.udaf(&func_name) {
            let args = func
                .args
                .into_iter()
                .map(|it| match it {
                    FunctionArg::Expr(expr) => Ok(vec![self.sqlexpr_to_expr(ctx, expr, schema)?]),
                    FunctionArg::Asterisk => Ok(udaf.expand_asterisk(schema)?),
                })
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .flatten()
                .collect::<Vec<_>>();
            return Ok(Expr::AggregateFunction(AggregateFunction::new(udaf, args)));
        }

        self.semantic_err(format!("Unknown func {}", func_name))
    }

    fn parse_in_subquery_to_expr(
        &self,
        ctx: &mut BindContext,
        sqlexpr: SQLExpr,
        subquery: Query,
        negated: bool,
        schema: &LogicalSchema,
    ) -> Result<Expr> {
        let prev_outer_query_schema = ctx.set_outer_query_schema(Some(schema.clone().into()));
        let subplan = self.query_to_plan(ctx, subquery)?;
        ctx.set_outer_query_schema(prev_outer_query_schema);

        // validate the subplan produce a single column.
        let fields = subplan.schema().fields();
        if fields.len() > 1 {
            return self.semantic_err(format!(
                "Too many columns: {}, Select only one column in the subquery",
                fields.names().join(",")
            ));
        }

        let expr = self.sqlexpr_to_expr(ctx, sqlexpr, schema)?;
        Ok(Expr::InSubquery(InSubquery::try_new(subplan, expr, negated)?))
    }

    fn parse_scalar_subquery_to_expr(
        &self,
        ctx: &mut BindContext,
        subquery: Query,
        schema: &LogicalSchema,
    ) -> Result<Expr> {
        let prev_outer_query_schema = ctx.set_outer_query_schema(Some(schema.clone().into()));
        let subplan = self.query_to_plan(ctx, subquery)?;
        ctx.set_outer_query_schema(prev_outer_query_schema);

        // validate the subplan produce a single column.
        let fields = subplan.schema().fields();
        if fields.len() > 1 {
            return self.semantic_err(format!(
                "Too many columns: {}, Select only one column in the subquery",
                fields.names().join(",")
            ));
        }
        Ok(Expr::ScalarSubquery(Subquery::try_new(subplan)?))
    }

    fn parse_exists_subquery_to_expr(
        &self,
        ctx: &mut BindContext,
        subquery: Query,
        negated: bool,
        schema: &LogicalSchema,
    ) -> Result<Expr> {
        let prev_outer_query_schema = ctx.set_outer_query_schema(Some(schema.clone().into()));
        let subplan = self.query_to_plan(ctx, subquery)?;
        ctx.set_outer_query_schema(prev_outer_query_schema);

        Ok(Expr::Exists(Exists::try_new(subplan, negated)?))
    }

    fn parse_inlist_to_expr(
        &self,
        ctx: &mut BindContext,
        sqlexpr: SQLExpr,
        list: Vec<SQLExpr>,
        negated: bool,
        schema: &LogicalSchema,
    ) -> Result<Expr> {
        let expr = self.sqlexpr_to_expr(ctx, sqlexpr, schema)?;
        let list_exprs = list
            .into_iter()
            .map(|it| self.sqlexpr_to_expr(ctx, it, schema))
            .collect::<Result<Vec<_>>>()?;
        Ok(Expr::InList(InList::new(expr, list_exprs, negated)))
    }

    fn parse_like_to_expr(
        &self,
        ctx: &mut BindContext,
        sqlexpr: SQLExpr,
        pattern: SQLExpr,
        negated: bool,
        schema: &LogicalSchema,
        case_insensitive: bool,
    ) -> Result<Expr> {
        let pattern = self.sqlexpr_to_expr(ctx, pattern, schema)?;
        let expr = self.sqlexpr_to_expr(ctx, sqlexpr, schema)?;
        Ok(Expr::Like(Like::new(expr, pattern, negated, case_insensitive)))
    }

    fn parse_unaryop_to_expr(
        &self,
        ctx: &mut BindContext,
        op: UnaryOperator,
        sqlexpr: SQLExpr,
        schema: &LogicalSchema,
    ) -> Result<Expr> {
        match op {
            UnaryOperator::Plus => {
                let operand = self.sqlexpr_to_expr(ctx, sqlexpr, schema)?;
                let (datatype, _) = operand.datatype_and_nullable(schema)?;
                match datatype {
                    DataType::Integer | DataType::Float => Ok(operand),
                    _ => self.semantic_err(format!("+ cannot be used with {datatype}")),
                }
            }
            UnaryOperator::Minus => match sqlexpr {
                SQLExpr::Value(SQLValue::Number(n)) => {
                    // if it is a numeric value, apply the minus directly.
                    Ok(Expr::Value(self.parse_value_number(&n, true)?))
                }
                _ => {
                    // otherwise wrap it with negative operator
                    let operand = self.sqlexpr_to_expr(ctx, sqlexpr, schema)?;
                    let (datatype, _) = operand.datatype_and_nullable(schema)?;
                    if !datatype.is_numeric() {
                        return self.semantic_err(format!("- cannot be used with {datatype}"));
                    }
                    Ok(Expr::Negative(Box::new(operand)))
                }
            },
            UnaryOperator::Not => {
                Ok(Expr::Not(Box::new(self.sqlexpr_to_expr(ctx, sqlexpr, schema)?)))
            }
        }
    }

    fn parse_compound_ident_to_expr(
        &self,
        ctx: &BindContext,
        mut idents: Vec<Ident>,
        schema: &LogicalSchema,
    ) -> Result<Expr> {
        if idents.len() != 2 {
            return self.semantic_err(format!(
                "invalid compound identifier, expect _to_ idents got {}",
                idents.len()
            ));
        }
        let table = idents.remove(0);
        let table = self.ident_normalizer.normalize(table);
        let ident = idents.remove(0);
        let ident = self.ident_normalizer.normalize(ident);

        if let Some((field, Some(_))) =
            schema.field_reference_by_qname(&Some(TableReference::new(&table)), &ident)
        {
            return Ok(Expr::FieldReference(field));
        }

        // check the outer query schema
        if let Some(outer) = &ctx.outer_query_schema {
            if let Some((field, Some(f))) =
                outer.field_reference_by_qname(&Some(TableReference::new(&table)), &ident)
            {
                return Ok(Expr::OuterFieldReference(f.datatype.clone(), field));
            }
        }

        self.semantic_err(format!("Unknown compound ident {}.{}", table, ident))
    }

    fn parse_identifier_to_expr(
        &self,
        ctx: &BindContext,
        ident: Ident,
        schema: &LogicalSchema,
    ) -> Result<Expr> {
        let ident = self.ident_normalizer.normalize(ident);
        if let Some((field, Some(_))) = schema.field_reference_by_name(&ident) {
            return Ok(Expr::FieldReference(field));
        }
        // check the outer query schema
        if let Some(outer) = &ctx.outer_query_schema {
            if let Some((field, Some(col))) = outer.field_reference_by_name(&ident) {
                return Ok(Expr::OuterFieldReference(col.datatype.clone(), field));
            }
        }
        self.semantic_err(format!("Unknown identifier {}", ident))
    }

    fn parse_value(&self, sql_value: SQLValue) -> Result<Value> {
        match sql_value {
            SQLValue::Number(n) => self.parse_value_number(&n, false),
            SQLValue::String(s) => Ok(Value::String(s)),
            SQLValue::Null => Ok(Value::Null),
            SQLValue::Boolean(b) => Ok(Value::Boolean(b)),
        }
    }

    fn parse_value_number(&self, n: &str, negative: bool) -> Result<Value> {
        let num = if negative { Cow::Owned(format!("-{n}")) } else { Cow::Borrowed(n) };
        // Try to parse as i64 first, then f64
        if let Ok(n) = num.parse::<i64>() {
            return Ok(Value::Integer(n));
        }
        if let Ok(n) = num.parse::<f64>() {
            return Ok(Value::Float(n));
        }
        self.semantic_err(format!("invalid number {}", n))
    }

    fn sql_convert_data_type(&self, sql_type: &SQLDataType) -> Result<DataType> {
        match sql_type {
            SQLDataType::Boolean => Ok(DataType::Boolean),
            SQLDataType::Integer => Ok(DataType::Integer),
            SQLDataType::Float => Ok(DataType::Float),
            SQLDataType::String => Ok(DataType::String),
        }
    }

    fn normalize_ident(&self, ident: &Ident) -> String {
        self.ident_normalizer.normalize(ident.clone())
    }

    fn semantic_err<T, E: ToString>(&self, msg: E) -> Result<T> {
        Err(parse_err!("{}", msg.to_string()))
    }

    fn unimplemented_err<T, E: ToString>(&self, msg: E) -> Result<T> {
        Err(unimplemented_err!("{}", msg.to_string()))
    }
}

#[derive(Clone)]
pub struct IdentNormalizer {
    normalize: bool,
}

impl Default for IdentNormalizer {
    fn default() -> Self {
        Self { normalize: true }
    }
}

impl IdentNormalizer {
    pub fn new(normalize: bool) -> Self {
        Self { normalize }
    }

    pub fn normalize(&self, ident: Ident) -> String {
        if !self.normalize || ident.double_quoted {
            return ident.value;
        }
        ident.value.to_ascii_lowercase()
    }
}

#[cfg(test)]
pub mod tests {
    use std::io::Write;

    use goldenfile::Mint;

    use super::*;
    use crate::catalog::column::ColumnBuilder;
    use crate::catalog::index::Index;
    use crate::catalog::index::Indexes;
    use crate::catalog::schema::Schema;
    use crate::catalog::schema::Schemas;
    use crate::sql::parser::display_utils;
    use crate::sql::parser::Parser;
    use crate::value_err;

    const GOLDEN_DIR: &str = "src/sql/plan/golden";

    macro_rules! test_logical_planner {
        ($($name:ident: $stmt:expr, )*) => {
            $(
                #[test]
                fn $name() -> Result<()> {
                    let mut parser = Parser::new($stmt)?;
                    let stmt = parser.parse_statement()?;

                    let planner = Planner::new();
                    let catalog: Arc<dyn Catalog> = Arc::new(TestCatalog {});
                    let mut ctx = BindContext::new(catalog);
                    let plan = planner.sql_statement_to_plan(&mut ctx, stmt)?;

                    let mut mint = Mint::new(GOLDEN_DIR);
                    let mut f = mint.new_goldenfile(format!("{}", stringify!($name)))?;

                    write!(f, "Stmt: \n{}\n\n", display_utils::dedent($stmt))?;

                    write!(f, "Logical Plan:\n")?;
                    write!(f, "--------------\n\n")?;
                    write!(f, "{}\n\n", plan)?;

                    write!(f, "Logical Plan with schema:\n")?;
                    write!(f, "-------------------------\n\n")?;
                    write!(f, "{:#}\n", plan)?;
                    Ok(())
                }
            )*
        }
    }

    test_logical_planner! {
        query_with_join: r#"
            SELECT
                users.name AS user_name,
                orders.id AS order_id,
                products.name AS product_name
            FROM
                users
                JOIN orders ON users.id = orders.user_id
                JOIN products ON orders.product_id = products.id
            WHERE
               users.name IN (SELECT name from users WHERE email = 'admin@example.com');        
        "#,
        query_with_alias_join: r#"
            SELECT
                a.name AS user_name,
                orders.id AS order_id,
                products.name AS product_name
            FROM
                users AS a
                JOIN orders ON a.id = orders.user_id
                JOIN products ON orders.product_id = products.id
            WHERE
               a.name IN (SELECT name from users WHERE email = 'admin@example.com');        
        "#,
        query_with_agg: r#"
            SELECT
                UPPER(name),
                COUNT(*)
            FROM
                users
            WHERE name = 'a'
            GROUP by name;
        "#,
        query_with_groupby_limit_offset: "SELECT name FROM users GROUP BY name ORDER BY name DESC limit 1 offset 1;",
        insert_with_select: r#"
            INSERT INTO users (id, name, email)
            SELECT id, name, email
            FROM old_users
            WHERE active = true;            
        "#,
        insert_with_values: r#"
            INSERT INTO users (id, name, email)
            VALUES
              (1, 'Alice', 'alice@example.com'),
              (2, 'Bob', 'bob@example.com'),
              (3, 'Charlie', 'charlie@example.com');
        "#,
        update: r#"
            UPDATE users
            SET name = 'Alice Smith',
                email = 'alice.smith@example.com'
            WHERE id = 1;
        "#,
        delete: "DELETE FROM users where id = 1;",
    }

    pub struct TestCatalog {}

    impl Catalog for TestCatalog {
        fn read_table(&self, table: &str) -> Result<Option<Schema>> {
            match table {
                "users" => {
                    let columns = vec![
                        ColumnBuilder::new("id", DataType::Integer).build_unchecked(),
                        ColumnBuilder::new("name", DataType::String).build_unchecked(),
                        ColumnBuilder::new("email", DataType::String).build_unchecked(),
                    ];
                    Ok(Some(Schema::new("users", columns, vec![])))
                }
                "old_users" => {
                    let columns = vec![
                        ColumnBuilder::new("id", DataType::Integer).build_unchecked(),
                        ColumnBuilder::new("name", DataType::String).build_unchecked(),
                        ColumnBuilder::new("email", DataType::String).build_unchecked(),
                        ColumnBuilder::new("active", DataType::Boolean).build_unchecked(),
                    ];
                    Ok(Some(Schema::new("old_users", columns, vec![])))
                }
                "orders" => {
                    let columns = vec![
                        ColumnBuilder::new("id", DataType::Integer).build_unchecked(),
                        ColumnBuilder::new("user_id", DataType::Integer).build_unchecked(),
                        ColumnBuilder::new("product_id", DataType::Integer).build_unchecked(),
                        ColumnBuilder::new("amound", DataType::Float).build_unchecked(),
                    ];
                    Ok(Some(Schema::new("orders", columns, vec![])))
                }
                "products" => {
                    let columns = vec![
                        ColumnBuilder::new("id", DataType::Integer).build_unchecked(),
                        ColumnBuilder::new("name", DataType::String).build_unchecked(),
                    ];
                    Ok(Some(Schema::new("products", columns, vec![])))
                }
                _ => Err(value_err!("Table {} not found", table)),
            }
        }

        fn create_table(&self, _schema: Schema) -> Result<()> {
            todo!()
        }

        fn delete_table(&self, _table: &str) -> Result<()> {
            todo!()
        }

        fn scan_tables(&self) -> Result<Schemas> {
            todo!()
        }

        fn read_index(&self, _index: &str, _table: &str) -> Result<Option<Index>> {
            todo!()
        }

        fn create_index(&self, _index: Index) -> Result<()> {
            todo!()
        }

        fn delete_index(&self, _index: &str, _table: &str) -> Result<()> {
            todo!()
        }

        fn scan_table_indexes(&self, _table: &str) -> Result<Indexes> {
            todo!()
        }
    }
}
