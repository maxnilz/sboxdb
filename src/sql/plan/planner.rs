use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use crate::catalog::catalog::Catalog;
use crate::catalog::r#type::DataType;
use crate::catalog::r#type::Value;
use crate::error::Error;
use crate::error::Result;
use crate::sql::parser::ast::BinaryOperator;
use crate::sql::parser::ast::CreateIndex as SQLCreateIndex;
use crate::sql::parser::ast::CreateTable as SQLCreateTable;
use crate::sql::parser::ast::DataType as SQLDataType;
use crate::sql::parser::ast::Expr as SQLExpr;
use crate::sql::parser::ast::Ident;
use crate::sql::parser::ast::Insert as SQLInsert;
use crate::sql::parser::ast::InsertSource;
use crate::sql::parser::ast::Join as SQLJoin;
use crate::sql::parser::ast::JoinConstraint;
use crate::sql::parser::ast::JoinOperator;
use crate::sql::parser::ast::ObjectType;
use crate::sql::parser::ast::Query;
use crate::sql::parser::ast::SelectItem;
use crate::sql::parser::ast::Statement;
use crate::sql::parser::ast::TableFactor;
use crate::sql::parser::ast::TableWithJoins;
use crate::sql::parser::ast::UnaryOperator;
use crate::sql::parser::ast::Update as SQLUpdate;
use crate::sql::parser::ast::Value as SQLValue;
use crate::sql::parser::ast::WildcardExpr;
use crate::sql::plan::expr::Alias;
use crate::sql::plan::expr::BinaryExpr;
use crate::sql::plan::expr::Exists;
use crate::sql::plan::expr::Expr;
use crate::sql::plan::expr::InList;
use crate::sql::plan::expr::InSubquery;
use crate::sql::plan::expr::Like;
use crate::sql::plan::expr::Operator;
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
use crate::sql::plan::plan::Transaction;
use crate::sql::plan::plan::Update;
use crate::sql::plan::plan::Values;
use crate::sql::plan::schema::Field;
use crate::sql::plan::schema::FieldReference;
use crate::sql::plan::schema::Fields;
use crate::sql::plan::schema::LogicalSchema;
use crate::sql::plan::schema::TableReference;

struct Context {
    /// The query schema of the outer query plan, used to resolve
    /// the fields in subquery.
    outer_query_schema: Option<LogicalSchema>,
    /// The joined schemas of all FROM clauses planned so far.
    outer_from_schema: Option<LogicalSchema>,
}

impl Context {
    pub fn new() -> Self {
        Self { outer_from_schema: None, outer_query_schema: None }
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
    pub fn extend_outer_from_schema(&mut self, schema: &LogicalSchema) -> Result<()> {
        match self.outer_from_schema.as_mut() {
            Some(from_schema) => from_schema.merge(schema),
            None => self.outer_from_schema = Some(schema.clone()),
        };
        Ok(())
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
struct SqlToRel {
    catalog: Box<dyn Catalog>,
    ident_normalizer: IdentNormalizer,
}

impl SqlToRel {
    fn sql_statement_to_plan(&self, statement: Statement, context: &mut Context) -> Result<Plan> {
        match statement {
            Statement::Begin { read_only, as_of } => {
                let as_of =
                    if let Some(as_of) = as_of { Some(as_of.parse::<u64>()?) } else { None };
                Ok(Plan::Transaction(Transaction::Begin { read_only, as_of }))
            }
            Statement::Commit => Ok(Plan::Transaction(Transaction::Commit)),
            Statement::Rollback => Ok(Plan::Transaction(Transaction::Abort)),
            Statement::CreateTable(sql_create_table) => {
                self.create_table_to_plan(context, sql_create_table)
            }
            Statement::CreateIndex(sql_create_index) => self.create_index_to_plan(sql_create_index),
            Statement::Drop { object_type, object_name, if_exists } => {
                self.drop_obj_to_plan(object_type, object_name, if_exists)
            }
            Statement::AlterTable { .. } => {
                self.semantic_err(Error::parse("ALTER TABLE not supported yet"))
            }
            Statement::Insert(insert) => self.insert_to_plan(context, insert),
            Statement::Update(update) => self.update_to_plan(context, update),
            Statement::Delete { table, selection } => {
                self.delete_to_plan(context, table, selection)
            }
            Statement::Select { query } => self.query_to_plan(context, *query),
            Statement::Explain { analyze, verbose, statement } => {
                let plan = self.sql_statement_to_plan(*statement, context)?;
                Ok(Plan::Explain(Explain::new(plan, verbose, analyze)))
            }
        }
    }

    /// Generate a logical plan from an SQL query/subquery
    fn query_to_plan(&self, context: &mut Context, query: Query) -> Result<Plan> {
        // plan table with joins
        let mut base_plan = self.plan_table_with_joins(context, query.from)?;

        // plan the selection
        if let Some(sqlexpr) = query.selection {
            let schema = base_plan.schema();
            let mut expr = self.sqlexpr_to_expr(context, sqlexpr, schema)?;
            expr = expr.qualify_field_reference(&[schema])?;
            base_plan = Plan::Filter(Filter::try_new(expr, base_plan)?)
        }

        // plan projection
        let mut plan =
            self.parse_projection_to_plan(context, base_plan.clone(), query.projection)?;
        let select_exprs: &[Expr] = match &plan {
            Plan::Projection(proj) => proj.exprs.as_ref(),
            _ => unreachable!(),
        };
        // TODO: add agg function support later.
        _ = select_exprs;
        let aggr_exprs: Vec<Expr> = vec![];

        // Process group by
        // group-by expressions prioritize referencing columns from the FROM clause
        // then the select list.
        let mut group_by_schema = base_plan.schema().clone();
        group_by_schema.merge(plan.schema());
        let group_by_exprs = query
            .group_by
            .into_iter()
            .map(|it| {
                let expr = self.sqlexpr_to_expr(context, it, &group_by_schema)?;
                expr.qualify_field_reference(&[&group_by_schema])
            })
            .collect::<Result<Vec<_>>>()?;

        if !group_by_exprs.is_empty() || !aggr_exprs.is_empty() {
            plan = Plan::Aggregate(Aggregate::try_new(plan, group_by_exprs, aggr_exprs)?);
        }

        // Process order by, order-by expressions prioritize referencing columns
        // from the select list, then from the FROM clause.
        let mut order_by_schema = plan.schema().clone();
        order_by_schema.merge(base_plan.schema());
        let order_by_exprs = query
            .order_by
            .into_iter()
            .map(|it| {
                let mut expr = self.sqlexpr_to_expr(context, it.expr, &order_by_schema)?;
                expr = expr.qualify_field_reference(&[&order_by_schema])?;
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

    fn parse_projection_to_plan(
        &self,
        context: &mut Context,
        input: Plan,
        projection: Vec<SelectItem>,
    ) -> Result<Plan> {
        let exprs = projection
            .into_iter()
            .map(|it| self.parse_select_item(context, &input, it))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        Ok(Plan::Projection(Projection::try_new(exprs, input)?))
    }

    fn parse_select_item(
        &self,
        context: &mut Context,
        input: &Plan,
        item: SelectItem,
    ) -> Result<Vec<Expr>> {
        let schema = input.schema();
        match item {
            SelectItem::UnnamedExpr(expr) => {
                let mut expr = self.sqlexpr_to_expr(context, expr, schema)?;
                expr = expr.qualify_field_reference(&[schema])?;
                Ok(vec![expr])
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let mut expr = self.sqlexpr_to_expr(context, expr, schema)?;
                expr = expr.qualify_field_reference(&[schema])?;
                let alias = self.normalize_ident(&alias);
                Ok(vec![Expr::Alias(Alias::new(expr, None::<&str>, alias))])
            }
            SelectItem::WildcardExpr(wildcard) => match wildcard {
                WildcardExpr::QualifiedWildcard(idents) => {
                    if idents.len() != 1 {
                        return self
                            .semantic_err("Multiple qualified wildcard idents not supported yet");
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

    fn plan_table_with_joins(&self, context: &mut Context, t: TableWithJoins) -> Result<Plan> {
        let mut left = self.plan_table_factor(context, t.relation)?;
        let old_outer_from_schema = context.outer_from_schema();
        for join in t.joins {
            context.extend_outer_from_schema(left.schema())?;
            left = self.plan_relation_join(context, left, join)?;
        }
        context.set_outer_from_schema(old_outer_from_schema);
        Ok(left)
    }

    fn plan_relation_join(&self, context: &mut Context, left: Plan, join: SQLJoin) -> Result<Plan> {
        let right = self.plan_table_factor(context, join.relation)?;
        match join.join_operator {
            JoinOperator::Join(jc) | JoinOperator::Inner(jc) => {
                self.parse_join_to_plan(context, left, right, jc, JoinType::Inner)
            }
            JoinOperator::Left(jc) | JoinOperator::LeftOuter(jc) => {
                self.parse_join_to_plan(context, left, right, jc, JoinType::Left)
            }
            JoinOperator::Right(jc) | JoinOperator::RightOuter(jc) => {
                self.parse_join_to_plan(context, left, right, jc, JoinType::Right)
            }
            JoinOperator::Full(jc) | JoinOperator::FullOuter(jc) => {
                self.parse_join_to_plan(context, left, right, jc, JoinType::Full)
            }
        }
    }

    fn parse_join_to_plan(
        &self,
        context: &mut Context,
        left: Plan,
        right: Plan,
        constraint: JoinConstraint,
        typ: JoinType,
    ) -> Result<Plan> {
        match constraint {
            JoinConstraint::On(expr) => {
                let schema = left.schema().join(right.schema())?;
                let mut expr = self.sqlexpr_to_expr(context, expr, &schema)?;
                expr = expr.qualify_field_reference(&[&schema])?;
                Ok(Plan::Join(Join::new(left, right, typ, expr, schema)))
            }
        }
    }

    fn plan_table_factor(&self, context: &mut Context, relation: TableFactor) -> Result<Plan> {
        let (plan, alias) = match relation {
            TableFactor::Table { name, alias } => {
                let table = self.normalize_ident(&name);
                let table_schema: LogicalSchema = self
                    .catalog
                    .read_table(&table)?
                    .map(LogicalSchema::from)
                    .ok_or(Error::parse(format!("Table {} not exits", table)))?;
                let scan = TableScanBuilder::new(table.as_str(), &table_schema).build();
                (Plan::TableScan(scan), alias)
            }
            TableFactor::Derived { subquery, alias } => {
                let subplan = self.query_to_plan(context, *subquery)?;
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
        context: &mut Context,
        table: Ident,
        selection: Option<SQLExpr>,
    ) -> Result<Plan> {
        let table = self.normalize_ident(&table);
        let table_schema: LogicalSchema = self
            .catalog
            .read_table(&table)?
            .map(LogicalSchema::from)
            .ok_or(Error::parse(format!("Table {} not exits", table)))?;

        // build a full table scan node
        let mut scan_builder = TableScanBuilder::new(table.as_str(), &table_schema);
        if let Some(expr) = selection {
            let expr = self.sqlexpr_to_expr(context, expr, &table_schema)?;
            scan_builder = scan_builder.filter(expr);
        }
        let scan = Plan::TableScan(scan_builder.build());
        Ok(Plan::Delete(Delete::new(table.as_str(), scan)))
    }

    fn update_to_plan(&self, context: &mut Context, update: SQLUpdate) -> Result<Plan> {
        let table = self.normalize_ident(&update.table);
        let table_schema: LogicalSchema = self
            .catalog
            .read_table(&table)?
            .map(LogicalSchema::from)
            .ok_or(Error::parse(format!("Table {} not exits", table)))?;
        if update.assignments.is_empty() {
            return Err(Error::parse(format!("No assignments found for update table {}", table)));
        }

        // build a full table scan node
        let mut scan_builder = TableScanBuilder::new(table.as_str(), &table_schema);
        if let Some(expr) = update.selection {
            let expr = self.sqlexpr_to_expr(context, expr, &table_schema)?;
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
                        let expr = self.sqlexpr_to_expr(context, sqlexpr, scan.schema())?;
                        expr.cast_to(&f.datatype, scan.schema())
                    }
                }?;
                Ok(Expr::Alias(Alias::new(expr, None::<&str>, &f.name)))
            })
            .collect::<Result<Vec<_>>>()?;

        // build a projection plan node with source node and exprs as reference
        // to ensure the insertion values align to the table schema.
        let schema = scan.schema().clone();
        let projection = Plan::Projection(Projection::new(exprs, scan, schema));
        Ok(Plan::Update(Update::new(table.as_str(), projection)))
    }

    fn insert_to_plan(&self, context: &mut Context, insert: SQLInsert) -> Result<Plan> {
        let table = self.normalize_ident(&insert.table);
        let table_schema: LogicalSchema = self
            .catalog
            .read_table(&table)?
            .map(LogicalSchema::from)
            .ok_or(Error::parse(format!("Table {} not exits", table)))?;

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
            let value_indices = (0..table_schema.fields().len()).map(Some).collect::<Vec<_>>();
            (table_schema.clone(), value_indices)
        } else {
            let mut value_indices = vec![None; table_schema.fields().len()];
            let fields: Fields = insert
                .columns
                .into_iter()
                .enumerate()
                .map(|(i, id)| {
                    let name = self.normalize_ident(&id);
                    let idx = table_schema
                        .field_index_by_name(&None, &name)
                        .ok_or(Error::parse(format!("Column {} not exists", name)))?;
                    if value_indices[idx].is_some() {
                        return Err(Error::parse(format!("Duplicated column {}", name)));
                    }
                    value_indices[idx] = Some(i);
                    let field = table_schema.field(idx);
                    Ok(field.clone())
                })
                .collect::<Result<Vec<_>>>()?
                .into();
            let qualifiers = vec![Some(TableReference::new(&table)); fields.len()];
            (LogicalSchema::new(fields, qualifiers)?, value_indices)
        };

        // parse the origin source plan with the input insertion schema.
        let source = match insert.source {
            InsertSource::Select(query) => {
                let plan = self.query_to_plan(context, *query)?;
                if plan.schema().fields().len() != insertion_schema.fields().len() {
                    return Err(Error::parse(
                        "Query output columns doesn't match the insert query",
                    ));
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
                            .map(|it| self.sqlexpr_to_expr(context, it, &empty_schema))
                            .collect::<Result<Vec<_>>>()
                    })
                    .collect::<Result<Vec<_>>>()?;
                let values = Values::new(exprs, insertion_schema)?;
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
                        .cast_to(&target_field.datatype, &LogicalSchema::empty())?,
                    Some(j) => Expr::FieldReference(source.schema().field_reference(j))
                        .cast_to(&target_field.datatype, source.schema())?,
                };
                Ok(Expr::Alias(Alias::new(expr, None::<&str>, &target_field.name)))
            })
            .collect::<Result<Vec<_>>>()?;

        // build a projection plan node with source node and exprs as reference
        // to ensure the insertion values align to the table schema.
        // TODO: maybe we need infer the output schema from the exprs list instead
        //  of using the table schema directly.
        let projection = Plan::Projection(Projection::new(exprs, source, table_schema));

        Ok(Plan::Insert(Insert::new(table.as_str(), projection)))
    }

    fn drop_obj_to_plan(
        &self,
        object_type: ObjectType,
        object_name: Ident,
        if_exists: bool,
    ) -> Result<Plan> {
        match object_type {
            ObjectType::Table => {
                let relation = TableReference::new(&self.normalize_ident(&object_name));
                Ok(Plan::DropTable(DropTable { relation, if_exists }))
            }
            ObjectType::Index => {
                let name = self.normalize_ident(&object_name);
                Ok(Plan::DropIndex(DropIndex { name, if_exists }))
            }
        }
    }

    fn create_index_to_plan(&self, a: SQLCreateIndex) -> Result<Plan> {
        let name = self.normalize_ident(&a.name);
        let table = self.normalize_ident(&a.table_name);
        let table_schema: LogicalSchema = self
            .catalog
            .read_table(&table)?
            .ok_or(Error::parse(format!("Table {} not exists", &table)))?
            .into();
        let columns: Fields = a
            .column_names
            .into_iter()
            .map(|it| {
                let name = self.normalize_ident(&it);
                let (_, field_ref) = table_schema.find(&name).ok_or(Error::parse(format!(
                    "Column {} not exists on table {}",
                    name, table
                )))?;
                Ok(field_ref)
            })
            .collect::<Result<Vec<_>>>()?
            .into();
        Ok(Plan::CreateIndex(CreateIndex {
            name,
            relation: TableReference::new(&table),
            columns,
            unique: a.unique,
            if_not_exists: a.if_not_exists,
        }))
    }

    fn create_table_to_plan(&self, context: &mut Context, a: SQLCreateTable) -> Result<Plan> {
        let relation = TableReference::new(&self.normalize_ident(&a.name));
        let fields: Fields = a
            .columns
            .into_iter()
            .map(|it| {
                let empty_schema = Arc::new(LogicalSchema::empty());
                let default = it
                    .default
                    .map(|sqlexpr| self.sqlexpr_to_expr(context, sqlexpr, &empty_schema))
                    .transpose()?;
                let datatype = self.sql_convert_data_type(&it.datatype)?;
                Ok(Field {
                    name: self.normalize_ident(&it.name),
                    datatype,
                    primary_key: it.primary_key,
                    nullable: it.nullable,
                    unique: it.unique,
                    default,
                })
            })
            .collect::<Result<Vec<_>>>()?
            .into();
        let schema = LogicalSchema::from_unqualified_fields(fields)?;
        Ok(Plan::CreateTable(CreateTable { relation, schema, if_not_exists: a.if_not_exists }))
    }

    /// Generate a relational/logical expression from a SQL expression.
    fn sqlexpr_to_expr(
        &self,
        context: &mut Context,
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
                    _ => eval_stack.push(self.sqlexpr_to_expr_internal(context, *sqlexpr, schema)?),
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
        context: &mut Context,
        sqlexpr: SQLExpr,
        schema: &LogicalSchema,
    ) -> Result<Expr> {
        match sqlexpr {
            SQLExpr::Value(value) => Ok(Expr::Value(self.parse_value(value)?)),
            SQLExpr::Identifier(ident) => self.parse_identifier_to_expr(context, ident, schema),
            SQLExpr::CompoundIdentifier(idents) => {
                self.parse_compound_ident_to_expr(context, idents, schema)
            }
            SQLExpr::UnaryOp { op, expr } => self.parse_unaryop_to_expr(context, op, *expr, schema),
            SQLExpr::Nested(expr) => self.sqlexpr_to_expr(context, *expr, schema),
            SQLExpr::IsNull(expr) => {
                Ok(Expr::IsNull(Box::new(self.sqlexpr_to_expr(context, *expr, schema)?)))
            }
            SQLExpr::IsNotNull(expr) => {
                Ok(Expr::IsNotNull(Box::new(self.sqlexpr_to_expr(context, *expr, schema)?)))
            }
            SQLExpr::IsTrue(expr) => {
                Ok(Expr::IsTrue(Box::new(self.sqlexpr_to_expr(context, *expr, schema)?)))
            }
            SQLExpr::IsNotTrue(expr) => {
                Ok(Expr::IsNotTrue(Box::new(self.sqlexpr_to_expr(context, *expr, schema)?)))
            }
            SQLExpr::IsFalse(expr) => {
                Ok(Expr::IsFalse(Box::new(self.sqlexpr_to_expr(context, *expr, schema)?)))
            }
            SQLExpr::IsNotFalse(expr) => {
                Ok(Expr::IsNotFalse(Box::new(self.sqlexpr_to_expr(context, *expr, schema)?)))
            }
            SQLExpr::Like { negated, expr, pattern } => {
                self.parse_like_to_expr(context, *expr, *pattern, negated, schema, false)
            }
            SQLExpr::ILike { negated, expr, pattern } => {
                self.parse_like_to_expr(context, *expr, *pattern, negated, schema, true)
            }
            SQLExpr::InList { expr, list, negated } => {
                self.parse_inlist_to_expr(context, *expr, list, negated, schema)
            }
            SQLExpr::Tuple(_) => self.semantic_err("Expr tuple not supported yet"),
            SQLExpr::Exists { subquery, negated } => {
                self.parse_exists_subquery_to_expr(context, *subquery, negated, schema)
            }
            SQLExpr::ScalarSubquery(query) => {
                self.parse_scalar_subquery_to_expr(context, *query, schema)
            }
            SQLExpr::InSubquery { expr, subquery, negated } => {
                self.parse_in_subquery_to_expr(context, *expr, *subquery, negated, schema)
            }
            SQLExpr::Function(_) => self.semantic_err("Function not supported yet"),
            _ => self.semantic_err(format!("Unsupported sqlexpr {}", sqlexpr)),
        }
    }

    fn parse_in_subquery_to_expr(
        &self,
        context: &mut Context,
        sqlexpr: SQLExpr,
        subquery: Query,
        negated: bool,
        schema: &LogicalSchema,
    ) -> Result<Expr> {
        let old_outer_query_schema = context.set_outer_query_schema(Some(schema.clone().into()));
        let subplan = self.query_to_plan(context, subquery)?;
        context.set_outer_query_schema(old_outer_query_schema);

        // TODO: validate the subplan produce a single column.

        let expr = self.sqlexpr_to_expr(context, sqlexpr, schema)?;
        Ok(Expr::InSubquery(InSubquery::new(subplan, expr, negated)))
    }

    fn parse_scalar_subquery_to_expr(
        &self,
        context: &mut Context,
        subquery: Query,
        schema: &LogicalSchema,
    ) -> Result<Expr> {
        let old_outer_query_schema = context.set_outer_query_schema(Some(schema.clone().into()));
        let subplan = self.query_to_plan(context, subquery)?;
        context.set_outer_query_schema(old_outer_query_schema);

        // TODO: validate the subplan produce a single column.

        Ok(Expr::ScalarSubquery(Subquery::new(subplan)))
    }

    fn parse_exists_subquery_to_expr(
        &self,
        context: &mut Context,
        subquery: Query,
        negated: bool,
        schema: &LogicalSchema,
    ) -> Result<Expr> {
        let old_outer_query_schema = context.set_outer_query_schema(Some(schema.clone().into()));
        let subplan = self.query_to_plan(context, subquery)?;
        context.set_outer_query_schema(old_outer_query_schema);

        Ok(Expr::Exists(Exists::new(subplan, negated)))
    }

    fn parse_inlist_to_expr(
        &self,
        context: &mut Context,
        sqlexpr: SQLExpr,
        list: Vec<SQLExpr>,
        negated: bool,
        schema: &LogicalSchema,
    ) -> Result<Expr> {
        let expr = self.sqlexpr_to_expr(context, sqlexpr, schema)?;
        let list_exprs = list
            .into_iter()
            .map(|it| self.sqlexpr_to_expr(context, it, schema))
            .collect::<Result<Vec<_>>>()?;
        Ok(Expr::InList(InList::new(expr, list_exprs, negated)))
    }

    fn parse_like_to_expr(
        &self,
        context: &mut Context,
        sqlexpr: SQLExpr,
        pattern: SQLExpr,
        negated: bool,
        schema: &LogicalSchema,
        case_insensitive: bool,
    ) -> Result<Expr> {
        let pattern = self.sqlexpr_to_expr(context, pattern, schema)?;
        let expr = self.sqlexpr_to_expr(context, sqlexpr, schema)?;
        Ok(Expr::Like(Like::new(expr, pattern, negated, case_insensitive)))
    }

    fn parse_unaryop_to_expr(
        &self,
        context: &mut Context,
        op: UnaryOperator,
        sqlexpr: SQLExpr,
        schema: &LogicalSchema,
    ) -> Result<Expr> {
        match op {
            UnaryOperator::Plus => {
                let operand = self.sqlexpr_to_expr(context, sqlexpr, schema)?;
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
                    let operand = self.sqlexpr_to_expr(context, sqlexpr, schema)?;
                    let (datatype, _) = operand.datatype_and_nullable(schema)?;
                    match datatype {
                        DataType::Integer | DataType::Float => {
                            Ok(Expr::Negative(Box::new(operand)))
                        }
                        _ => self.semantic_err(format!("- cannot be used with {datatype}")),
                    }
                }
            },
            UnaryOperator::Not => {
                Ok(Expr::Not(Box::new(self.sqlexpr_to_expr(context, sqlexpr, schema)?)))
            }
        }
    }

    fn parse_compound_ident_to_expr(
        &self,
        context: &Context,
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
        let ident = idents.remove(1);
        let ident = self.ident_normalizer.normalize(ident);

        if let Some((field, Some(_))) =
            schema.field_reference_by_qname(&Some(TableReference::new(&table)), &ident)
        {
            return Ok(Expr::FieldReference(field));
        }

        // check the outer query schema
        if let Some(outer) = &context.outer_from_schema {
            if let Some((field, Some(f))) =
                outer.field_reference_by_qname(&Some(TableReference::new(&table)), &ident)
            {
                return Ok(Expr::OuterReferenceColumn(f.datatype.clone(), field));
            }
        }

        Ok(Expr::FieldReference(FieldReference::new(ident, Some(TableReference::new(&table)))))
    }

    fn parse_identifier_to_expr(
        &self,
        context: &Context,
        ident: Ident,
        schema: &LogicalSchema,
    ) -> Result<Expr> {
        let ident = self.ident_normalizer.normalize(ident);
        if let Some((field, Some(_))) = schema.field_reference_by_name(&ident) {
            return Ok(Expr::FieldReference(field));
        }
        // check the outer query schema
        if let Some(outer) = &context.outer_from_schema {
            if let Some((field, Some(col))) = outer.field_reference_by_name(&ident) {
                return Ok(Expr::OuterReferenceColumn(col.datatype.clone(), field));
            }
        }
        Ok(Expr::FieldReference(FieldReference::new_unqualified(ident)))
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
        Err(Error::parse(msg))
    }
}

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
