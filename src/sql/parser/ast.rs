use std::fmt::Debug;
use std::fmt::Formatter;

use sqlparser_derive::Visit;
use sqlparser_derive::VisitMut;

use crate::sql::parser::display_utils::display_comma_separated;
use crate::sql::parser::display_utils::display_dot_separated;
use crate::sql::parser::display_utils::display_inline_dot_separated;
use crate::sql::parser::display_utils::display_space_separated;
use crate::sql::parser::display_utils::Indent;
use crate::sql::parser::display_utils::NewLine;
use crate::sql::parser::display_utils::SpaceOrNewline;
use crate::sql::parser::lexer::Token;

#[derive(Visit, VisitMut)]
#[visit(with = "visit_statement")]
pub enum Statement {
    /// ```sql
    ///  BEGIN TRANSACTION
    /// ```
    Begin { read_only: bool, as_of: Option<String> },
    ///```sql
    ///  COMMIT
    /// ```
    Commit,
    ///```sql
    ///  ROLLBACK
    /// ```
    Rollback,
    /// ```sql
    /// CREATE TABLE
    /// ```
    CreateTable(CreateTable),
    /// ```sql
    /// `CREATE INDEX`
    /// ```
    CreateIndex(CreateIndex),
    /// ```sql
    /// DROP [TABLE, INDEX, ...]
    /// ```
    Drop {
        /// The type of the object to drop: TABLE, INDEX, etc.
        object_type: ObjectType,
        if_exists: bool,
        object_name: Ident,
    },
    /// ```sql
    /// ALTER TABLE
    /// ```
    AlterTable {
        #[visit(with = "visit_relation")]
        table_name: Ident,
        if_exists: bool,
        operations: Vec<AlterTableOperation>,
    },
    /// ```sql
    /// SELECT
    /// ```
    Select { query: Box<Query> },
    ///```sql
    ///  INSERT INTO
    /// ```
    Insert(Insert),
    /// ```sql
    /// UPDATE
    /// ```
    Update(Update),
    /// ```sql
    /// DELETE
    /// ```
    ///
    Delete {
        #[visit(with = "visit_relation")]
        table: Ident,
        selection: Option<Expr>,
    },
    /// ```sql
    /// EXPLAIN <statement>
    /// ```
    Explain {
        /// Carry out the command and show actual run times and other statistics.
        analyze: bool,
        /// Display additional information regarding the plan.
        verbose: bool,
        /// A SQL query that specifies what to explain
        statement: Box<Statement>,
    },
}

impl std::fmt::Display for Statement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Statement::Begin { read_only, as_of } => {
                write!(
                    f,
                    "BEGIN{mode}{s}",
                    mode = if *read_only { " READ ONLY" } else { "" },
                    s = if let Some(n) = as_of { format!(" AS OF {}", n) } else { "".to_string() }
                )
            }
            Statement::Commit => write!(f, "COMMIT"),
            Statement::Rollback => write!(f, "ROLLBACK"),
            Statement::CreateTable(create_table) => create_table.fmt(f),
            Statement::CreateIndex(create_index) => create_index.fmt(f),
            Statement::Drop { object_type, if_exists, object_name } => {
                write!(
                    f,
                    "DROP {object_type} {s_if_exists}{object_name}",
                    s_if_exists = if *if_exists { "IF EXISTS " } else { "" }
                )
            }
            Statement::AlterTable { table_name, if_exists, operations } => {
                write!(
                    f,
                    "ALTER TABLE {s}{table_name} ",
                    s = if *if_exists { "IF EXISTS " } else { "" }
                )?;
                NewLine.fmt(f)?;
                Indent(display_comma_separated(operations)).fmt(f)?;
                Ok(())
            }
            Statement::Select { query } => query.fmt(f),
            Statement::Insert(insert) => insert.fmt(f),
            Statement::Update(update) => update.fmt(f),
            Statement::Delete { table, selection } => {
                write!(f, "DELETE FROM {}", table)?;
                if let Some(expr) = selection {
                    SpaceOrNewline.fmt(f)?;
                    write!(f, "WHERE")?;

                    SpaceOrNewline.fmt(f)?;
                    expr.fmt(f)?;
                };
                Ok(())
            }
            Statement::Explain { analyze, verbose, statement } => {
                write!(f, "EXPLAIN")?;
                if *analyze {
                    write!(f, " ANALYZE")?;
                }
                if *verbose {
                    write!(f, " VERBOSE")?;
                }
                SpaceOrNewline.fmt(f)?;
                statement.fmt(f)?;
                Ok(())
            }
        }
    }
}

/// An identifier, decomposed into its value or character data and the quote style.
#[derive(Debug, Clone, Visit, VisitMut)]
pub struct Ident {
    /// The value of the identifier without quotes.
    pub value: String,
    /// Whether the identifier is double-quoted.
    pub double_quoted: bool,
}
impl Ident {
    pub fn new(value: &str) -> Ident {
        Ident { value: value.to_string(), double_quoted: false }
    }

    pub fn from_ident_token(tok: &Token) -> Ident {
        if let Token::Ident(value, double_quoted) = tok {
            Ident { value: value.to_string(), double_quoted: *double_quoted }
        } else {
            panic!("Expected Token::Ident, got {:?}", tok)
        }
    }
}

impl std::fmt::Display for Ident {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.double_quoted {
            write!(f, "\"{}\"", self.value)
        } else {
            f.write_str(&self.value)
        }
    }
}

#[derive(Visit, VisitMut)]
pub struct Update {
    /// TABLE
    #[visit(with = "visit_relation")]
    pub table: Ident,
    /// Column assignments
    pub assignments: Vec<Assignment>,
    /// WHERE
    pub selection: Option<Expr>,
}

impl std::fmt::Display for Update {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "UPDATE {}", self.table)?;

        SpaceOrNewline.fmt(f)?;
        write!(f, "SET")?;

        SpaceOrNewline.fmt(f)?;
        Indent(display_comma_separated(&self.assignments)).fmt(f)?;

        if let Some(expr) = &self.selection {
            SpaceOrNewline.fmt(f)?;
            write!(f, "WHERE")?;

            SpaceOrNewline.fmt(f)?;
            expr.fmt(f)?;
        };

        Ok(())
    }
}

/// SQL assignment `foo = expr` as used in SQLUpdate
#[derive(Visit, VisitMut)]
pub struct Assignment {
    pub column: Ident,
    pub value: Expr,
}

impl std::fmt::Display for Assignment {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} = {}", self.column, self.value)
    }
}

/// INSERT statement.
#[derive(Visit, VisitMut)]
pub struct Insert {
    /// TABLE
    #[visit(with = "visit_relation")]
    pub table: Ident,
    /// COLUMNS
    pub columns: Vec<Ident>,
    pub source: InsertSource,
}

impl std::fmt::Display for Insert {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "INERT INTO {} (", self.table)?;

        NewLine.fmt(f)?;
        Indent(display_comma_separated(&self.columns)).fmt(f)?;

        NewLine.fmt(f)?;
        write!(f, ")")?;

        SpaceOrNewline.fmt(f)?;
        self.source.fmt(f)
    }
}

#[derive(Visit, VisitMut)]
pub enum InsertSource {
    Select(Box<Query>),
    Values(Values),
}

impl std::fmt::Display for InsertSource {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            InsertSource::Select(q) => q.fmt(f)?,
            InsertSource::Values(values) => {
                f.write_str("VALUES")?;
                values.fmt(f)?;
            }
        };
        Ok(())
    }
}

#[derive(Visit, VisitMut)]
pub struct Values {
    pub rows: Vec<Vec<Expr>>,
}

impl std::fmt::Display for Values {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut delim = "";
        for row in &self.rows {
            f.write_str(delim)?;
            delim = ",";
            SpaceOrNewline.fmt(f)?;
            Indent(format_args!("({})", display_comma_separated(row))).fmt(f)?;
        }
        Ok(())
    }
}

/// The `SELECT` query expression.
#[derive(Visit, VisitMut)]
#[visit(with = "visit_query")]
pub struct Query {
    /// projection expressions
    pub projection: Vec<SelectItem>,
    /// FROM
    pub from: TableWithJoins,
    /// WHERE
    pub selection: Option<Expr>,
    /// GROUP BY
    pub group_by: Vec<Expr>,
    /// ORDER BY
    pub order_by: Vec<OrderByExpr>,
    /// `LIMIT ... OFFSET ... | LIMIT <offset>, <limit>`
    pub limit_clause: Option<LimitClause>,
}

impl std::fmt::Display for Query {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SELECT")?;

        SpaceOrNewline.fmt(f)?;
        Indent(display_comma_separated(&self.projection)).fmt(f)?;

        SpaceOrNewline.fmt(f)?;
        write!(f, "FROM")?;

        SpaceOrNewline.fmt(f)?;
        Indent(&self.from).fmt(f)?;

        if let Some(s) = &self.selection {
            SpaceOrNewline.fmt(f)?;
            write!(f, "WHERE")?;

            SpaceOrNewline.fmt(f)?;
            Indent(s).fmt(f)?;
        }

        if !self.group_by.is_empty() {
            SpaceOrNewline.fmt(f)?;
            write!(f, "GROUP BY")?;

            SpaceOrNewline.fmt(f)?;
            Indent(display_comma_separated(&self.group_by)).fmt(f)?;
        }

        if !self.order_by.is_empty() {
            SpaceOrNewline.fmt(f)?;
            write!(f, "ORDER BY")?;

            SpaceOrNewline.fmt(f)?;
            Indent(display_comma_separated(&self.order_by)).fmt(f)?;
        }

        if let Some(c) = &self.limit_clause {
            SpaceOrNewline.fmt(f)?;
            c.fmt(f)?;
        }

        Ok(())
    }
}

/// An `ORDER BY` expression
#[derive(Visit, VisitMut)]
pub struct OrderByExpr {
    pub expr: Expr,
    /// Optional `ASC` or `DESC`
    pub desc: Option<bool>,
}

impl std::fmt::Display for OrderByExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.expr.fmt(f)?;
        write!(
            f,
            " {s}",
            s = if let Some(desc) = self.desc {
                if desc {
                    "DESC"
                } else {
                    "ASC"
                }
            } else {
                "ASC"
            }
        )
    }
}

/// `LIMIT ... OFFSET ... | LIMIT <offset>, <limit>`
#[derive(Visit, VisitMut)]
pub struct LimitClause {
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

impl std::fmt::Display for LimitClause {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(limit) = self.limit {
            write!(f, "LIMIT {}", limit)?;
        }
        if let Some(offset) = self.offset {
            write!(f, " OFFSET {}", offset)?;
        }
        Ok(())
    }
}

#[derive(Visit, VisitMut)]
pub enum WildcardExpr {
    /// An expression, followed by a wildcard expansion.
    /// e.g. `alias.*``, the idents here represent the
    /// prefix without the `*`.
    QualifiedWildcard(Vec<Ident>),
    /// An unqualified `*`
    Wildcard,
}

impl std::fmt::Display for WildcardExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            WildcardExpr::QualifiedWildcard(idents) => {
                write!(f, "{}.*", display_dot_separated(idents))
            }
            WildcardExpr::Wildcard => write!(f, "*"),
        }
    }
}
/// One item of the comma-separated list following `SELECT`
#[derive(Visit, VisitMut)]
pub enum SelectItem {
    /// Any expression, not followed by `[ AS ] alias`
    UnnamedExpr(Expr),
    /// An expression, followed by `[ AS ] alias`
    ExprWithAlias {
        expr: Expr,
        alias: Ident,
    },
    WildcardExpr(WildcardExpr),
}

impl std::fmt::Display for SelectItem {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SelectItem::UnnamedExpr(expr) => write!(f, "{expr}"),
            SelectItem::ExprWithAlias { expr, alias } => write!(f, "{expr} AS {alias}"),
            SelectItem::WildcardExpr(w) => w.fmt(f),
        }
    }
}

#[derive(Visit, VisitMut)]
pub struct TableWithJoins {
    pub relation: TableFactor,
    pub joins: Vec<Join>,
}

impl std::fmt::Display for TableWithJoins {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.relation)?;
        if !self.joins.is_empty() {
            SpaceOrNewline.fmt(f)?;
            display_space_separated(&self.joins).fmt(f)?
        };
        Ok(())
    }
}

#[derive(Visit, VisitMut)]
pub struct Join {
    pub join_operator: JoinOperator,
    pub relation: TableFactor,
}

impl std::fmt::Display for Join {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let (join_type, jc) = match &self.join_operator {
            JoinOperator::Join(c) => ("", c),
            JoinOperator::Inner(c) => ("INNER ", c),
            JoinOperator::Left(c) => ("LEFT ", c),
            JoinOperator::LeftOuter(c) => ("LEFT OUTER ", c),
            JoinOperator::Right(c) => ("RIGHT ", c),
            JoinOperator::RightOuter(c) => ("RIGHT OUTER ", c),
            JoinOperator::Full(c) => ("FULL ", c),
            JoinOperator::FullOuter(c) => ("FULL OUTER ", c),
        };
        write!(f, "{}JOIN {} {}", join_type, self.relation, jc)
    }
}

#[derive(Visit, VisitMut)]
pub enum JoinOperator {
    Join(JoinConstraint),
    Inner(JoinConstraint),
    Left(JoinConstraint),
    LeftOuter(JoinConstraint),
    Right(JoinConstraint),
    RightOuter(JoinConstraint),
    Full(JoinConstraint),
    FullOuter(JoinConstraint),
}

#[derive(Visit, VisitMut)]
pub enum JoinConstraint {
    On(Expr),
}

impl std::fmt::Display for JoinConstraint {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            JoinConstraint::On(expr) => write!(f, "ON {expr}"),
        }
    }
}

#[derive(Visit, VisitMut)]
#[visit(with = "visit_table_factor")]
pub enum TableFactor {
    Table {
        #[visit(with = "visit_relation")]
        name: Ident,
        alias: Option<String>,
    },
    Derived {
        subquery: Box<Query>,
        alias: Option<String>,
    },
}

impl std::fmt::Display for TableFactor {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TableFactor::Table { name, alias } => {
                write!(
                    f,
                    "{name}{s}",
                    s = if let Some(s) = alias { format!(" {s}") } else { "".to_string() }
                )
            }
            TableFactor::Derived { subquery, alias } => {
                write!(
                    f,
                    "({subquery}){s}",
                    s = if let Some(s) = alias { format!(" {s}") } else { "".to_string() }
                )
            }
        }
    }
}

/// CREATE TABLE statement.
#[derive(Visit, VisitMut)]
pub struct CreateTable {
    #[visit(with = "visit_relation")]
    pub name: Ident,
    pub columns: Vec<Column>,
    pub if_not_exists: bool,
}

impl std::fmt::Display for CreateTable {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CREATE TABLE {if_not_exists}{name}",
            if_not_exists = if self.if_not_exists { "IF NOT EXISTS " } else { "" },
            name = self.name
        )?;
        if self.columns.is_empty() {
            return Ok(());
        }
        f.write_str(" (")?;
        NewLine.fmt(f)?;
        Indent(display_comma_separated(&self.columns)).fmt(f)?;
        NewLine.fmt(f)?;
        f.write_str(")")
    }
}

/// SQL column definition
#[derive(Visit, VisitMut)]
pub struct Column {
    pub name: Ident,
    pub datatype: DataType,
    pub primary_key: bool,
    pub nullable: bool,
    pub unique: bool,
    pub default: Option<Expr>,
}

impl Default for Column {
    fn default() -> Self {
        Column {
            name: Ident::new(""),
            datatype: DataType::Integer,
            primary_key: false,
            nullable: true,
            unique: false,
            default: None,
        }
    }
}

impl std::fmt::Display for Column {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.name, self.datatype)?;
        if self.primary_key {
            write!(f, " PRIMARY KEY")?;
        }
        if self.unique {
            write!(f, " UNIQUE")?;
        }
        if let Some(expr) = &self.default {
            write!(f, " DEFAULT ")?;
            expr.fmt(f)?;
        }
        if !self.nullable {
            write!(f, " NOT NULL")?
        }
        Ok(())
    }
}

#[derive(Visit, VisitMut)]
pub enum DataType {
    Boolean,
    Integer,
    Float,
    String,
}

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.write_str(match self {
            Self::Boolean => "BOOLEAN",
            Self::Integer => "INTEGER",
            Self::Float => "FLOAT",
            Self::String => "TEXT",
        })
    }
}

#[derive(Visit, VisitMut)]
pub struct CreateIndex {
    pub name: Ident,
    #[visit(with = "visit_relation")]
    pub table_name: Ident,
    pub column_names: Vec<Ident>,
    pub unique: bool,
    pub if_not_exists: bool,
}

impl std::fmt::Display for CreateIndex {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CREATE {unique}INDEX {if_not_exists}{name} ON {table_name}",
            unique = if self.unique { "UNIQUE " } else { "" },
            if_not_exists = if self.if_not_exists { "IF NOT EXISTS " } else { "" },
            name = self.name,
            table_name = self.table_name
        )?;
        if self.column_names.is_empty() {
            return Ok(());
        }
        write!(f, "(")?;
        write!(f, "{}", display_comma_separated(&self.column_names))?;
        write!(f, ")")?;
        Ok(())
    }
}

#[derive(Visit, VisitMut)]
pub enum ObjectType {
    Table,
    Index,
}

impl std::fmt::Display for ObjectType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ObjectType::Table => write!(f, "TABLE"),
            ObjectType::Index => write!(f, "INDEX"),
        }
    }
}

#[derive(Visit, VisitMut)]
pub enum AlterTableOperation {
    AddColumn { if_not_exists: bool, column: Column },
    DropColumn { if_exists: bool, column_name: Ident },
}

impl std::fmt::Display for AlterTableOperation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            AlterTableOperation::AddColumn { if_not_exists, column } => {
                write!(
                    f,
                    "ADD COLUMN {s}{column}",
                    s = if *if_not_exists { "IF NOT EXISTS " } else { "" }
                )
            }
            AlterTableOperation::DropColumn { if_exists, column_name } => {
                write!(
                    f,
                    "DROP COLUMN {s}{column_name}",
                    s = if *if_exists { "IF EXISTS " } else { "" }
                )
            }
        }
    }
}

/// An SQL expression.
#[derive(Visit, VisitMut)]
#[visit(with = "visit_expr")]
pub enum Expr {
    /// A literal value, such as string, number or NULL
    Value(Value),
    /// Identifier e.g. table name or column name
    Identifier(Ident),
    /// Multi-part identifier, e.g. `table_alias.column`
    CompoundIdentifier(Vec<Ident>),
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOperator,
        right: Box<Expr>,
    },
    UnaryOp {
        op: UnaryOperator,
        expr: Box<Expr>,
    },
    /// An exists expression `[ NOT ] EXISTS(SELECT ...)`, used in expressions like
    /// `WHERE [ NOT ] EXISTS (SELECT ...)`.
    Exists {
        subquery: Box<Query>,
        negated: bool,
    },
    /// Nested expression e.g. `(foo > bar)` or `(1)`
    Nested(Box<Expr>),
    /// ROW / TUPLE a single value, such as `SELECT (1, 2)`
    Tuple(Vec<Expr>),
    /// `IS NULL` operator
    IsNull(Box<Expr>),
    /// `IS NOT NULL` operator
    IsNotNull(Box<Expr>),
    /// `IS TRUE` operator
    IsTrue(Box<Expr>),
    /// `IS NOT TRUE` operator
    IsNotTrue(Box<Expr>),
    /// `IS FALSE` operator
    IsFalse(Box<Expr>),
    /// `IS NOT FALSE` operator
    IsNotFalse(Box<Expr>),
    /// `[NOT] LIKE <pattern>`
    Like {
        negated: bool,
        expr: Box<Expr>,
        pattern: Box<Expr>,
    },
    /// `ILIKE` (case-insensitive `LIKE`)
    ILike {
        negated: bool,
        expr: Box<Expr>,
        pattern: Box<Expr>,
    },
    /// A parenthesized scalar subquery `(SELECT ...)`, used in expression like
    /// `SELECT (subquery) AS x` or `WHERE (subquery) = x`.
    /// A scalar subquery must return exactly one column and at most one row.
    ScalarSubquery(Box<Query>),
    /// `[ NOT ] IN (val1, val2, ...)`
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    /// `[ NOT ] IN (SELECT ...)`
    InSubquery {
        expr: Box<Expr>,
        subquery: Box<Query>,
        negated: bool,
    },
    /// Scalar function call e.g. `LEFT(foo, 5)`
    Function(Function),
}

impl std::fmt::Display for Expr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Expr::Value(v) => write!(f, "{v}"),
            Expr::Identifier(ident) => write!(f, "{ident}"),
            Expr::CompoundIdentifier(s) => display_inline_dot_separated(s).fmt(f),
            Expr::BinaryOp { left, op, right } => write!(f, "{left} {op} {right}"),
            Expr::UnaryOp { op, expr } => write!(f, "{op}{expr}"),
            Expr::Exists { subquery, negated } => {
                write!(f, "{}EXISTS ({subquery})", if *negated { "NOT " } else { "" })
            }
            Expr::Nested(expr) => write!(f, "({expr})"),
            Expr::Tuple(exprs) => write!(f, "({})", display_comma_separated(exprs)),
            Expr::IsNull(expr) => write!(f, "({expr}) IS NULL"),
            Expr::IsNotNull(expr) => write!(f, "({expr}) IS NOT NULL"),
            Expr::IsTrue(expr) => write!(f, "({expr}) IS TRUE"),
            Expr::IsNotTrue(expr) => write!(f, "({expr}) IS NOT TRUE"),
            Expr::IsFalse(expr) => write!(f, "({expr}) IS FALSE"),
            Expr::IsNotFalse(expr) => write!(f, "({expr}) IS NOT FALSE"),
            Expr::Like { negated, expr, pattern } => {
                write!(f, "{} {}LIKE {}", expr, if *negated { "NOT " } else { "" }, pattern)
            }
            Expr::ILike { negated, expr, pattern } => {
                write!(f, "{} {}ILIKE {}", expr, if *negated { "NOT " } else { "" }, pattern)
            }
            Expr::ScalarSubquery(q) => write!(f, "{q}"),
            Expr::InList { expr, list, negated } => write!(
                f,
                "{} {}IN ({})",
                expr,
                if *negated { "NOT " } else { "" },
                display_comma_separated(list)
            ),
            Expr::InSubquery { expr, subquery, negated } => {
                write!(f, "{} {}IN ({})", expr, if *negated { "NOT " } else { "" }, subquery)
            }
            Expr::Function(func) => write!(f, "{func}"),
        }
    }
}

#[derive(Debug, Visit, VisitMut)]
#[visit(with = "visit_value")]
pub enum Value {
    Number(String),
    String(String),
    Null,
    Boolean(bool),
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Number(s) => write!(f, "{s}"),
            Value::String(s) => write!(f, "'{s}'"),
            Value::Null => write!(f, "NULL"),
            Value::Boolean(b) => write!(f, "{b}"),
        }
    }
}

#[derive(Debug, Visit, VisitMut)]
pub enum BinaryOperator {
    /// Plus, e.g. `a + b`
    Plus,
    /// Minus, e.g. `a - b`
    Minus,
    /// Multiply, e.g. `a * b`
    Multiply,
    /// Divide, e.g. `a / b`
    Divide,
    /// Modulo, e.g. `a % b`
    Modulo,
    /// Equal, e.g. `a = b`
    Eq,
    /// Not equal, e.g. `a != b`
    NotEq,
    /// Greater than, e.g. `a > b`
    Gt,
    /// Greater equal, e.g. `a >= b`
    GtEq,
    /// Greater equal, e.g. `a >= b`
    Lt,
    /// Less equal, e.g. `a <= b`
    LtEq,
    /// And, e.g. `a AND b`
    And,
    /// Or, e.g. `a OR b`
    Or,
}

impl std::fmt::Display for BinaryOperator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            BinaryOperator::Plus => f.write_str("+"),
            BinaryOperator::Minus => f.write_str("-"),
            BinaryOperator::Multiply => f.write_str("*"),
            BinaryOperator::Divide => f.write_str("/"),
            BinaryOperator::Modulo => f.write_str("%"),
            BinaryOperator::Eq => f.write_str("="),
            BinaryOperator::NotEq => f.write_str("!="),
            BinaryOperator::Gt => f.write_str(">"),
            BinaryOperator::GtEq => f.write_str(">="),
            BinaryOperator::Lt => f.write_str("<"),
            BinaryOperator::LtEq => f.write_str("<="),
            BinaryOperator::And => f.write_str("AND"),
            BinaryOperator::Or => f.write_str("OR"),
        }
    }
}

#[derive(Debug, Visit, VisitMut)]
pub enum UnaryOperator {
    /// Plus, e.g. `+9`
    Plus,
    /// Minus, e.g. `-9`
    Minus,
    /// Not, e.g. `NOT TRUE`
    Not,
}

impl std::fmt::Display for UnaryOperator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            UnaryOperator::Plus => f.write_str("+"),
            UnaryOperator::Minus => f.write_str("-"),
            UnaryOperator::Not => f.write_str("NOT "),
        }
    }
}

/// A function call
#[derive(Debug, Visit, VisitMut)]
pub struct Function {
    pub name: Ident,
    /// The arguments to the function, including any options specified within the
    /// delimiting parentheses.
    pub args: Vec<FunctionArg>,
}

impl std::fmt::Display for Function {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}({})", self.name, display_comma_separated(&self.args))
    }
}

#[derive(Debug, Visit, VisitMut)]
pub enum FunctionArg {
    Value(Value),
    /// An unqualified `*`
    Asterisk,
    /// Identifier e.g. table name or column name
    /// TODO: Support multi-part identifier, e.g. `table_alias.column` or `schema.table.col`
    Identifier(Ident),
    Function(Function),
}

impl std::fmt::Display for FunctionArg {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            FunctionArg::Value(v) => write!(f, "{v}"),
            FunctionArg::Asterisk => write!(f, "*"),
            FunctionArg::Identifier(s) => write!(f, "{s}"),
            FunctionArg::Function(func) => write!(f, "{func}"),
        }
    }
}

/// This represents the operators for which precedence must be defined
pub enum Precedence {
    UnaryOp,
    MulDivModOp,
    PlusMinus,
    Eq,
    Like,
    Is,
    And,
    Or,
}
