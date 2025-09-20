use std::fmt::Debug;
use std::fmt::Formatter;

use crate::sql::parser::display_utils::display_comma_separated;
use crate::sql::parser::display_utils::display_dot_separated;
use crate::sql::parser::display_utils::display_inline_dot_separated;
use crate::sql::parser::display_utils::display_space_separated;
use crate::sql::parser::display_utils::Indent;
use crate::sql::parser::display_utils::NewLine;
use crate::sql::parser::display_utils::SpaceOrNewline;
use crate::sql::parser::lexer::Token;

pub enum Statement {
    /// ```sql
    ///  BEGIN TRANSACTION
    /// ```
    Begin { read_only: bool, as_of: Option<u64> },
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
    /// DROP TABLE
    /// ```
    DropTable { if_exists: bool, table_name: Ident },
    /// ```sql
    /// DROP INDEX
    /// ```
    DropIndex { if_exists: bool, table_name: Ident, index_name: Ident },
    /// ```sql
    /// ALTER TABLE
    /// ```
    AlterTable { table_name: Ident, if_exists: bool, operations: Vec<AlterTableOperation> },
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
    Delete { table: Ident, selection: Option<Expr> },
    /// ```sql
    /// EXPLAIN <statement>
    /// ```
    Explain {
        /// Display physical plan
        physical: bool,
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
            Statement::DropTable { table_name, if_exists } => {
                write!(
                    f,
                    "DROP TABLE {s_if_exists}{table_name}",
                    s_if_exists = if *if_exists { "IF EXISTS " } else { "" }
                )
            }
            Statement::DropIndex { index_name, table_name, if_exists } => {
                write!(
                    f,
                    "DROP INDEX {s_if_exists}{index_name} ON {table_name}",
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
            Statement::Select { query } => std::fmt::Display::fmt(query, f),
            Statement::Insert(insert) => insert.fmt(f),
            Statement::Update(update) => update.fmt(f),
            Statement::Delete { table, selection } => {
                write!(f, "DELETE FROM {}", table)?;
                if let Some(expr) = selection {
                    SpaceOrNewline.fmt(f)?;
                    write!(f, "WHERE")?;

                    SpaceOrNewline.fmt(f)?;
                    std::fmt::Display::fmt(expr, f)?;
                };
                Ok(())
            }
            Statement::Explain { physical: analyze, verbose, statement } => {
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
#[derive(Debug, Clone)]
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

pub struct Update {
    /// TABLE
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
            std::fmt::Display::fmt(expr, f)?;
        };

        Ok(())
    }
}

/// SQL assignment `foo = expr` as used in SQLUpdate
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
pub struct Insert {
    /// TABLE
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

pub enum InsertSource {
    Select(Box<Query>),
    Values(Values),
}

impl std::fmt::Display for InsertSource {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            InsertSource::Select(q) => std::fmt::Display::fmt(q, f)?,
            InsertSource::Values(values) => {
                f.write_str("VALUES")?;
                values.fmt(f)?;
            }
        };
        Ok(())
    }
}

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
#[derive(Debug)]
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
            std::fmt::Display::fmt(c, f)?;
        }

        Ok(())
    }
}

/// An `ORDER BY` expression
#[derive(Debug)]
pub struct OrderByExpr {
    pub expr: Expr,
    /// Optional `ASC` or `DESC`
    pub desc: Option<bool>,
}

impl std::fmt::Display for OrderByExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.expr, f)?;
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
#[derive(Debug)]
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

#[derive(Debug)]
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
#[derive(Debug)]
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
            SelectItem::WildcardExpr(w) => std::fmt::Display::fmt(w, f),
        }
    }
}

#[derive(Debug)]
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

#[derive(Debug)]
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

#[derive(Debug)]
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

#[derive(Debug)]
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

#[derive(Debug)]
pub enum TableFactor {
    Table { name: Ident, alias: Option<String> },
    // TODO: does not support lateral derived subquery as table factor yet.
    //  e.g.,
    //  ```sql
    //  SELECT *
    //  FROM employees e
    //  JOIN LATERAL (
    //      SELECT *
    //      FROM projects p
    //      WHERE p.manager_id = e.id   -- ✅ OK: e.id visible
    //  ) AS sub ON true;
    //  ```
    Derived { subquery: Box<Query>, alias: Option<String> },
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
pub struct CreateTable {
    pub name: Ident,
    pub columns: Vec<Column>,
    pub table_constraints: Vec<TableConstraint>,
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
        if !self.table_constraints.is_empty() {
            f.write_str(",")?;
            SpaceOrNewline.fmt(f)?;
        }
        Indent(display_comma_separated(&self.table_constraints)).fmt(f)?;
        NewLine.fmt(f)?;
        f.write_str(")")
    }
}

/// SQL column definition
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
            std::fmt::Display::fmt(expr, f)?;
        }
        if !self.nullable {
            write!(f, " NOT NULL")?
        }
        Ok(())
    }
}

pub enum TableConstraint {
    /// Identifiers of the columns that form the primary key.
    PrimaryKey { columns: Vec<Ident> },
}

impl std::fmt::Display for TableConstraint {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TableConstraint::PrimaryKey { columns } => {
                write!(f, "PRIMARY KEY({})", display_comma_separated(columns))
            }
        }
    }
}

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

pub struct CreateIndex {
    pub name: Ident,
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
#[derive(Debug)]
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
    /// Scalar/Aggregate function.
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

#[derive(Debug)]
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

#[derive(Debug)]
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

#[derive(Debug)]
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
#[derive(Debug)]
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

#[derive(Debug)]
pub enum FunctionArg {
    /// The arg expr, Support Expr::Value and Expr::Identifier only for now.
    Expr(Expr),
    /// An unqualified `*`
    Asterisk,
}

impl std::fmt::Display for FunctionArg {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            FunctionArg::Expr(expr) => write!(f, "{}", expr),
            FunctionArg::Asterisk => write!(f, "*"),
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
