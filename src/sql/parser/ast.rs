use std::fmt::Formatter;

use crate::catalog::r#type::DataType;
use crate::sql::parser::display_utils::{
    display_comma_separated, DisplayCommaSeparated, Indent, NewLine,
};
use crate::sql::parser::lexer::Token;

pub enum Statement {
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
    AlterTable { table_name: Ident, if_exists: bool, operations: Vec<AlterTableOperation> },
}

impl std::fmt::Display for Statement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
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
                Indent(DisplayCommaSeparated(operations)).fmt(f)?;
                Ok(())
            }
        }
    }
}

/// An identifier, decomposed into its value or character data and the quote style.
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

    pub fn from_ident_token(tok: Token) -> Ident {
        if let Token::Ident(value, double_quoted) = tok {
            Ident { value, double_quoted }
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

/// CREATE TABLE statement.
pub struct CreateTable {
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
        Indent(DisplayCommaSeparated(&self.columns)).fmt(f)?;
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
            expr.fmt(f)?;
        }
        if !self.nullable {
            write!(f, " NOT NULL")?
        }
        Ok(())
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

/// The `SELECT` query expression.
pub struct Query {}

impl std::fmt::Display for Query {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

/// A name of a table, view, custom type, etc., possibly multi-part, i.e. db.schema.obj
struct ObjectName(Vec<ObjectNamePart>);

/// A single part of an ObjectName
enum ObjectNamePart {
    Identifier(String),
}

/// An SQL expression.
pub enum Expr {
    /// A literal value, such as string, number or NULL
    Value(Value),
    /// Identifier e.g. table name or column name
    Identifier(String),
    /// Multi-part identifier, e.g. `table_alias.column` or `schema.table.col`
    CompoundIdentifier(Vec<String>),
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
    /// A parenthesized subquery `(SELECT ...)`, used in expression like
    /// `SELECT (subquery) AS x` or `WHERE (subquery) = x`
    Subquery(Box<Query>),
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
            Expr::Identifier(s) => write!(f, "{s}"),
            Expr::CompoundIdentifier(s) => write!(f, "{}", s.join(".")),
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
            Expr::Subquery(q) => write!(f, "{q}"),
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
pub struct Function {
    pub name: String,
    /// The arguments to the function, including any options specified within the
    /// delimiting parentheses.
    pub args: Vec<FunctionArg>,
}

impl std::fmt::Display for Function {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}({})", self.name, display_comma_separated(&self.args))
    }
}

pub enum FunctionArg {
    Value(Value),
    /// An unqualified `*`
    Asterisk,
    /// Identifier e.g. table name or column name
    Identifier(String),
    /// Multi-part identifier, e.g. `table_alias.column` or `schema.table.col`
    CompoundIdentifier(Vec<String>),
    Function(Function),
}

impl std::fmt::Display for FunctionArg {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            FunctionArg::Value(v) => write!(f, "{v}"),
            FunctionArg::Asterisk => write!(f, "*"),
            FunctionArg::Identifier(s) => write!(f, "{s}"),
            FunctionArg::CompoundIdentifier(s) => write!(f, "{}", s.join(".")),
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
