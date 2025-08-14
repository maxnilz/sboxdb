use std::fmt::Display;
use std::fmt::Formatter;
use std::fmt::Write;
use std::sync::Arc;

use crate::apply_each;
use crate::catalog::r#type::DataType;
use crate::catalog::r#type::Value;
use crate::error::Error;
use crate::error::Result;
use crate::sql::plan::plan::Plan;
use crate::sql::plan::schema::Field;
use crate::sql::plan::schema::FieldBuilder;
use crate::sql::plan::schema::FieldRef;
use crate::sql::plan::schema::FieldReference;
use crate::sql::plan::schema::LogicalSchema;
use crate::sql::plan::schema::TableReference;
use crate::sql::plan::visitor::TreeNode;
use crate::sql::plan::visitor::VisitRecursion;

/// Represents logical expressions such as `A + 1`.
///
/// For example the expression `A + 1` will be represented as
///
///```text
///  BinaryExpr {
///    left: Expr::FieldReference(FieldReference{'A', None}),
///    op: Operator::Plus,
///    right: Expr::Value(Value::Integer(1))
/// }
#[derive(Clone, Debug)]
pub enum Expr {
    Alias(Alias),
    Value(Value),
    FieldReference(FieldReference),
    /// A placeholder which hold a reference to a qualified field
    /// in the outer query, used for correlated sub queries.
    OuterFieldReference(DataType, FieldReference),
    Not(Box<Expr>),
    IsNull(Box<Expr>),
    IsNotNull(Box<Expr>),
    IsTrue(Box<Expr>),
    IsNotTrue(Box<Expr>),
    IsFalse(Box<Expr>),
    IsNotFalse(Box<Expr>),
    /// Negative against on numeric expr
    Negative(Box<Expr>),
    Like(Like),
    /// Returns whether the list contains the expr value.
    InList(InList),
    /// EXISTS subquery, Returns a Boolean (true if any row exists), i.e., the exists only
    /// checks whether at least one row exists, not what columns or values are returned, the
    /// actual contents of the returned columns are ignored — only the presence or absence
    /// of rows matters.
    ///
    /// Can only be used where Boolean expressions are valid(e.g., WHERE, CASE, etc.).
    ///
    /// Not allowed in:
    /// - SELECT expressions, i.e., projection.
    /// - GROUP BY, ORDER BY, SET clause - only via CASE or logical use.
    ///
    /// Ideally, the subquery, can be correlated or non-correlated, should be optimized
    /// as join. For example:
    ///
    /// EXISTS Subquery → SEMI JOIN (or plain JOIN + DISTINCT)
    /// ```sql
    /// SELECT name
    /// FROM employees e
    /// WHERE EXISTS (
    ///     SELECT 1 FROM projects p WHERE p.emp_id = e.id
    /// );
    /// ```
    ///
    /// Rewrite to:
    /// ```sql
    /// SELECT DISTINCT e.name
    /// FROM employees e
    /// JOIN projects p ON p.emp_id = e.id;
    /// ```
    /// - DISTINCT removes duplicates if an employee has multiple projects.
    /// - This acts like a semi-join: include e only if p exists.
    /// - A semi-join between two tables A and B returns only rows from A that
    ///  have a matching row in B, but does not return any columns from B. i.e.,
    ///  Care whether a match exists in the second table, but don't care about
    ///  the matched data.
    Exists(Exists),
    /// Scalar subquery, produce exactly one column and at most one row.
    /// Used anywhere a single value is expected(except group by), i.e.,
    /// anywhere a scalar expression is allowed, e.g., SELECT, WHERE, SET, etc.
    ///
    /// Ideally, the subquery, can be correlated or non-correlated, should be optimized
    /// as join. For example:
    ///
    /// Scalar Subquery → JOIN + GROUP BY:
    ///
    /// ```sql
    /// SELECT e.name,
    ///        (SELECT AVG(salary)
    ///         FROM employees e2
    ///         WHERE e2.dept_id = e.dept_id) AS avg_salary
    /// FROM employees e;
    /// ```
    ///
    /// Rewrite to:
    ///
    /// ```sql
    /// SELECT e.name, d.avg_salary
    /// FROM employees e
    /// JOIN (
    ///     SELECT dept_id, AVG(salary) AS avg_salary
    ///     FROM employees
    ///     GROUP BY dept_id
    /// ) d ON e.dept_id = d.dept_id;
    /// ```
    ///
    /// Or, Scalar Subquery -> LATERAL JOIN:
    ///
    /// ```sql
    /// SELECT name,
    ///        (SELECT MAX(project_name)
    ///         FROM projects p
    ///         WHERE p.emp_id = e.id) AS top_project
    /// FROM employees e;
    /// ```
    ///
    /// Rewrite to with LATERAL JOIN:
    ///
    /// ```sql
    /// SELECT e.name, p.project_name
    /// FROM employees e
    /// LEFT JOIN LATERAL (
    ///     SELECT project_name
    ///     FROM projects p
    ///     WHERE p.emp_id = e.id
    ///     ORDER BY start_date DESC
    ///     LIMIT 1
    /// ) p ON true;
    /// ```
    ///
    /// NB:
    /// - If scalar subquery depends on a joinable key (like dept_id) and uses aggregation,
    ///  try rewriting it with GROUP BY + JOIN.
    /// - If it depends on per-row filtering, sorting, or limiting, and can’t be precomputed,
    ///  must use LATERAL.
    ScalarSubquery(Subquery),
    /// IN subquery, returns boolean, Left-hand side compared against a
    /// set of values from the subquery (which must return 1 column).
    /// Can appear in WHERE, CASE, etc. e.g., `... WHERE id IN (SELECT id FROM...)`
    ///
    /// Not allowed in:
    /// - SELECT value positions unless wrapped inside CASE or stored to a column alias.
    /// - GROUP BY or ORDER BY — only via computed expressions(e.g., use an alias or
    ///  expression computed in the SELECT list that contains a subquery, and refer to
    ///  that in ORDER BY.)
    ///
    /// Ideally, the subquery, can be correlated or non-correlated, should be optimized
    /// as join. For example:
    ///
    /// ```sql
    /// SELECT name
    /// FROM employees
    /// WHERE dept_id IN (
    ///     SELECT id FROM departments WHERE location = 'NY'
    /// );
    /// ```
    ///
    /// Rewrite to
    /// ```sql
    /// SELECT e.name
    /// FROM employees e
    /// JOIN departments d ON e.dept_id = d.id
    /// WHERE d.location = 'NY';
    /// ```
    InSubquery(InSubquery),
    /// A binary expression such as "age > 21"
    BinaryExpr(BinaryExpr),
    /// Casts the expression to a given type and will return a runtime error
    /// if the expression cannot be cast.
    Cast(Cast),
}

impl Expr {
    pub fn to_field(&self, schema: &LogicalSchema) -> Result<Field> {
        let name = match self {
            Expr::Alias(Alias { name, .. }) => name.clone(),
            Expr::FieldReference(FieldReference { name, .. }) => name.clone(),
            _ => self.schema_name().to_string(),
        };
        let (datatype, nullable) = self.datatype_and_nullable(schema)?;
        Ok(FieldBuilder::new(name, datatype).nullable(nullable).build())
    }

    /// Wrap this expr to in a `Expr::Cast` to the target `DataType`
    pub fn can_cast_to(self, cast_to_type: &DataType, schema: &LogicalSchema) -> Result<Expr> {
        let (this_type, _) = self.datatype_and_nullable(schema)?;
        if this_type == *cast_to_type {
            return Ok(self);
        }
        if !this_type.can_cast_to(cast_to_type.clone()) {
            return Err(Error::parse(format!(
                "Cannot automatically convert {this_type:?} to {cast_to_type:?}"
            )));
        }
        Ok(Expr::Cast(Cast::new(self, cast_to_type.clone())))
    }

    /// The datatype and nullability the expr would produce
    pub fn datatype_and_nullable(&self, schema: &LogicalSchema) -> Result<(DataType, bool)> {
        let (datatype, nullable) = match self {
            Expr::Alias(Alias { expr, .. }) => {
                let (_, field) = expr.field(schema)?;
                (field.datatype.clone(), field.nullable)
            }
            Expr::Value(value) => match value {
                Value::Null => (DataType::Null, true),
                Value::Boolean(_) => (DataType::Boolean, false),
                Value::Integer(_) => (DataType::Integer, false),
                Value::Float(_) => (DataType::Float, false),
                Value::String(_) => (DataType::String, false),
            },
            Expr::FieldReference(field_ref) => {
                let f = schema.field_by_ref(field_ref)?;
                (f.datatype.clone(), f.nullable)
            }
            Expr::OuterFieldReference(datatype, _) => (datatype.clone(), true),
            Expr::Not(_)
            | Expr::IsNull(_)
            | Expr::IsNotNull(_)
            | Expr::IsTrue(_)
            | Expr::IsNotTrue(_)
            | Expr::IsFalse(_)
            | Expr::Exists(_)
            | Expr::IsNotFalse(_) => (DataType::Boolean, false),
            Expr::Negative(expr) => expr.datatype_and_nullable(schema)?,
            Expr::Like(Like { expr, .. }) => {
                let (_, nullable) = expr.datatype_and_nullable(schema)?;
                (DataType::Boolean, nullable)
            }
            Expr::InList(InList { expr, list, .. }) => {
                let has_nullable = std::iter::once(expr)
                    .chain(list)
                    .find_map(|it| {
                        it.datatype_and_nullable(schema)
                            .map(|(_, nullable)| if nullable { Some(()) } else { None })
                            .transpose()
                    })
                    .transpose()?;
                (DataType::Boolean, has_nullable.is_some())
            }
            Expr::InSubquery(InSubquery { expr, .. }) => {
                let (_, nullable) = expr.datatype_and_nullable(schema)?;
                (DataType::Boolean, nullable)
            }
            Expr::ScalarSubquery(Subquery { subquery, .. }) => {
                let f = subquery.schema().field(0);
                (f.datatype.clone(), f.nullable)
            }
            Expr::BinaryExpr(binary_expr) => binary_expr.datatype_and_nullable(schema)?,
            Expr::Cast(Cast { expr, data_type, .. }) => {
                let (_, nullable) = expr.datatype_and_nullable(schema)?;
                (data_type.clone(), nullable)
            }
        };
        Ok((datatype, nullable))
    }

    /// The compatible field schema this [`Expr`] would produce.
    pub fn field(&self, schema: &LogicalSchema) -> Result<(Option<TableReference>, FieldRef)> {
        let (relation, schema_name) = self.qualified_name();
        let (datatype, nullable) = self.datatype_and_nullable(schema)?;
        let field = FieldBuilder::new(schema_name, datatype).nullable(nullable).build();
        Ok((relation, Arc::new(field)))
    }

    /// The column name of the expr
    fn qualified_name(&self) -> (Option<TableReference>, String) {
        match self {
            Expr::FieldReference(FieldReference { relation, name }) => {
                (relation.clone(), name.clone())
            }
            Expr::Alias(Alias { relation, name, .. }) => (relation.clone(), name.clone()),
            _ => (None, self.schema_name().to_string()),
        }
    }

    fn schema_name(&self) -> impl Display + '_ {
        SchemaDisplay(self)
    }
}

impl TreeNode for Expr {
    #[allow(clippy::needless_lifetimes)]
    fn visit_children<F>(&self, mut f: F) -> Result<VisitRecursion>
    where
        F: FnMut(&Self) -> Result<VisitRecursion>,
    {
        match self {
            Expr::Alias(Alias { expr, .. })
            | Expr::Not(expr)
            | Expr::IsNull(expr)
            | Expr::IsNotNull(expr)
            | Expr::IsTrue(expr)
            | Expr::IsNotTrue(expr)
            | Expr::IsFalse(expr)
            | Expr::IsNotFalse(expr)
            | Expr::Negative(expr)
            | Expr::InSubquery(InSubquery { expr, .. })
            | Expr::Cast(Cast { expr, .. }) => f(expr),
            Expr::BinaryExpr(BinaryExpr { left, right, .. }) => apply_each!(f, left, right),
            Expr::Like(Like { expr, pattern, .. }) => apply_each!(f, expr, pattern),
            Expr::InList(InList { expr, list, .. }) => {
                let mut out = vec![expr];
                out.extend(list);
                apply_each!(f; out)
            }
            Expr::Value(_)
            | Expr::FieldReference(_)
            | Expr::OuterFieldReference(_, _)
            | Expr::Exists(_)
            | Expr::ScalarSubquery(_) => Ok(VisitRecursion::Continue),
        }
    }
}

#[macro_export]
macro_rules! format_expr_vec {
    ( $ARRAY:expr ) => {{
        $ARRAY.iter().map(|e| format!("{e}")).collect::<Vec<String>>().join(", ")
    }};
}

impl Display for Expr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Expr::Alias(Alias { name, expr, .. }) => write!(f, "{expr} AS {name}"),
            Expr::Value(value) => write!(f, "{value}"),
            Expr::FieldReference(fr) => write!(f, "{fr}"),
            Expr::OuterFieldReference(_, fr) => write!(f, "{fr}"),
            Expr::Not(expr) => write!(f, "NOT {expr}"),
            Expr::IsNull(expr) => write!(f, "{expr} IS NULL"),
            Expr::IsNotNull(expr) => write!(f, "{expr} IS NOT NULL"),
            Expr::IsTrue(expr) => write!(f, "{expr} IS TRUE"),
            Expr::IsNotTrue(expr) => write!(f, "{expr} IS NOT TRUE"),
            Expr::IsFalse(expr) => write!(f, "{expr} IS FALSE"),
            Expr::IsNotFalse(expr) => write!(f, "{expr} IS NOT FALSE"),
            Expr::Negative(expr) => write!(f, "(-{expr})"),
            Expr::Like(Like { negated, expr, pattern, case_insensitive }) => {
                write!(f, "{expr}")?;
                let op_name = if *case_insensitive { "ILIKE" } else { "LIKE" };
                if *negated {
                    write!(f, " NOT")?;
                }
                write!(f, " {op_name} {pattern}")
            }
            Expr::InList(InList { expr, list, negated }) => {
                if *negated {
                    write!(f, "{expr} NOT IN ([{}])", format_expr_vec!(list))
                } else {
                    write!(f, "{expr} IN ([{}])", format_expr_vec!(list))
                }
            }
            Expr::Exists(Exists { subquery, negated }) => {
                let sub = if !f.alternate() {
                    format!("Subquery(correlated: {}, ...)", subquery.correlated)
                } else {
                    format!("{:#?}", subquery)
                };
                if *negated {
                    write!(f, "NOT EXISTS ({sub})")
                } else {
                    write!(f, "EXISTS ({sub})")
                }
            }
            Expr::ScalarSubquery(subquery) => {
                let sub = if !f.alternate() {
                    format!("Subquery(correlated: {}, ...)", subquery.correlated)
                } else {
                    format!("{:#?}", subquery)
                };
                write!(f, "({sub})")
            }
            Expr::InSubquery(InSubquery { expr, subquery, negated }) => {
                let sub = if !f.alternate() {
                    format!("Subquery(correlated: {}, ...)", subquery.correlated)
                } else {
                    format!("{:#?}", subquery)
                };
                if *negated {
                    write!(f, "{expr} NOT IN ({sub})")
                } else {
                    write!(f, "{expr} IN ({sub})")
                }
            }
            Expr::BinaryExpr(expr) => write!(f, "{expr}"),
            Expr::Cast(Cast { expr, data_type }) => {
                write!(f, "CAST({expr} AS {data_type:?})")
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct Cast {
    /// The expression being cast
    pub expr: Box<Expr>,
    /// The `DataType` the expression will yield
    pub data_type: DataType,
}

impl Cast {
    pub fn new(expr: Expr, data_type: DataType) -> Self {
        Self { expr: Box::new(expr), data_type }
    }
}

/// Binary expression
#[derive(Clone, Debug)]
pub struct BinaryExpr {
    /// Left-hand side of the expression
    pub left: Box<Expr>,
    /// The comparison operator
    pub op: Operator,
    /// Right-hand side of the expression
    pub right: Box<Expr>,
}

impl BinaryExpr {
    pub fn new(left: Expr, op: Operator, right: Expr) -> Self {
        Self { left: Box::new(left), op, right: Box::new(right) }
    }

    pub fn datatype_and_nullable(&self, schema: &LogicalSchema) -> Result<(DataType, bool)> {
        let (ld, ln) = self.left.datatype_and_nullable(schema)?;
        let (rd, rn) = self.right.datatype_and_nullable(schema)?;
        let type_coercer = BinaryTypeCoercer::new(&ld, &self.op, &rd);
        let datatype = type_coercer.get_result_type()?;
        Ok((datatype, ln || rn))
    }
}

impl Display for BinaryExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // Put parentheses around child binary expressions so that we
        // can see the difference between `(a OR b) AND c` and `a OR (b AND c)`.
        // We only insert parentheses when needed, based on operator precedence.
        // For example, `(a AND b) OR c` and `a AND b OR c` are equivalent and
        // the parentheses are not necessary.
        fn write_child(f: &mut Formatter<'_>, expr: &Expr, prec: u8) -> std::fmt::Result {
            match expr {
                Expr::BinaryExpr(child) => {
                    if child.op.prec_value() < prec {
                        write!(f, "({child})")
                    } else {
                        write!(f, "{child}")
                    }
                }
                _ => write!(f, "{expr}"),
            }
        }

        let prec = self.op.prec_value();
        write_child(f, self.left.as_ref(), prec)?;
        write!(f, "{}", self.op)?;
        write_child(f, self.right.as_ref(), prec)?;
        Ok(())
    }
}

pub struct BinaryTypeCoercer<'a> {
    lhs: &'a DataType,
    op: &'a Operator,
    rhs: &'a DataType,
}

impl<'a> BinaryTypeCoercer<'a> {
    pub fn new(lhs: &'a DataType, op: &'a Operator, rhs: &'a DataType) -> Self {
        Self { lhs, op, rhs }
    }

    pub fn get_result_type(&self) -> Result<DataType> {
        let datatype = match self.op {
            Operator::Plus
            | Operator::Minus
            | Operator::Multiply
            | Operator::Divide
            | Operator::Modulo => {
                if !self.lhs.is_numeric() && !self.rhs.is_numeric() {
                    return Err(Error::parse(format!(
                        "Cannot perform binary {:?} between type: {}, {}",
                        self.op, self.lhs, self.rhs
                    )));
                }
                match (self.lhs, self.rhs) {
                    (_, DataType::Float) | (DataType::Float, _) => DataType::Float,
                    _ => DataType::Integer,
                }
            }

            Operator::Eq
            | Operator::NotEq
            | Operator::Gt
            | Operator::GtEq
            | Operator::Lt
            | Operator::LtEq => match (self.lhs, self.rhs) {
                (DataType::Float, DataType::Integer)
                | (DataType::Integer, DataType::Float)
                | (DataType::Integer, DataType::Integer)
                | (DataType::String, DataType::String)
                | (DataType::Boolean, DataType::Boolean)
                | (DataType::Null, _)
                | (_, DataType::Null) => DataType::Boolean,
                _ => {
                    return Err(Error::parse(format!(
                        "Cannot perform binary {:?} between type: {}, {}",
                        self.op, self.lhs, self.rhs
                    )))
                }
            },

            Operator::And | Operator::Or => match (self.lhs, self.rhs) {
                (DataType::Boolean, DataType::Boolean)
                | (DataType::Boolean, DataType::Null)
                | (DataType::Null, DataType::Boolean) => DataType::Boolean,
                _ => {
                    return Err(Error::parse(format!(
                        "Cannot perform binary {:?} between type: {}, {}",
                        self.op, self.lhs, self.rhs
                    )))
                }
            },
        };
        Ok(datatype)
    }
}

/// Logical binary operators applied to logical expressions
#[derive(Clone, Debug)]
pub enum Operator {
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

impl Operator {
    /// Refer to `crate::sql::parser::ast::Precedence`
    pub fn prec_value(&self) -> u8 {
        match self {
            Operator::Multiply | Operator::Divide | Operator::Modulo => 40,
            Operator::Plus | Operator::Minus => 30,
            Operator::Eq
            | Operator::NotEq
            | Operator::Gt
            | Operator::GtEq
            | Operator::Lt
            | Operator::LtEq => 20,
            Operator::And => 10,
            Operator::Or => 5,
        }
    }
}

impl Display for Operator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Operator::Plus => "+",
            Operator::Minus => "-",
            Operator::Multiply => "*",
            Operator::Divide => "/",
            Operator::Modulo => "%",
            Operator::Eq => "=",
            Operator::NotEq => "!=",
            Operator::Gt => ">",
            Operator::GtEq => ">=",
            Operator::Lt => "<",
            Operator::LtEq => "<=",
            Operator::And => "AND",
            Operator::Or => "OR",
        };
        write!(f, "{s}")
    }
}

#[derive(Clone, Debug)]
pub struct InSubquery {
    /// The expression to compare
    pub expr: Box<Expr>,
    /// Subquery that will produce a single column of data to compare against.
    pub subquery: Subquery,
    /// Whether the expression is negated
    pub negated: bool,
}

impl InSubquery {
    pub fn try_new(subplan: Plan, expr: Expr, negated: bool) -> Result<Self> {
        let subquery = Subquery::try_new(subplan)?;
        Ok(Self { expr: Box::new(expr), subquery, negated })
    }
}

/// Exists expr logical node
#[derive(Clone, Debug)]
pub struct Exists {
    /// Subquery that will produce rows.
    pub subquery: Subquery,
    /// Whether the expression is negated
    pub negated: bool,
}

impl Exists {
    pub fn try_new(subplan: Plan, negated: bool) -> Result<Self> {
        let subquery = Subquery::try_new(subplan)?;
        Ok(Self { subquery, negated })
    }
}

#[derive(Clone, Debug)]
pub struct Subquery {
    /// The subquery plan
    pub subquery: Box<Plan>,
    /// Whether the subquery is correlated
    pub correlated: bool,
}

impl Subquery {
    pub fn try_new(subplan: Plan) -> Result<Self> {
        let mut correlated = false;
        subplan.visit_exprs(|expr| {
            if matches!(expr, Expr::OuterFieldReference(_, _)) {
                correlated = true
            }
            Ok(VisitRecursion::Continue)
        })?;
        Ok(Self { subquery: Box::new(subplan), correlated })
    }
}

#[derive(Clone, Debug)]
pub struct InList {
    /// The expression to compare
    pub expr: Box<Expr>,
    /// The list of values to compare against
    pub list: Vec<Box<Expr>>,
    /// Whether the expression is negated
    pub negated: bool,
}

impl InList {
    pub fn new(expr: Expr, list: Vec<Expr>, negated: bool) -> Self {
        let list = list.into_iter().map(|it| Box::new(it)).collect();
        Self { expr: Box::new(expr), list, negated }
    }
}

#[derive(Clone, Debug)]
pub struct Like {
    pub negated: bool,
    pub expr: Box<Expr>,
    pub pattern: Box<Expr>,
    /// Whether to ignore case on comparing
    pub case_insensitive: bool,
}

impl Like {
    pub fn new(expr: Expr, pattern: Expr, negated: bool, case_insensitive: bool) -> Self {
        Self { negated, expr: Box::new(expr), pattern: Box::new(pattern), case_insensitive }
    }
}

/// Alias expression
#[derive(Clone, Debug)]
pub struct Alias {
    pub expr: Box<Expr>,
    pub relation: Option<TableReference>,
    pub name: String,
}

impl Alias {
    pub fn new(
        expr: Expr,
        relation: Option<impl Into<TableReference>>,
        name: impl Into<String>,
    ) -> Self {
        Self { expr: Box::new(expr), relation: relation.map(|it| it.into()), name: name.into() }
    }
}

struct SchemaDisplay<'a>(&'a Expr);

impl Display for SchemaDisplay<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            Expr::Alias(Alias { relation: Some(rel), name, .. }) => write!(f, "{}.{}", rel, name),
            Expr::Alias(Alias { name, .. }) => write!(f, "{}", name),
            Expr::Value(_) | Expr::FieldReference(_) | Expr::OuterFieldReference(..) => {
                // same as the Expr::Display
                write!(f, "{}", self.0)
            }
            Expr::Not(expr) => write!(f, "NOT {}", SchemaDisplay(expr)),
            Expr::IsNull(expr) => write!(f, "IS NULL {}", SchemaDisplay(expr)),
            Expr::IsNotNull(expr) => write!(f, "{} IS NOT NULL", SchemaDisplay(expr)),
            Expr::IsTrue(expr) => write!(f, "{} IS TRUE", SchemaDisplay(expr)),
            Expr::IsNotTrue(expr) => write!(f, "{} IS NOT TRUE", SchemaDisplay(expr)),
            Expr::IsFalse(expr) => write!(f, "{} IS FALSE", SchemaDisplay(expr)),
            Expr::IsNotFalse(expr) => write!(f, "{} IS NOT FALSE", SchemaDisplay(expr)),
            Expr::Negative(expr) => write!(f, "(- {})", SchemaDisplay(expr)),
            Expr::Like(Like { negated, expr, pattern, case_insensitive }) => write!(
                f,
                "{} {}{} {}",
                SchemaDisplay(expr),
                if *negated { "NOT " } else { "" },
                if *case_insensitive { "ILIKE" } else { "LIKE" },
                SchemaDisplay(pattern),
            ),

            Expr::InList(InList { expr, list, negated }) => {
                let mut names = String::new();
                for (i, e) in list.iter().enumerate() {
                    if i > 0 {
                        write!(&mut names, ",")?;
                    }
                    write!(&mut names, "{}", SchemaDisplay(e))?;
                }
                if *negated {
                    write!(f, "{} NOT IN {}", SchemaDisplay(expr), names)
                } else {
                    write!(f, "{} IN {}", SchemaDisplay(expr), names)
                }
            }
            Expr::Exists(Exists { negated, .. }) => {
                if *negated {
                    write!(f, "NOT EXISTS")
                } else {
                    write!(f, "EXISTS")
                }
            }
            Expr::ScalarSubquery(Subquery { subquery, .. }) => {
                write!(f, "{}", subquery.schema().field(0).name)
            }
            Expr::InSubquery(InSubquery { negated, .. }) => {
                if *negated {
                    write!(f, "NOT IN")
                } else {
                    write!(f, "IN")
                }
            }
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                write!(f, "{} {op} {}", SchemaDisplay(left), SchemaDisplay(right),)
            }
            Expr::Cast(Cast { expr, .. }) => {
                write!(f, "{}", SchemaDisplay(expr))
            }
        }
    }
}
