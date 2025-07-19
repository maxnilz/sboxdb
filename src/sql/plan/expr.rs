use std::assert_matches::assert_matches;
use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::Arc;

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
    OuterReferenceColumn(DataType, FieldReference),
    Not(Box<Expr>),
    IsNull(Box<Expr>),
    IsNotNull(Box<Expr>),
    IsTrue(Box<Expr>),
    IsNotTrue(Box<Expr>),
    IsFalse(Box<Expr>),
    IsNotFalse(Box<Expr>),
    Negative(Box<Expr>),
    Like(Like),
    /// Returns whether the list contains the expr value.
    InList(InList),
    /// EXISTS subquery
    Exists(Exists),
    /// Scalar subquery, produce exactly one column and at most one row
    ScalarSubquery(Subquery),
    /// IN subquery
    InSubquery(InSubquery),
    /// A binary expression such as "age > 21"
    BinaryExpr(BinaryExpr),
    /// Casts the expression to a given type and will return a runtime error
    /// if the expression cannot be cast.
    Cast(Cast),
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
        match self.op {
            Operator::Plus => {}
            Operator::Minus => {}
            Operator::Multiply => {}
            Operator::Divide => {}
            Operator::Modulo => {}
            Operator::Eq => {}
            Operator::NotEq => {}
            Operator::Gt => {}
            Operator::GtEq => {}
            Operator::Lt => {}
            Operator::LtEq => {}
            Operator::And => {}
            Operator::Or => {}
        }
        todo!()
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

#[derive(Clone, Debug)]
pub struct InSubquery {
    /// The expression to compare
    pub expr: Box<Expr>,
    /// Subquery that will produce a single column of data to compare against
    pub subquery: Subquery,
    /// Whether the expression is negated
    pub negated: bool,
}

impl InSubquery {
    pub fn new(subplan: Plan, expr: Expr, negated: bool) -> Self {
        Self { expr: Box::new(expr), subquery: Subquery::new(subplan), negated }
    }
}

#[derive(Clone, Debug)]
pub struct Exists {
    /// Subquery that will produce rows. The exists only checks whether at least one
    /// row exists, not what columns or values are returned. The actual contents of
    /// the returned columns are ignored — only the presence or absence of rows matters.
    pub subquery: Subquery,
    /// Whether the expression is negated
    pub negated: bool,
}

impl Exists {
    pub fn new(subplan: Plan, negated: bool) -> Self {
        Self { subquery: Subquery { subquery: Arc::new(subplan) }, negated }
    }
}

#[derive(Clone, Debug)]
pub struct Subquery {
    /// The subquery plan
    pub subquery: Arc<Plan>,
}

impl Subquery {
    pub fn new(subplan: Plan) -> Self {
        Self { subquery: Arc::new(subplan) }
    }
}

#[derive(Clone, Debug)]
pub struct InList {
    /// The expression to compare
    pub expr: Box<Expr>,
    /// The list of values to compare against
    pub list: Vec<Expr>,
    /// Whether the expression is negated
    pub negated: bool,
}

impl InList {
    pub fn new(expr: Expr, list: Vec<Expr>, negated: bool) -> Self {
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

impl Expr {
    pub fn to_field(&self) -> Result<Field> {
        todo!()
    }
    /// Qualify any field reference in the expr if not done yet, and perform the
    /// ambiguity check.
    /// Return field not fond error if there is no field found from the given schemas.
    /// If multiple field are found, consider it as an ambiguity error.
    pub fn qualify_field_reference(self, _schemas: &[&LogicalSchema]) -> Result<Self> {
        // TODO: qualify any Expr::FieldReference in the expr and perform ambiguity check
        Ok(self)
    }

    /// Wrap this expr to in a `Expr::Cast` to the target `DataType`
    pub fn cast_to(self, cast_to_type: &DataType, schema: &LogicalSchema) -> Result<Expr> {
        let (this_type, _) = self.datatype_and_nullable(schema)?;
        if this_type == *cast_to_type {
            return Ok(self);
        }
        let can_cast = match (&this_type, cast_to_type) {
            (DataType::Null, _) => true,
            (DataType::Boolean, DataType::Integer | DataType::Float | DataType::String) => true,
            (DataType::Integer, DataType::Boolean | DataType::Float | DataType::String) => true,
            (DataType::Float, DataType::Boolean | DataType::String) => true,
            (DataType::String, DataType::Boolean) => true,
            _ => false,
        };
        if !can_cast {
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
            Expr::OuterReferenceColumn(datatype, _) => (datatype.clone(), true),
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
                let has_nullable = std::iter::once(expr.as_ref())
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
            Expr::ScalarSubquery(Subquery { subquery }) => {
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

struct SchemaDisplay<'a>(&'a Expr);

impl Display for SchemaDisplay<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}
