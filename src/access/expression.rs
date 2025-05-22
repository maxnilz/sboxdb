use std::fmt::Formatter;

use regex::Regex;

use crate::access::value::Tuple;
use crate::catalog::r#type::Value;
use crate::error::{Error, Result};

pub enum Expression {
    // Terminal operations
    Constant(Value),
    // Field index or {{table}.column}
    Field(usize, Option<(Option<String>, String)>),

    // Logical operations
    And(Box<Expression>, Box<Expression>),
    Not(Box<Expression>),
    Or(Box<Expression>, Box<Expression>),

    // Comparisons operations(GTE, LTE, and NEQ are composite operations)
    Equal(Box<Expression>, Box<Expression>),
    GreaterThan(Box<Expression>, Box<Expression>),
    LessThan(Box<Expression>, Box<Expression>),
    IsNull(Box<Expression>),

    // Mathematical operations
    Add(Box<Expression>, Box<Expression>),
    Assert(Box<Expression>),
    Divide(Box<Expression>, Box<Expression>),
    Exponentiate(Box<Expression>, Box<Expression>),
    Factorial(Box<Expression>),
    Modulo(Box<Expression>, Box<Expression>),
    Multiply(Box<Expression>, Box<Expression>),
    Negate(Box<Expression>),
    Subtract(Box<Expression>, Box<Expression>),

    // String operations
    Like(Box<Expression>, Box<Expression>),
}

impl Expression {
    pub fn evaluate(&self, tuple: Option<&Tuple>) -> Result<Value> {
        use Value::*;
        let ans: Value = match self {
            // Terminal operations
            Expression::Constant(c) => c.clone(),
            Expression::Field(i, _) => tuple.and_then(|t| t.get_value(*i).cloned()).unwrap_or(Null),

            // Logical operations
            Expression::And(lhs, rhs) => match (lhs.evaluate(tuple)?, rhs.evaluate(tuple)?) {
                (Boolean(lhs), Boolean(rhs)) => Boolean(lhs && rhs),
                (Boolean(lhs), Null) if !lhs => Boolean(false),
                (Boolean(_), Null) => Null,
                (Null, Boolean(rhs)) if !rhs => Boolean(false),
                (Null, Boolean(_)) => Null,
                (Null, Null) => Null,
                (lhs, rhs) => {
                    return Err(Error::value(format!("Invalid logical AND on {} and {}", lhs, rhs)))
                }
            },
            Expression::Not(expr) => match expr.evaluate(tuple)? {
                Null => Null,
                Boolean(b) => Boolean(b),
                value => return Err(Error::value(format!("Can't negate {}", value))),
            },
            Expression::Or(lhs, rhs) => match (lhs.evaluate(tuple)?, rhs.evaluate(tuple)?) {
                (Boolean(lhs), Boolean(rhs)) => Boolean(lhs || rhs),
                (Boolean(lhs), Null) if lhs => Boolean(true),
                (Boolean(_), Null) => Null,
                (Null, Boolean(rhs)) if rhs => Boolean(true),
                (Null, Boolean(_)) => Null,
                (Null, Null) => Null,
                (lhs, rhs) => {
                    return Err(Error::value(format!("Invalid logical OR on {} or {}", lhs, rhs)))
                }
            },

            // Comparisons operations
            Expression::Equal(lhs, rhs) => match (lhs.evaluate(tuple)?, rhs.evaluate(tuple)?) {
                (Boolean(lhs), Boolean(rhs)) => Boolean(lhs == rhs),
                (Integer(lhs), Integer(rhs)) => Boolean(lhs == rhs),
                (Integer(lhs), Float(rhs)) => Boolean(lhs as f64 == rhs),
                (Float(lhs), Integer(rhs)) => Boolean(lhs == rhs as f64),
                (Float(lhs), Float(rhs)) => Boolean(lhs == rhs),
                (String(lhs), String(rhs)) => Boolean(lhs == rhs),
                (Null, _) | (_, Null) => Null,
                (lhs, rhs) => {
                    return Err(Error::value(format!("Can't compare {} and {}", lhs, rhs)))
                }
            },
            Expression::GreaterThan(lhs, rhs) => {
                match (lhs.evaluate(tuple)?, rhs.evaluate(tuple)?) {
                    (Boolean(lhs), Boolean(rhs)) => Boolean(lhs > rhs),
                    (Integer(lhs), Integer(rhs)) => Boolean(lhs > rhs),
                    (Integer(lhs), Float(rhs)) => Boolean(lhs as f64 > rhs),
                    (Float(lhs), Integer(rhs)) => Boolean(lhs > rhs as f64),
                    (Float(lhs), Float(rhs)) => Boolean(lhs > rhs),
                    (String(lhs), String(rhs)) => Boolean(lhs > rhs),
                    (Null, _) | (_, Null) => Null,
                    (lhs, rhs) => {
                        return Err(Error::value(format!("Can't compare {} and {}", lhs, rhs)))
                    }
                }
            }
            Expression::LessThan(lhs, rhs) => match (lhs.evaluate(tuple)?, rhs.evaluate(tuple)?) {
                (Boolean(lhs), Boolean(rhs)) => Boolean(lhs < rhs),
                (Integer(lhs), Integer(rhs)) => Boolean(lhs < rhs),
                (Integer(lhs), Float(rhs)) => Boolean((lhs as f64) < rhs),
                (Float(lhs), Integer(rhs)) => Boolean(lhs < rhs as f64),
                (Float(lhs), Float(rhs)) => Boolean(lhs < rhs),
                (String(lhs), String(rhs)) => Boolean(lhs < rhs),
                (Null, _) | (_, Null) => Null,
                (lhs, rhs) => {
                    return Err(Error::value(format!("Can't compare {} and {}", lhs, rhs)))
                }
            },
            Expression::IsNull(expr) => match expr.evaluate(tuple)? {
                Null => Boolean(true),
                _ => Boolean(false),
            },

            // Mathematical operations
            Expression::Add(lhs, rhs) => match (lhs.evaluate(tuple)?, rhs.evaluate(tuple)?) {
                (Integer(lhs), Integer(rhs)) => {
                    Integer(lhs.checked_add(rhs).ok_or_else(|| Error::value("Integer overflow"))?)
                }
                (Integer(lhs), Float(rhs)) => Float(lhs as f64 + rhs),
                (Integer(_), Null) => Null,
                (Float(lhs), Float(rhs)) => Float(lhs + rhs),
                (Float(lhs), Integer(rhs)) => Float(lhs + rhs as f64),
                (Float(_), Null) => Null,
                (Null, Float(_)) => Null,
                (Null, Integer(_)) => Null,
                (Null, Null) => Null,
                (lhs, rhs) => return Err(Error::value(format!("Can't add {} and {}", lhs, rhs))),
            },
            Expression::Assert(expr) => match expr.evaluate(tuple)? {
                Float(f) => Float(f),
                Integer(i) => Integer(i),
                Null => Null,
                expr => return Err(Error::value(format!("Can't take the positive of {}", expr))),
            },
            Expression::Divide(lhs, rhs) => match (lhs.evaluate(tuple)?, rhs.evaluate(tuple)?) {
                (Integer(_), Integer(0)) => return Err(Error::value("Can't divide by zero")),
                (Integer(lhs), Integer(rhs)) => Integer(lhs / rhs),
                (Integer(lhs), Float(rhs)) => Float(lhs as f64 / rhs),
                (Integer(_), Null) => Null,
                (Float(lhs), Integer(rhs)) => Float(lhs / rhs as f64),
                (Float(lhs), Float(rhs)) => Float(lhs / rhs),
                (Float(_), Null) => Null,
                (Null, Float(_)) => Null,
                (Null, Integer(_)) => Null,
                (Null, Null) => Null,
                (lhs, rhs) => {
                    return Err(Error::value(format!("Can't divide {} and {}", lhs, rhs)))
                }
            },
            Expression::Exponentiate(lhs, rhs) => {
                match (lhs.evaluate(tuple)?, rhs.evaluate(tuple)?) {
                    (Integer(lhs), Integer(rhs)) if rhs >= 0 => Integer(
                        lhs.checked_pow(rhs as u32)
                            .ok_or_else(|| Error::value("Integer overflow"))?,
                    ),
                    (Integer(lhs), Integer(rhs)) => Float((lhs as f64).powf(rhs as f64)),
                    (Integer(lhs), Float(rhs)) => Float((lhs as f64).powf(rhs)),
                    (Integer(_), Null) => Null,
                    (Float(lhs), Integer(rhs)) => Float((lhs).powi(rhs as i32)),
                    (Float(lhs), Float(rhs)) => Float((lhs).powf(rhs)),
                    (Float(_), Null) => Null,
                    (Null, Float(_)) => Null,
                    (Null, Integer(_)) => Null,
                    (Null, Null) => Null,
                    (lhs, rhs) => {
                        return Err(Error::value(format!("Can't exponentiate {} and {}", lhs, rhs)))
                    }
                }
            }
            Expression::Factorial(expr) => match expr.evaluate(tuple)? {
                Integer(i) if i < 0 => {
                    return Err(Error::value("Can't take factorial of negative number"))
                }
                Integer(i) => Integer((1..=i).product()),
                Null => Null,
                value => return Err(Error::value(format!("Can't take factorial of {}", value))),
            },
            Expression::Modulo(lhs, rhs) => match (lhs.evaluate(tuple)?, rhs.evaluate(tuple)?) {
                // This uses remainder semantics, like Postgres.
                (Integer(_), Integer(0)) => return Err(Error::value("Can't divide by zero")),
                (Integer(lhs), Integer(rhs)) => Integer(lhs % rhs),
                (Integer(lhs), Float(rhs)) => Float(lhs as f64 % rhs),
                (Integer(_), Null) => Null,
                (Float(lhs), Integer(rhs)) => Float(lhs % rhs as f64),
                (Float(lhs), Float(rhs)) => Float(lhs % rhs),
                (Float(_), Null) => Null,
                (Null, Float(_)) => Null,
                (Null, Integer(_)) => Null,
                (Null, Null) => Null,
                (lhs, rhs) => {
                    return Err(Error::value(format!("Can't take modulo of {} and {}", lhs, rhs)))
                }
            },
            Expression::Multiply(lhs, rhs) => match (lhs.evaluate(tuple)?, rhs.evaluate(tuple)?) {
                (Integer(lhs), Integer(rhs)) => {
                    Integer(lhs.checked_mul(rhs).ok_or_else(|| Error::value("Integer overflow"))?)
                }
                (Integer(lhs), Float(rhs)) => Float(lhs as f64 * rhs),
                (Integer(_), Null) => Null,
                (Float(lhs), Integer(rhs)) => Float(lhs * rhs as f64),
                (Float(lhs), Float(rhs)) => Float(lhs * rhs),
                (Float(_), Null) => Null,
                (Null, Float(_)) => Null,
                (Null, Integer(_)) => Null,
                (Null, Null) => Null,
                (lhs, rhs) => {
                    return Err(Error::value(format!("Can't multiply {} and {}", lhs, rhs)))
                }
            },
            Expression::Negate(expr) => match expr.evaluate(tuple)? {
                Integer(i) => Integer(-i),
                Float(f) => Float(-f),
                Null => Null,
                value => return Err(Error::value(format!("Can't negate {}", value))),
            },
            Expression::Subtract(lhs, rhs) => match (lhs.evaluate(tuple)?, rhs.evaluate(tuple)?) {
                (Integer(lhs), Integer(rhs)) => {
                    Integer(lhs.checked_sub(rhs).ok_or_else(|| Error::value("Integer overflow"))?)
                }
                (Integer(lhs), Float(rhs)) => Float(lhs as f64 - rhs),
                (Integer(_), Null) => Null,
                (Float(lhs), Integer(rhs)) => Float(lhs - rhs as f64),
                (Float(lhs), Float(rhs)) => Float(lhs - rhs),
                (Float(_), Null) => Null,
                (Null, Float(_)) => Null,
                (Null, Integer(_)) => Null,
                (Null, Null) => Null,
                (lhs, rhs) => {
                    return Err(Error::value(format!("Can't subtract {} and {}", lhs, rhs)))
                }
            },

            // String operations
            Expression::Like(lhs, rhs) => match (lhs.evaluate(tuple)?, rhs.evaluate(tuple)?) {
                (String(lhs), String(rhs)) => {
                    let pattern = regex::escape(&rhs).replace('%', ".*").replace('_', ".");
                    let pattern = format!("^{}$", pattern);
                    Boolean(Regex::new(&pattern)?.is_match(&lhs))
                }
                (String(_), Null) => Null,
                (Null, String(_)) => Null,
                (lhs, rhs) => return Err(Error::value(format!("Can't LIKE {} and {}", lhs, rhs))),
            },
        };
        Ok(ans)
    }
}

impl std::fmt::Display for Expression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Expression::Constant(c) => c.to_string(),
            Expression::Field(i, None) => format!("#{}", i),
            Expression::Field(_, Some((None, name))) => name.to_string(),
            Expression::Field(_, Some((Some(table), name))) => format!("{}.{}", table, name),

            Expression::And(lhs, rhs) => format!("{} AND {}", lhs, rhs),
            Expression::Not(expr) => format!("NOT {}", expr),
            Expression::Or(lhs, rhs) => format!("{} OR {}", lhs, rhs),

            Expression::Equal(lhs, rhs) => format!("{} = {}", lhs, rhs),
            Expression::GreaterThan(lhs, rhs) => format!("{} > {}", lhs, rhs),
            Expression::LessThan(lhs, rhs) => format!("{} < {}", lhs, rhs),
            Expression::IsNull(expr) => format!("{} IS NULL", expr),

            Expression::Add(lhs, rhs) => format!("{} + {}", lhs, rhs),
            Expression::Assert(expr) => expr.to_string(),
            Expression::Divide(lhs, rhs) => format!("{} / {}", lhs, rhs),
            Expression::Exponentiate(lhs, rhs) => format!("{} ^ {}", lhs, rhs),
            Expression::Factorial(expr) => format!("!{}", expr),
            Expression::Modulo(lhs, rhs) => format!("{} % {}", lhs, rhs),
            Expression::Multiply(lhs, rhs) => format!("{} * {}", lhs, rhs),
            Expression::Negate(expr) => format!("-{}", expr),
            Expression::Subtract(lhs, rhs) => format!("{} - {}", lhs, rhs),

            Expression::Like(lhs, rhs) => format!("{} LIKE {}", lhs, rhs),
        };
        write!(f, "{}", s)
    }
}
