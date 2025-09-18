use std::any::Any;
use std::fmt::Debug;

use crate::access::value::Tuple;
use crate::catalog::r#type::DataType;
use crate::catalog::r#type::Value;
use crate::error::Error;
use crate::error::Result;
use crate::sql::plan::expr::Expr;
use crate::sql::plan::schema::FieldRef;
use crate::sql::udf::signature::ScalarFunctionArgs;
use crate::sql::udf::signature::Signature;

/// Trait for implementing user defined scalar functions.
pub trait ScalarUDF: Debug + Sync + Send {
    /// Returns this object as an [`Any`] trait object
    #[allow(dead_code)]
    fn as_any(&self) -> &dyn Any;

    /// Returns this function's name.
    fn name(&self) -> &str;

    /// Returns the name of the column this expression would create
    fn schema_name(&self, args: &[Expr]) -> String {
        format!(
            "{}({})",
            self.name(),
            args.iter().map(|it| it.schema_name().to_string()).collect::<Vec<_>>().join(", ")
        )
    }

    /// Return the datatype this function returns given the input argument types.
    fn return_field(&self, arg_fields: &[FieldRef]) -> Result<FieldRef>;

    /// Invoke the function with row-wise batched args, returning the appropriate
    /// columnar result that paired with the input rows, i.e., each row args would
    /// expect a single scalar value as output thus form a columnar-wise tuple.
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<Tuple>;
}

macro_rules! make_udf_function {
    ($UDF:ty, $NAME:ident) => {
        pub fn $NAME() -> std::sync::Arc<dyn ScalarUDF> {
            static INSTANCE: std::sync::LazyLock<std::sync::Arc<dyn ScalarUDF>> =
                std::sync::LazyLock::new(|| std::sync::Arc::new(<$UDF>::new()));
            std::sync::Arc::clone(&INSTANCE)
        }
    };
}

make_udf_function!(UpperFunc, upper);

#[derive(Debug)]
pub struct UpperFunc {
    _signature: Signature,
}

impl UpperFunc {
    pub fn new() -> Self {
        Self { _signature: Signature::Exact(vec![DataType::String]) }
    }
}

impl ScalarUDF for UpperFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "upper"
    }

    fn return_field(&self, arg_fields: &[FieldRef]) -> Result<FieldRef> {
        if arg_fields.len() != 1 {
            return Err(Error::internal(format!(
                "upper function expect exact one argument, got {}",
                arg_fields.len()
            )));
        }
        if arg_fields[0].datatype != DataType::String {
            return Err(Error::internal(format!(
                "upper function expect string argument, got {}",
                arg_fields[0].datatype
            )));
        }
        Ok(arg_fields[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<Tuple> {
        let arg_values = args
            .args
            .into_iter()
            .map(|it| match it.scalar() {
                Ok(value) => match value {
                    Value::String(s) => Ok(s),
                    _ => Err(Error::internal(format!(
                        "Expect String value got {}",
                        value.datatype()
                    ))),
                },
                Err(err) => Err(err),
            })
            .collect::<Result<Vec<_>>>()?;
        let out_values =
            arg_values.into_iter().map(|it| Value::String(it.to_uppercase())).collect::<Vec<_>>();
        Ok(Tuple::from(out_values))
    }
}
