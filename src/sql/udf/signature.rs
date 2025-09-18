use crate::access::value::Tuple;
use crate::catalog::r#type::DataType;

#[allow(dead_code)]
#[derive(Debug)]
pub enum Signature {
    /// One or more arguments with exactly the specified types in order.
    Exact(Vec<DataType>),
    /// No arguments
    Nullary,
}

#[derive(Debug, Clone)]
pub struct ScalarFunctionArgs {
    /// The evaluated row-wise arguments to the function in batch. e.g.,
    /// -------------------
    /// row |  arg0 |  arg1
    /// --- |------ |------
    /// #1  |  val1 |  val2
    /// --- |------ |------
    /// #2  |  val3 |  val4
    pub args: Vec<Tuple>,
    /// The number of rows in record batch being evaluated,
    /// paired with the args.
    pub _num_rows: usize,
}

impl ScalarFunctionArgs {
    pub fn new(args: Vec<Tuple>, num_rows: usize) -> Self {
        Self { args, _num_rows: num_rows }
    }
}
