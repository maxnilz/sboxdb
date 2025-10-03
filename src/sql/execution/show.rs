use std::any::Any;
use std::fmt::Display;
use std::fmt::Formatter;
use std::fmt::Write;

use crate::access::value::Tuple;
use crate::catalog::index::Index;
use crate::catalog::r#type::Value;
use crate::catalog::schema::Schema;
use crate::error::Result;
use crate::sql::execution::compiler::ExecutionPlan;
use crate::sql::execution::compiler::RecordBatch;
use crate::sql::execution::compiler::RecordBatchBuilder;
use crate::sql::execution::context::Context;
use crate::sql::format::display_comma_separated;
use crate::sql::format::Indent;
use crate::sql::plan::schema::LogicalSchema;

/// Show tables physical executor
#[derive(Debug)]
pub struct ShowTablesExec {
    output_schema: LogicalSchema,
}

impl ShowTablesExec {
    pub fn new(output_schema: LogicalSchema) -> Self {
        Self { output_schema }
    }
}

impl ExecutionPlan for ShowTablesExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> LogicalSchema {
        self.output_schema.clone()
    }

    fn init(&self, _ctx: &mut dyn Context) -> Result<()> {
        Ok(())
    }

    fn execute(&self, ctx: &mut dyn Context) -> Result<Option<RecordBatch>> {
        let txn = ctx.txn();
        let mut schemas = txn.scan_tables()?;
        let mut rows = vec![];
        while let Some(schema) = schemas.next() {
            rows.push(Tuple::from(vec![Value::String(schema.name)]));
        }
        let rb = RecordBatchBuilder::new(&self.output_schema).extend(rows).nomore().build();
        Ok(Some(rb))
    }
}

impl Display for ShowTablesExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ShowTablesExec")
    }
}

/// Show create table physical executor
#[derive(Debug)]
pub struct ShowCreateTableExec {
    table: String,
    output_schema: LogicalSchema,
}

impl ShowCreateTableExec {
    pub fn new(table: String, output_schema: LogicalSchema) -> Self {
        Self { table, output_schema }
    }
}

impl ExecutionPlan for ShowCreateTableExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> LogicalSchema {
        self.output_schema.clone()
    }

    fn init(&self, _ctx: &mut dyn Context) -> Result<()> {
        Ok(())
    }

    fn execute(&self, ctx: &mut dyn Context) -> Result<Option<RecordBatch>> {
        let txn = ctx.txn();
        let schema = txn.must_read_table(&self.table)?;

        let mut output = String::new();
        write!(output, "{:#}", SchemaWrapper(&schema))?;

        let indexes = txn.scan_table_indexes(&schema.name)?;
        for index in indexes.iter() {
            write!(output, "{}", IndexWrapper(index))?;
            write!(output, "\n")?;
        }
        let rows = vec![Tuple::from(vec![Value::String(output)])];
        let rb = RecordBatchBuilder::new(&self.output_schema).extend(rows).nomore().build();
        Ok(Some(rb))
    }
}

impl Display for ShowCreateTableExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ShowCreateTableExec: {}", self.table)
    }
}

struct SchemaWrapper<'a>(&'a Schema);
impl<'a> Display for SchemaWrapper<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CREATE TABLE {} (\n", self.0.name)?;

        let mut primary_key = vec![];
        for (_i, c) in self.0.columns.iter().enumerate() {
            if c.primary_key || c.part_of_key {
                primary_key.push(&c.name)
            }
            let s = format!(
                "{name} {dt}{notnull}{default}",
                name = c.name,
                dt = c.datatype,
                notnull = if !c.nullable { " NOT NULL" } else { "" },
                default = if let Some(d) = &c.default { format!(" {}", d) } else { String::new() }
            );
            write!(f, "{:#}", Indent(s))?;
            write!(f, ",\n")?;
        }

        let mut pk = String::new();
        write!(pk, "PRIMARY KEY(")?;
        write!(pk, "{}", display_comma_separated(&primary_key))?;
        write!(pk, ")")?;

        write!(f, "{:#}", Indent(pk))?;
        write!(f, "\n);")?;
        Ok(())
    }
}

struct IndexWrapper<'a>(&'a Index);
impl<'a> Display for IndexWrapper<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CREATE{unique} INDEX {name} (",
            unique = if self.0.uniqueness { " UNIQUE" } else { "" },
            name = self.0.name
        )?;
        let column_names = self.0.columns.iter().map(|it| &it.name).collect::<Vec<_>>();
        write!(f, "{}", display_comma_separated(&column_names))?;
        write!(f, ");")
    }
}

/// Echo physical executor that return the parse sql directly.
#[derive(Debug)]
pub struct EchoExec {
    sql: String,
    output_schema: LogicalSchema,
}
impl EchoExec {
    pub fn new(sql: impl Into<String>, output_schema: LogicalSchema) -> Self {
        Self { sql: sql.into(), output_schema }
    }
}

impl ExecutionPlan for EchoExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> LogicalSchema {
        self.output_schema.clone()
    }

    fn init(&self, _ctx: &mut dyn Context) -> Result<()> {
        Ok(())
    }

    fn execute(&self, _ctx: &mut dyn Context) -> Result<Option<RecordBatch>> {
        let rows = vec![Tuple::from(vec![Value::String(self.sql.clone())])];
        let rb = RecordBatchBuilder::new(&self.output_schema).extend(rows).nomore().build();
        Ok(Some(rb))
    }
}

impl Display for EchoExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "EchoExec: {}", self.sql)
    }
}
