use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::Arc;

use crate::access::value::Values;
use crate::catalog::r#type::Value;
use crate::error::Result;
use crate::sql::execution::compiler::ExecutionPlan;
use crate::sql::plan::schema::LogicalSchema;
use crate::sql::plan::visitor::TreeNode;
use crate::sql::plan::visitor::TreeNodeVisitor;
use crate::sql::plan::visitor::VisitRecursion;

/// Wraps an `ExecutionPlan` for formatting
pub struct DisplayableExecutionPlan<'a> {
    inner: &'a Arc<dyn ExecutionPlan>,
}

impl<'a> DisplayableExecutionPlan<'a> {
    pub fn new(inner: &'a Arc<dyn ExecutionPlan>) -> Self {
        Self { inner }
    }
}

impl<'a> Display for DisplayableExecutionPlan<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut visitor = IndentVisitor::new(f);
        match self.inner.visit(&mut visitor) {
            Ok(_) => Ok(()),
            Err(_) => Err(std::fmt::Error),
        }
    }
}

struct IndentVisitor<'a, 'b> {
    f: &'a mut Formatter<'b>,
    /// The current indent
    indent: usize,
}

impl<'a, 'b> IndentVisitor<'a, 'b> {
    pub fn new(f: &'a mut Formatter<'b>) -> Self {
        Self { f, indent: 0 }
    }
}

impl<'a, 'b, 'n> TreeNodeVisitor<'n> for IndentVisitor<'a, 'b> {
    type Node = Arc<dyn ExecutionPlan>;

    fn f_down(&mut self, node: &'n Self::Node) -> Result<VisitRecursion> {
        if self.indent > 0 {
            writeln!(self.f)?;
        }
        write!(self.f, "{:indent$}", "", indent = self.indent * 2)?;
        write!(self.f, "{}", node)?;

        self.indent += 1;
        Ok(VisitRecursion::Continue)
    }

    fn f_up(&mut self, _node: &'n Self::Node) -> Result<VisitRecursion> {
        self.indent -= 1;
        Ok(VisitRecursion::Continue)
    }
}

pub struct TabularDisplay<'a, 'b> {
    schema: &'a LogicalSchema,
    tuples: &'b [Values],
}

impl<'a, 'b> TabularDisplay<'a, 'b> {
    pub fn new(schema: &'a LogicalSchema, tuples: &'b [Values]) -> Self {
        Self { schema, tuples }
    }
}

impl<'a, 'b> Display for TabularDisplay<'a, 'b> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.tuples.is_empty() {
            return writeln!(f, "Empty result set");
        }
        let fields = self.schema.fields();

        // Calculate initial widths with column name
        let mut widths = vec![0; fields.len()];
        for (i, field) in fields.iter().enumerate() {
            widths[i] = field.name.len();
        }

        // Prepare tuple data with split lines and calculate widths
        let mut tuple_lines: Vec<Vec<Vec<String>>> = Vec::new();
        let mut max_row_heights = Vec::new();

        for tuple in self.tuples {
            let mut row_lines: Vec<Vec<String>> = Vec::new();
            let mut max_height = 1;

            for (i, value) in tuple.iter().enumerate() {
                let lines: Vec<String> = match value {
                    Value::String(s) => s.lines().map(|line| line.to_string()).collect(),
                    _ => vec![value.to_string()],
                };

                // Update column width based on longest line
                let max_line_length = lines.iter().map(|line| line.len()).max().unwrap_or(0);
                widths[i] = widths[i].max(max_line_length);

                max_height = max_height.max(lines.len());
                row_lines.push(lines);
            }

            tuple_lines.push(row_lines);
            max_row_heights.push(max_height);
        }

        let print_border = |f: &mut Formatter<'_>| -> std::fmt::Result {
            write!(f, "+")?;
            for width in &widths {
                write!(f, "{:-<width$}+", "", width = width + 2)?;
            }
            writeln!(f)
        };

        // Print header
        print_border(f)?;
        write!(f, "|")?;
        for (i, field) in fields.iter().enumerate() {
            write!(f, " {:width$} |", field.name, width = widths[i])?;
        }
        writeln!(f)?;
        print_border(f)?;

        // Print rows with proper multi-line handling
        for (tuple_idx, row_lines) in tuple_lines.iter().enumerate() {
            let row_height = max_row_heights[tuple_idx];

            for line_idx in 0..row_height {
                write!(f, "|")?;
                for (col_idx, col_lines) in row_lines.iter().enumerate() {
                    let line = col_lines.get(line_idx).map(|s| s.as_str()).unwrap_or("");
                    write!(f, " {:width$} |", line, width = widths[col_idx])?;
                }
                writeln!(f)?;
            }
        }
        print_border(f)?;

        Ok(())
    }
}
