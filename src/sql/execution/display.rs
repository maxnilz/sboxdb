use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::Arc;

use crate::error::Result;
use crate::sql::execution::compiler::ExecutionPlan;
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
