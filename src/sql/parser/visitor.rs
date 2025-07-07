use core::ops::ControlFlow;

use crate::sql::parser::ast::Ident;
use crate::sql::parser::{Expr, Query, Statement, TableFactor, Value};

/// A type that can be visited by a [`Visitor`]. See [`Visitor`] for
/// recursively visiting parsed SQL statements.
///
/// # Note
///
/// This trait should be automatically derived for the AST nodes
/// using the [Visit](sqlparser_derive::Visit) proc macro.
///
/// ```text
/// #[cfg_attr(derive(Visit, VisitMut))]
/// ```
pub trait Visit {
    fn visit<V: Visitor>(&self, visitor: &mut V) -> ControlFlow<V::Break>;
}

/// A type that can be visited by a [`VisitorMut`]. See [`VisitorMut`] for
/// recursively visiting parsed SQL statements.
///
/// # Note
///
/// This trait should be automatically derived for the AST nodes
/// using the [VisitMut](sqlparser_derive::VisitMut) proc macro.
///
/// ```text
/// #[cfg_attr(derive(Visit, VisitMut))]
/// ```
pub trait VisitMut {
    fn visit<V: VisitorMut>(&mut self, visitor: &mut V) -> ControlFlow<V::Break>;
}

impl<T: Visit> Visit for Option<T> {
    fn visit<V: Visitor>(&self, visitor: &mut V) -> ControlFlow<V::Break> {
        if let Some(s) = self {
            s.visit(visitor)?;
        }
        ControlFlow::Continue(())
    }
}

impl<T: Visit> Visit for Vec<T> {
    fn visit<V: Visitor>(&self, visitor: &mut V) -> ControlFlow<V::Break> {
        for v in self {
            v.visit(visitor)?;
        }
        ControlFlow::Continue(())
    }
}

impl<T: Visit> Visit for Box<T> {
    fn visit<V: Visitor>(&self, visitor: &mut V) -> ControlFlow<V::Break> {
        T::visit(self, visitor)
    }
}

impl<T: VisitMut> VisitMut for Option<T> {
    fn visit<V: VisitorMut>(&mut self, visitor: &mut V) -> ControlFlow<V::Break> {
        if let Some(s) = self {
            s.visit(visitor)?;
        }
        ControlFlow::Continue(())
    }
}

impl<T: VisitMut> VisitMut for Vec<T> {
    fn visit<V: VisitorMut>(&mut self, visitor: &mut V) -> ControlFlow<V::Break> {
        for v in self {
            v.visit(visitor)?;
        }
        ControlFlow::Continue(())
    }
}

impl<T: VisitMut> VisitMut for Box<T> {
    fn visit<V: VisitorMut>(&mut self, visitor: &mut V) -> ControlFlow<V::Break> {
        T::visit(self, visitor)
    }
}

macro_rules! visit_noop {
    ($($t:ty),+) => {
        $(impl Visit for $t {
            fn visit<V: Visitor>(&self, _visitor: &mut V) -> ControlFlow<V::Break> {
               ControlFlow::Continue(())
            }
        })+
        $(impl VisitMut for $t {
            fn visit<V: VisitorMut>(&mut self, _visitor: &mut V) -> ControlFlow<V::Break> {
               ControlFlow::Continue(())
            }
        })+
    };
}

visit_noop!(u8, u16, u32, u64, i8, i16, i32, i64, char, bool, String);

/// A visitor that can be used to walk an AST tree.
///
/// `pre_visit_` methods are invoked before visiting all children of the
/// node and `post_visit_` methods are invoked after visiting all
/// children of the node.
pub trait Visitor {
    /// Type returned when the recursion returns early.
    type Break;

    /// Invoked for any queries that appear in the AST before visiting children
    fn pre_visit_query(&mut self, _query: &Query) -> ControlFlow<Self::Break> {
        ControlFlow::Continue(())
    }

    /// Invoked for any queries that appear in the AST after visiting children
    fn post_visit_query(&mut self, _query: &Query) -> ControlFlow<Self::Break> {
        ControlFlow::Continue(())
    }

    /// Invoked for any relations (e.g. tables) that appear in the AST before visiting children
    fn pre_visit_relation(&mut self, _relation: &Ident) -> ControlFlow<Self::Break> {
        ControlFlow::Continue(())
    }

    /// Invoked for any relations (e.g. tables) that appear in the AST after visiting children
    fn post_visit_relation(&mut self, _relation: &Ident) -> ControlFlow<Self::Break> {
        ControlFlow::Continue(())
    }

    /// Invoked for any table factors that appear in the AST before visiting children
    fn pre_visit_table_factor(&mut self, _table_factor: &TableFactor) -> ControlFlow<Self::Break> {
        ControlFlow::Continue(())
    }

    /// Invoked for any table factors that appear in the AST after visiting children
    fn post_visit_table_factor(&mut self, _table_factor: &TableFactor) -> ControlFlow<Self::Break> {
        ControlFlow::Continue(())
    }

    /// Invoked for any expressions that appear in the AST before visiting children
    fn pre_visit_expr(&mut self, _expr: &Expr) -> ControlFlow<Self::Break> {
        ControlFlow::Continue(())
    }

    /// Invoked for any expressions that appear in the AST
    fn post_visit_expr(&mut self, _expr: &Expr) -> ControlFlow<Self::Break> {
        ControlFlow::Continue(())
    }

    /// Invoked for any statements that appear in the AST before visiting children
    fn pre_visit_statement(&mut self, _statement: &Statement) -> ControlFlow<Self::Break> {
        ControlFlow::Continue(())
    }

    /// Invoked for any statements that appear in the AST after visiting children
    fn post_visit_statement(&mut self, _statement: &Statement) -> ControlFlow<Self::Break> {
        ControlFlow::Continue(())
    }

    /// Invoked for any Value that appear in the AST before visiting children
    fn pre_visit_value(&mut self, _value: &Value) -> ControlFlow<Self::Break> {
        ControlFlow::Continue(())
    }

    /// Invoked for any Value that appear in the AST after visiting children
    fn post_visit_value(&mut self, _value: &Value) -> ControlFlow<Self::Break> {
        ControlFlow::Continue(())
    }
}

/// A visitor that can be used to mutate an AST tree.
///
/// `pre_visit_` methods are invoked before visiting all children of the
/// node and `post_visit_` methods are invoked after visiting all
/// children of the node.
pub trait VisitorMut {
    /// Type returned when the recursion returns early.
    type Break;

    /// Invoked for any queries that appear in the AST before visiting children
    fn pre_visit_query(&mut self, _query: &mut Query) -> ControlFlow<Self::Break> {
        ControlFlow::Continue(())
    }

    /// Invoked for any queries that appear in the AST after visiting children
    fn post_visit_query(&mut self, _query: &mut Query) -> ControlFlow<Self::Break> {
        ControlFlow::Continue(())
    }

    /// Invoked for any relations (e.g. tables) that appear in the AST before visiting children
    fn pre_visit_relation(&mut self, _relation: &mut Ident) -> ControlFlow<Self::Break> {
        ControlFlow::Continue(())
    }

    /// Invoked for any relations (e.g. tables) that appear in the AST after visiting children
    fn post_visit_relation(&mut self, _relation: &mut Ident) -> ControlFlow<Self::Break> {
        ControlFlow::Continue(())
    }

    /// Invoked for any table factors that appear in the AST before visiting children
    fn pre_visit_table_factor(
        &mut self,
        _table_factor: &mut TableFactor,
    ) -> ControlFlow<Self::Break> {
        ControlFlow::Continue(())
    }

    /// Invoked for any table factors that appear in the AST after visiting children
    fn post_visit_table_factor(
        &mut self,
        _table_factor: &mut TableFactor,
    ) -> ControlFlow<Self::Break> {
        ControlFlow::Continue(())
    }

    /// Invoked for any expressions that appear in the AST before visiting children
    fn pre_visit_expr(&mut self, _expr: &mut Expr) -> ControlFlow<Self::Break> {
        ControlFlow::Continue(())
    }

    /// Invoked for any expressions that appear in the AST
    fn post_visit_expr(&mut self, _expr: &mut Expr) -> ControlFlow<Self::Break> {
        ControlFlow::Continue(())
    }

    /// Invoked for any statements that appear in the AST before visiting children
    fn pre_visit_statement(&mut self, _statement: &mut Statement) -> ControlFlow<Self::Break> {
        ControlFlow::Continue(())
    }

    /// Invoked for any statements that appear in the AST after visiting children
    fn post_visit_statement(&mut self, _statement: &mut Statement) -> ControlFlow<Self::Break> {
        ControlFlow::Continue(())
    }

    /// Invoked for any value that appear in the AST before visiting children
    fn pre_visit_value(&mut self, _value: &mut Value) -> ControlFlow<Self::Break> {
        ControlFlow::Continue(())
    }

    /// Invoked for any statements that appear in the AST after visiting children
    fn post_visit_value(&mut self, _value: &mut Value) -> ControlFlow<Self::Break> {
        ControlFlow::Continue(())
    }
}
