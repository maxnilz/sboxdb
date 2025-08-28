use std::sync::Arc;

use crate::error::Result;

#[macro_export]
macro_rules! apply_each {
    ($f:expr; $ARRAY:expr) => {{
        let mut action: VisitRecursion = VisitRecursion::Continue;
        for it in $ARRAY.iter() {
            action = $f(it)?;
            match action {
                VisitRecursion::Continue | VisitRecursion::Jump => {}
                VisitRecursion::Stop => return Ok(VisitRecursion::Stop)
            }
        }
        Ok::<VisitRecursion, crate::error::Error>(action)
    }};
    ($f:expr, $($x:expr),+ $(,)?) => {{
        let items = vec![$($x),+];
        let mut action: VisitRecursion = VisitRecursion::Continue;
        for it in items.iter() {
            action = $f(it)?;
            match action {
                VisitRecursion::Continue | VisitRecursion::Jump => {}
                VisitRecursion::Stop => return Ok(VisitRecursion::Stop)
            }
        }
        Ok::<VisitRecursion, crate::error::Error>(action)
    }};
}

#[macro_export]
macro_rules! map_each_children {
    ($f:expr; $ARRAY:expr) => {{
        let mut children = vec![];
        let mut recursion = VisitRecursion::Continue;
        let mut transformed = false;
        for it in $ARRAY.into_iter() {
            let res = $f(it)?;
            recursion = res.recursion;
            transformed |= transformed;
            children.push(res.data);
            match recursion {
                VisitRecursion::Continue | VisitRecursion::Jump => {}
                VisitRecursion::Stop => break,
            }
        }
        Ok::<Transformed<Vec<_>>, crate::error::Error>(Transformed::new(children, transformed, recursion))
    }};
    ($f:expr, $($x:expr),+ $(,)?) => {{
        let items = vec![$($x),+];
        let mut children = vec![];
        let mut recursion = VisitRecursion::Continue;
        let mut transformed = false;
        for it in items.into_iter() {
            let res = $f(it)?;
            recursion = res.recursion;
            transformed |= transformed;
            children.push(res.data);
            match recursion {
                VisitRecursion::Continue | VisitRecursion::Jump => {}
                VisitRecursion::Stop => break,
            }
        }
        Ok::<Transformed<Vec<_>>, crate::error::Error>(Transformed::new(children, transformed, recursion))
    }}
}

pub trait TreeNode: Sized {
    /// Visit the tree node with a [`TreeNodeVisitor`], performing a
    /// depth-first walk of the node and its children.
    ///
    /// [`TreeNodeVisitor::f_down()`] is called in top-down order (pre-order,
    /// or, before children are visited), [`TreeNodeVisitor::f_up()`] is
    /// called in bottom-up order (post-order, or, after children are visited).
    ///
    /// Use higher-ranked trait bounds(HRTB) to allow the visitor handle tree
    /// nodes borrowed with any lifetime, instead of a fixed one which is not
    /// feasible in case of the visitor pattern.
    ///
    /// NB: datafusion is using the fixed lifetime annotation on self, i.e.,
    /// fn visit<'n, V>(&'n self, visitor: &mut V)...,  which can be improved
    /// to get more flexibility. e.g., the main shortcoming is related to closure
    /// capture, like the following example:
    ///
    /// ```ignore
    ///
    /// use crate::error::Result;
    ///
    /// trait TreeNode: Sized {
    ///     fn visit<'n, V>(&'n self, visitor: &mut V) -> Result<VisitRecursion>
    ///         where
    ///             V: TreeNodeVisitor<'n, Node = Self>{}
    ///
    ///     // With the above visit lifetime bound, it would require the visit_children
    ///     // also use the same lifetime bound on the node.
    ///     fn visit_children<'n, F>(&'n self, f: F) -> Result<VisitRecursion>
    ///         where
    ///             F: FnMut(&'n Self) -> Result<VisitRecursion>;
    /// }
    ///
    /// // And this would limit the closure capture like this:
    /// struct Node;
    /// impl Node {
    ///     fn new() -> Self { Node }
    /// }
    /// impl TreeNode for Node {
    ///     fn visit_children<'n, F>(&'n self, f: F) -> Result<VisitRecursion>
    ///     where
    ///         F: FnMut(&'n Self) -> Result<VisitRecursion>
    ///     {
    ///         // This would limit closure capture - can't create temporary nodes
    ///         f(&Node::new()) // This won't work with fixed lifetime bounds
    ///     }
    /// }
    ///
    /// ```
    fn visit<V>(&self, visitor: &mut V) -> Result<VisitRecursion>
    where
        V: for<'n> TreeNodeVisitor<'n, Node = Self>,
    {
        visitor
            .f_down(self)?
            .when_children(|| self.visit_children(|c| c.visit(visitor)))?
            .when_parent(|| visitor.f_up(self))
    }

    /// Walk the tree node by performing a depth-first walk of the node and its
    /// children.
    ///
    /// Unlike the [`Self::visit`] is calling both f_down, and f_up hooks, walk
    /// call the given f before the children are visited.
    fn walk<F>(&self, mut f: F) -> Result<VisitRecursion>
    where
        F: FnMut(&Self) -> Result<VisitRecursion>,
    {
        fn walk_impl<N: TreeNode, F>(node: &N, f: &mut F) -> Result<VisitRecursion>
        where
            F: FnMut(&N) -> Result<VisitRecursion>,
        {
            f(node)?.when_children(|| node.visit_children(|c| walk_impl(c, f)))
        }

        walk_impl(self, &mut f)
    }

    /// Apply `f` to visit node's children (but **NOT** the node itself).
    fn visit_children<F>(&self, f: F) -> Result<VisitRecursion>
    where
        F: FnMut(&Self) -> Result<VisitRecursion>;

    /// Recursively rewrite the tree using `f` in a top-down (pre-order)
    /// fashion.
    ///
    /// `f` is applied to the node first, and then its children.
    fn transform_down<F>(self, mut f: F) -> Result<Transformed<Self>>
    where
        F: FnMut(Self) -> Result<Transformed<Self>>,
    {
        fn transform_down_impl<N: TreeNode, F>(node: N, f: &mut F) -> Result<Transformed<N>>
        where
            F: FnMut(N) -> Result<Transformed<N>>,
        {
            f(node)?.when_children(|n| n.map_children(|c| transform_down_impl(c, f)))
        }
        transform_down_impl(self, &mut f)
    }

    /// Apply `f` to rewrite the node's children (but **NOT** the node itself)
    fn map_children<F>(self, f: F) -> Result<Transformed<Self>>
    where
        F: FnMut(Self) -> Result<Transformed<Self>>;
}

/// Helper trait for implementing [`TreeNode`] that have children stored as
/// `Arc`s. If some trait object, such as `dyn T`, implements this trait,
/// its related `Arc<dyn T>` will automatically implement [`TreeNode`].
pub trait DynTreeNode {
    /// Returns all children of the specified `TreeNode`.
    fn arc_children(&self) -> Vec<&Arc<Self>>;
}

/// Blanket implementation for any `Arc<T>` where `T` implements [`DynTreeNode`]
impl<T: DynTreeNode + ?Sized> TreeNode for Arc<T> {
    fn visit_children<F>(&self, mut f: F) -> Result<VisitRecursion>
    where
        F: FnMut(&Self) -> Result<VisitRecursion>,
    {
        apply_each!(f; self.arc_children())
    }

    fn map_children<F>(self, _f: F) -> Result<Transformed<Self>>
    where
        F: FnMut(Self) -> Result<Transformed<Self>>,
    {
        let children = self.arc_children();
        if children.is_empty() {
            return Ok(Transformed::no(self));
        }
        todo!()
    }
}

pub trait TreeNodeVisitor<'n> {
    type Node: TreeNode;

    fn f_down(&mut self, _node: &'n Self::Node) -> Result<VisitRecursion> {
        Ok(VisitRecursion::Continue)
    }

    fn f_up(&mut self, _node: &'n Self::Node) -> Result<VisitRecursion> {
        Ok(VisitRecursion::Continue)
    }
}

/// VisitRecursion is used to drive the traversal control flow, i.e.,
/// Each actual visit function/closure would return [`VisitRecursion`]
/// to the traversal controller, e.g., [`TreeNode::visit`], to have it
/// to decide the next nodes to traverse.
#[derive(Debug, Copy, Clone, Eq, Hash, PartialEq)]
#[allow(dead_code)]
pub enum VisitRecursion {
    /// Continue recursion with the next node.
    Continue,
    /// In top-down traversals, skip recursing into children but continue with
    /// the next node, which actually means pruning of the subtree.
    ///
    /// In bottom-up traversals, bypass calling bottom-up closures till the next
    /// leaf node.
    ///
    /// In combined traversals, if it is the `f_down` (pre-order) phase, execution
    /// "jumps" to the next `f_up` (post-order) phase by shortcutting its children.
    /// If it is the `f_up` (post-order) phase, execution "jumps" to the next `f_down`
    /// (pre-order) phase by shortcutting its parent nodes until the first parent node
    /// having unvisited children path.
    Jump,
    /// Stop recursion.
    Stop,
}

impl VisitRecursion {
    /// Assuming we are getting a [`VisitRecursion`] result from a node, based on the
    /// returning state, decided whether to execute the provided closure to its
    /// children.
    pub fn when_children<F>(self, f: F) -> Result<VisitRecursion>
    where
        F: FnOnce() -> Result<VisitRecursion>,
    {
        match self {
            VisitRecursion::Continue => f(),
            VisitRecursion::Jump => Ok(VisitRecursion::Continue),
            VisitRecursion::Stop => Ok(self),
        }
    }

    /// Assuming we are getting a [`VisitRecursion`] result from a node, based on the
    /// returning state, decided whether to execute the provided closure to its
    /// sibling.
    pub fn when_sibling<F>(self, f: F) -> Result<VisitRecursion>
    where
        F: FnOnce() -> Result<VisitRecursion>,
    {
        match self {
            VisitRecursion::Continue | VisitRecursion::Jump => f(),
            VisitRecursion::Stop => Ok(self),
        }
    }

    /// Assuming we are getting a [`VisitRecursion`] result from a node, based on the
    /// returning state, decided whether to execute the provided closure to its
    /// parent.
    pub fn when_parent<F>(self, f: F) -> Result<VisitRecursion>
    where
        F: FnOnce() -> Result<VisitRecursion>,
    {
        match self {
            VisitRecursion::Continue => f(),
            VisitRecursion::Jump | VisitRecursion::Stop => Ok(self),
        }
    }
}

/// Result of tree walk / transformation APIs
///
/// `Transformed` is a wrapper around the tree node data (e.g. `Expr` or
/// `LogicalPlan`). It is used to indicate whether the node was transformed
/// and how the recursion should proceed.
#[derive(Debug, Eq, Hash, PartialEq, Clone)]
pub struct Transformed<T> {
    pub data: T,
    pub transformed: bool,
    pub recursion: VisitRecursion,
}

impl<T> Transformed<T> {
    pub fn new(data: T, transformed: bool, recursion: VisitRecursion) -> Self {
        Self { data, transformed, recursion }
    }

    pub fn yes(data: T) -> Self {
        Self { data, transformed: true, recursion: VisitRecursion::Continue }
    }

    pub fn no(data: T) -> Self {
        Self { data, transformed: false, recursion: VisitRecursion::Continue }
    }

    /// Assuming we are getting a [`VisitRecursion`] result from a node, based on the
    /// returning state, decided whether to execute the provided closure to its
    /// children.
    pub fn when_children<F>(mut self, f: F) -> Result<Transformed<T>>
    where
        F: FnOnce(T) -> Result<Transformed<T>>,
    {
        match self.recursion {
            VisitRecursion::Continue => f(self.data).map(|mut t| {
                t.transformed |= self.transformed;
                t
            }),
            VisitRecursion::Jump => {
                self.recursion = VisitRecursion::Continue;
                Ok(self)
            }
            VisitRecursion::Stop => Ok(self),
        }
    }

    /// Assuming we are getting a [`VisitRecursion`] result from a node, based on the
    /// returning state, decided whether to execute the provided closure to its
    /// sibling.
    pub fn when_sibling<F>(self, f: F) -> Result<Transformed<T>>
    where
        F: FnOnce(T) -> Result<Transformed<T>>,
    {
        match self.recursion {
            VisitRecursion::Continue | VisitRecursion::Jump => f(self.data).map(|mut t| {
                t.transformed |= self.transformed;
                t
            }),
            VisitRecursion::Stop => Ok(self),
        }
    }

    /// Assuming we are getting a [`VisitRecursion`] result from a node, based on the
    /// returning state, decided whether to execute the provided closure to its
    /// parent.
    pub fn when_parent<F>(self, f: F) -> Result<Transformed<T>>
    where
        F: FnOnce(T) -> Result<Transformed<T>>,
    {
        match self.recursion {
            VisitRecursion::Continue => f(self.data).map(|mut t| {
                t.transformed |= self.transformed;
                t
            }),
            VisitRecursion::Jump | VisitRecursion::Stop => Ok(self),
        }
    }

    /// Applies an infallible `f` to the data of this [`Transformed`] object,
    /// without modifying the `transformed` flag.
    pub fn update_data<U, F: FnOnce(T) -> U>(self, f: F) -> Transformed<U> {
        Transformed::new(f(self.data), self.transformed, self.recursion)
    }

    /// Applies a fallible `f` (returns `Result`) to the data of this
    /// [`Transformed`] object, without modifying the `transformed` flag.
    pub fn map_data<U, F: FnOnce(T) -> Result<U>>(self, f: F) -> Result<Transformed<U>> {
        f(self.data).map(|data| Transformed::new(data, self.transformed, self.recursion))
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Display;

    use super::*;

    #[derive(Debug, Eq, Hash, PartialEq, Clone)]
    struct TestTreeNode<T> {
        data: T,
        children: Vec<TestTreeNode<T>>,
    }

    impl<T> TestTreeNode<T> {
        fn new(data: T, children: Vec<TestTreeNode<T>>) -> Self {
            Self { data, children }
        }

        fn new_leaf(data: T) -> Self {
            Self { data, children: vec![] }
        }
    }

    impl<T> TreeNode for TestTreeNode<T> {
        fn visit_children<F>(&self, mut f: F) -> Result<VisitRecursion>
        where
            F: FnMut(&Self) -> Result<VisitRecursion>,
        {
            apply_each!(f; self.children)
        }

        fn map_children<F>(self, mut f: F) -> Result<Transformed<Self>>
        where
            F: FnMut(Self) -> Result<Transformed<Self>>,
        {
            map_each_children!(f; self.children)?.map_data(|new_children| {
                // new node with new children and other origin attrs.
                Ok(TestTreeNode { children: new_children, ..self })
            })
        }
    }

    type TestVisitorF<T> = Box<dyn FnMut(&TestTreeNode<T>) -> Result<VisitRecursion>>;
    struct TestVisitor<T> {
        visits: Vec<String>,
        f_down: TestVisitorF<T>,
        f_up: TestVisitorF<T>,
    }

    impl<T> TestVisitor<T> {
        fn new(f_down: TestVisitorF<T>, f_up: TestVisitorF<T>) -> Self {
            Self { visits: vec![], f_down, f_up }
        }
    }

    impl<T: Display> TreeNodeVisitor<'_> for TestVisitor<T> {
        type Node = TestTreeNode<T>;

        fn f_down(&mut self, node: &'_ Self::Node) -> Result<VisitRecursion> {
            self.visits.push(format!("f_down({})", node.data));
            (*self.f_down)(node)
        }

        fn f_up(&mut self, node: &'_ Self::Node) -> Result<VisitRecursion> {
            self.visits.push(format!("f_up({})", node.data));
            (*self.f_up)(node)
        }
    }

    fn visit_continue<T>(_node: &TestTreeNode<T>) -> Result<VisitRecursion> {
        Ok(VisitRecursion::Continue)
    }

    fn visit_event_on<T: PartialEq, D: Into<T>>(
        data: D,
        event: VisitRecursion,
    ) -> impl FnMut(&TestTreeNode<T>) -> Result<VisitRecursion> {
        let d = data.into();
        move |n| {
            if n.data == d {
                Ok(event)
            } else {
                Ok(VisitRecursion::Continue)
            }
        }
    }

    //       J
    //       |
    //       I
    //       |
    //       F
    //     /   \
    //    E     G
    //    |     |
    //    C     H
    //  /   \
    // B     D
    //       |
    //       A
    fn test_tree() -> TestTreeNode<String> {
        let node_a = TestTreeNode::new_leaf("a".to_string());
        let node_b = TestTreeNode::new_leaf("b".to_string());
        let node_d = TestTreeNode::new("d".to_string(), vec![node_a]);
        let node_c = TestTreeNode::new("c".to_string(), vec![node_b, node_d]);
        let node_e = TestTreeNode::new("e".to_string(), vec![node_c]);
        let node_h = TestTreeNode::new_leaf("h".to_string());
        let node_g = TestTreeNode::new("g".to_string(), vec![node_h]);
        let node_f = TestTreeNode::new("f".to_string(), vec![node_e, node_g]);
        let node_i = TestTreeNode::new("i".to_string(), vec![node_f]);
        TestTreeNode::new("j".to_string(), vec![node_i])
    }

    fn all_visits() -> Vec<String> {
        vec![
            "f_down(j)",
            "f_down(i)",
            "f_down(f)",
            "f_down(e)",
            "f_down(c)",
            "f_down(b)",
            "f_up(b)",
            "f_down(d)",
            "f_down(a)",
            "f_up(a)",
            "f_up(d)",
            "f_up(c)",
            "f_up(e)",
            "f_down(g)",
            "f_down(h)",
            "f_up(h)",
            "f_up(g)",
            "f_up(f)",
            "f_up(i)",
            "f_up(j)",
        ]
        .into_iter()
        .map(|s| s.to_string())
        .collect()
    }

    fn f_down_jump_on_e_visits() -> Vec<String> {
        vec![
            "f_down(j)",
            "f_down(i)",
            "f_down(f)",
            "f_down(e)",
            "f_up(e)",
            "f_down(g)",
            "f_down(h)",
            "f_up(h)",
            "f_up(g)",
            "f_up(f)",
            "f_up(i)",
            "f_up(j)",
        ]
        .into_iter()
        .map(|s| s.to_string())
        .collect()
    }

    // f_down Stop on E node
    fn f_down_stop_on_e_visits() -> Vec<String> {
        vec!["f_down(j)", "f_down(i)", "f_down(f)", "f_down(e)"]
            .into_iter()
            .map(|s| s.to_string())
            .collect()
    }
    // f_up Jump on E node
    fn f_up_jump_on_e_visits() -> Vec<String> {
        vec![
            "f_down(j)",
            "f_down(i)",
            "f_down(f)",
            "f_down(e)",
            "f_down(c)",
            "f_down(b)",
            "f_up(b)",
            "f_down(d)",
            "f_down(a)",
            "f_up(a)",
            "f_up(d)",
            "f_up(c)",
            "f_up(e)",
            "f_down(g)",
            "f_down(h)",
            "f_up(h)",
            "f_up(g)",
            "f_up(f)",
            "f_up(i)",
            "f_up(j)",
        ]
        .into_iter()
        .map(|s| s.to_string())
        .collect()
    }

    // f_up Stop on E node
    fn f_up_stop_on_e_visits() -> Vec<String> {
        vec![
            "f_down(j)",
            "f_down(i)",
            "f_down(f)",
            "f_down(e)",
            "f_down(c)",
            "f_down(b)",
            "f_up(b)",
            "f_down(d)",
            "f_down(a)",
            "f_up(a)",
            "f_up(d)",
            "f_up(c)",
            "f_up(e)",
        ]
        .into_iter()
        .map(|s| s.to_string())
        .collect()
    }

    // Expected transformed tree after a top-down traversal
    fn transformed_down_tree() -> TestTreeNode<String> {
        let node_a = TestTreeNode::new_leaf("f_down(a)".to_string());
        let node_b = TestTreeNode::new_leaf("f_down(b)".to_string());
        let node_d = TestTreeNode::new("f_down(d)".to_string(), vec![node_a]);
        let node_c = TestTreeNode::new("f_down(c)".to_string(), vec![node_b, node_d]);
        let node_e = TestTreeNode::new("f_down(e)".to_string(), vec![node_c]);
        let node_h = TestTreeNode::new_leaf("f_down(h)".to_string());
        let node_g = TestTreeNode::new("f_down(g)".to_string(), vec![node_h]);
        let node_f = TestTreeNode::new("f_down(f)".to_string(), vec![node_e, node_g]);
        let node_i = TestTreeNode::new("f_down(i)".to_string(), vec![node_f]);
        TestTreeNode::new("f_down(j)".to_string(), vec![node_i])
    }

    fn f_down_jump_on_a_transformed_down_tree() -> TestTreeNode<String> {
        let node_a = TestTreeNode::new_leaf("f_down(a)".to_string());
        let node_b = TestTreeNode::new_leaf("f_down(b)".to_string());
        let node_d = TestTreeNode::new("f_down(d)".to_string(), vec![node_a]);
        let node_c = TestTreeNode::new("f_down(c)".to_string(), vec![node_b, node_d]);
        let node_e = TestTreeNode::new("f_down(e)".to_string(), vec![node_c]);
        let node_h = TestTreeNode::new_leaf("f_down(h)".to_string());
        let node_g = TestTreeNode::new("f_down(g)".to_string(), vec![node_h]);
        let node_f = TestTreeNode::new("f_down(f)".to_string(), vec![node_e, node_g]);
        let node_i = TestTreeNode::new("f_down(i)".to_string(), vec![node_f]);
        TestTreeNode::new("f_down(j)".to_string(), vec![node_i])
    }

    fn f_down_jump_on_e_transformed_down_tree() -> TestTreeNode<String> {
        let node_a = TestTreeNode::new_leaf("a".to_string());
        let node_b = TestTreeNode::new_leaf("b".to_string());
        let node_d = TestTreeNode::new("d".to_string(), vec![node_a]);
        let node_c = TestTreeNode::new("c".to_string(), vec![node_b, node_d]);
        let node_e = TestTreeNode::new("f_down(e)".to_string(), vec![node_c]);
        let node_h = TestTreeNode::new_leaf("f_down(h)".to_string());
        let node_g = TestTreeNode::new("f_down(g)".to_string(), vec![node_h]);
        let node_f = TestTreeNode::new("f_down(f)".to_string(), vec![node_e, node_g]);
        let node_i = TestTreeNode::new("f_down(i)".to_string(), vec![node_f]);
        TestTreeNode::new("f_down(j)".to_string(), vec![node_i])
    }

    #[test]
    fn test_visit() -> Result<()> {
        let tree = test_tree();
        let mut visitor: TestVisitor<String> =
            TestVisitor::new(Box::new(visit_continue), Box::new(visit_continue));
        tree.visit(&mut visitor)?;
        assert_eq!(visitor.visits, all_visits());
        Ok(())
    }

    #[test]
    fn test_visit_f_down_jump_on_e() -> Result<()> {
        let tree = test_tree();
        let mut visitor: TestVisitor<String> = TestVisitor::new(
            Box::new(visit_event_on("e", VisitRecursion::Jump)),
            Box::new(visit_continue),
        );
        tree.visit(&mut visitor)?;
        assert_eq!(visitor.visits, f_down_jump_on_e_visits());
        Ok(())
    }

    #[test]
    fn test_visit_f_down_stop_on_e() -> Result<()> {
        let tree = test_tree();
        let mut visitor: TestVisitor<String> = TestVisitor::new(
            Box::new(visit_event_on("e", VisitRecursion::Stop)),
            Box::new(visit_continue),
        );
        tree.visit(&mut visitor)?;
        assert_eq!(visitor.visits, f_down_stop_on_e_visits());
        Ok(())
    }

    #[test]
    fn test_visit_f_up_jump_on_e() -> Result<()> {
        let tree = test_tree();
        let mut visitor: TestVisitor<String> = TestVisitor::new(
            Box::new(visit_continue),
            Box::new(visit_event_on("e", VisitRecursion::Jump)),
        );
        tree.visit(&mut visitor)?;
        for it in visitor.visits.iter() {
            println!("{it}")
        }
        assert_eq!(visitor.visits, f_up_jump_on_e_visits());
        Ok(())
    }

    #[test]
    fn test_visit_f_up_stop_on_e() -> Result<()> {
        let tree = test_tree();
        let mut visitor: TestVisitor<String> = TestVisitor::new(
            Box::new(visit_continue),
            Box::new(visit_event_on("e", VisitRecursion::Stop)),
        );
        tree.visit(&mut visitor)?;
        assert_eq!(visitor.visits, f_up_stop_on_e_visits());
        Ok(())
    }

    fn transform_yes<N: Display, T: Display + From<String>>(
        transformation_name: N,
    ) -> impl FnMut(TestTreeNode<T>) -> Result<Transformed<TestTreeNode<T>>> {
        move |node| {
            Ok(Transformed::yes(TestTreeNode::new(
                format!("{}({})", transformation_name, node.data).into(),
                node.children,
            )))
        }
    }
    fn transform_and_event_on<N: Display, T: PartialEq + Display + From<String>, D: Into<T>>(
        transformation_name: N,
        data: D,
        event: VisitRecursion,
    ) -> impl FnMut(TestTreeNode<T>) -> Result<Transformed<TestTreeNode<T>>> {
        let d = data.into();
        move |node| {
            let new_node = TestTreeNode::new(
                format!("{}({})", transformation_name, node.data).into(),
                node.children,
            );
            Ok(if node.data == d {
                Transformed::new(new_node, true, event)
            } else {
                Transformed::yes(new_node)
            })
        }
    }

    #[test]
    fn test_transform_down() -> Result<()> {
        let tree = test_tree();
        let f = transform_yes("f_down");
        let got = tree.transform_down(f)?;
        assert_eq!(got, Transformed::yes(transformed_down_tree()));
        Ok(())
    }

    #[test]
    fn test_transform_down_f_down_jump_on_a() -> Result<()> {
        let tree = test_tree();
        let f = transform_and_event_on("f_down", "a", VisitRecursion::Jump);
        let got = tree.transform_down(f)?;
        assert_eq!(got, Transformed::yes(f_down_jump_on_a_transformed_down_tree()));
        Ok(())
    }

    #[test]
    fn test_transform_down_f_down_jump_on_e() -> Result<()> {
        let tree = test_tree();
        let f = transform_and_event_on("f_down", "e", VisitRecursion::Jump);
        let got = tree.transform_down(f)?;
        assert_eq!(got, Transformed::yes(f_down_jump_on_e_transformed_down_tree()));
        Ok(())
    }

    struct TestWalker<T> {
        visits: Vec<String>,
        f: TestVisitorF<T>,
    }

    impl<T: Display> TestWalker<T> {
        fn new(f: TestVisitorF<T>) -> Self {
            Self { visits: vec![], f }
        }
        fn walk(&mut self, node: &TestTreeNode<T>) -> Result<VisitRecursion> {
            self.visits.push(format!("walk({})", node.data));
            (*self.f)(node)
        }
    }

    fn walk_stop_on_e() -> Vec<String> {
        vec!["walk(j)", "walk(i)", "walk(f)", "walk(e)"]
            .into_iter()
            .map(|s| s.to_string())
            .collect()
    }

    fn walk_jump_on_e() -> Vec<String> {
        vec!["walk(j)", "walk(i)", "walk(f)", "walk(e)", "walk(g)", "walk(h)"]
            .into_iter()
            .map(|s| s.to_string())
            .collect()
    }
    fn walk_all() -> Vec<String> {
        vec![
            "walk(j)", "walk(i)", "walk(f)", "walk(e)", "walk(c)", "walk(b)", "walk(d)", "walk(a)",
            "walk(g)", "walk(h)",
        ]
        .into_iter()
        .map(|s| s.to_string())
        .collect()
    }

    #[test]
    fn test_walk() -> Result<()> {
        let tree = test_tree();
        let mut waker = TestWalker::new(Box::new(visit_continue));
        tree.walk(|node| waker.walk(node))?;
        assert_eq!(waker.visits, walk_all());
        Ok(())
    }

    #[test]
    fn test_walk_jump_on_e() -> Result<()> {
        let tree = test_tree();
        let mut waker = TestWalker::new(Box::new(visit_event_on("e", VisitRecursion::Jump)));
        tree.walk(|node| waker.walk(node))?;
        println!("{:?}", waker.visits);
        assert_eq!(waker.visits, walk_jump_on_e());
        Ok(())
    }

    #[test]
    fn test_walk_stop_on_e() -> Result<()> {
        let tree = test_tree();
        let mut waker = TestWalker::new(Box::new(visit_event_on("e", VisitRecursion::Stop)));
        tree.walk(|node| waker.walk(node))?;
        assert_eq!(waker.visits, walk_stop_on_e());
        Ok(())
    }
}
