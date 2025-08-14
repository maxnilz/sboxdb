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

    fn visit_children<F>(&self, f: F) -> Result<VisitRecursion>
    where
        F: FnMut(&Self) -> Result<VisitRecursion>;
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
#[derive(Copy, Clone)]
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
}

#[cfg(test)]
mod tests {
    use std::fmt::Display;

    use super::*;

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
