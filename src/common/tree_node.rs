use anyhow::Result;
use std::sync::Arc;
#[derive(Debug)]
pub enum VisitRecursion {
    Continue,
    Skip,
    Stop,
}

#[derive(Debug)]
pub enum RewriteRecursion {
    Continue,
    Mutate, // call op right now and return
    Stop,
    Skip,
}

pub enum Transformed<T> {
    Yes(T),
    No(T),
}

impl<T> Transformed<T> {
    pub fn into(self) -> T {
        match self {
            Transformed::Yes(t) => t,
            Transformed::No(t) => t,
        }
    }

    pub fn into_pair(self) -> (T, bool) {
        match self {
            Transformed::Yes(t) => (t, true),
            Transformed::No(t) => (t, false),
        }
    }
}

pub trait TreeNode: Sized {
    fn apply<F>(&self, op: &mut F) -> Result<VisitRecursion>
    where
        F: FnMut(&Self) -> Result<VisitRecursion>,
    {
        match op(self)? {
            VisitRecursion::Continue => {}
            VisitRecursion::Skip => return Ok(VisitRecursion::Continue),
            VisitRecursion::Stop => return Ok(VisitRecursion::Stop),
        }
        self.apply_children(&mut |node| node.apply(op))
    }

    fn apply_children<F>(&self, op: &mut F) -> Result<VisitRecursion>
    where
        F: FnMut(&Self) -> Result<VisitRecursion>;

    /// apply can visit the tree, but here again we have this visit function
    /// the main motivation is to provide an interface that can do some job
    /// before and after the visit, using apply can simply visit but cannot record
    /// before and after node visit status
    ///
    /// similar motivation for below tranform and rewrite operation
    fn visit<V: TreeNodeVisitor<N = Self>>(&self, visitor: &mut V) -> Result<VisitRecursion> {
        match visitor.pre_visit(self)? {
            VisitRecursion::Continue => {}
            VisitRecursion::Skip => return Ok(VisitRecursion::Continue),
            VisitRecursion::Stop => return Ok(VisitRecursion::Stop),
        };
        match self.apply_children(&mut |node| node.visit(visitor))? {
            VisitRecursion::Continue => {}
            VisitRecursion::Skip => return Ok(VisitRecursion::Continue),
            VisitRecursion::Stop => return Ok(VisitRecursion::Stop),
        }
        visitor.post_visit(self)
    }

    fn transform<F>(self, op: &F) -> Result<Self>
    where
        F: Fn(Self) -> Result<Transformed<Self>>,
    {
        self.transform_up(op)
    }

    fn transform_down<F>(self, op: &F) -> Result<Self>
    where
        F: Fn(Self) -> Result<Transformed<Self>>,
    {
        let after_op = op(self)?.into();
        after_op.map_children(|node| node.transform_down(op))
    }

    fn transform_up<F>(self, op: &F) -> Result<Self>
    where
        F: Fn(Self) -> Result<Transformed<Self>>,
    {
        let after_op_children = self.map_children(|node| node.transform_up(op))?;
        let new_node = op(after_op_children)?.into();
        Ok(new_node)
    }

    fn map_children<F>(self, transform: F) -> Result<Self>
    where
        F: FnMut(Self) -> Result<Self>;

    fn rewrite<R: TreeNodeRewriter<N = Self>>(self, rewriter: &mut R) -> Result<Self> {
        let need_mutate = match rewriter.pre_visit(&self)? {
            RewriteRecursion::Mutate => return rewriter.mutate(self),
            RewriteRecursion::Stop => return Ok(self),
            RewriteRecursion::Continue => true,
            RewriteRecursion::Skip => false,
        };
        let after_op_children = self.map_children(|node| node.rewrite(rewriter))?;
        if need_mutate {
            rewriter.mutate(after_op_children)
        } else {
            Ok(after_op_children)
        }
    }
}

pub trait TreeNodeVisitor: Sized {
    type N: TreeNode;
    fn pre_visit(&mut self, node: &Self::N) -> Result<VisitRecursion>;
    fn post_visit(&mut self, _node: &Self::N) -> Result<VisitRecursion> {
        Ok(VisitRecursion::Continue)
    }
}

pub trait TreeNodeRewriter: Sized {
    type N: TreeNode;
    fn pre_visit(&mut self, _node: &Self::N) -> Result<RewriteRecursion> {
        Ok(RewriteRecursion::Continue)
    }
    fn mutate(&mut self, node: Self::N) -> Result<Self::N>;
}

pub trait DynTreeNode {
    fn arc_chilren(&self) -> Vec<Arc<Self>>;
    fn with_new_arc_children(
        &self,
        arc_self: Arc<Self>,
        new_children: Vec<Arc<Self>>,
    ) -> Result<Arc<Self>>;
}

impl<T: DynTreeNode + ?Sized> TreeNode for Arc<T> {
    fn apply_children<F>(&self, op: &mut F) -> Result<VisitRecursion>
    where
        F: FnMut(&Self) -> Result<VisitRecursion>,
    {
        for child in self.arc_chilren() {
            match op(&child)? {
                VisitRecursion::Continue => {}
                VisitRecursion::Skip => return Ok(VisitRecursion::Continue),
                VisitRecursion::Stop => return Ok(VisitRecursion::Stop),
            }
        }
        Ok(VisitRecursion::Continue)
    }

    fn map_children<F>(self, transform: F) -> Result<Self>
    where
        F: FnMut(Self) -> Result<Self>,
    {
        let children = self.arc_chilren();
        if !children.is_empty() {
            let new_children: Result<Vec<_>> = children.into_iter().map(transform).collect();
            let arc_self = Arc::clone(&self);
            self.with_new_arc_children(arc_self, new_children?)
        } else {
            Ok(self)
        }
    }
}
