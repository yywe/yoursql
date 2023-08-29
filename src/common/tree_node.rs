use anyhow::Result;

#[derive(Debug)]
pub enum VisitRecursion {
    Continue,
    Skip,
    Stop,
}

pub trait TreeNode: Sized {
    fn apply<F>(&self, op: &mut F) -> Result<VisitRecursion>
    where F: FnMut(&Self) -> Result<VisitRecursion> {
        match op(self)? {
            VisitRecursion::Continue => {}
            VisitRecursion::Skip => return Ok(VisitRecursion::Continue),
            VisitRecursion::Stop => return Ok(VisitRecursion::Stop),
        }
        self.apply_children(&mut |node| node.apply(op))
    }


    fn apply_children<F>(&self, op: &mut F) -> Result<VisitRecursion> 
    where  F: FnMut(&Self) -> Result<VisitRecursion>;

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
}


pub trait TreeNodeVisitor: Sized {
    type N: TreeNode;
    fn pre_visit(&mut self, node: &Self::N) -> Result<VisitRecursion>;
    fn post_visit(&mut self, _node: &Self::N) -> Result<VisitRecursion> {
        Ok(VisitRecursion::Continue)
    }
}