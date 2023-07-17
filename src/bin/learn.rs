// some interesting exploration of Rust.
use std::ops::ControlFlow;

pub struct TreeNode<T> {
    value: T,
    left: Option<Box<TreeNode<T>>>,
    right: Option<Box<TreeNode<T>>>,
}

impl<T> TreeNode<T> {
    pub fn traverse_inorder1(&self, f: &mut impl FnMut(&T)) {
        if let Some(left) = &self.left {
            left.traverse_inorder1(f);
        }
        f(&self.value);
        if let Some(right) = &self.right {
            right.traverse_inorder1(f);
        }
    }

    pub fn traverse_inorder2<B>(&self, f: &mut impl FnMut(&T) -> ControlFlow<B>) -> ControlFlow<B> {
        if let Some(left) = &self.left {
            left.traverse_inorder2(f)?;
        }
        f(&self.value)?;
        if let Some(right) = &self.right {
            right.traverse_inorder2(f)?;
        }
        ControlFlow::Continue(())
    }

    fn leaf(value: T) -> Option<Box<TreeNode<T>>> {
        Some(Box::new(Self {
            value,
            left: None,
            right: None,
        }))
    }
}

fn main() {
    let node = TreeNode {
        value: 0,
        left: TreeNode::leaf(1),
        right: Some(Box::new(TreeNode {
            value: -1,
            left: TreeNode::leaf(5),
            right: TreeNode::leaf(2),
        })),
    };

    let mut sum1 = 0;
    node.traverse_inorder1(&mut |val| {
        sum1 += val;
    });
    println!("The sum1 is {}", sum1);

    let mut sum2 = 0;
    let res = node.traverse_inorder2(&mut |val| {
        if *val < 0 {
            ControlFlow::Break(*val)
        } else {
            sum2 += *val;
            ControlFlow::Continue(())
        }
    });
    println!("The sum2 is {}", sum2);
    println!("The res is {:?}", res);
}
