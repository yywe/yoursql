use crate::common::types::DataValue;
use crate::expr::expr::Expr;
pub trait Literal {
    fn lit(&self) -> Expr;
}
pub fn lit<T: Literal>(n: T) -> Expr {
    n.lit()
}

impl Literal for &str {
    fn lit(&self) -> Expr {
        Expr::Literal(DataValue::Utf8(Some((*self).to_owned())))
    }
}

impl Literal for String {
    fn lit(&self) -> Expr {
        Expr::Literal(DataValue::Utf8(Some((*self).to_owned())))
    }
}

impl Literal for &String {
    fn lit(&self) -> Expr {
        Expr::Literal(DataValue::Utf8(Some((*self).to_owned())))
    }
}

impl Literal for Vec<u8> {
    fn lit(&self) -> Expr {
        Expr::Literal(DataValue::Binary(Some((*self).to_owned())))
    }
}

impl Literal for &[u8] {
    fn lit(&self) -> Expr {
        Expr::Literal(DataValue::Binary(Some((*self).to_owned())))
    }
}

macro_rules! make_literal {
    ($TYPE: ty, $SCALAR: ident, $DOC: expr) => {
        #[doc=$DOC]
        impl Literal for $TYPE {
            fn lit(&self) -> Expr {
                Expr::Literal(DataValue::$SCALAR(Some(self.clone())))
            }
        }
    };
}

make_literal!(bool, Boolean, "literal expression containing a bool");
make_literal!(f32, Float32, "literal expression containing a f32");
make_literal!(f64, Float64, "literal expression containing a f64");
make_literal!(i8, Int8, "literal expression containing a i8");
make_literal!(i16, Int16, "literal expression containing a i16");
make_literal!(i32, Int32, "literal expression containing a i32");
make_literal!(i64, Int64, "literal expression containing a i64");
make_literal!(u8, UInt8, "literal expression containing a u8");
make_literal!(u16, UInt16, "literal expression containing a u16");
make_literal!(u32, UInt32, "literal expression containing a u32");
make_literal!(u64, UInt64, "literal expression containing a u64");
