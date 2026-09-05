use std::mem::offset_of;

/// A scalar representation supported by typed monitor evaluation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum TypedKind {
    Int,
    Float,
    Bool,
}

/// One field in a typed tuple row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TypedField {
    kind: TypedKind,
    offset: usize,
}

impl TypedField {
    const fn new(kind: TypedKind, offset: usize) -> Self {
        Self { kind, offset }
    }

    pub fn kind(self) -> TypedKind {
        self.kind
    }

    pub fn offset(self) -> usize {
        self.offset
    }
}

mod sealed {
    pub trait Scalar {}
    pub trait Input {}
    pub trait Output {}
}

/// A primitive scalar that can be loaded directly by a typed monitor backend.
pub trait TypedScalar: sealed::Scalar + Copy + 'static {
    const KIND: TypedKind;
}

impl sealed::Scalar for i64 {}
impl TypedScalar for i64 {
    const KIND: TypedKind = TypedKind::Int;
}
impl sealed::Scalar for f64 {}
impl TypedScalar for f64 {
    const KIND: TypedKind = TypedKind::Float;
}
impl sealed::Scalar for bool {}
impl TypedScalar for bool {
    const KIND: TypedKind = TypedKind::Bool;
}

/// A positional tuple accepted as a typed monitor input row.
pub trait TypedInput: sealed::Input + Sized + 'static {
    #[doc(hidden)]
    fn typed_fields() -> Box<[TypedField]>;
}

/// A positional tuple produced as a typed monitor output row.
pub trait TypedOutput: sealed::Output + Sized + 'static {
    #[doc(hidden)]
    fn typed_fields() -> Box<[TypedField]>;
}

macro_rules! impl_direct_row {
    ($type:ident, $trait:ident; $($index:tt => $T:ident),+ $(,)?) => {
        impl<$($T: TypedScalar),+> sealed::$type for ($($T,)+) {}
        impl<$($T: TypedScalar),+> $trait for ($($T,)+) {
            fn typed_fields() -> Box<[TypedField]> {
                vec![$(
                    TypedField::new($T::KIND, offset_of!(Self, $index)),
                )+].into_boxed_slice()
            }
        }
    };
}

impl sealed::Input for () {}
impl TypedInput for () {
    fn typed_fields() -> Box<[TypedField]> {
        Box::new([])
    }
}
impl sealed::Output for () {}
impl TypedOutput for () {
    fn typed_fields() -> Box<[TypedField]> {
        Box::new([])
    }
}

impl_direct_row!(Input, TypedInput; 0 => A);
impl_direct_row!(Input, TypedInput; 0 => A, 1 => B);
impl_direct_row!(Input, TypedInput; 0 => A, 1 => B, 2 => C);
impl_direct_row!(Input, TypedInput; 0 => A, 1 => B, 2 => C, 3 => D);
impl_direct_row!(Input, TypedInput; 0 => A, 1 => B, 2 => C, 3 => D, 4 => E);
impl_direct_row!(Input, TypedInput; 0 => A, 1 => B, 2 => C, 3 => D, 4 => E, 5 => F);
impl_direct_row!(Input, TypedInput; 0 => A, 1 => B, 2 => C, 3 => D, 4 => E, 5 => F, 6 => G);
impl_direct_row!(Input, TypedInput; 0 => A, 1 => B, 2 => C, 3 => D, 4 => E, 5 => F, 6 => G, 7 => H);
impl_direct_row!(Output, TypedOutput; 0 => A);
impl_direct_row!(Output, TypedOutput; 0 => A, 1 => B);
impl_direct_row!(Output, TypedOutput; 0 => A, 1 => B, 2 => C);
impl_direct_row!(Output, TypedOutput; 0 => A, 1 => B, 2 => C, 3 => D);
impl_direct_row!(Output, TypedOutput; 0 => A, 1 => B, 2 => C, 3 => D, 4 => E);
impl_direct_row!(Output, TypedOutput; 0 => A, 1 => B, 2 => C, 3 => D, 4 => E, 5 => F);
impl_direct_row!(Output, TypedOutput; 0 => A, 1 => B, 2 => C, 3 => D, 4 => E, 5 => F, 6 => G);
impl_direct_row!(Output, TypedOutput; 0 => A, 1 => B, 2 => C, 3 => D, 4 => E, 5 => F, 6 => G, 7 => H);
