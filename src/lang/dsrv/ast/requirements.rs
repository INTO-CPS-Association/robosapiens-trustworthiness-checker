//! Which expressions need a capability that not every runtime provides.

use contiguous_tree::TreeCursorExt;

use super::{ExprKind, ExprRef};
use crate::core::{Capabilities, Capability, Requirement};
use crate::lang::dsrv::expand::language::IfPolicy;

/// The capability an expression needs, with a name for messages. Every kind
/// is listed, with no wildcard, so a new kind must be placed before this
/// compiles.
pub(super) fn requirement(node: ExprRef<'_>) -> Option<(Capability, &'static str)> {
    match node.kind() {
        // Only the policy of the module that wrote the `if` decides, so an
        // `if` inlined from a lazy module into an eager one still needs it.
        ExprKind::If(..) if node.if_policy() == IfPolicy::Lazy => {
            Some((Capability::LazyIf, "a lazy `if`"))
        }
        ExprKind::MonitoredAt(..) => Some((Capability::Distribution, "`monitored_at`")),
        ExprKind::Dist(..) => Some((Capability::Distribution, "`dist`")),
        ExprKind::Constructor(..) => Some((Capability::TaggedUnions, "a union constructor")),
        ExprKind::Match(..) => Some((Capability::PatternMatching, "`match`")),
        ExprKind::Matches(..) => Some((Capability::PatternMatching, "`matches`")),
        ExprKind::If(..)
        | ExprKind::SIndex(..)
        | ExprKind::Val(..)
        | ExprKind::BinOp(..)
        | ExprKind::Cast(..)
        | ExprKind::Var(..)
        | ExprKind::Dynamic(..)
        | ExprKind::Defer(..)
        | ExprKind::Update(..)
        | ExprKind::Default(..)
        | ExprKind::IsDefined(..)
        | ExprKind::When(..)
        | ExprKind::Latch(..)
        | ExprKind::Init(..)
        | ExprKind::Not(..)
        | ExprKind::Neg(..)
        | ExprKind::Lambda(..)
        | ExprKind::Apply(..)
        | ExprKind::Fix(..)
        | ExprKind::Partial(..)
        | ExprKind::List(..)
        | ExprKind::Tuple(..)
        | ExprKind::LIndex(..)
        | ExprKind::LAppend(..)
        | ExprKind::LConcat(..)
        | ExprKind::LHead(..)
        | ExprKind::LTail(..)
        | ExprKind::LLen(..)
        | ExprKind::LMap(..)
        | ExprKind::LFilter(..)
        | ExprKind::LFold(..)
        | ExprKind::Map(..)
        | ExprKind::Struct(..)
        | ExprKind::ObjectLiteral(..)
        | ExprKind::MGet(..)
        | ExprKind::SGet(..)
        | ExprKind::MInsert(..)
        | ExprKind::MRemove(..)
        | ExprKind::MHasKey(..)
        | ExprKind::Sin(..)
        | ExprKind::Cos(..)
        | ExprKind::Tan(..)
        | ExprKind::Abs(..)
        | ExprKind::Trunc(..)
        | ExprKind::Floor(..)
        | ExprKind::Ceil(..)
        | ExprKind::Round(..) => None,
    }
}

pub(crate) fn first_unsupported(root: ExprRef<'_>, supported: Capabilities) -> Option<Requirement> {
    root.postorder().find_map(|node| {
        let (capability, construct) = requirement(node)?;
        (!supported.contains(capability)).then_some(Requirement {
            capability,
            construct,
            span: node.span(),
        })
    })
}
