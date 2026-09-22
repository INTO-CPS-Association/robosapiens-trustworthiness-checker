//! Which expressions need a capability that not every runtime provides.

use contiguous_tree::TreeCursorExt;

use super::{ExprKind, ExprRef};
use crate::core::{RuntimeCapabilities, RuntimeCapability, RuntimeCapabilityRequirement};
use crate::lang::dsrv::expand::language::IfPolicy;

/// The capability an expression needs, with a name for messages. Every kind
/// is listed, with no wildcard, so a new kind must be placed before this
/// compiles.
pub(super) fn requirement(node: ExprRef<'_>) -> Option<(RuntimeCapability, &'static str)> {
    match node.kind() {
        // Only the policy of the module that wrote the `if` decides, so an
        // `if` inlined from a lazy module into an eager one still needs it.
        ExprKind::If(..) if node.if_policy() == IfPolicy::Lazy => {
            Some((RuntimeCapability::LazyIf, "a lazy `if`"))
        }
        ExprKind::MonitoredAt(..) => Some((RuntimeCapability::Distribution, "`monitored_at`")),
        ExprKind::Dist(..) => Some((RuntimeCapability::Distribution, "`dist`")),
        ExprKind::Constructor(..) => Some((RuntimeCapability::TaggedUnions, "a union constructor")),
        ExprKind::Match(..) => Some((RuntimeCapability::PatternMatching, "`match`")),
        ExprKind::Matches(..) => Some((RuntimeCapability::PatternMatching, "`matches`")),
        ExprKind::If(..)
        | ExprKind::SIndex(..)
        | ExprKind::Val(..)
        | ExprKind::BinOp(..)
        | ExprKind::Cast(..)
        | ExprKind::Ascribe(..)
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

pub(crate) fn first_unsupported_construct(
    root: ExprRef<'_>,
    supported: RuntimeCapabilities,
) -> Option<RuntimeCapabilityRequirement> {
    root.postorder().find_map(|node| {
        let (capability, construct) = requirement(node)?;
        (!supported.contains(capability)).then_some(RuntimeCapabilityRequirement {
            capability,
            construct,
            span: node.span(),
        })
    })
}
