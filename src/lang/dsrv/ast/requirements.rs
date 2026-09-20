//! Which expressions need a capability that not every runtime provides.

use super::ExprKind;
use crate::core::Capability;

/// The capability an expression needs, with a name for messages. Every kind
/// is listed, with no wildcard, so a new kind must be placed before this
/// compiles.
pub(super) fn requirement(kind: &ExprKind) -> Option<(Capability, &'static str)> {
    match kind {
        ExprKind::MonitoredAt(..) => Some((Capability::Distribution, "`monitored_at`")),
        ExprKind::Dist(..) => Some((Capability::Distribution, "`dist`")),
        ExprKind::If(..)
        | ExprKind::SIndex(..)
        | ExprKind::Val(..)
        | ExprKind::BinOp(..)
        | ExprKind::Var(..)
        // A constructor never reaches a runtime: elaboration resolves it
        // into a union value, which is where the capability is required.
        | ExprKind::Constructor(..)
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
        | ExprKind::Abs(..) => None,
    }
}
