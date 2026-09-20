//! Patterns, and what it means for a value to match one.
//!
//! Nothing here touches streams: a pattern is matched against one value, and
//! an arm is selected from a list of them. The stream and graph evaluators
//! both go through [`select_arm`], so they agree on which arm runs and on
//! what its binders hold, and the rules can be tested on values alone.
//!
//! Case decides what a name means, here as in an expression: a capitalised
//! name is a tag, a lower-case one binds.

use ecow::{EcoString, EcoVec};

use crate::VarName;
use crate::core::Value;
use crate::lang::dsrv::ast::SyntaxLiteral;

use super::span::Span;

/// The names a pattern bound, in the order it bound them.
pub type Bindings = EcoVec<(VarName, Value)>;

#[derive(Clone, Debug, PartialEq)]
pub struct MatchPattern {
    pub kind: PatternKind,
    pub span: Span,
}

impl MatchPattern {
    pub fn new(kind: PatternKind, span: Span) -> Self {
        Self { kind, span }
    }

    /// Whether this pattern matches every value of its type, which is what
    /// makes a later arm unreachable.
    pub fn is_irrefutable(&self) -> bool {
        match &self.kind {
            PatternKind::Wildcard | PatternKind::Bind(_) => true,
            PatternKind::As(_, inner) => inner.is_irrefutable(),
            PatternKind::Or(alternatives) => alternatives.iter().any(MatchPattern::is_irrefutable),
            PatternKind::Tag { .. }
            | PatternKind::Tuple(_)
            | PatternKind::Struct { .. }
            | PatternKind::List(_)
            | PatternKind::Literal(_)
            | PatternKind::Range { .. } => false,
        }
    }

    /// Every name this pattern binds, in source order. An or-pattern binds
    /// the names of its first alternative; checking requires the rest to
    /// agree.
    pub fn bound_names(&self) -> Vec<VarName> {
        let mut names = Vec::new();
        self.collect_names(&mut names);
        names
    }

    /// The same names, borrowed from the pattern, for a traversal that
    /// tracks what is in scope without cloning.
    pub fn bound_name_refs(&self) -> Vec<&VarName> {
        let mut names = Vec::new();
        self.collect_name_refs(&mut names);
        names
    }

    fn collect_name_refs<'a>(&'a self, names: &mut Vec<&'a VarName>) {
        match &self.kind {
            PatternKind::Wildcard | PatternKind::Literal(_) | PatternKind::Range { .. } => {}
            PatternKind::Bind(name) => names.push(name),
            PatternKind::As(name, inner) => {
                names.push(name);
                inner.collect_name_refs(names);
            }
            PatternKind::Tag { payload, .. } => {
                if let Some(payload) = payload {
                    payload.collect_name_refs(names);
                }
            }
            PatternKind::Tuple(items) | PatternKind::List(items) => {
                items.iter().for_each(|item| item.collect_name_refs(names));
            }
            PatternKind::Struct { fields, .. } => {
                fields
                    .iter()
                    .for_each(|(_, pattern)| pattern.collect_name_refs(names));
            }
            PatternKind::Or(alternatives) => {
                if let Some(first) = alternatives.first() {
                    first.collect_name_refs(names);
                }
            }
        }
    }

    fn collect_names(&self, names: &mut Vec<VarName>) {
        match &self.kind {
            PatternKind::Wildcard | PatternKind::Literal(_) | PatternKind::Range { .. } => {}
            PatternKind::Bind(name) => names.push(name.clone()),
            PatternKind::As(name, inner) => {
                names.push(name.clone());
                inner.collect_names(names);
            }
            PatternKind::Tag { payload, .. } => {
                if let Some(payload) = payload {
                    payload.collect_names(names);
                }
            }
            PatternKind::Tuple(items) | PatternKind::List(items) => {
                items.iter().for_each(|item| item.collect_names(names));
            }
            PatternKind::Struct { fields, .. } => {
                fields
                    .iter()
                    .for_each(|(_, pattern)| pattern.collect_names(names));
            }
            PatternKind::Or(alternatives) => {
                if let Some(first) = alternatives.first() {
                    first.collect_names(names);
                }
            }
        }
    }
}

/// One arm of a `match`: what it matches, and whether a guard stands between
/// the pattern and the arm being selected.
#[derive(Clone, Debug, PartialEq)]
pub struct MatchArm {
    pub pattern: MatchPattern,
    pub guarded: bool,
}

impl MatchArm {
    /// How many children this arm contributes to its `match` node.
    pub fn children(&self) -> usize {
        1 + usize::from(self.guarded)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum PatternKind {
    /// `_`
    Wildcard,
    /// A lower-case name, which matches anything and binds it.
    Bind(VarName),
    /// `name @ pattern`
    As(VarName, Box<MatchPattern>),
    /// `Tag`, or `Tag(pattern)` for an alternative carrying a payload.
    Tag {
        tag: EcoString,
        payload: Option<Box<MatchPattern>>,
    },
    /// `(a, b)`
    Tuple(EcoVec<MatchPattern>),
    /// `{ a: p, .. }`; `rest` is whether the pattern ends in `..`, which
    /// allows fields it does not name.
    Struct {
        fields: EcoVec<(EcoString, MatchPattern)>,
        rest: bool,
    },
    /// `[a, b]`, matching a list of exactly that length.
    List(EcoVec<MatchPattern>),
    /// An `Int`, `Str`, `Bool` or `Unit` literal. Float literals are not
    /// patterns: equality on them is not what a reader would expect.
    Literal(SyntaxLiteral),
    /// `1..5` or `1..=5`, over `Int` only.
    Range {
        start: i64,
        end: i64,
        inclusive: bool,
    },
    /// `p | q`, matching when any alternative does.
    Or(EcoVec<MatchPattern>),
}

impl std::fmt::Display for MatchPattern {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.kind {
            PatternKind::Wildcard => f.write_str("_"),
            PatternKind::Bind(name) => write!(f, "{name}"),
            PatternKind::As(name, inner) => write!(f, "{name} @ {inner}"),
            PatternKind::Tag { tag, payload } => match payload {
                Some(payload) => write!(f, "{tag}({payload})"),
                None => write!(f, "{tag}"),
            },
            PatternKind::Tuple(items) => write!(f, "({})", separated(items, ", ")),
            PatternKind::List(items) => write!(f, "[{}]", separated(items, ", ")),
            PatternKind::Struct { fields, rest } => {
                let fields = fields
                    .iter()
                    .map(|(name, pattern)| format!("{name}: {pattern}"))
                    .chain(rest.then(|| "..".to_owned()))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "{{ {fields} }}")
            }
            PatternKind::Literal(literal) => {
                write!(f, "{}", Value::from(literal.clone()).dsrv_source())
            }
            PatternKind::Range {
                start,
                end,
                inclusive,
            } => write!(f, "{start}..{}{end}", if *inclusive { "=" } else { "" }),
            PatternKind::Or(alternatives) => write!(f, "{}", separated(alternatives, " | ")),
        }
    }
}

fn separated(patterns: &[MatchPattern], separator: &str) -> String {
    patterns
        .iter()
        .map(MatchPattern::to_string)
        .collect::<Vec<_>>()
        .join(separator)
}

/// Whether `value` matches `pattern`, adding what it bound to `bindings`.
///
/// On a failure the bindings are left as they were, so a caller may try the
/// next alternative or the next arm with the same vector.
pub fn match_value(pattern: &MatchPattern, value: &Value, bindings: &mut Bindings) -> bool {
    let bound_before = bindings.len();
    if match_value_inner(pattern, value, bindings) {
        return true;
    }
    bindings.truncate(bound_before);
    false
}

fn match_value_inner(pattern: &MatchPattern, value: &Value, bindings: &mut Bindings) -> bool {
    match &pattern.kind {
        PatternKind::Wildcard => true,
        PatternKind::Bind(name) => {
            bindings.push((name.clone(), value.clone()));
            true
        }
        PatternKind::As(name, inner) => {
            bindings.push((name.clone(), value.clone()));
            match_value_inner(inner, value, bindings)
        }
        PatternKind::Tag { tag, payload } => match value {
            Value::Union(union) if union.tag() == tag => match (payload, union.payload()) {
                (None, None) => true,
                (Some(pattern), Some(payload)) => match_value_inner(pattern, payload, bindings),
                _ => false,
            },
            _ => false,
        },
        PatternKind::Tuple(items) => match value {
            Value::Tuple(values) if values.len() == items.len() => items
                .iter()
                .zip(values.iter())
                .all(|(pattern, value)| match_value_inner(pattern, value, bindings)),
            _ => false,
        },
        PatternKind::List(items) => match value {
            Value::List(values) if values.len() == items.len() => items
                .iter()
                .zip(values.iter())
                .all(|(pattern, value)| match_value_inner(pattern, value, bindings)),
            _ => false,
        },
        PatternKind::Struct { fields, rest } => match value {
            Value::Map(entries) => {
                if !rest && entries.len() != fields.len() {
                    return false;
                }
                fields.iter().all(|(name, pattern)| {
                    entries
                        .get(name)
                        .is_some_and(|value| match_value_inner(pattern, value, bindings))
                })
            }
            _ => false,
        },
        // A pattern is written in source, so it carries a syntax literal;
        // what it is matched against is a runtime value.
        PatternKind::Literal(literal) => &Value::from(literal.clone()) == value,
        PatternKind::Range {
            start,
            end,
            inclusive,
        } => match value {
            Value::Int(value) => {
                value >= start
                    && if *inclusive {
                        value <= end
                    } else {
                        value < end
                    }
            }
            _ => false,
        },
        // Alternatives are tried in order, and the first to match binds.
        PatternKind::Or(alternatives) => alternatives
            .iter()
            .any(|alternative| match_value(alternative, value, bindings)),
    }
}

/// Which arm a value selects.
#[derive(Clone, Debug, PartialEq)]
pub enum ArmSelection {
    Selected {
        arm: usize,
        bindings: Bindings,
    },
    /// The scrutinee or a guard had no value, so neither has the match. The
    /// value carried is the one to propagate.
    Absent(Value),
    /// The value matched no arm. Checking rejects a match that can reach
    /// this, so a runtime meeting it has been given a value outside the
    /// schema it was checked against.
    Unmatched,
}

/// What a guard evaluated to. A guard that has no value is not false: it
/// makes the whole match absent, as any other absent operand would.
#[derive(Clone, Debug, PartialEq)]
pub enum GuardOutcome {
    True,
    False,
    Absent(Value),
}

/// Select the first arm whose pattern matches and whose guard holds.
///
/// Arms are tried in source order, and a guard runs only for an arm whose
/// pattern matched, with that pattern's bindings in scope. `guard` is given
/// the arm's index and its bindings, and returns `None` for an arm with no
/// guard.
pub fn select_arm(
    patterns: &[MatchPattern],
    value: &Value,
    mut guard: impl FnMut(usize, &Bindings) -> Option<GuardOutcome>,
) -> ArmSelection {
    if matches!(value, Value::NoVal | Value::Deferred) {
        return ArmSelection::Absent(value.clone());
    }
    let mut bindings = Bindings::new();
    for (arm, pattern) in patterns.iter().enumerate() {
        if !match_value(pattern, value, &mut bindings) {
            continue;
        }
        match guard(arm, &bindings) {
            None | Some(GuardOutcome::True) => return ArmSelection::Selected { arm, bindings },
            Some(GuardOutcome::Absent(value)) => return ArmSelection::Absent(value),
            Some(GuardOutcome::False) => bindings.clear(),
        }
    }
    ArmSelection::Unmatched
}

#[cfg(test)]
mod tests {
    use ecow::eco_vec;

    use super::*;
    use crate::core::UnionValue;

    fn pattern(kind: PatternKind) -> MatchPattern {
        MatchPattern::new(kind, Span::default())
    }

    fn wildcard() -> MatchPattern {
        pattern(PatternKind::Wildcard)
    }

    fn bind(name: &str) -> MatchPattern {
        pattern(PatternKind::Bind(VarName::from(name)))
    }

    fn tag(tag: &str, payload: Option<MatchPattern>) -> MatchPattern {
        pattern(PatternKind::Tag {
            tag: tag.into(),
            payload: payload.map(Box::new),
        })
    }

    fn union(name: &str, payload: Option<Value>) -> Value {
        UnionValue::new(name, payload).into()
    }

    fn matches(pattern: &MatchPattern, value: &Value) -> Option<Bindings> {
        let mut bindings = Bindings::new();
        match_value(pattern, value, &mut bindings).then_some(bindings)
    }

    // R9.1: a tag matches its own alternative and no other, and its payload
    // pattern is matched against the payload.
    #[test]
    fn a_tag_matches_its_alternative() {
        let moving = tag("Moving", Some(bind("n")));
        assert_eq!(
            matches(&moving, &union("Moving", Some(Value::Int(3)))),
            Some(eco_vec![(VarName::from("n"), Value::Int(3))])
        );
        assert_eq!(matches(&moving, &union("Stopped", None)), None);
        // Arity is part of matching: a nullary value is not a payload one.
        assert_eq!(matches(&moving, &union("Moving", None)), None);
        assert_eq!(
            matches(&tag("Stopped", None), &union("Stopped", None)),
            Some(Bindings::new())
        );
        assert_eq!(
            matches(&tag("Stopped", None), &union("Stopped", Some(Value::Unit))),
            None
        );
    }

    // R9.2: a pattern that fails binds nothing, so the next arm starts clean.
    #[test]
    fn a_failed_match_leaves_no_bindings() {
        let pair = pattern(PatternKind::Tuple(eco_vec![
            bind("first"),
            tag("Stopped", None)
        ]));
        let mut bindings = Bindings::new();
        assert!(!match_value(
            &pair,
            &Value::Tuple(eco_vec![
                Value::Int(1),
                union("Moving", Some(Value::Int(2)))
            ]),
            &mut bindings
        ));
        assert!(bindings.is_empty(), "{bindings:?}");
    }

    // R9.3: `name @ pattern` binds the whole value as well as what the
    // pattern inside it binds.
    #[test]
    fn an_as_pattern_binds_the_whole_value() {
        let whole = pattern(PatternKind::As(
            VarName::from("whole"),
            Box::new(tag("Moving", Some(bind("n")))),
        ));
        let value = union("Moving", Some(Value::Int(4)));
        assert_eq!(
            matches(&whole, &value),
            Some(eco_vec![
                (VarName::from("whole"), value.clone()),
                (VarName::from("n"), Value::Int(4)),
            ])
        );
    }

    // R9.4: alternatives are tried in order and the first to match binds, so
    // the names an or-pattern binds do not depend on the value.
    #[test]
    fn an_or_pattern_binds_through_its_first_matching_alternative() {
        let either = pattern(PatternKind::Or(eco_vec![
            tag("Moving", Some(bind("n"))),
            tag("Rolling", Some(bind("n"))),
        ]));
        for name in ["Moving", "Rolling"] {
            assert_eq!(
                matches(&either, &union(name, Some(Value::Int(7)))),
                Some(eco_vec![(VarName::from("n"), Value::Int(7))]),
                "{name}"
            );
        }
        assert_eq!(matches(&either, &union("Stopped", None)), None);
        assert_eq!(either.bound_names(), vec![VarName::from("n")]);
    }

    // R9.5: a struct pattern names the fields it cares about; without `..`
    // it must name them all.
    #[test]
    fn a_struct_pattern_names_fields() {
        let value = Value::Map(
            [
                (EcoString::from("model"), Value::Int(2)),
                (EcoString::from("samples"), Value::Int(9)),
            ]
            .into_iter()
            .collect(),
        );
        let open = pattern(PatternKind::Struct {
            fields: eco_vec![(EcoString::from("model"), bind("m"))],
            rest: true,
        });
        assert_eq!(
            matches(&open, &value),
            Some(eco_vec![(VarName::from("m"), Value::Int(2))])
        );
        let closed = pattern(PatternKind::Struct {
            fields: eco_vec![(EcoString::from("model"), bind("m"))],
            rest: false,
        });
        assert_eq!(matches(&closed, &value), None);
    }

    // R9.6: literals and ranges compare values, and a Float is not a literal
    // pattern so nothing here compares one.
    #[test]
    fn literals_and_ranges_compare_values() {
        let one = pattern(PatternKind::Literal(SyntaxLiteral::Int(1)));
        assert!(matches(&one, &Value::Int(1)).is_some());
        assert!(matches(&one, &Value::Int(2)).is_none());
        assert!(matches(&one, &Value::Str("1".into())).is_none());

        let range = pattern(PatternKind::Range {
            start: 1,
            end: 3,
            inclusive: false,
        });
        assert!(matches(&range, &Value::Int(1)).is_some());
        assert!(matches(&range, &Value::Int(3)).is_none());
        let inclusive = pattern(PatternKind::Range {
            start: 1,
            end: 3,
            inclusive: true,
        });
        assert!(matches(&inclusive, &Value::Int(3)).is_some());
    }

    // R9.7: arms are tried in order, and a guard runs only for an arm whose
    // pattern matched, with that arm's bindings in scope.
    #[test]
    fn arms_are_selected_in_order_with_their_guards() {
        let patterns = [
            tag("Moving", Some(bind("n"))),
            tag("Moving", Some(bind("m"))),
            wildcard(),
        ];
        let mut guarded = Vec::new();
        let selection = select_arm(
            &patterns,
            &union("Moving", Some(Value::Int(5))),
            |arm, bindings| {
                guarded.push((arm, bindings.clone()));
                // The first arm's guard fails, so the second is tried.
                (arm == 0).then_some(GuardOutcome::False)
            },
        );
        assert_eq!(
            selection,
            ArmSelection::Selected {
                arm: 1,
                bindings: eco_vec![(VarName::from("m"), Value::Int(5))],
            }
        );
        assert_eq!(guarded.len(), 2, "only matching arms have guards run");
        assert_eq!(guarded[0].1, eco_vec![(VarName::from("n"), Value::Int(5))]);
    }

    // R9.8: absence is not falsity. A scrutinee or a guard without a value
    // makes the match absent, rather than falling through to a later arm.
    #[test]
    fn absence_propagates_rather_than_selecting_an_arm() {
        let patterns = [wildcard()];
        for absent in [Value::NoVal, Value::Deferred] {
            assert_eq!(
                select_arm(&patterns, &absent, |_, _| None),
                ArmSelection::Absent(absent.clone())
            );
        }
        assert_eq!(
            select_arm(&patterns, &Value::Int(1), |_, _| Some(
                GuardOutcome::Absent(Value::Deferred)
            )),
            ArmSelection::Absent(Value::Deferred)
        );
    }

    // R9.9: a value matching no arm is reported as that, for a caller to
    // treat as the failure it is.
    #[test]
    fn a_value_matching_no_arm_is_unmatched() {
        let patterns = [tag("Stopped", None)];
        assert_eq!(
            select_arm(&patterns, &union("Moving", Some(Value::Int(1))), |_, _| {
                None
            }),
            ArmSelection::Unmatched
        );
    }

    // R9.10: which patterns match everything is what makes a later arm
    // unreachable, so it is answered on the pattern alone.
    #[test]
    fn irrefutable_patterns_are_recognised() {
        assert!(wildcard().is_irrefutable());
        assert!(bind("x").is_irrefutable());
        assert!(
            pattern(PatternKind::As(VarName::from("x"), Box::new(wildcard()))).is_irrefutable()
        );
        assert!(
            pattern(PatternKind::Or(eco_vec![tag("Stopped", None), wildcard()])).is_irrefutable()
        );
        assert!(!tag("Stopped", None).is_irrefutable());
        assert!(!pattern(PatternKind::Literal(SyntaxLiteral::Int(1))).is_irrefutable());
    }
}
