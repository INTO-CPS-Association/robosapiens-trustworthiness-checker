//! Source-level type names and immutable, expanded alias namespaces.
//!
//! Source types remain private to the DSRV frontend. Consumers receive only
//! structural [`StreamType`]s; names never become nominal type identities.

use super::ast::AstShared as Rc;
use std::{collections::BTreeMap, fmt};

use ecow::{EcoString, EcoVec};

use crate::core::StreamType;

use super::span::Span;

/// Source spelling of structural types. Unlike core diagnostics, source fields
/// must be quoted so whitespace and reserved words keep their field-name role.
pub(crate) struct SourceTypeDisplay<'a>(pub &'a StreamType);

impl fmt::Display for SourceTypeDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let list = |f: &mut fmt::Formatter<'_>, types: &[StreamType]| {
            for (index, ty) in types.iter().enumerate() {
                if index != 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{}", SourceTypeDisplay(ty))?;
            }
            Ok(())
        };
        match self.0 {
            StreamType::List(ty) => write!(f, "List<{}>", Self(ty)),
            StreamType::Map(ty) => write!(f, "Map<{}>", Self(ty)),
            StreamType::Expr(ty) => write!(f, "Expr<{}>", Self(ty)),
            StreamType::Tuple(types) => {
                write!(f, "(")?;
                list(f, types)?;
                if types.len() == 1 {
                    write!(f, ",")?;
                }
                write!(f, ")")
            }
            StreamType::Function(args, ret) => {
                write!(f, "(")?;
                list(f, args)?;
                write!(f, " -> {})", Self(ret))
            }
            StreamType::Struct(fields, open) => {
                write!(f, "Struct<")?;
                for (index, (name, ty)) in fields.iter().enumerate() {
                    if index != 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "\"{name}\": {}", Self(ty))?;
                }
                if *open {
                    if !fields.is_empty() {
                        write!(f, ", ")?;
                    }
                    write!(f, "...")?;
                }
                write!(f, ">")
            }
            ty => ty.fmt(f),
        }
    }
}

/// A type-namespace name, distinct from stream and local variable names.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize)]
pub struct TypeName(EcoString);

impl TypeName {
    pub fn new(name: impl Into<EcoString>) -> Result<Self, SourceResolveError> {
        let name = name.into();
        let mut chars = name.chars();
        if !chars
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
            || !chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
        {
            return Err(SourceResolveError::InvalidName { name });
        }
        if matches!(
            name.as_str(),
            "Int"
                | "Float"
                | "Str"
                | "Bool"
                | "Unit"
                | "Any"
                | "List"
                | "Tuple"
                | "Map"
                | "Struct"
                | "Expr"
                | "type"
        ) {
            return Err(SourceResolveError::ReservedName { name });
        }
        Ok(Self(name))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TypeName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Resolution failures retain byte spans in the original source.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum SourceResolveError {
    #[error("invalid type alias name {name:?}")]
    InvalidName { name: EcoString },
    #[error("reserved type alias name {name:?}")]
    ReservedName { name: EcoString },
    #[error("duplicate type alias {name} at {span:?} (first declared at {first_span:?})")]
    DuplicateAlias {
        name: TypeName,
        first_span: Span,
        span: Span,
    },
    #[error("unknown type alias {name} at {span:?}")]
    UnknownAlias { name: TypeName, span: Span },
    #[error("cyclic type aliases {path:?} at {span:?}")]
    AliasCycle { path: Vec<TypeName>, span: Span },
}

/// Collision-free structural namespace fingerprint.
///
/// This is the entire sorted name-to-expanded-type mapping, not a process-local
/// hash or an address. Declaration order and source spelling of references do
/// not affect it; changing even an unused alias does.
#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, serde::Serialize)]
pub struct SourceFingerprint(BTreeMap<TypeName, StreamType>);

/// A namespace snapshot. Clones share immutable storage.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct SourceContext {
    fingerprint: Rc<SourceFingerprint>,
}

impl SourceContext {
    pub fn builder() -> SourceContextBuilder {
        SourceContextBuilder::default()
    }

    pub fn aliases(&self) -> &BTreeMap<TypeName, StreamType> {
        &self.fingerprint.0
    }

    pub fn get(&self, name: &TypeName) -> Option<&StreamType> {
        self.aliases().get(name)
    }

    pub fn fingerprint(&self) -> &SourceFingerprint {
        &self.fingerprint
    }

    pub(crate) fn resolve_type(
        &self,
        source: &SourceType,
    ) -> Result<StreamType, SourceResolveError> {
        resolve_type(source, &mut |name, span| {
            self.get(name)
                .cloned()
                .ok_or_else(|| SourceResolveError::UnknownAlias {
                    name: name.clone(),
                    span,
                })
        })
    }
}

/// Programmatic callers supply already structural types; parser callers add
/// private unresolved declarations and resolve the whole namespace at `build`.
#[derive(Clone, Debug, Default)]
pub struct SourceContextBuilder {
    definitions: BTreeMap<TypeName, AliasDeclaration>,
}

impl SourceContextBuilder {
    pub fn insert(&mut self, name: TypeName, ty: StreamType) -> Result<(), SourceResolveError> {
        self.insert_source(AliasDeclaration {
            name,
            ty: SourceType::from(ty),
            span: Span::default(),
        })
    }

    pub(crate) fn insert_source(
        &mut self,
        declaration: AliasDeclaration,
    ) -> Result<(), SourceResolveError> {
        if let Some(first) = self.definitions.get(&declaration.name) {
            return Err(SourceResolveError::DuplicateAlias {
                name: declaration.name,
                first_span: first.span,
                span: declaration.span,
            });
        }
        self.definitions
            .insert(declaration.name.clone(), declaration);
        Ok(())
    }

    pub fn build(self) -> Result<SourceContext, SourceResolveError> {
        let mut resolver = AliasResolver {
            definitions: &self.definitions,
            expanded: BTreeMap::new(),
            active: Vec::new(),
        };
        for (name, definition) in &self.definitions {
            resolver.resolve(name, definition.span)?;
        }
        Ok(SourceContext {
            fingerprint: Rc::new(SourceFingerprint(resolver.expanded)),
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct AliasDeclaration {
    pub name: TypeName,
    pub ty: SourceType,
    pub span: Span,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SourceType {
    pub kind: SourceTypeKind,
    pub span: Span,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum SourceTypeKind {
    Named(TypeName),
    Int,
    Float,
    Str,
    Bool,
    Unit,
    Any,
    List(Box<SourceType>),
    Tuple(EcoVec<SourceType>),
    Map(Box<SourceType>),
    Expr(Box<SourceType>),
    Struct(EcoVec<(EcoString, SourceType)>, bool),
    Function(EcoVec<SourceType>, Box<SourceType>),
}

impl From<StreamType> for SourceType {
    fn from(ty: StreamType) -> Self {
        let kind = match ty {
            StreamType::Int => SourceTypeKind::Int,
            StreamType::Float => SourceTypeKind::Float,
            StreamType::Str => SourceTypeKind::Str,
            StreamType::Bool => SourceTypeKind::Bool,
            StreamType::Unit => SourceTypeKind::Unit,
            StreamType::Any => SourceTypeKind::Any,
            StreamType::List(ty) => SourceTypeKind::List(Box::new((*ty).into())),
            StreamType::Map(ty) => SourceTypeKind::Map(Box::new((*ty).into())),
            StreamType::Expr(ty) => SourceTypeKind::Expr(Box::new((*ty).into())),
            StreamType::Tuple(types) => {
                SourceTypeKind::Tuple(types.into_iter().map(Into::into).collect())
            }
            StreamType::Struct(fields, open) => SourceTypeKind::Struct(
                fields
                    .into_iter()
                    .map(|(name, ty)| (name, ty.into()))
                    .collect(),
                open,
            ),
            StreamType::Function(args, ret) => SourceTypeKind::Function(
                args.into_iter().map(Into::into).collect(),
                Box::new((*ret).into()),
            ),
        };
        Self {
            kind,
            span: Span::default(),
        }
    }
}

struct AliasResolver<'a> {
    definitions: &'a BTreeMap<TypeName, AliasDeclaration>,
    expanded: BTreeMap<TypeName, StreamType>,
    active: Vec<TypeName>,
}

impl AliasResolver<'_> {
    fn resolve(&mut self, name: &TypeName, span: Span) -> Result<StreamType, SourceResolveError> {
        if let Some(ty) = self.expanded.get(name) {
            return Ok(ty.clone());
        }
        if let Some(start) = self.active.iter().position(|active| active == name) {
            let mut path = self.active[start..].to_vec();
            path.push(name.clone());
            return Err(SourceResolveError::AliasCycle { path, span });
        }
        let definition =
            self.definitions
                .get(name)
                .ok_or_else(|| SourceResolveError::UnknownAlias {
                    name: name.clone(),
                    span,
                })?;
        self.active.push(name.clone());
        let result = resolve_type(&definition.ty, &mut |name, span| self.resolve(name, span));
        self.active.pop();
        let ty = result?;
        self.expanded.insert(name.clone(), ty.clone());
        Ok(ty)
    }
}

fn resolve_type(
    source: &SourceType,
    lookup: &mut impl FnMut(&TypeName, Span) -> Result<StreamType, SourceResolveError>,
) -> Result<StreamType, SourceResolveError> {
    Ok(match &source.kind {
        SourceTypeKind::Named(name) => lookup(name, source.span)?,
        SourceTypeKind::Int => StreamType::Int,
        SourceTypeKind::Float => StreamType::Float,
        SourceTypeKind::Str => StreamType::Str,
        SourceTypeKind::Bool => StreamType::Bool,
        SourceTypeKind::Unit => StreamType::Unit,
        SourceTypeKind::Any => StreamType::Any,
        SourceTypeKind::List(ty) => StreamType::List(Box::new(resolve_type(ty, lookup)?)),
        SourceTypeKind::Map(ty) => StreamType::Map(Box::new(resolve_type(ty, lookup)?)),
        SourceTypeKind::Expr(ty) => StreamType::Expr(Box::new(resolve_type(ty, lookup)?)),
        SourceTypeKind::Tuple(types) => StreamType::Tuple(
            types
                .iter()
                .map(|ty| resolve_type(ty, lookup))
                .collect::<Result<_, _>>()?,
        ),
        SourceTypeKind::Struct(fields, open) => StreamType::Struct(
            fields
                .iter()
                .map(|(name, ty)| Ok((name.clone(), resolve_type(ty, lookup)?)))
                .collect::<Result<_, _>>()?,
            *open,
        ),
        SourceTypeKind::Function(args, ret) => StreamType::Function(
            args.iter()
                .map(|ty| resolve_type(ty, lookup))
                .collect::<Result<_, _>>()?,
            Box::new(resolve_type(ret, lookup)?),
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::super::parser::parse_str;
    use super::*;
    use contiguous_tree::TreeCursorExt;

    fn name(s: &str) -> TypeName {
        TypeName::new(s).unwrap()
    }

    #[test]
    fn extracted_expressions_and_reconfigurable_nodes_keep_the_whole_namespace() {
        let spec =
            parse_str("type Unused = Int out x out y x = dynamic(\"1\") y = defer(\"2\")").unwrap();
        assert!(spec.source_context().get(&name("Unused")).is_some());
        for variable in ["x", "y"] {
            let extracted = spec.var_expr(&variable.into()).unwrap();
            for node in extracted.as_ref().postorder() {
                assert!(Rc::ptr_eq(
                    node.metadata().context.as_ref().unwrap(),
                    spec.source_context()
                ));
            }
        }
    }

    fn named(s: &str, span: Span) -> SourceType {
        SourceType {
            kind: SourceTypeKind::Named(name(s)),
            span,
        }
    }

    fn declaration(s: &str, ty: SourceType) -> AliasDeclaration {
        AliasDeclaration {
            name: name(s),
            span: ty.span,
            ty,
        }
    }

    #[test]
    fn forward_aliases_expand_and_fingerprint_the_whole_namespace() {
        let mut builder = SourceContext::builder();
        builder
            .insert_source(declaration("A", named("Z", Span::new(9, 10))))
            .unwrap();
        builder.insert(name("Z"), StreamType::Int).unwrap();
        let context = builder.build().unwrap();
        assert_eq!(context.get(&name("A")), Some(&StreamType::Int));
        assert_eq!(
            context.resolve_type(&named("A", Span::default())).unwrap(),
            StreamType::Int
        );
        let mut other = SourceContext::builder();
        other.insert(name("Z"), StreamType::Int).unwrap();
        other.insert(name("A"), StreamType::Int).unwrap();
        assert_eq!(
            context.fingerprint(),
            other.clone().build().unwrap().fingerprint()
        );
        other.insert(name("Unused"), StreamType::Bool).unwrap();
        assert_ne!(context.fingerprint(), other.build().unwrap().fingerprint());
    }

    #[test]
    fn duplicate_unknown_and_cycles_retain_locations() {
        let span = Span::new(12, 19);
        let mut builder = SourceContext::builder();
        builder
            .insert_source(declaration("A", named("Missing", span)))
            .unwrap();
        assert_eq!(
            builder.build().unwrap_err(),
            SourceResolveError::UnknownAlias {
                name: name("Missing"),
                span
            }
        );
        for (target, expected) in [
            ("A", vec![name("A"), name("A")]),
            ("B", vec![name("A"), name("B"), name("A")]),
        ] {
            let mut builder = SourceContext::builder();
            builder
                .insert_source(declaration("A", named(target, span)))
                .unwrap();
            if target == "B" {
                builder
                    .insert_source(declaration("B", named("A", span)))
                    .unwrap();
            }
            assert_eq!(
                builder.build().unwrap_err(),
                SourceResolveError::AliasCycle {
                    path: expected,
                    span
                }
            );
        }
        let mut builder = SourceContext::builder();
        builder.insert(name("A"), StreamType::Int).unwrap();
        assert!(
            matches!(builder.insert_source(declaration("A", named("A", span))), Err(SourceResolveError::DuplicateAlias { span: actual, .. }) if actual == span)
        );
        assert_eq!(
            builder.build().unwrap().get(&name("A")),
            Some(&StreamType::Int)
        );
    }

    #[test]
    fn namespace_rejects_builtins_but_not_underscore() {
        assert!(matches!(
            TypeName::new("Int"),
            Err(SourceResolveError::ReservedName { .. })
        ));
        assert!(matches!(
            TypeName::new("a-b"),
            Err(SourceResolveError::InvalidName { .. })
        ));
        assert!(TypeName::new("_").is_ok());
    }
}
