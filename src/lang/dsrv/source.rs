//! Source-level type names and immutable, expanded alias namespaces.
//!
//! Source types remain private to the DSRV frontend. Consumers receive
//! [`StreamType`]s. An alias's name never becomes a type identity: a
//! `type` declaration is structural. A `newtype` declaration is the one
//! exception, and its identity is not its name as written but its brand,
//! the canonical path of the module that declared it with the declaration's
//! name and resolved arguments ([`crate::core::NewtypeSchema`]). However a
//! newtype is reached, by alias, import or re-import, it resolves to the
//! same brand.

use super::ast::AstShared as Rc;
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

use ecow::{EcoString, EcoVec};

use crate::core::{
    ClosedUnion, NewtypeSchema, StreamType, UnionAlternative, UnionPayload, UnionSchemaError,
};
use crate::lang::dsrv::path::TypePath;

use super::expand::language::{Dialect, LanguageConfig};
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
                | "Union"
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
    UnknownAlias { name: TypePath, span: Span },

    #[error("`{name}` at {span:?} names no function: a module-qualified value must name a `def`")]
    UnknownModuleItem { name: EcoString, span: Span },

    #[error("`{name}` at {span:?} names no constant: a stream offset must be a number")]
    UnknownOffset { name: EcoString, span: Span },
    #[error("{name} at {span:?} takes {expected} type arguments, given {found}")]
    AliasArity {
        name: TypePath,
        expected: usize,
        found: usize,
        span: Span,
    },
    #[error("invalid union type at {span:?}: {cause}")]
    Union { cause: UnionSchemaError, span: Span },
    #[error("cyclic type aliases {path:?} at {span:?}")]
    AliasCycle { path: Vec<TypePath>, span: Span },
    #[error(
        "`{name}` at {span:?} is not an operation of a newtype: a newtype has `new` and `value`"
    )]
    UnknownNewtypeOperation { name: EcoString, span: Span },
    #[error("`{operation}` at {span:?} takes exactly one argument")]
    NewtypeOperationArity { operation: EcoString, span: Span },
    #[error(
        "`{operation}` at {span:?} is written in {writer}, but only {owner}, which declares the \
         newtype, may build or read its representation; call a function it exports instead"
    )]
    NewtypeRepresentation {
        operation: EcoString,
        owner: EcoString,
        writer: EcoString,
        span: Span,
    },
    #[error(
        "`{operation}` at {span:?} is in text supplied at run time, which may not build or read \
         a newtype's representation; call a function its declaring module exports instead"
    )]
    NewtypeRuntimeText { operation: EcoString, span: Span },
}

/// Collision-free structural namespace fingerprint.
///
/// This is the entire sorted name-to-expanded-type mapping, not a process-local
/// hash or an address. Declaration order and source spelling of references do
/// not affect it; changing even an unused alias does.
#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct SourceFingerprint {
    #[serde(serialize_with = "as_pairs")]
    aliases: BTreeMap<TypePath, StreamType>,
    /// Generic aliases have no type until they are used, so the namespace
    /// carries them as written. Changing one changes the program even when
    /// nothing has used it yet, as changing an unused alias does.
    #[serde(skip)]
    generic: BTreeMap<TypePath, AliasDeclaration>,
    /// Span-free projection used for identity and serialization. The original
    /// declarations above retain their locations for later diagnostics.
    #[serde(
        rename = "generic",
        skip_serializing_if = "BTreeMap::is_empty",
        serialize_with = "as_pairs"
    )]
    generic_identity: BTreeMap<TypePath, AliasDeclaration>,
    /// The names this module keeps to itself, which no import may take.
    #[serde(skip_serializing_if = "BTreeSet::is_empty")]
    internal: BTreeSet<TypePath>,
    /// Whether this is the namespace text supplied at run time sees: the
    /// `internal` names stay in it, so the aliases that name them still
    /// resolve in their own context, but text may not name them directly.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    runtime_text: bool,
    /// The union each imported tag builds, so a bare constructor can be
    /// resolved by name where no expected type says which union it is.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    constructors: BTreeMap<EcoString, TypePath>,
    /// Two sources with different language settings are different programs.
    language: LanguageConfig,
    /// With experiments on, the same settings can mean different programs in
    /// different releases.
    #[serde(skip_serializing_if = "Option::is_none")]
    experimental_revision: Option<u32>,
}

impl PartialEq for SourceFingerprint {
    fn eq(&self, other: &Self) -> bool {
        self.aliases == other.aliases
            && self.generic_identity == other.generic_identity
            && self.internal == other.internal
            && self.runtime_text == other.runtime_text
            && self.constructors == other.constructors
            && self.language == other.language
            && self.experimental_revision == other.experimental_revision
    }
}
impl Eq for SourceFingerprint {}

impl PartialOrd for SourceFingerprint {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for SourceFingerprint {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (
            &self.aliases,
            &self.generic_identity,
            &self.internal,
            self.runtime_text,
            &self.constructors,
            &self.language,
            self.experimental_revision,
        )
            .cmp(&(
                &other.aliases,
                &other.generic_identity,
                &other.internal,
                other.runtime_text,
                &other.constructors,
                &other.language,
                other.experimental_revision,
            ))
    }
}

fn semantic_alias(declaration: &AliasDeclaration) -> AliasDeclaration {
    let mut declaration = declaration.clone();
    declaration.span = Span::default();
    clear_type_spans(&mut declaration.ty);
    declaration
}

fn clear_type_spans(ty: &mut SourceType) {
    ty.span = Span::default();
    match &mut ty.kind {
        SourceTypeKind::Named(_, arguments) | SourceTypeKind::Tuple(arguments) => {
            arguments.make_mut().iter_mut().for_each(clear_type_spans);
        }
        SourceTypeKind::List(inner) | SourceTypeKind::Map(inner) | SourceTypeKind::Expr(inner) => {
            clear_type_spans(inner);
        }
        SourceTypeKind::Struct(fields, _) => {
            fields
                .make_mut()
                .iter_mut()
                .for_each(|(_, ty)| clear_type_spans(ty));
        }
        SourceTypeKind::Function(arguments, result) => {
            arguments.make_mut().iter_mut().for_each(clear_type_spans);
            clear_type_spans(result);
        }
        SourceTypeKind::Union(alternatives) => {
            for alternative in alternatives.make_mut() {
                alternative.span = Span::default();
                if let Some(payload) = &mut alternative.payload {
                    clear_type_spans(payload);
                }
            }
        }
        SourceTypeKind::Newtype(schema) => {
            *schema = schema.map(|ty| {
                let mut ty = ty.clone();
                clear_type_spans(&mut ty);
                ty
            });
        }
        SourceTypeKind::NewtypeInstance(instance) => {
            let instance = instance.as_mut();
            instance
                .arguments
                .make_mut()
                .iter_mut()
                .for_each(clear_type_spans);
            clear_type_spans(&mut instance.repr);
        }
        SourceTypeKind::Int
        | SourceTypeKind::Float
        | SourceTypeKind::Str
        | SourceTypeKind::Bool
        | SourceTypeKind::Unit
        | SourceTypeKind::Any => {}
    }
}

/// A map keyed by paths, written as its entries in order. A path is a
/// structure, which a format such as JSON cannot use as a key.
fn as_pairs<V: serde::Serialize, S: serde::Serializer>(
    map: &BTreeMap<TypePath, V>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.collect_seq(map)
}

impl SourceFingerprint {
    fn new(
        aliases: BTreeMap<TypePath, StreamType>,
        generic: BTreeMap<TypePath, AliasDeclaration>,
        internal: BTreeSet<TypePath>,
        language: LanguageConfig,
    ) -> Self {
        let generic_identity = generic
            .iter()
            .map(|(path, declaration)| (path.clone(), semantic_alias(declaration)))
            .collect();
        Self {
            aliases,
            generic,
            generic_identity,
            internal,
            runtime_text: false,
            constructors: BTreeMap::new(),
            experimental_revision: language.experimental_revision(),
            language,
        }
    }
}

/// A namespace snapshot. Clones share immutable storage.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct SourceContext {
    fingerprint: Rc<SourceFingerprint>,
}

impl SourceContext {
    pub fn builder() -> SourceContextBuilder {
        SourceContextBuilder::default()
    }

    pub fn aliases(&self) -> &BTreeMap<TypePath, StreamType> {
        &self.fingerprint.aliases
    }

    /// The language settings every expression expanded in this namespace
    /// was written under.
    pub fn language(&self) -> &LanguageConfig {
        &self.fingerprint.language
    }

    /// Whether a name is kept inside the module that declares it.
    pub(crate) fn is_internal(&self, name: &TypePath) -> bool {
        self.fingerprint.internal.contains(name)
    }

    /// Whether the module keeps any alias to itself.
    pub(crate) fn has_internal(&self) -> bool {
        !self.fingerprint.internal.is_empty()
    }

    /// The namespace as text supplied at run time sees the module that owns
    /// it: as an importer would, without the aliases the module keeps
    /// internal.
    pub(crate) fn for_runtime_text(&self) -> Self {
        if self.fingerprint.internal.is_empty() || self.fingerprint.runtime_text {
            return self.clone();
        }
        let mut fingerprint = (*self.fingerprint).clone();
        fingerprint.runtime_text = true;
        Self {
            fingerprint: Rc::new(fingerprint),
        }
    }

    /// The aliases that take parameters, which have no type until used.
    pub(crate) fn generic(&self) -> &BTreeMap<TypePath, AliasDeclaration> {
        &self.fingerprint.generic
    }

    /// The union an imported tag builds, if one was imported.
    pub(crate) fn constructor_union(&self, tag: &EcoString) -> Option<&TypePath> {
        self.fingerprint.constructors.get(tag)
    }

    /// The same namespace with imported constructors recorded.
    ///
    /// They are attached after building because a module may import its own
    /// tags, whose union is only resolved once its aliases are.
    pub(crate) fn with_constructors(self, constructors: BTreeMap<EcoString, TypePath>) -> Self {
        let mut fingerprint = (*self.fingerprint).clone();
        fingerprint.constructors = constructors;
        Self {
            fingerprint: Rc::new(fingerprint),
        }
    }

    /// The same namespace inside a program of `dialect`, which is how a
    /// module's text is read when another module calls into it.
    pub(crate) fn admitted_as(&self, dialect: Dialect) -> Self {
        let mut fingerprint = (*self.fingerprint).clone();
        fingerprint.language = fingerprint.language.admitted_as(dialect);
        Self {
            fingerprint: Rc::new(fingerprint),
        }
    }

    pub fn get(&self, name: &TypePath) -> Option<&StreamType> {
        self.aliases().get(name)
    }

    pub fn fingerprint(&self) -> &SourceFingerprint {
        &self.fingerprint
    }

    pub(crate) fn resolve_type(
        &self,
        source: &SourceType,
    ) -> Result<StreamType, SourceResolveError> {
        let mut active = Vec::new();
        resolve_type(source, &mut |name, arguments, span| {
            instantiate(self, name, arguments, span, &mut active, true)
        })
    }
}

/// Programmatic callers supply already structural types; parser callers add
/// private unresolved declarations and resolve the whole namespace at `build`.
#[derive(Clone, Debug, Default)]
pub struct SourceContextBuilder {
    definitions: BTreeMap<TypePath, AliasDeclaration>,
    internal: BTreeSet<TypePath>,
    language: LanguageConfig,
}

impl SourceContextBuilder {
    pub fn insert(&mut self, name: TypeName, ty: StreamType) -> Result<(), SourceResolveError> {
        self.insert_source(AliasDeclaration {
            name,
            parameters: EcoVec::new(),
            internal: false,
            kind: AliasKind::Structural,
            ty: SourceType::from(ty),
            span: Span::default(),
        })
    }

    /// Insert under a key the caller chose, which is how an import lands
    /// under `m::X` rather than under its own name.
    pub(crate) fn insert_keyed(
        &mut self,
        key: TypePath,
        declaration: AliasDeclaration,
    ) -> Result<(), SourceResolveError> {
        if let Some(first) = self.definitions.get(&key) {
            return Err(SourceResolveError::DuplicateAlias {
                name: declaration.name,
                first_span: first.span,
                span: declaration.span,
            });
        }
        if declaration.internal {
            self.internal.insert(key.clone());
        }
        self.definitions.insert(key, declaration);
        Ok(())
    }

    /// Insert a declaration under its own name, which is where a file's own
    /// `type` lands.
    pub(crate) fn insert_source(
        &mut self,
        declaration: AliasDeclaration,
    ) -> Result<(), SourceResolveError> {
        let key = TypePath::local(declaration.name.clone());
        self.insert_keyed(key, declaration)
    }

    /// Record the settings of the source this namespace belongs to.
    pub(crate) fn language(&mut self, language: LanguageConfig) {
        self.language = language;
    }

    pub fn build(self) -> Result<SourceContext, SourceResolveError> {
        let generic: BTreeMap<TypePath, AliasDeclaration> = self
            .definitions
            .iter()
            .filter(|(_, declaration)| !declaration.parameters.is_empty())
            .map(|(name, declaration)| (name.clone(), declaration.clone()))
            .collect();
        let mut resolver = AliasResolver {
            definitions: &self.definitions,
            generic: &generic,
            expanded: BTreeMap::new(),
            active: Vec::new(),
        };
        for (name, definition) in &self.definitions {
            // A generic alias has no type of its own; its uses have one each.
            if definition.parameters.is_empty() {
                resolver.resolve(name, &[], definition.span)?;
            }
        }
        let internal = self.internal;
        Ok(SourceContext {
            fingerprint: Rc::new(SourceFingerprint::new(
                resolver.expanded,
                generic,
                internal,
                self.language,
            )),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Serialize)]
pub(crate) struct AliasDeclaration {
    pub name: TypeName,
    /// The type parameters the alias takes, in order. An alias with none is
    /// a type; an alias with parameters is a way of making one.
    pub parameters: EcoVec<TypeName>,
    /// An internal alias may be used inside the module that declares it and
    /// is never reachable from another one (S12).
    pub internal: bool,
    /// A structural `type`, or a nominal `newtype`.
    pub kind: AliasKind,
    pub ty: SourceType,
    pub span: Span,
}

/// What a declaration's name stands for once it resolves.
#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, serde::Serialize)]
pub(crate) enum AliasKind {
    /// `type`: the structure it expands to, interchangeable with any other
    /// spelling of that structure.
    #[default]
    Structural,
    /// `newtype`: a nominal type over the body, its representation.
    ///
    /// `stem` is the declaration's canonical path, the module that
    /// declared it and its name, which every brand it makes begins with.
    /// Parsing records the bare name; the module graph qualifies it when the
    /// declaration is placed in its module, and imports carry it unchanged.
    Newtype { stem: EcoString },
}

impl AliasKind {
    pub(crate) fn newtype_stem(&self) -> Option<&EcoString> {
        match self {
            Self::Structural => None,
            Self::Newtype { stem } => Some(stem),
        }
    }
}

impl AliasDeclaration {
    /// This declaration placed in `module`, spelled as a brand spells a
    /// module path (empty for the root).
    pub(crate) fn placed_in(mut self, module: &str) -> Self {
        if let AliasKind::Newtype { stem } = &mut self.kind
            && !module.is_empty()
        {
            *stem = format!("{module}::{}", self.name).into();
        }
        self
    }
}

/// A generic newtype applied to arguments that may still name the
/// parameters of an enclosing generic declaration, carrying its own
/// representation so it resolves wherever it is imported.
///
/// This is what an exported generic declaration's body holds where it
/// applies a generic newtype: the newtype cannot be inlined structurally,
/// as a generic alias is, without losing its identity.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Serialize)]
pub(crate) struct NewtypeInstance {
    pub stem: EcoString,
    pub parameters: EcoVec<TypeName>,
    pub arguments: EcoVec<SourceType>,
    /// The declaration's body, resolved where it was declared except for
    /// its own `parameters`.
    pub repr: SourceType,
}

/// The brand of `stem` applied to `arguments`.
pub(crate) fn brand_of(stem: &str, arguments: &[StreamType]) -> EcoString {
    crate::core::canonical_brand(stem, arguments)
}

/// The newtype instance `stem` makes from resolved `arguments` and the
/// representation its body resolved to.
fn newtype_instance(stem: &str, arguments: EcoVec<StreamType>, repr: StreamType) -> StreamType {
    StreamType::Newtype(NewtypeSchema::new(
        brand_of(stem, &arguments),
        arguments,
        repr,
    ))
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Serialize)]
pub(crate) struct SourceType {
    pub kind: SourceTypeKind,
    pub span: Span,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Serialize)]
pub(crate) enum SourceTypeKind {
    /// A name, with the type arguments it is applied to. A name standing on
    /// its own is applied to none.
    Named(TypePath, EcoVec<SourceType>),
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
    Union(EcoVec<SourceAlternative>),
    /// A newtype instance that has already resolved. No source spells one;
    /// it is how a resolved newtype travels through an import.
    Newtype(NewtypeSchema<SourceType>),
    /// A generic newtype applied inside an exported generic declaration.
    /// No source spells one either.
    NewtypeInstance(Box<NewtypeInstance>),
}

/// One alternative of a source-level union type, before its payload resolves.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Serialize)]
pub(crate) struct SourceAlternative {
    pub tag: EcoString,
    pub payload: Option<SourceType>,
    pub span: Span,
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
            StreamType::Union(union) => SourceTypeKind::Union(
                union
                    .alternatives()
                    .iter()
                    .map(|alternative| SourceAlternative {
                        tag: alternative.tag().clone(),
                        payload: match alternative.payload() {
                            UnionPayload::Nullary => None,
                            UnionPayload::Of(ty) => Some(ty.clone().into()),
                        },
                        span: Span::default(),
                    })
                    .collect(),
            ),
            StreamType::Newtype(schema) => {
                SourceTypeKind::Newtype(schema.map(|ty| ty.clone().into()))
            }
        };
        Self {
            kind,
            span: Span::default(),
        }
    }
}

struct AliasResolver<'a> {
    definitions: &'a BTreeMap<TypePath, AliasDeclaration>,
    /// The subset of `definitions` taking parameters, which have no type of
    /// their own and resolve once per use site.
    generic: &'a BTreeMap<TypePath, AliasDeclaration>,
    expanded: BTreeMap<TypePath, StreamType>,
    active: Vec<TypePath>,
}

impl AliasResolver<'_> {
    fn resolve(
        &mut self,
        name: &TypePath,
        arguments: &[SourceType],
        span: Span,
    ) -> Result<StreamType, SourceResolveError> {
        if !arguments.is_empty() {
            return self.instantiate(name, arguments, span);
        }
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
        let result = resolve_type(&definition.ty, &mut |name, arguments, span| {
            self.resolve(name, arguments, span)
        });
        self.active.pop();
        let ty = match definition.kind.newtype_stem() {
            Some(stem) => newtype_instance(stem, EcoVec::new(), result?),
            None => result?,
        };
        self.expanded.insert(name.clone(), ty.clone());
        Ok(ty)
    }

    fn resolve_source(&mut self, source: &SourceType) -> Result<StreamType, SourceResolveError> {
        resolve_type(source, &mut |name, arguments, span| {
            self.resolve(name, arguments, span)
        })
    }

    /// A generic alias resolves per use site rather than once, so its result
    /// is not cached under its name.
    fn instantiate(
        &mut self,
        name: &TypePath,
        arguments: &[SourceType],
        span: Span,
    ) -> Result<StreamType, SourceResolveError> {
        let declaration =
            self.generic
                .get(name)
                .ok_or_else(|| match self.definitions.get(name) {
                    Some(_) => SourceResolveError::AliasArity {
                        name: name.clone(),
                        expected: 0,
                        found: arguments.len(),
                        span,
                    },
                    None => SourceResolveError::UnknownAlias {
                        name: name.clone(),
                        span,
                    },
                })?;
        if let Some(start) = self.active.iter().position(|active| active == name) {
            let mut path = self.active[start..].to_vec();
            path.push(name.clone());
            return Err(SourceResolveError::AliasCycle { path, span });
        }
        let declaration = declaration.clone();
        let arguments = arguments
            .iter()
            .map(|argument| self.resolve_source(argument))
            .collect::<Result<EcoVec<_>, _>>()?;
        let body = substitute(&declaration, &arguments, span)?;
        self.active.push(name.clone());
        let result = resolve_type(&body, &mut |name, arguments, span| {
            self.resolve(name, arguments, span)
        });
        self.active.pop();
        match declaration.kind.newtype_stem() {
            Some(stem) => Ok(newtype_instance(stem, arguments, result?)),
            None => result,
        }
    }
}

/// The same instantiation against a namespace that is already built.
fn instantiate(
    context: &SourceContext,
    name: &TypePath,
    arguments: &[SourceType],
    span: Span,
    active: &mut Vec<TypePath>,
    written: bool,
) -> Result<StreamType, SourceResolveError> {
    // A name written in text supplied at run time may not be one the owning
    // module keeps internal. An alias's own body was written in the module,
    // so the names it uses are resolved as the module sees them.
    if written && context.fingerprint.runtime_text && context.fingerprint.internal.contains(name) {
        return Err(SourceResolveError::UnknownAlias {
            name: name.clone(),
            span,
        });
    }
    let generic = &context.fingerprint.generic;
    if arguments.is_empty() {
        return match context.get(name) {
            Some(ty) => Ok(ty.clone()),
            None => Err(match generic.get(name) {
                Some(declaration) => SourceResolveError::AliasArity {
                    name: name.clone(),
                    expected: declaration.parameters.len(),
                    found: 0,
                    span,
                },
                None => SourceResolveError::UnknownAlias {
                    name: name.clone(),
                    span,
                },
            }),
        };
    }
    let declaration = generic.get(name).ok_or_else(|| match context.get(name) {
        Some(_) => SourceResolveError::AliasArity {
            name: name.clone(),
            expected: 0,
            found: arguments.len(),
            span,
        },
        None => SourceResolveError::UnknownAlias {
            name: name.clone(),
            span,
        },
    })?;
    if let Some(start) = active.iter().position(|entry| entry == name) {
        let mut path = active[start..].to_vec();
        path.push(name.clone());
        return Err(SourceResolveError::AliasCycle { path, span });
    }
    let arguments = arguments
        .iter()
        .map(|argument| {
            resolve_type(argument, &mut |name, arguments, span| {
                instantiate(context, name, arguments, span, active, written)
            })
        })
        .collect::<Result<EcoVec<_>, _>>()?;
    let body = substitute(declaration, &arguments, span)?;
    active.push(name.clone());
    let result = resolve_type(&body, &mut |name, arguments, span| {
        instantiate(context, name, arguments, span, active, false)
    });
    active.pop();
    match declaration.kind.newtype_stem() {
        Some(stem) => Ok(newtype_instance(stem, arguments, result?)),
        None => result,
    }
}

/// A generic alias's body with its parameters replaced by the arguments it
/// was applied to.
///
/// The arguments are the use site's own source types, so they resolve in the
/// scope that wrote them rather than in the alias's.
fn substitute(
    declaration: &AliasDeclaration,
    arguments: &[StreamType],
    span: Span,
) -> Result<SourceType, SourceResolveError> {
    if declaration.parameters.len() != arguments.len() {
        return Err(SourceResolveError::AliasArity {
            name: TypePath::local(declaration.name.clone()),
            expected: declaration.parameters.len(),
            found: arguments.len(),
            span,
        });
    }
    let bound: BTreeMap<&TypeName, &StreamType> =
        declaration.parameters.iter().zip(arguments).collect();
    Ok(substituted(&declaration.ty, &bound))
}

/// Replace a generic alias's parameters with source types rather than resolved
/// ones, which is what inlining one generic alias into another needs.
pub(crate) fn substitute_source(
    source: &SourceType,
    bound: &BTreeMap<&TypeName, &SourceType>,
) -> SourceType {
    let kind = match &source.kind {
        SourceTypeKind::Named(name, arguments) if arguments.is_empty() && !name.is_qualified() => {
            match bound.get(name.name()) {
                Some(argument) => return (*argument).clone(),
                None => SourceTypeKind::Named(name.clone(), EcoVec::new()),
            }
        }
        SourceTypeKind::Named(name, arguments) => SourceTypeKind::Named(
            name.clone(),
            arguments
                .iter()
                .map(|ty| substitute_source(ty, bound))
                .collect(),
        ),
        SourceTypeKind::List(ty) => SourceTypeKind::List(Box::new(substitute_source(ty, bound))),
        SourceTypeKind::Map(ty) => SourceTypeKind::Map(Box::new(substitute_source(ty, bound))),
        SourceTypeKind::Expr(ty) => SourceTypeKind::Expr(Box::new(substitute_source(ty, bound))),
        SourceTypeKind::Tuple(types) => SourceTypeKind::Tuple(
            types
                .iter()
                .map(|ty| substitute_source(ty, bound))
                .collect(),
        ),
        SourceTypeKind::Struct(fields, open) => SourceTypeKind::Struct(
            fields
                .iter()
                .map(|(name, ty)| (name.clone(), substitute_source(ty, bound)))
                .collect(),
            *open,
        ),
        SourceTypeKind::Function(arguments, result) => SourceTypeKind::Function(
            arguments
                .iter()
                .map(|ty| substitute_source(ty, bound))
                .collect(),
            Box::new(substitute_source(result, bound)),
        ),
        SourceTypeKind::Union(alternatives) => SourceTypeKind::Union(
            alternatives
                .iter()
                .map(|alternative| SourceAlternative {
                    tag: alternative.tag.clone(),
                    payload: alternative
                        .payload
                        .as_ref()
                        .map(|ty| substitute_source(ty, bound)),
                    span: alternative.span,
                })
                .collect(),
        ),
        // Only the arguments can name an enclosing parameter: the
        // representation is closed over the newtype's own.
        SourceTypeKind::NewtypeInstance(instance) => {
            SourceTypeKind::NewtypeInstance(Box::new(NewtypeInstance {
                arguments: instance
                    .arguments
                    .iter()
                    .map(|ty| substitute_source(ty, bound))
                    .collect(),
                ..(**instance).clone()
            }))
        }
        kind => kind.clone(),
    };
    SourceType {
        kind,
        span: source.span,
    }
}

/// Match a generic source pattern against one concrete structural type.
pub(crate) fn match_source_type(
    context: &SourceContext,
    pattern: &SourceType,
    actual: &StreamType,
    parameters: &BTreeSet<TypeName>,
    bound: &mut BTreeMap<TypeName, StreamType>,
) -> bool {
    fn matches(
        context: &SourceContext,
        pattern: &SourceType,
        actual: &StreamType,
        parameters: &BTreeSet<TypeName>,
        bound: &mut BTreeMap<TypeName, StreamType>,
        active: &mut Vec<(TypePath, *const StreamType)>,
    ) -> bool {
        use SourceTypeKind::*;
        if let Named(path, arguments) = &pattern.kind {
            if arguments.is_empty() && !path.is_qualified() {
                if let Some(parameter) = parameters.get(path.name()) {
                    return match bound.get(parameter) {
                        Some(previous) => {
                            previous == actual
                                || previous == &StreamType::Any
                                || actual == &StreamType::Any
                        }
                        None => {
                            bound.insert(parameter.clone(), actual.clone());
                            true
                        }
                    };
                }
            }
            let alias = context.generic().get(path).or_else(|| {
                (!path.is_qualified()).then(|| {
                    context
                        .generic()
                        .iter()
                        .find(|(name, _)| name.name() == path.name())
                        .map(|(_, alias)| alias)
                })?
            });
            if let Some(alias) = alias {
                // An alias expanding back to itself at the same position of
                // `actual` would never consume it. The same alias nested
                // deeper, as in `Option<Struct<x: Option<T>>>`, is matched.
                let position = (path.clone(), std::ptr::from_ref(actual));
                if alias.parameters.len() != arguments.len() || active.contains(&position) {
                    return false;
                }
                // A newtype is matched by identity, and its arguments
                // against the instance's, which is how a phantom argument
                // its representation never mentions is still inferred.
                if let Some(stem) = alias.kind.newtype_stem() {
                    return match actual {
                        StreamType::Newtype(schema) if schema.stem() == stem.as_str() => arguments
                            .iter()
                            .zip(schema.arguments())
                            .all(|(pattern, actual)| {
                                matches(context, pattern, actual, parameters, bound, active)
                            }),
                        StreamType::Any => true,
                        _ => false,
                    };
                }
                let substitutions = alias.parameters.iter().zip(arguments).collect();
                let expanded = substitute_source(&alias.ty, &substitutions);
                active.push(position);
                let result = matches(context, &expanded, actual, parameters, bound, active);
                active.pop();
                return result;
            }
            return arguments.is_empty()
                && context.get(path).is_some_and(|resolved| resolved == actual);
        }
        // `Any` remains gradually consistent with every structural shape.
        // Bare parameters are handled above so an `Any` argument still binds
        // the parameter and can ground a result-only inference.
        if actual == &StreamType::Any {
            return true;
        }
        match (&pattern.kind, actual) {
            (Any, _) => true,
            (Int, StreamType::Int)
            | (Float, StreamType::Float)
            | (Str, StreamType::Str)
            | (Bool, StreamType::Bool)
            | (Unit, StreamType::Unit) => true,
            (List(pattern), StreamType::List(actual))
            | (Map(pattern), StreamType::Map(actual))
            | (Expr(pattern), StreamType::Expr(actual)) => {
                matches(context, pattern, actual, parameters, bound, active)
            }
            (Tuple(patterns), StreamType::Tuple(actuals)) => {
                patterns.len() == actuals.len()
                    && patterns.iter().zip(actuals).all(|(pattern, actual)| {
                        matches(context, pattern, actual, parameters, bound, active)
                    })
            }
            (Struct(patterns, pattern_open), StreamType::Struct(actuals, actual_open)) => {
                let fields_match = patterns.iter().all(|(name, pattern)| {
                    actuals
                        .iter()
                        .find(|(actual, _)| actual == name)
                        .is_some_and(|(_, actual)| {
                            matches(context, pattern, actual, parameters, bound, active)
                        })
                });
                let pattern_fields_present = patterns
                    .iter()
                    .all(|(name, _)| actuals.iter().any(|(actual, _)| actual == name));
                let actual_fields_present = actuals
                    .iter()
                    .all(|(name, _)| patterns.iter().any(|(pattern, _)| pattern == name));
                fields_match
                    && ((*pattern_open && pattern_fields_present)
                        || (*actual_open && actual_fields_present)
                        || (!*pattern_open && !*actual_open && patterns.len() == actuals.len()))
            }
            (
                Function(pattern_args, pattern_result),
                StreamType::Function(actual_args, actual_result),
            ) => {
                pattern_args.len() == actual_args.len()
                    && pattern_args
                        .iter()
                        .zip(actual_args)
                        .all(|(pattern, actual)| {
                            matches(context, pattern, actual, parameters, bound, active)
                        })
                    && matches(
                        context,
                        pattern_result,
                        actual_result,
                        parameters,
                        bound,
                        active,
                    )
            }
            (Newtype(pattern), StreamType::Newtype(actual)) => pattern.brand() == actual.brand(),
            (NewtypeInstance(pattern), StreamType::Newtype(actual)) => {
                actual.stem() == pattern.stem.as_str()
                    && pattern.arguments.len() == actual.arguments().len()
                    && pattern
                        .arguments
                        .iter()
                        .zip(actual.arguments())
                        .all(|(pattern, actual)| {
                            matches(context, pattern, actual, parameters, bound, active)
                        })
            }
            (Union(patterns), StreamType::Union(actual)) => {
                patterns.len() == actual.alternatives().len()
                    && patterns.iter().all(|pattern| {
                        actual
                            .alternatives()
                            .iter()
                            .find(|actual| pattern.tag == *actual.tag())
                            .is_some_and(|actual| match (&pattern.payload, actual.payload()) {
                                (None, UnionPayload::Nullary) => true,
                                (Some(pattern), UnionPayload::Of(actual)) => {
                                    matches(context, pattern, actual, parameters, bound, active)
                                }
                                _ => false,
                            })
                    })
            }
            _ => false,
        }
    }
    matches(context, pattern, actual, parameters, bound, &mut Vec::new())
}

fn substituted(source: &SourceType, bound: &BTreeMap<&TypeName, &StreamType>) -> SourceType {
    let kind = match &source.kind {
        // A parameter stands for the argument itself, so a parameter applied
        // to nothing becomes whatever was passed, however complex.
        SourceTypeKind::Named(name, arguments) if arguments.is_empty() && !name.is_qualified() => {
            match bound.get(name.name()) {
                Some(argument) => return (*argument).clone().into(),
                None => SourceTypeKind::Named(name.clone(), EcoVec::new()),
            }
        }
        SourceTypeKind::Named(name, arguments) => SourceTypeKind::Named(
            name.clone(),
            arguments.iter().map(|ty| substituted(ty, bound)).collect(),
        ),
        SourceTypeKind::List(ty) => SourceTypeKind::List(Box::new(substituted(ty, bound))),
        SourceTypeKind::Map(ty) => SourceTypeKind::Map(Box::new(substituted(ty, bound))),
        SourceTypeKind::Expr(ty) => SourceTypeKind::Expr(Box::new(substituted(ty, bound))),
        SourceTypeKind::Tuple(types) => {
            SourceTypeKind::Tuple(types.iter().map(|ty| substituted(ty, bound)).collect())
        }
        SourceTypeKind::Struct(fields, open) => SourceTypeKind::Struct(
            fields
                .iter()
                .map(|(name, ty)| (name.clone(), substituted(ty, bound)))
                .collect(),
            *open,
        ),
        SourceTypeKind::Function(arguments, result) => SourceTypeKind::Function(
            arguments.iter().map(|ty| substituted(ty, bound)).collect(),
            Box::new(substituted(result, bound)),
        ),
        SourceTypeKind::Union(alternatives) => SourceTypeKind::Union(
            alternatives
                .iter()
                .map(|alternative| SourceAlternative {
                    tag: alternative.tag.clone(),
                    payload: alternative
                        .payload
                        .as_ref()
                        .map(|ty| substituted(ty, bound)),
                    span: alternative.span,
                })
                .collect(),
        ),
        SourceTypeKind::NewtypeInstance(instance) => {
            SourceTypeKind::NewtypeInstance(Box::new(NewtypeInstance {
                arguments: instance
                    .arguments
                    .iter()
                    .map(|ty| substituted(ty, bound))
                    .collect(),
                ..(**instance).clone()
            }))
        }
        kind => kind.clone(),
    };
    SourceType {
        kind,
        span: source.span,
    }
}

fn resolve_type(
    source: &SourceType,
    lookup: &mut impl FnMut(&TypePath, &[SourceType], Span) -> Result<StreamType, SourceResolveError>,
) -> Result<StreamType, SourceResolveError> {
    Ok(match &source.kind {
        SourceTypeKind::Named(name, arguments) => lookup(name, arguments, source.span)?,
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
        SourceTypeKind::Union(alternatives) => {
            let mut seen = BTreeMap::new();
            let mut resolved = Vec::with_capacity(alternatives.len());
            for alternative in alternatives {
                if seen.insert(&alternative.tag, ()).is_some() {
                    return Err(SourceResolveError::Union {
                        cause: UnionSchemaError::DuplicateTag(alternative.tag.clone()),
                        span: alternative.span,
                    });
                }
                resolved.push(UnionAlternative::new(
                    alternative.tag.clone(),
                    match &alternative.payload {
                        None => UnionPayload::Nullary,
                        Some(ty) => UnionPayload::Of(resolve_type(ty, lookup)?),
                    },
                ));
            }
            StreamType::Union(ClosedUnion::new(resolved).map_err(|cause| {
                SourceResolveError::Union {
                    cause,
                    span: source.span,
                }
            })?)
        }
        SourceTypeKind::Newtype(schema) => {
            StreamType::Newtype(schema.try_map(|ty| resolve_type(ty, lookup))?)
        }
        SourceTypeKind::NewtypeInstance(instance) => {
            let arguments = instance
                .arguments
                .iter()
                .map(|ty| resolve_type(ty, lookup))
                .collect::<Result<EcoVec<_>, _>>()?;
            if arguments.len() != instance.parameters.len() {
                return Err(SourceResolveError::AliasArity {
                    name: TypePath::local(TypeName(instance.stem.clone())),
                    expected: instance.parameters.len(),
                    found: arguments.len(),
                    span: source.span,
                });
            }
            let bound: BTreeMap<&TypeName, &StreamType> =
                instance.parameters.iter().zip(&arguments).collect();
            let repr = resolve_type(&substituted(&instance.repr, &bound), lookup)?;
            newtype_instance(&instance.stem, arguments, repr)
        }
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
        assert!(spec.source_context().get(&path("Unused")).is_some());
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

    fn path(s: &str) -> TypePath {
        TypePath::local(name(s))
    }

    fn named(s: &str, span: Span) -> SourceType {
        SourceType {
            kind: SourceTypeKind::Named(TypePath::local(name(s)), EcoVec::new()),
            span,
        }
    }

    fn declaration(s: &str, ty: SourceType) -> AliasDeclaration {
        AliasDeclaration {
            name: name(s),
            parameters: EcoVec::new(),
            internal: false,
            kind: AliasKind::Structural,
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
        assert_eq!(context.get(&path("A")), Some(&StreamType::Int));
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

    /// A runtime-expression site writes its namespace's fingerprint into
    /// its identity as JSON, whose keys must be strings, so aliases keyed by
    /// path are written as entries rather than as a map.
    #[test]
    fn a_fingerprint_with_aliases_serializes_as_json() {
        let mut builder = SourceContext::builder();
        builder.insert(name("Count"), StreamType::Int).unwrap();
        builder
            .insert_source(AliasDeclaration {
                parameters: EcoVec::from([name("A")]),
                ..declaration("Box", named("A", Span::new(3, 4)))
            })
            .unwrap();
        let context = builder.build().unwrap();
        let written = serde_json::to_string(context.fingerprint()).unwrap();
        assert!(
            written.contains(r#""aliases":[[{"name":"Count"},"Int"]]"#),
            "{written}"
        );
        assert!(
            written.contains(r#""generic":[[{"name":"Box"},"#),
            "{written}"
        );
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
                name: path("Missing"),
                span
            }
        );
        for (target, expected) in [
            ("A", vec![path("A"), path("A")]),
            ("B", vec![path("A"), path("B"), path("A")]),
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
            builder.build().unwrap().get(&path("A")),
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
