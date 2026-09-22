//! Language settings: the dialect, edition and experiments a specification
//! declares, resolved once during expansion.
//!
//! Expansion is the only stage that may branch on a feature, so the accessor
//! that asks whether one is enabled is private to this module's parent. Later
//! stages see the settings only as part of the source fingerprint, through
//! the dialect (which decides admission), through the [`IfPolicy`] each
//! node's settings give its `if`, and through the header text.

use std::collections::BTreeSet;
use std::fmt;
use std::str::FromStr;

use ecow::EcoString;

use crate::core::StreamType;
use crate::lang::dsrv::ast::{Declaration, DsrvSpecification, ExprKind, ExprRef};
use crate::lang::dsrv::path::{ImportKind, PathSegment, UseTree};
use crate::lang::dsrv::source::{AliasDeclaration, SourceType, SourceTypeKind};
use crate::lang::dsrv::span::Span;
use crate::lang::dsrv::syntax::ParsedDeclaration;

/// Which language a specification is written in. The dialects are nested:
/// Core ⊂ Full ⊂ Distributed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize)]
pub enum Dialect {
    /// The basic temporal language, closest to the published calculus.
    Core,
    /// The whole language, and the default.
    #[default]
    Full,
    /// Full DSRV plus the distribution primitives.
    Distributed,
}

impl Dialect {
    /// The name used after `language` in a header.
    fn header_name(self) -> Option<&'static str> {
        match self {
            Self::Core => Some("core"),
            Self::Full => None,
            Self::Distributed => Some("distributed"),
        }
    }

    fn from_header_name(name: &str) -> Option<Self> {
        [Self::Core, Self::Distributed]
            .into_iter()
            .find(|dialect| dialect.header_name() == Some(name))
    }
}

impl fmt::Display for Dialect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Core => "Core DSRV",
            Self::Full => "Full DSRV",
            Self::Distributed => "Distributed DSRV",
        })
    }
}

/// A dated set of default behaviours. Each edition is a variant, so an
/// edition is always one the checker knows.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize)]
pub enum Edition {
    /// The language as of 16 September 2026: eager `if` and failing lookups.
    #[default]
    E2026_09,
}

impl Edition {
    /// The edition of a specification that names none.
    pub const BASE: Self = Self::E2026_09;
    /// Every edition, oldest first.
    pub const ALL: &'static [Self] = &[Self::E2026_09];

    fn year_month(self) -> (u16, u8) {
        match self {
            Self::E2026_09 => (2026, 9),
        }
    }

    fn known() -> String {
        Self::ALL
            .iter()
            .map(Self::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    }
}

impl fmt::Display for Edition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (year, month) = self.year_month();
        write!(f, "{year}-{month:02}")
    }
}

impl FromStr for Edition {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .iter()
            .copied()
            .find(|edition| edition.to_string() == text)
            .ok_or_else(|| {
                format!(
                    "unknown edition `{text}`; known editions: {}",
                    Self::known()
                )
            })
    }
}

/// An experiment a specification opts into with `use experimental::…`.
///
/// Only experiments whose constructs exist are listed; each feature commit
/// adds its own variant.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize)]
pub enum Feature {
    TaggedUnions,
    PatternMatching,
    Generics,
    Modules,
    Functions,
    Constants,
    Casts,
    LazyIf,
}

impl Feature {
    /// Every current experiment, which is what `use experimental::*` enables.
    pub const ALL: &'static [Self] = &[
        Self::TaggedUnions,
        Self::PatternMatching,
        Self::Generics,
        Self::Modules,
        Self::Functions,
        Self::Constants,
        Self::Casts,
        Self::LazyIf,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Self::TaggedUnions => "tagged_unions",
            Self::PatternMatching => "pattern_matching",
            Self::Generics => "generics",
            Self::Modules => "modules",
            Self::Functions => "functions",
            Self::Constants => "constants",
            Self::Casts => "casts",
            Self::LazyIf => "lazy_if",
        }
    }

    fn from_name(name: &str) -> Option<Self> {
        Self::ALL
            .iter()
            .copied()
            .find(|feature| feature.name() == name)
    }

    fn known() -> String {
        Self::ALL
            .iter()
            .map(|feature| feature.name())
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// Whether `meta_feature` enables this experiment. The match names every
    /// experiment, so a new one does not compile until it is placed in or
    /// out of each meta-feature.
    fn is_in(self, meta_feature: MetaFeature) -> bool {
        match meta_feature {
            // Every Full DSRV preview this release implements. Growing this
            // set changes what a file naming the meta-feature means, so it also
            // bumps [`EXPERIMENTAL_REVISION`].
            MetaFeature::HighLevelDsrv => match self {
                Self::TaggedUnions
                | Self::PatternMatching
                | Self::Generics
                | Self::Modules
                | Self::Functions
                | Self::Constants
                | Self::Casts
                | Self::LazyIf => true,
            },
        }
    }
}

/// A name that enables a fixed set of experiments at once, as in
/// `use experimental::high_level_dsrv`.
///
/// A meta-feature is resolved when the header is read, so the settings, the
/// fingerprint and the printed header hold its experiments rather than its
/// name: a file that names it is the same program as one that lists them.
/// A meta-feature enables experiments only; it never changes the dialect, so it
/// does not reach Distributed DSRV and does not change which runtimes ensure_runtime_support a
/// specification.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize)]
pub enum MetaFeature {
    /// Every Full DSRV preview experiment this release implements.
    HighLevelDsrv,
}

impl MetaFeature {
    /// Every meta-feature.
    pub const ALL: &'static [Self] = &[Self::HighLevelDsrv];

    pub fn name(self) -> &'static str {
        match self {
            Self::HighLevelDsrv => "high_level_dsrv",
        }
    }

    /// The experiments this meta-feature enables, in [`Feature::ALL`] order.
    pub fn experiments(self) -> impl Iterator<Item = Feature> {
        Feature::ALL
            .iter()
            .copied()
            .filter(move |feature| feature.is_in(self))
    }

    fn from_name(name: &str) -> Option<Self> {
        Self::ALL
            .iter()
            .copied()
            .find(|meta_feature| meta_feature.name() == name)
    }

    fn known() -> String {
        Self::ALL
            .iter()
            .map(|meta_feature| meta_feature.name())
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// The revision of the experiments in this release. Bump it whenever any
/// experiment's meaning changes, so compiled code cached under one meaning is
/// never reused under another, and whenever a [`MetaFeature`] gains an
/// experiment.
///
/// Revision 2: code inlined from another module's def is read under that
/// module's settings and in its namespace, rather than its caller's.
///
/// Revision 3: `lazy_if` joins the experiments, and with it
/// `high_level_dsrv`.
pub const EXPERIMENTAL_REVISION: u32 = 3;

/// How an `if` chooses between its branches.
///
/// This is a property of where the `if` was written: an `if` inlined from
/// another module's def, or arriving as runtime expression source, keeps the policy of the
/// module that wrote it. Stages after expansion read it through
/// [`crate::lang::dsrv::ast::ExprRef::if_policy`], never through the
/// experiment itself.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize)]
pub enum IfPolicy {
    /// Both branches advance on every tick the `if` is evaluated, and only
    /// the condition decides which value is published. The language's
    /// behaviour without `lazy_if`.
    #[default]
    Eager,
    /// Only the selected branch runs. Each branch has its own timeline,
    /// which advances only on the ticks that select it, so an unselected
    /// branch neither executes, advances or commits temporal state, nor
    /// reports errors or sparse values.
    Lazy,
}

/// The resolved settings of one specification.
#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize)]
pub struct LanguageConfig {
    dialect: Dialect,
    edition: Edition,
    experiments: BTreeSet<Feature>,
}

impl LanguageConfig {
    /// The dialect decides which runtimes may ensure_runtime_support a specification.
    pub fn dialect(&self) -> Dialect {
        self.dialect
    }

    /// The edition whose defaults a specification is read under.
    pub fn edition(&self) -> Edition {
        self.edition
    }

    /// These settings inside a program of `dialect`. A module's own header
    /// decides the syntax its text may use, but which runtimes may ensure_runtime_support
    /// that text is the program's to decide.
    pub(crate) fn admitted_as(&self, dialect: Dialect) -> Self {
        Self {
            dialect,
            ..self.clone()
        }
    }

    /// Experiment names, for diagnostics such as the warning printed when a
    /// specification runs with experiments.
    pub fn experiment_names(&self) -> impl Iterator<Item = &'static str> + '_ {
        self.experiments.iter().map(|feature| feature.name())
    }

    /// The experimental revision these settings depend on: `None` without
    /// experiments, since stable behaviour does not change between releases.
    pub(crate) fn experimental_revision(&self) -> Option<u32> {
        (!self.experiments.is_empty()).then_some(EXPERIMENTAL_REVISION)
    }

    /// Whether `def` is available.
    pub(crate) fn has_functions(&self) -> bool {
        self.has(Feature::Functions)
    }

    /// Whether imports beyond `use experimental` are available.
    pub(crate) fn has_modules(&self) -> bool {
        self.has(Feature::Modules)
    }

    /// Whether `const` is available.
    pub(crate) fn has_constants(&self) -> bool {
        self.has(Feature::Constants)
    }

    /// How an `if` written under these settings chooses its branches.
    pub(crate) fn if_policy(&self) -> IfPolicy {
        if self.has(Feature::LazyIf) {
            IfPolicy::Lazy
        } else {
            IfPolicy::Eager
        }
    }

    pub(super) fn has(&self, feature: Feature) -> bool {
        self.experiments.contains(&feature)
    }

    /// The header lines that declare these settings; empty for the defaults.
    pub(crate) fn header(&self) -> String {
        let Self {
            dialect,
            edition,
            experiments,
        } = self;
        let mut header = String::new();
        if let Some(name) = dialect.header_name() {
            header.push_str(&format!("language {name}\n"));
        }
        if *edition != Edition::BASE {
            header.push_str(&format!("edition {edition}\n"));
        }
        if !experiments.is_empty() {
            let names = experiments
                .iter()
                .map(|feature| feature.name())
                .collect::<Vec<_>>()
                .join(", ");
            header.push_str(&format!("use experimental::{{{names}}}\n"));
        }
        header
    }
}

impl fmt::Display for LanguageConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}, edition {}", self.dialect, self.edition)?;
        let names = self.experiment_names().collect::<Vec<_>>();
        if !names.is_empty() {
            write!(f, ", experiments {}", names.join(", "))?;
        }
        Ok(())
    }
}

/// Settings requested from outside a file, such as on the command line.
/// They apply to a file that does not declare them; a file that declares
/// something different is an error.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LanguageRequest {
    pub dialect: Option<Dialect>,
    pub edition: Option<Edition>,
}

/// A problem with a specification's language settings.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum LanguageError {
    #[error("unknown language `{name}` at {span:?}; expected `core` or `distributed`")]
    UnknownDialect { name: EcoString, span: Span },

    #[error("unknown edition `{text}` at {span:?}; known editions: {known}")]
    UnknownEdition {
        text: EcoString,
        known: String,
        span: Span,
    },

    #[error(
        "unknown experimental feature `{name}` at {span:?}; current experiments: {known}; meta-features: {meta_features}"
    )]
    UnknownFeature {
        name: EcoString,
        known: String,
        meta_features: String,
        span: Span,
    },

    #[error("a second `{keyword}` line at {span:?}; the first is at {first:?}")]
    DuplicateHeader {
        keyword: &'static str,
        first: Span,
        span: Span,
    },

    #[error("`{keyword}` at {span:?} must come before every other declaration")]
    HeaderAfterDeclaration { keyword: &'static str, span: Span },

    #[error("Core DSRV accepts no experiments, but `use experimental` is declared at {span:?}")]
    ExperimentsInCore { span: Span },

    #[error("the file declares {declared} but {requested} was requested")]
    RequestConflict { declared: String, requested: String },

    #[error("{construct} at {span:?} is not part of Core DSRV")]
    NotCore { construct: &'static str, span: Span },

    #[error("{construct} at {span:?} needs `language distributed`")]
    NeedsDistributed { construct: &'static str, span: Span },

    #[error(
        "{construct} `{name}` at {span:?} is capitalised, and under `use experimental::tagged_unions` a capitalised name is a union tag"
    )]
    CapitalisedName {
        construct: &'static str,
        name: EcoString,
        span: Span,
    },

    #[error("{construct} at {span:?} needs `use experimental::{feature}`")]
    NeedsExperiment {
        construct: &'static str,
        feature: &'static str,
        span: Span,
    },
}

/// The experiments one `use experimental::…` line enables.
///
/// The name may be grouped, starred or written on its own, so
/// `use experimental::{a, b}`, `use experimental::*` and
/// `use experimental::a` all read the same way. A meta-feature name reads as
/// the experiments it stands for.
fn experimental_features(tree: &UseTree) -> Result<Vec<Feature>, LanguageError> {
    match (tree.path(), tree.kind()) {
        ([_], ImportKind::Glob) => Ok(Feature::ALL.to_vec()),
        ([_], ImportKind::Group(items)) => items
            .iter()
            .map(named_experiments)
            .collect::<Result<Vec<_>, _>>()
            .map(|groups| groups.concat()),
        ([_, PathSegment::Module(name)], ImportKind::Item) => {
            experiments_by_name(name.as_str(), tree.span())
        }
        _ => Err(unknown_experiment(tree)),
    }
}

/// One entry of `use experimental::{…}`, which is a plain lowercase name.
fn named_experiments(item: &UseTree) -> Result<Vec<Feature>, LanguageError> {
    match (item.sole_segment(), item.kind()) {
        (Some(PathSegment::Module(name)), ImportKind::Item) => {
            experiments_by_name(name.as_str(), item.span())
        }
        _ => Err(unknown_experiment(item)),
    }
}

fn experiments_by_name(name: &str, span: Span) -> Result<Vec<Feature>, LanguageError> {
    if let Some(feature) = Feature::from_name(name) {
        return Ok(vec![feature]);
    }
    if let Some(meta_feature) = MetaFeature::from_name(name) {
        return Ok(meta_feature.experiments().collect());
    }
    Err(unknown_feature(name.into(), span))
}

fn unknown_experiment(tree: &UseTree) -> LanguageError {
    unknown_feature(tree.to_string().into(), tree.span())
}

fn unknown_feature(name: EcoString, span: Span) -> LanguageError {
    LanguageError::UnknownFeature {
        name,
        known: Feature::known(),
        meta_features: MetaFeature::known(),
        span,
    }
}

/// Read the header declarations and combine them with any outside request.
pub(crate) fn resolve_language(
    declarations: &[ParsedDeclaration],
    request: LanguageRequest,
) -> Result<LanguageConfig, LanguageError> {
    let mut dialect: Option<(Dialect, Span)> = None;
    let mut edition: Option<(Edition, Span)> = None;
    let mut experiments = BTreeSet::new();
    let mut experiments_span = None;
    let mut body_started = false;

    for declaration in declarations {
        let (keyword, span) = match declaration {
            ParsedDeclaration::Language(_, span) => ("language", *span),
            // `mod` pulls a file into the program, so it stays in the header
            // region (S6) even though an item import no longer does (S9).
            ParsedDeclaration::Mod { span, .. } => ("mod", *span),
            ParsedDeclaration::Edition(_, span) => ("edition", *span),
            // An item import populates a namespace rather than configuring
            // the file, so it belongs to the body and may sit anywhere (S9).
            ParsedDeclaration::Use { tree, span } => {
                if !tree.is_experimental() {
                    body_started = true;
                    continue;
                }
                ("use experimental", *span)
            }
            _ => {
                body_started = true;
                continue;
            }
        };
        if body_started {
            return Err(LanguageError::HeaderAfterDeclaration { keyword, span });
        }
        match declaration {
            ParsedDeclaration::Language(name, span) => {
                if let Some((_, first)) = dialect {
                    return Err(LanguageError::DuplicateHeader {
                        keyword,
                        first,
                        span: *span,
                    });
                }
                let resolved = Dialect::from_header_name(name).ok_or_else(|| {
                    LanguageError::UnknownDialect {
                        name: name.clone(),
                        span: *span,
                    }
                })?;
                dialect = Some((resolved, *span));
            }
            ParsedDeclaration::Edition(text, span) => {
                if let Some((_, first)) = edition {
                    return Err(LanguageError::DuplicateHeader {
                        keyword,
                        first,
                        span: *span,
                    });
                }
                let resolved =
                    text.parse::<Edition>()
                        .map_err(|_| LanguageError::UnknownEdition {
                            text: text.clone(),
                            known: Edition::known(),
                            span: *span,
                        })?;
                edition = Some((resolved, *span));
            }
            // Several `use experimental` lines add up, so they are not
            // duplicate headers.
            ParsedDeclaration::Use { tree, span } => {
                experiments_span.get_or_insert(*span);
                experiments.extend(experimental_features(tree)?);
            }
            // Which files `mod` names is the collector's work, not the
            // language configuration's.
            ParsedDeclaration::Mod { .. } => {}
            _ => unreachable!("only header declarations reach here"),
        }
    }

    let dialect = reconcile(
        dialect.map(|(value, _)| value),
        request.dialect,
        |dialect| match dialect.header_name() {
            Some(name) => format!("`language {name}`"),
            None => "Full DSRV".to_owned(),
        },
    )?;
    let edition = reconcile(
        edition.map(|(value, _)| value),
        request.edition,
        |edition| format!("edition {edition}"),
    )?;
    if let (Dialect::Core, Some(span)) = (dialect, experiments_span) {
        return Err(LanguageError::ExperimentsInCore { span });
    }
    Ok(LanguageConfig {
        dialect,
        edition,
        experiments,
    })
}

/// A setting declared in the file wins only when the request agrees with it.
fn reconcile<T: Copy + PartialEq + Default>(
    declared: Option<T>,
    requested: Option<T>,
    describe: impl Fn(T) -> String,
) -> Result<T, LanguageError> {
    match (declared, requested) {
        (Some(declared), Some(requested)) if declared != requested => {
            Err(LanguageError::RequestConflict {
                declared: describe(declared),
                requested: describe(requested),
            })
        }
        (Some(value), _) | (None, Some(value)) => Ok(value),
        (None, None) => Ok(T::default()),
    }
}

/// A specification checked to be Core DSRV. Only [`CoreDsrvSpecification::check`]
/// constructs one, so a function that takes this type only ever sees Core.
#[derive(Clone, Debug)]
pub struct CoreDsrvSpecification(DsrvSpecification);

impl CoreDsrvSpecification {
    /// Accept a specification whose dialect is Core and which uses only Core
    /// constructs and types.
    pub fn check(specification: DsrvSpecification) -> Result<Self, LanguageError> {
        let dialect = specification.source_context().language().dialect();
        if dialect != Dialect::Core {
            return Err(LanguageError::RequestConflict {
                declared: dialect.to_string(),
                requested: Dialect::Core.to_string(),
            });
        }
        for node in specification.nodes() {
            check_core_node(node)?;
        }
        for entry in specification.declarations() {
            match entry {
                Declaration::Input {
                    annotation: Some(ty),
                    span,
                    ..
                }
                | Declaration::Output {
                    annotation: Some(ty),
                    span,
                    ..
                }
                | Declaration::Aux {
                    annotation: Some(ty),
                    span,
                    ..
                } => check_core_type(ty, *span)?,
                Declaration::TypeAlias { span, .. } => {
                    return Err(LanguageError::NotCore {
                        construct: "a type alias",
                        span: *span,
                    });
                }
                Declaration::Input { .. }
                | Declaration::Output { .. }
                | Declaration::Aux { .. }
                | Declaration::Equation { .. } => {}
            }
        }
        Ok(Self(specification))
    }

    pub fn specification(&self) -> &DsrvSpecification {
        &self.0
    }

    pub fn into_specification(self) -> DsrvSpecification {
        self.0
    }
}

/// Check one expression tree against Core, as runtime sources are.
pub(crate) fn is_core_fragment(root: ExprRef<'_>) -> Result<(), LanguageError> {
    use contiguous_tree::TreeCursorExt;
    root.postorder().try_for_each(check_core_node)
}

/// The distribution primitives belong to Distributed DSRV only. Core
/// specifications report them through the Core check instead.
/// Whether a name is a union tag rather than a stream or a binder. Case
/// decides, in expressions as in patterns, so a mistyped tag is an unknown
/// tag rather than a name that resolves to something else.
pub(super) fn is_tag_name(name: &str) -> bool {
    name.starts_with(|c: char| c.is_ascii_uppercase())
}

/// A name a file writes where only a lower-case one belongs. Reported at the
/// declaration, because that is what has to change.
pub(super) fn check_declared_name(
    construct: &'static str,
    name: &str,
    span: Span,
    language: &LanguageConfig,
) -> Result<(), LanguageError> {
    if !language.has(Feature::TaggedUnions) || !is_tag_name(name) {
        return Ok(());
    }
    Err(LanguageError::CapitalisedName {
        construct,
        name: name.into(),
        span,
    })
}

/// Every construct an experiment gates is refused here unless its file opted
/// in, so the rest of the front end never meets one it cannot handle.
pub(crate) fn check_experiment_node(
    node: ExprRef<'_>,
    language: &LanguageConfig,
) -> Result<(), LanguageError> {
    // A qualifier that reaches through a module needs `modules`, whatever
    // the constructor itself needs.
    if let ExprKind::Constructor(_, _, Some(qualifier)) = node.kind()
        && qualifier.is_qualified()
        && !language.has(Feature::Modules)
    {
        return Err(LanguageError::NeedsExperiment {
            construct: "a module-qualified name",
            feature: Feature::Modules.name(),
            span: node.span(),
        });
    }
    let (construct, feature) = match node.kind() {
        ExprKind::Constructor(..) => ("a union constructor", Feature::TaggedUnions),
        ExprKind::Match(..) => ("`match`", Feature::PatternMatching),
        ExprKind::Matches(..) => ("`matches`", Feature::PatternMatching),
        ExprKind::Cast(..)
        | ExprKind::Trunc(..)
        | ExprKind::Floor(..)
        | ExprKind::Ceil(..)
        | ExprKind::Round(..) => ("a cast", Feature::Casts),
        _ => return Ok(()),
    };
    if language.has(feature) {
        return Ok(());
    }
    Err(LanguageError::NeedsExperiment {
        construct,
        feature: feature.name(),
        span: node.span(),
    })
}

/// A source type may name a construct an experiment gates. Alias definitions
/// and annotations are both checked before they resolve, so the gate reports
/// the spelling the writer used, at its own span.
/// A type alias's own declaration, which takes parameters only under
/// `generics`, plus everything its body needs.
pub(crate) fn check_alias(
    alias: &AliasDeclaration,
    language: &LanguageConfig,
) -> Result<(), LanguageError> {
    if alias.internal && !language.has(Feature::Modules) {
        return Err(LanguageError::NeedsExperiment {
            construct: "an internal type alias",
            feature: Feature::Modules.name(),
            span: alias.span,
        });
    }
    if !alias.parameters.is_empty() && !language.has(Feature::Generics) {
        return Err(LanguageError::NeedsExperiment {
            construct: "a generic type alias",
            feature: Feature::Generics.name(),
            span: alias.span,
        });
    }
    check_experiment_type(&alias.ty, language)
}

pub(crate) fn check_experiment_type(
    source: &SourceType,
    language: &LanguageConfig,
) -> Result<(), LanguageError> {
    match &source.kind {
        SourceTypeKind::Union(alternatives) => {
            if !language.has(Feature::TaggedUnions) {
                return Err(LanguageError::NeedsExperiment {
                    construct: "a tagged union type",
                    feature: Feature::TaggedUnions.name(),
                    span: source.span,
                });
            }
            for payload in alternatives
                .iter()
                .filter_map(|alternative| alternative.payload.as_ref())
            {
                check_experiment_type(payload, language)?;
            }
        }
        SourceTypeKind::List(ty) | SourceTypeKind::Map(ty) | SourceTypeKind::Expr(ty) => {
            check_experiment_type(ty, language)?;
        }
        SourceTypeKind::Tuple(types) => {
            for ty in types {
                check_experiment_type(ty, language)?;
            }
        }
        SourceTypeKind::Struct(fields, _) => {
            for (_, ty) in fields {
                check_experiment_type(ty, language)?;
            }
        }
        SourceTypeKind::Function(arguments, ret) => {
            for ty in arguments {
                check_experiment_type(ty, language)?;
            }
            check_experiment_type(ret, language)?;
        }
        // Applying a name is what generics adds; naming one always worked.
        // Reaching through a module is what `modules` adds.
        SourceTypeKind::Named(name, arguments) => {
            if name.is_qualified() && !language.has(Feature::Modules) {
                return Err(LanguageError::NeedsExperiment {
                    construct: "a module-qualified type",
                    feature: Feature::Modules.name(),
                    span: source.span,
                });
            }
            if !arguments.is_empty() {
                if !language.has(Feature::Generics) {
                    return Err(LanguageError::NeedsExperiment {
                        construct: "a generic type",
                        feature: Feature::Generics.name(),
                        span: source.span,
                    });
                }
                for argument in arguments {
                    check_experiment_type(argument, language)?;
                }
            }
        }
        SourceTypeKind::Int
        | SourceTypeKind::Float
        | SourceTypeKind::Str
        | SourceTypeKind::Bool
        | SourceTypeKind::Unit
        | SourceTypeKind::Any => {}
    }
    Ok(())
}

pub(crate) fn check_dialect_node(node: ExprRef<'_>, dialect: Dialect) -> Result<(), LanguageError> {
    let construct = match node.kind() {
        ExprKind::MonitoredAt(..) => "`monitored_at`",
        ExprKind::Dist(..) => "`dist`",
        _ => return Ok(()),
    };
    match dialect {
        Dialect::Distributed => Ok(()),
        Dialect::Core => check_core_node(node),
        Dialect::Full => Err(LanguageError::NeedsDistributed {
            construct,
            span: node.span(),
        }),
    }
}

/// Every expression kind is listed, with no wildcard, so a new kind must be
/// placed inside or outside Core before this compiles.
pub(crate) fn check_core_node(node: ExprRef<'_>) -> Result<(), LanguageError> {
    use crate::lang::dsrv::ast::SyntaxLiteral;
    let construct = match node.kind() {
        ExprKind::Val(value) => match value {
            SyntaxLiteral::Int(_)
            | SyntaxLiteral::Float(_)
            | SyntaxLiteral::Str(_)
            | SyntaxLiteral::Bool(_)
            | SyntaxLiteral::Unit
            | SyntaxLiteral::NoVal => None,
            SyntaxLiteral::List(_) => Some("a list literal"),
            SyntaxLiteral::Tuple(_) => Some("a tuple literal"),
            SyntaxLiteral::Map(_) => Some("a map literal"),
            SyntaxLiteral::Struct(_) => Some("a struct literal"),
        },
        ExprKind::If(..)
        | ExprKind::SIndex(..)
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
        | ExprKind::Trunc(..)
        | ExprKind::Floor(..)
        | ExprKind::Ceil(..)
        | ExprKind::Round(..) => None,
        ExprKind::Constructor(..) => Some("a union constructor"),
        ExprKind::Match(..) => Some("`match`"),
        ExprKind::Matches(..) => Some("`matches`"),
        ExprKind::Lambda(..) => Some("a lambda"),
        ExprKind::Apply(..) => Some("a function call"),
        ExprKind::Fix(..) => Some("`fix`"),
        ExprKind::Partial(..) => Some("`partial`"),
        ExprKind::List(..) => Some("a list"),
        ExprKind::Tuple(..) => Some("a tuple"),
        ExprKind::LIndex(..)
        | ExprKind::LAppend(..)
        | ExprKind::LConcat(..)
        | ExprKind::LHead(..)
        | ExprKind::LTail(..)
        | ExprKind::LLen(..)
        | ExprKind::LMap(..)
        | ExprKind::LFilter(..)
        | ExprKind::LFold(..) => Some("a list operation"),
        ExprKind::Map(..) => Some("a map"),
        ExprKind::Struct(..) => Some("a struct"),
        ExprKind::ObjectLiteral(..) => Some("an object literal"),
        ExprKind::MGet(..)
        | ExprKind::MInsert(..)
        | ExprKind::MRemove(..)
        | ExprKind::MHasKey(..) => Some("a map operation"),
        ExprKind::SGet(..) => Some("a field read"),
        ExprKind::Sin(..) => Some("`sin`"),
        ExprKind::Cos(..) => Some("`cos`"),
        ExprKind::Tan(..) => Some("`tan`"),
        ExprKind::Abs(..) => Some("`abs`"),
        ExprKind::MonitoredAt(..) => Some("`monitored_at`"),
        ExprKind::Dist(..) => Some("`dist`"),
    };
    match construct {
        Some(construct) => Err(LanguageError::NotCore {
            construct,
            span: node.span(),
        }),
        None => Ok(()),
    }
}

/// Core has the scalar types, the gradual type, and `Expr` of those.
pub(crate) fn check_core_type(ty: &StreamType, span: Span) -> Result<(), LanguageError> {
    let construct = match ty {
        StreamType::Int
        | StreamType::Float
        | StreamType::Str
        | StreamType::Bool
        | StreamType::Unit
        | StreamType::Any => return Ok(()),
        StreamType::Expr(inner) => return check_core_type(inner, span),
        StreamType::List(_) => "a list type",
        StreamType::Tuple(_) => "a tuple type",
        StreamType::Map(_) => "a map type",
        StreamType::Struct(..) => "a struct type",
        StreamType::Function(..) => "a function type",
        StreamType::Union(..) => "a tagged union type",
    };
    Err(LanguageError::NotCore { construct, span })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lang::dsrv::parser::{
        DsrvParseError, check_core_source, parse_expr_with_context, parse_str, parse_str_with,
    };

    fn language_of(source: &str) -> LanguageConfig {
        parse_str(source)
            .unwrap()
            .source_context()
            .language()
            .clone()
    }

    fn language_error(source: &str) -> LanguageError {
        language_error_with(source, LanguageRequest::default())
    }

    fn language_error_with(source: &str, request: LanguageRequest) -> LanguageError {
        match parse_str_with(source, request) {
            Err(DsrvParseError::Language(error)) => error,
            other => panic!("expected a language error for {source:?}, got {other:?}"),
        }
    }

    fn config(dialect: Dialect, experiments: &[Feature]) -> LanguageConfig {
        LanguageConfig {
            dialect,
            edition: Edition::BASE,
            experiments: experiments.iter().copied().collect(),
        }
    }

    const BODY: &str = "in x: Int\nout y: Int\ny = x + 1\n";

    // R3.1-a
    #[test]
    fn a_file_without_a_header_is_full_base_edition_without_experiments() {
        assert_eq!(language_of(BODY), LanguageConfig::default());
        assert_eq!(LanguageConfig::default(), config(Dialect::Full, &[]));
    }

    // R3.1-b
    #[test]
    fn each_header_form_resolves() {
        let cases: [(&str, LanguageConfig); 5] = [
            ("language core\n", config(Dialect::Core, &[])),
            ("language distributed\n", config(Dialect::Distributed, &[])),
            ("edition 2026-09\n", config(Dialect::Full, &[])),
            (
                "use experimental::{tagged_unions}\n",
                config(Dialect::Full, &[Feature::TaggedUnions]),
            ),
            ("use experimental::*\n", config(Dialect::Full, Feature::ALL)),
        ];
        for (header, expected) in cases {
            assert_eq!(
                language_of(&format!("{header}{BODY}")),
                expected,
                "{header}"
            );
        }
        assert_eq!(
            language_of(&format!(
                "language distributed\nedition 2026-09\nuse experimental::{{tagged_unions}}\n{BODY}"
            )),
            config(Dialect::Distributed, &[Feature::TaggedUnions])
        );
    }

    // R3.1-c: several `use` lines add up rather than conflicting.
    #[test]
    fn experiments_from_several_use_lines_add_up() {
        assert_eq!(
            language_of(&format!(
                "use experimental::{{tagged_unions}}\nuse experimental::{{tagged_unions}}\n{BODY}"
            )),
            config(Dialect::Full, &[Feature::TaggedUnions])
        );
    }

    // R3.1-d: the settings a specification prints parse back to themselves.
    #[test]
    fn printed_settings_parse_back() {
        let source = format!("use experimental::{{tagged_unions}}\n{BODY}");
        let specification = parse_str(&source).unwrap();
        let printed = specification.to_string();
        assert!(
            printed.starts_with("use experimental::{tagged_unions}\n"),
            "{printed}"
        );
        assert_eq!(
            language_of(&printed),
            config(Dialect::Full, &[Feature::TaggedUnions])
        );
    }

    // R3.1-e: the fingerprint separates sources that differ only in their
    // experiments, and records the revision their meaning depends on.
    #[test]
    fn experiments_change_the_source_fingerprint() {
        let plain = parse_str(BODY).unwrap();
        let experimental =
            parse_str(&format!("use experimental::{{tagged_unions}}\n{BODY}")).unwrap();
        assert_ne!(
            plain.source_context().fingerprint(),
            experimental.source_context().fingerprint()
        );
        assert_eq!(
            plain.source_context().language().experimental_revision(),
            None
        );
        assert_eq!(
            experimental
                .source_context()
                .language()
                .experimental_revision(),
            Some(EXPERIMENTAL_REVISION)
        );
    }

    // R3.2
    #[test]
    fn header_errors_name_the_problem() {
        use LanguageError::*;
        let cases: [(&str, fn(&LanguageError) -> bool); 6] = [
            (
                "language paper\n",
                |e| matches!(e, UnknownDialect { name, .. } if name == "paper"),
            ),
            (
                "edition 2027-03\n",
                |e| matches!(e, UnknownEdition { text, known, .. } if text == "2027-03" && known == "2026-09"),
            ),
            (
                "edition 2026-9\n",
                |e| matches!(e, UnknownEdition { text, .. } if text == "2026-9"),
            ),
            ("edition 2026-13\n", |e| matches!(e, UnknownEdition { .. })),
            ("language core\nlanguage core\n", |e| {
                matches!(
                    e,
                    DuplicateHeader {
                        keyword: "language",
                        ..
                    }
                )
            }),
            ("edition 2026-09\nedition 2026-09\n", |e| {
                matches!(
                    e,
                    DuplicateHeader {
                        keyword: "edition",
                        ..
                    }
                )
            }),
        ];
        for (header, check) in cases {
            let error = language_error(&format!("{header}{BODY}"));
            assert!(check(&error), "{header}: {error}");
        }
        let unknown = language_error(&format!("use experimental::{{teleporting}}\n{BODY}"));
        assert!(
            matches!(&unknown, UnknownFeature { name, known, .. }
                if name == "teleporting"
                    && known
                        == "tagged_unions, pattern_matching, generics, modules, functions, constants, casts, lazy_if"),
            "{unknown}"
        );
        // An import of anything but `experimental` is an ordinary item
        // import, which needs `modules` rather than being unsupported.
        let namespace = language_error(&format!("use std::{{option}}\n{BODY}"));
        assert!(
            matches!(
                &namespace,
                NeedsExperiment {
                    feature: "modules",
                    ..
                }
            ),
            "{namespace}"
        );
        let in_core = language_error(&format!(
            "language core\nuse experimental::{{tagged_unions}}\n{BODY}"
        ));
        assert!(matches!(in_core, ExperimentsInCore { .. }), "{in_core}");

        let late = language_error(&format!("{BODY}edition 2026-09\n"));
        assert!(
            matches!(
                late,
                HeaderAfterDeclaration {
                    keyword: "edition",
                    ..
                }
            ),
            "{late}"
        );
    }

    // R3.3
    #[test]
    fn header_keywords_are_reserved_but_the_names_they_take_are_not() {
        for source in ["out edition\nedition = 1", "in language\n", "in use\n"] {
            assert!(
                matches!(parse_str(source), Err(DsrvParseError::Syntax(_))),
                "{source}"
            );
        }
        for source in [
            "in core\nout y\ny = core",
            "in distributed\nout y\ny = distributed",
            "in experimental\nout y\ny = experimental",
        ] {
            parse_str(source).unwrap_or_else(|error| panic!("{source}: {error}"));
        }
    }

    // R3.4-a
    #[test]
    fn a_core_file_yields_a_core_specification() {
        let source = "in x: Int\nout y: Int\ny = if x > 0 then default(x[1], 0) else -x\n";
        let core = check_core_source(source).unwrap();
        assert_eq!(
            core.specification().source_context().language().dialect(),
            Dialect::Core
        );
        // A header that agrees with the request is fine; another dialect is not.
        check_core_source(&format!("language core\n{source}")).unwrap();
        assert!(matches!(
            check_core_source(&format!("language distributed\n{source}")),
            Err(DsrvParseError::Language(
                LanguageError::RequestConflict { .. }
            ))
        ));
        // A Full specification is never Core, even if its constructs are.
        assert!(CoreDsrvSpecification::check(parse_str(source).unwrap()).is_err());
    }

    // R3.4-b
    #[test]
    fn every_construct_outside_core_is_named_at_its_span() {
        let cases = [
            ("sin(x)", "`sin`"),
            ("cos(x)", "`cos`"),
            ("tan(x)", "`tan`"),
            ("abs(x)", "`abs`"),
            ("List(x)", "a list"),
            ("Tuple(x, x)", "a tuple"),
            ("List.len(x)", "a list operation"),
            ("Map(\"a\": x)", "a map"),
            ("Map.get(x, \"a\")", "a map operation"),
            ("Struct(\"a\": x)", "a struct"),
            ("{ a: x }", "an object literal"),
            ("\\v: Int -> v", "a lambda"),
            ("x(x)", "a function call"),
            ("fix(x)", "`fix`"),
            ("partial(x, x)", "`partial`"),
            ("x.a", "a field read"),
            ("monitored_at(x, n)", "`monitored_at`"),
            ("dist(x, n)", "`dist`"),
        ];
        for (expression, construct) in cases {
            let source = format!("language core\nin x\nout y\ny = {expression}\n");
            match language_error(&source) {
                LanguageError::NotCore {
                    construct: found,
                    span,
                } => {
                    assert_eq!(found, construct, "{expression}");
                    let start = source.find(expression).unwrap() as u32;
                    assert!(span.start >= start, "{expression}: {span:?}");
                }
                other => panic!("{expression}: {other}"),
            }
        }
    }

    // R3.4-b: aliases and non-scalar types are outside Core too.
    #[test]
    fn core_rejects_aliases_and_collection_types() {
        assert!(matches!(
            language_error("language core\ntype T = Int\nin x: T\n"),
            LanguageError::NotCore {
                construct: "a type alias",
                ..
            }
        ));
        assert!(matches!(
            language_error("language core\nin x: List<Int>\n"),
            LanguageError::NotCore {
                construct: "a list type",
                ..
            }
        ));
    }

    // R3.4-c
    #[test]
    fn runtime_sources_under_a_core_specification_stay_in_core() {
        let spec = parse_str("language core\nin x\nout y\ny = x\n").unwrap();
        let context = spec.source_context().clone();
        parse_expr_with_context("x + 1", context.clone()).unwrap();
        assert!(matches!(
            parse_expr_with_context("abs(x)", context),
            Err(DsrvParseError::Language(LanguageError::NotCore { .. }))
        ));
    }

    // R3b.1
    #[test]
    fn distribution_primitives_need_the_distributed_dialect() {
        for (expression, construct) in [
            ("monitored_at(x, n)", "`monitored_at`"),
            ("dist(x, n)", "`dist`"),
        ] {
            let body = format!("in x\nout y\ny = {expression}\n");
            let error = language_error(&body);
            let LanguageError::NeedsDistributed {
                construct: found,
                span,
            } = error
            else {
                panic!("{expression}: {error}");
            };
            assert_eq!(found, construct);
            assert_eq!(span.start as usize, body.find(expression).unwrap());
            assert_eq!(
                error.to_string(),
                format!("{construct} at {span:?} needs `language distributed`")
            );
            let distributed = parse_str(&format!("language distributed\n{body}")).unwrap();
            assert_eq!(
                distributed.source_context().language().dialect(),
                Dialect::Distributed
            );
            // Runtime expression source follows the dialect of the specification it runs in.
            let full = parse_str(BODY).unwrap();
            assert!(matches!(
                parse_expr_with_context(expression, full.source_context().clone()),
                Err(DsrvParseError::Language(
                    LanguageError::NeedsDistributed { .. }
                ))
            ));
            parse_expr_with_context(expression, distributed.source_context().clone()).unwrap();
        }
    }

    // R3.4-d
    #[test]
    fn full_and_distributed_files_are_not_core_checked() {
        parse_str("in x\nout y\ny = abs(x)\n").unwrap();
        parse_str("language distributed\nin x\nout y\ny = abs(x)\n").unwrap();
    }

    // R3.5-a
    #[test]
    fn different_settings_give_different_fingerprints() {
        let full = parse_str(BODY).unwrap();
        let core = parse_str(&format!("language core\n{BODY}")).unwrap();
        let distributed = parse_str(&format!("language distributed\n{BODY}")).unwrap();
        let fingerprints = [&full, &core, &distributed]
            .map(|spec| serde_json::to_string(spec.source_context().fingerprint()).unwrap());
        assert_ne!(fingerprints[0], fingerprints[1]);
        assert_ne!(fingerprints[0], fingerprints[2]);
        assert_ne!(fingerprints[1], fingerprints[2]);
    }

    // R3.7
    #[test]
    fn printing_keeps_the_settings() {
        for header in ["language core\n", "language distributed\n"] {
            let spec = parse_str(&format!("{header}{BODY}")).unwrap();
            let printed = spec.to_string();
            assert!(printed.starts_with(header), "{printed}");
            let reparsed = parse_str(&printed).unwrap();
            assert_eq!(
                reparsed.source_context().language(),
                spec.source_context().language()
            );
            assert_eq!(reparsed, spec);
        }
        assert!(!parse_str(BODY).unwrap().to_string().contains("language"));
    }

    // R3.8-a, b
    #[test]
    fn a_request_applies_only_where_the_file_is_silent() {
        let core = LanguageRequest {
            dialect: Some(Dialect::Core),
            edition: None,
        };
        assert_eq!(
            parse_str_with(BODY, core)
                .unwrap()
                .source_context()
                .language(),
            &config(Dialect::Core, &[])
        );
        let error = language_error_with(&format!("language distributed\n{BODY}"), core);
        assert_eq!(
            error.to_string(),
            "the file declares `language distributed` but `language core` was requested"
        );
        let edition = LanguageRequest {
            dialect: None,
            edition: Some(Edition::BASE),
        };
        parse_str_with(&format!("edition 2026-09\n{BODY}"), edition).unwrap();
    }

    const HIGH_LEVEL: &str = "use experimental::high_level_dsrv\n";

    fn fingerprint_of(source: &str) -> String {
        serde_json::to_string(parse_str(source).unwrap().source_context().fingerprint()).unwrap()
    }

    /// One construct for each experiment `high_level_dsrv` enables, written
    /// without a header.
    const GATED: &[(Feature, &str)] = &[
        (Feature::TaggedUnions, "in s: Union<Stopped, Moving: Int>\n"),
        (
            Feature::PatternMatching,
            "in x: Int\nout y: Bool\ny = matches(x, _)\n",
        ),
        (Feature::Generics, "type Box<A> = List<A>\nin x: Int\n"),
        (
            Feature::Modules,
            "internal type Hidden = Int\nin x: Hidden\n",
        ),
        (Feature::Functions, "def f(v: Int) -> Int = v\nin x: Int\n"),
        (Feature::Constants, "const limit: Int = 3\nin x: Int\n"),
        (Feature::Casts, "in x: Int\nout y: Float\ny = x as Float\n"),
    ];

    // The resolved set is pinned together with the revision: an experiment
    // joining the meta-feature changes what files naming it mean, so it must
    // bump `EXPERIMENTAL_REVISION` and update both lines here. Planned
    // experiments are absent until they are implemented.
    #[test]
    fn high_level_dsrv_is_exactly_this_releases_full_previews() {
        assert_eq!(
            (
                MetaFeature::HighLevelDsrv.experiments().collect::<Vec<_>>(),
                EXPERIMENTAL_REVISION,
            ),
            (
                vec![
                    Feature::TaggedUnions,
                    Feature::PatternMatching,
                    Feature::Generics,
                    Feature::Modules,
                    Feature::Functions,
                    Feature::Constants,
                    Feature::Casts,
                    Feature::LazyIf,
                ],
                3,
            )
        );
        assert_eq!(
            language_of(&format!("{HIGH_LEVEL}{BODY}")),
            config(
                Dialect::Full,
                &MetaFeature::HighLevelDsrv.experiments().collect::<Vec<_>>()
            )
        );
        // Every experiment is placed in or out of every meta-feature; for now
        // every one is a Full DSRV preview.
        assert!(
            Feature::ALL
                .iter()
                .all(|f| f.is_in(MetaFeature::HighLevelDsrv))
        );
    }

    #[test]
    fn naming_the_meta_feature_is_listing_its_experiments() {
        let listed = MetaFeature::HighLevelDsrv
            .experiments()
            .map(Feature::name)
            .collect::<Vec<_>>()
            .join(", ");
        let explicit = format!("use experimental::{{{listed}}}\n{BODY}");
        let spellings = [
            format!("{HIGH_LEVEL}{BODY}"),
            format!("use experimental::{{high_level_dsrv}}\n{BODY}"),
            // Naming the meta-feature and some of its experiments, or the
            // meta-feature twice, adds up as other repeated names do.
            format!("use experimental::{{casts, high_level_dsrv, tagged_unions}}\n{BODY}"),
            format!("{HIGH_LEVEL}{HIGH_LEVEL}use experimental::generics\n{BODY}"),
        ];
        for source in &spellings {
            assert_eq!(language_of(source), language_of(&explicit), "{source}");
            assert_eq!(
                fingerprint_of(source),
                fingerprint_of(&explicit),
                "{source}"
            );
            assert_eq!(
                parse_str(source)
                    .unwrap()
                    .source_context()
                    .language()
                    .experimental_revision(),
                Some(EXPERIMENTAL_REVISION)
            );
        }
        // The settings print as the experiments, not the meta-feature, so a
        // printed file keeps its meaning in a release where the meta-feature
        // has grown.
        let printed = parse_str(&spellings[0]).unwrap().to_string();
        assert!(
            printed.starts_with(&format!(
                "use experimental::{{{}}}\n",
                language_of(&explicit)
                    .experiment_names()
                    .collect::<Vec<_>>()
                    .join(", ")
            )),
            "{printed}"
        );
        assert!(!printed.contains("high_level_dsrv"), "{printed}");
        assert_eq!(language_of(&printed), language_of(&explicit));
        // Fewer experiments are a different program.
        assert_ne!(
            fingerprint_of(&format!("{HIGH_LEVEL}{BODY}")),
            fingerprint_of(&format!("use experimental::{{tagged_unions}}\n{BODY}"))
        );
    }

    #[test]
    fn meta_feature_names_are_their_own() {
        for meta_feature in MetaFeature::ALL {
            assert_eq!(Feature::from_name(meta_feature.name()), None);
            assert_eq!(
                MetaFeature::from_name(meta_feature.name()),
                Some(*meta_feature)
            );
        }
        // A meta-feature is a name, not a path or a namespace.
        for header in [
            "use experimental::high_level_dsrv::casts\n",
            "use experimental::{high_level_dsrv::casts}\n",
            "use experimental::{high_level_dsrv::*}\n",
            "use experimental::high_level\n",
        ] {
            let error = language_error(&format!("{header}{BODY}"));
            assert!(
                matches!(&error, LanguageError::UnknownFeature { .. }),
                "{header}: {error}"
            );
        }
        let error = language_error(&format!("use experimental::{{teleporting}}\n{BODY}"));
        assert!(
            matches!(&error, LanguageError::UnknownFeature { meta_features, .. }
                if meta_features == "high_level_dsrv"),
            "{error}"
        );
        assert!(
            error
                .to_string()
                .ends_with("; meta-features: high_level_dsrv")
        );
    }

    #[test]
    fn the_meta_feature_satisfies_every_experiment_it_enables() {
        let mut covered = Vec::new();
        for (feature, body) in GATED {
            let error = language_error(body);
            assert!(
                matches!(&error, LanguageError::NeedsExperiment { feature: needed, .. }
                    if *needed == feature.name()),
                "{body}: {error}"
            );
            parse_str(&format!("use experimental::{}\n{body}", feature.name()))
                .unwrap_or_else(|error| panic!("{body}: {error}"));
            parse_str(&format!("{HIGH_LEVEL}{body}"))
                .unwrap_or_else(|error| panic!("{body}: {error}"));
            covered.push(*feature);
        }
        // `lazy_if` gates no syntax; it changes how every `if` runs.
        let lazy = parse_str(&format!("{HIGH_LEVEL}{BODY}")).unwrap();
        assert_eq!(lazy.source_context().language().if_policy(), IfPolicy::Lazy);
        covered.push(Feature::LazyIf);
        assert_eq!(
            covered,
            MetaFeature::HighLevelDsrv.experiments().collect::<Vec<_>>()
        );
    }

    // `lazy_if` changes how every `if` in the file runs, so it is part of
    // the program's identity, and it is still only an experiment: it
    // chooses no dialect, and Core refuses it.
    #[test]
    fn lazy_if_selects_the_if_policy_and_nothing_else() {
        let lazy = format!("use experimental::lazy_if\n{BODY}");
        assert_eq!(LanguageConfig::default().if_policy(), IfPolicy::Eager);
        assert_eq!(language_of(BODY).if_policy(), IfPolicy::Eager);
        let others = Feature::ALL
            .iter()
            .copied()
            .filter(|feature| *feature != Feature::LazyIf)
            .collect::<Vec<_>>();
        assert_eq!(config(Dialect::Full, &others).if_policy(), IfPolicy::Eager);
        assert_eq!(language_of(&lazy).if_policy(), IfPolicy::Lazy);
        assert_eq!(
            language_of(&lazy),
            config(Dialect::Full, &[Feature::LazyIf])
        );
        assert_eq!(
            language_of(&lazy).experimental_revision(),
            Some(EXPERIMENTAL_REVISION)
        );
        assert_ne!(fingerprint_of(BODY), fingerprint_of(&lazy));

        let distributed = format!("language distributed\n{lazy}");
        assert_eq!(language_of(&distributed).dialect(), Dialect::Distributed);
        assert_eq!(language_of(&distributed).if_policy(), IfPolicy::Lazy);
        assert_ne!(fingerprint_of(&lazy), fingerprint_of(&distributed));
        assert!(matches!(
            language_error(&format!("language core\n{lazy}")),
            LanguageError::ExperimentsInCore { .. }
        ));
    }

    #[test]
    fn the_meta_feature_does_not_choose_the_dialect() {
        let full = parse_str(&format!("{HIGH_LEVEL}{BODY}")).unwrap();
        assert_eq!(full.source_context().language().dialect(), Dialect::Full);
        // Distribution primitives still need `language distributed`.
        let dist = format!("{HIGH_LEVEL}in x\nout y\ny = dist(x, n)\n");
        assert!(
            matches!(
                language_error(&dist),
                LanguageError::NeedsDistributed { .. }
            ),
            "{dist}"
        );
        assert!(matches!(
            parse_expr_with_context("monitored_at(x, n)", full.source_context().clone()),
            Err(DsrvParseError::Language(
                LanguageError::NeedsDistributed { .. }
            ))
        ));
        let distributed = parse_str(&format!("language distributed\n{dist}")).unwrap();
        assert_eq!(
            distributed.source_context().language(),
            &config(
                Dialect::Distributed,
                &MetaFeature::HighLevelDsrv.experiments().collect::<Vec<_>>()
            )
        );
        assert_ne!(
            fingerprint_of(&format!("{HIGH_LEVEL}{BODY}")),
            fingerprint_of(&format!("language distributed\n{HIGH_LEVEL}{BODY}"))
        );
        // Core accepts no experiments, however they are named, and a Core
        // request is not overridden by the meta-feature.
        assert!(matches!(
            language_error(&format!("language core\n{HIGH_LEVEL}{BODY}")),
            LanguageError::ExperimentsInCore { .. }
        ));
        let core = LanguageRequest {
            dialect: Some(Dialect::Core),
            edition: None,
        };
        assert!(matches!(
            language_error_with(&format!("{HIGH_LEVEL}{BODY}"), core),
            LanguageError::ExperimentsInCore { .. }
        ));
    }

    #[test]
    fn the_meta_feature_does_not_widen_runtime_capabilities() {
        use crate::core::{RuntimeCapabilities, RuntimeCapability, ensure_runtime_support};
        // Enabling experiments needs nothing from a runtime by itself.
        let plain = parse_str(&format!("{HIGH_LEVEL}{BODY}")).unwrap();
        ensure_runtime_support(&plain, RuntimeCapabilities::NONE, "test").unwrap();
        // A construct still needs its capability, whatever enabled it.
        let source = format!(
            "{HIGH_LEVEL}type State = Union<Stopped, Moving: Int>\n\
             in x: Int\nout y: State\ny = State::Moving(x)\n"
        );
        let unions = parse_str(&source).unwrap();
        let error = ensure_runtime_support(&unions, RuntimeCapabilities::NONE, "test").unwrap_err();
        assert_eq!(
            error.requirement.capability,
            RuntimeCapability::TaggedUnions
        );
        ensure_runtime_support(
            &unions,
            RuntimeCapabilities::NONE.with(RuntimeCapability::TaggedUnions),
            "test",
        )
        .unwrap();
    }

    // Embedded modules declare the experiments they use by name, so what
    // they mean does not change when a meta-feature grows.
    #[test]
    fn embedded_modules_name_their_experiments() {
        use crate::lang::dsrv::catalogue::Catalogue;
        for module in Catalogue::STANDARD.modules {
            for line in module
                .source
                .lines()
                .filter(|line| line.trim_start().starts_with("use experimental"))
            {
                assert!(!line.contains('*'), "{}: {line}", module.file);
                for meta_feature in MetaFeature::ALL {
                    assert!(
                        !line.contains(meta_feature.name()),
                        "{}: {line}",
                        module.file
                    );
                }
            }
        }
    }
}
