//! Where DSRV source came from.
//!
//! A program owns one immutable [`SourceArchive`] holding the text of every
//! file it was expanded from. Nodes do not hold files: parsed and semantic
//! metadata, and function-table entries, carry a compact [`NodeOrigin`] made
//! of archive-local [`SourceId`]s beside their byte [`Span`]. An ID means
//! something only in the archive that issued it, or in one derived from it
//! by [`merge`](SourceArchive::merge) or [`capture`](SourceArchive::capture);
//! it is never a persistent identity and is never part of a semantic
//! fingerprint.
//!
//! File handles are materialised only where they must outlive the program:
//!
//! - a finding materialises an owned [`SourceLocation`] when its checking
//!   attempt finishes, so it can be rendered after the program, its loader
//!   and the rest of the report are gone;
//! - a `dynamic` or `defer` site captures the files its runtime expression source may
//!   need, so that text can be located after the program is dropped.
//!
//! File handles are [`Arc`] whatever `thread-safe-ast` selects: findings
//! travel inside errors that must be `Send` and `Sync`, as they always were.
//! Only the archive containers are shared through `AstShared`.
//!
//! A node's primary site is its span in its own source: for code inlined
//! from another module that is the call site. Such code additionally
//! records its definition site, which a diagnostic presents as a note.
//! Programmatically constructed trees carry no source at all, and their
//! findings stay unlocated rather than being attributed to a guessed file.

use std::fmt;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use ecow::{EcoString, EcoVec};

use super::modules::{ModulePath, show_path};
use super::span::Span;

/// What a source file is called in a diagnostic.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum SourceLabel {
    /// A file read from the filesystem, named by the path it was read from.
    Path(EcoString),
    /// Source compiled into the binary, named by its logical path.
    Embedded(EcoString),
    /// Text supplied directly rather than read from a file, such as a
    /// string handed to the parser or an expression supplied at run time.
    Supplied(EcoString),
}

impl fmt::Display for SourceLabel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Path(path) => f.write_str(path),
            Self::Embedded(name) => write!(f, "<embedded {name}>"),
            Self::Supplied(name) => f.write_str(name),
        }
    }
}

/// The label of text given to the parser as a string.
pub(crate) const STRING_LABEL: &str = "<string>";

/// The label of an expression supplied to `dynamic` or `defer` at run time.
pub(crate) const RUNTIME_EXPRESSION_LABEL: &str = "<runtime expression>";

/// One file of a program: its label, the module it provides, and its text
/// exactly as it was read. A file refers to no tree and no callable, so
/// holding one never keeps a program alive.
pub struct SourceFile {
    label: SourceLabel,
    module: ModulePath,
    text: EcoString,
}

impl SourceFile {
    pub(crate) fn new(label: SourceLabel, module: ModulePath, text: impl Into<EcoString>) -> Self {
        Self {
            label,
            module,
            text: text.into(),
        }
    }

    pub fn label(&self) -> &SourceLabel {
        &self.label
    }

    /// The module this file provides; empty for a program's root.
    pub fn module(&self) -> &ModulePath {
        &self.module
    }

    pub fn text(&self) -> &str {
        &self.text
    }
}

/// Printed by label: the text is too large to be useful in a message.
impl fmt::Debug for SourceFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SourceFile")
            .field("label", &self.label)
            .field("module", &show_path(&self.module))
            .finish_non_exhaustive()
    }
}

/// A file of one archive. Local to that archive and those derived from it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct SourceId(NonZeroU32);

impl SourceId {
    fn from_index(index: usize) -> Self {
        let id = u32::try_from(index + 1).expect("an archive holds fewer than u32::MAX files");
        Self(NonZeroU32::new(id).expect("one more than an index is never zero"))
    }

    fn index(self) -> usize {
        self.0.get() as usize - 1
    }
}

/// A byte range of one archived file.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct SourceSite {
    pub(crate) source: SourceId,
    pub(crate) span: Span,
}

impl SourceSite {
    pub(crate) fn new(source: SourceId, span: Span) -> Self {
        Self { source, span }
    }
}

/// Where a node came from, beside the span the node already carries.
///
/// `source` is the file its span is in: the call site's file, for code
/// inlined from another module. `definition` is where such code was written.
/// Both are absent on programmatically constructed trees. Neither takes part
/// in equality of the node, nor in any semantic identity.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub(crate) struct NodeOrigin {
    pub(crate) source: Option<SourceId>,
    pub(crate) definition: Option<SourceSite>,
}

impl NodeOrigin {
    pub(crate) const UNLOCATED: Self = Self {
        source: None,
        definition: None,
    };

    pub(crate) fn new(source: Option<SourceId>, definition: Option<SourceSite>) -> Self {
        Self { source, definition }
    }

    pub(crate) fn is_unlocated(&self) -> bool {
        self.source.is_none() && self.definition.is_none()
    }
}

/// Which archive issued an ID. Tokens are never reused within a process.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ArchiveToken(u64);

impl ArchiveToken {
    fn fresh() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        Self(NEXT.fetch_add(1, Ordering::Relaxed))
    }
}

/// Grafting provenance between archives that do not share IDs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ProvenanceError {
    #[error("source provenance from another program cannot be grafted without remapping it")]
    ForeignArchive,
}

/// The immutable text of every file of one program, addressed by
/// [`SourceId`].
///
/// A program's specification owns it, and its checked and elaborated forms
/// share it. A capture keeps only some of its files, at the same IDs.
#[derive(Clone)]
pub(crate) struct SourceArchive {
    token: ArchiveToken,
    /// Archives whose IDs mean the same files here: those this one was
    /// merged onto or captured from.
    covers: EcoVec<ArchiveToken>,
    files: Vec<Option<Arc<SourceFile>>>,
}

impl Default for SourceArchive {
    fn default() -> Self {
        Self::new()
    }
}

impl SourceArchive {
    pub(crate) fn new() -> Self {
        Self {
            token: ArchiveToken::fresh(),
            covers: EcoVec::new(),
            files: Vec::new(),
        }
    }

    /// An archive of one file.
    pub(crate) fn single(file: SourceFile) -> (Self, SourceId) {
        let mut archive = Self::new();
        let id = archive.push(file);
        (archive, id)
    }

    /// Add a file while the archive is being built.
    pub(crate) fn push(&mut self, file: SourceFile) -> SourceId {
        self.files.push(Some(Arc::new(file)));
        SourceId::from_index(self.files.len() - 1)
    }

    pub(crate) fn token(&self) -> ArchiveToken {
        self.token
    }

    /// Whether IDs issued by `token` address the same files here.
    pub(crate) fn resolves(&self, token: ArchiveToken) -> bool {
        self.token == token || self.covers.contains(&token)
    }

    pub(crate) fn file(&self, id: SourceId) -> Option<&Arc<SourceFile>> {
        self.files.get(id.index()).and_then(Option::as_ref)
    }

    /// The first file, which is a program's root module.
    pub(crate) fn root(&self) -> Option<SourceId> {
        self.files
            .first()
            .is_some_and(Option::is_some)
            .then(|| SourceId::from_index(0))
    }

    /// Every file still held, with its ID.
    pub(crate) fn files(&self) -> impl Iterator<Item = (SourceId, &Arc<SourceFile>)> {
        self.files
            .iter()
            .enumerate()
            .filter_map(|(index, file)| Some((SourceId::from_index(index), file.as_ref()?)))
    }

    /// # Panics
    ///
    /// If this archive does not hold `site`'s file: an ID from another
    /// archive, or a file its capture released, is an invariant violation.
    pub(crate) fn resolve(&self, site: SourceSite) -> OwnedSite {
        let file = self
            .file(site.source)
            .unwrap_or_else(|| panic!("source {:?} is not held by this archive", site.source));
        OwnedSite {
            file: Arc::clone(file),
            span: site.span,
        }
    }

    /// Materialise the owned location of a finding at `span` in a node
    /// from `origin`.
    pub(crate) fn locate(&self, origin: NodeOrigin, span: Span) -> SourceLocation {
        SourceLocation::owned(
            origin
                .source
                .map(|source| self.resolve(SourceSite::new(source, span))),
            origin.definition.map(|site| self.resolve(site)),
        )
    }

    /// This archive with `other`'s files added after its own.
    ///
    /// This archive's IDs keep their meaning in the result; `other`'s are
    /// renumbered, and the returned remap says how. A file both hold, by
    /// handle, is kept once.
    pub(crate) fn merge(&self, other: &Self) -> (Self, SourceRemap) {
        let mut covers = self.covers.clone();
        covers.push(self.token);
        let mut merged = Self {
            token: ArchiveToken::fresh(),
            covers,
            files: self.files.clone(),
        };
        let ids = other
            .files
            .iter()
            .map(|file| {
                let file = file.as_ref()?;
                let existing = merged
                    .files
                    .iter()
                    .position(|held| held.as_ref().is_some_and(|held| Arc::ptr_eq(held, file)));
                Some(match existing {
                    Some(index) => SourceId::from_index(index),
                    None => {
                        merged.files.push(Some(Arc::clone(file)));
                        SourceId::from_index(merged.files.len() - 1)
                    }
                })
            })
            .collect();
        (merged, SourceRemap { ids })
    }

    /// Keep only the files `ids` name, at the same IDs, so the result
    /// addresses exactly what this archive did for them and nothing else
    /// stays alive.
    pub(crate) fn capture(&self, ids: impl IntoIterator<Item = SourceId>) -> Self {
        let mut files = vec![None; self.files.len()];
        for id in ids {
            let file = self
                .file(id)
                .unwrap_or_else(|| panic!("source {id:?} is not held by this archive"));
            files[id.index()] = Some(Arc::clone(file));
        }
        // Released files at the end need no placeholder.
        while files.last().is_some_and(Option::is_none) {
            files.pop();
        }
        Self {
            token: self.token,
            covers: self.covers.clone(),
            files,
        }
    }

    /// How many files are held.
    #[cfg(test)]
    pub(crate) fn held(&self) -> usize {
        self.files.iter().flatten().count()
    }
}

impl fmt::Debug for SourceArchive {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list()
            .entries(self.files().map(|(_, file)| file.label()))
            .finish()
    }
}

/// How one archive's IDs are numbered in the archive it was merged into.
#[derive(Clone, Debug)]
pub(crate) struct SourceRemap {
    ids: Vec<Option<SourceId>>,
}

impl SourceRemap {
    /// The merged archive's ID for `id`. An ID the merged-in archive did
    /// not hold is refused rather than guessed.
    pub(crate) fn id(&self, id: SourceId) -> Result<SourceId, ProvenanceError> {
        self.ids
            .get(id.index())
            .copied()
            .flatten()
            .ok_or(ProvenanceError::ForeignArchive)
    }
}

/// A byte range of a file this site owns, so it can be presented after
/// the program it came from has been dropped.
#[derive(Clone)]
pub struct OwnedSite {
    file: Arc<SourceFile>,
    span: Span,
}

impl OwnedSite {
    pub fn file(&self) -> &SourceFile {
        &self.file
    }

    pub fn label(&self) -> &SourceLabel {
        self.file.label()
    }

    pub fn span(&self) -> Span {
        self.span
    }

    /// The text the span covers, if it lies within the file on character
    /// boundaries.
    pub fn snippet(&self) -> Option<&str> {
        self.file.text().get(self.span.to_range())
    }

    /// The one-based line and Unicode-scalar column at which the span
    /// starts, if it lies within the file on character boundaries.
    pub fn position(&self) -> Option<(usize, usize)> {
        position(self.file.text(), self.span)
    }
}

impl PartialEq for OwnedSite {
    fn eq(&self, other: &Self) -> bool {
        self.span == other.span
            && (Arc::ptr_eq(&self.file, &other.file)
                || (self.file.label == other.file.label && self.file.text == other.file.text))
    }
}

impl Eq for OwnedSite {}

impl fmt::Debug for OwnedSite {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}..{}", self.label(), self.span.start, self.span.end)
    }
}

/// The one-based line and Unicode-scalar column at which `span` starts, if
/// `span` lies within `source` on character boundaries.
pub fn position(source: &str, span: Span) -> Option<(usize, usize)> {
    let (start, end) = (span.start as usize, span.end as usize);
    if start > end || !source.is_char_boundary(start) || !source.is_char_boundary(end) {
        return None;
    }
    let before = &source[..start];
    let line_start = before.rfind('\n').map_or(0, |newline| newline + 1);
    Some((
        before.matches('\n').count() + 1,
        before[line_start..].chars().count() + 1,
    ))
}

/// Where a finding applies, owning the files it names.
///
/// The primary site is where the finding is reported: for code inlined
/// from another module, the call. The definition site, when there is one,
/// is where that code was written, presented as a note. A finding about a
/// programmatically constructed tree has neither.
///
/// A location is presentation, not meaning: findings that agree in
/// everything else are equal wherever they are located.
#[derive(Clone, Default)]
pub struct SourceLocation {
    state: LocationState,
}

#[derive(Clone, Default)]
enum LocationState {
    #[default]
    Unlocated,
    /// Archive-local, while its checking attempt is still running. Never
    /// escapes that attempt.
    Pending { origin: NodeOrigin, span: Span },
    Owned {
        primary: Option<OwnedSite>,
        definition: Option<OwnedSite>,
    },
}

impl SourceLocation {
    pub(crate) fn pending(origin: NodeOrigin, span: Span) -> Self {
        Self {
            state: if origin.is_unlocated() {
                LocationState::Unlocated
            } else {
                LocationState::Pending { origin, span }
            },
        }
    }

    pub(crate) fn owned(primary: Option<OwnedSite>, definition: Option<OwnedSite>) -> Self {
        Self {
            state: if primary.is_none() && definition.is_none() {
                LocationState::Unlocated
            } else {
                LocationState::Owned {
                    primary,
                    definition,
                }
            },
        }
    }

    /// Whether nothing has located this finding yet.
    pub(crate) fn is_unset(&self) -> bool {
        matches!(self.state, LocationState::Unlocated)
    }

    /// Turn archive-local IDs into owned files. Without an archive the
    /// finding stays unlocated: an ID is never resolved against an archive
    /// other than its own.
    pub(crate) fn materialise(&mut self, archive: Option<&SourceArchive>) {
        if let LocationState::Pending { origin, span } = self.state {
            *self = match archive {
                Some(archive) => archive.locate(origin, span),
                None => Self::default(),
            };
        }
    }

    pub fn primary(&self) -> Option<&OwnedSite> {
        match &self.state {
            LocationState::Owned { primary, .. } => primary.as_ref(),
            _ => None,
        }
    }

    pub fn definition(&self) -> Option<&OwnedSite> {
        match &self.state {
            LocationState::Owned { definition, .. } => definition.as_ref(),
            _ => None,
        }
    }
}

impl PartialEq for SourceLocation {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}

impl Eq for SourceLocation {}

impl fmt::Debug for SourceLocation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.state {
            LocationState::Unlocated => f.write_str("unlocated"),
            LocationState::Pending { .. } => f.write_str("pending"),
            LocationState::Owned {
                primary,
                definition,
            } => {
                let mut tuple = f.debug_tuple("at");
                if let Some(primary) = primary {
                    tuple.field(primary);
                }
                if let Some(definition) = definition {
                    tuple.field(&format_args!("defined at {definition:?}"));
                }
                tuple.finish()
            }
        }
    }
}

#[cfg(test)]
mod tests;
