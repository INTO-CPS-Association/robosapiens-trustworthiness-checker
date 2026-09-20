//! Qualified names: where a module path ends, and what follows it.
//!
//! The split is lexical. A module segment is lowercase and a type or tag is
//! capitalised, so the parser fixes the boundary from the spelling alone and
//! never resolves a segment. That matters because parsing resolves no names:
//! a lowercase qualifier names a module item, a capitalised one names a union
//! whose tag follows.
//!
//! ```text
//! lib::opt::Option::Some   module lib::opt, type Option, tag Some
//! lib::other::Tick          module lib::other, item Tick
//! Kind::Idle                 type Kind, tag Idle
//! option::is_some_and         module option, item is_some_and
//! ```

use std::fmt;

use ecow::{EcoString, EcoVec};

use crate::lang::dsrv::source::{SourceResolveError, TypeName};
use crate::lang::dsrv::span::Span;

/// One lowercase segment of a module path.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize)]
pub struct ModuleName(EcoString);

impl ModuleName {
    pub fn new(name: impl Into<EcoString>) -> Result<Self, SourceResolveError> {
        let name = name.into();
        let mut chars = name.chars();
        if !chars
            .next()
            .is_some_and(|c| c.is_ascii_lowercase() || c == '_')
            || !chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
        {
            return Err(SourceResolveError::InvalidName { name });
        }
        Ok(Self(name))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ModuleName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A module path and the name reached through it.
///
/// An empty module path is a name written without a qualifier, which is the
/// only form that exists before `modules`.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize)]
pub struct TypePath {
    #[serde(skip_serializing_if = "EcoVec::is_empty")]
    module: EcoVec<ModuleName>,
    name: TypeName,
}

impl TypePath {
    pub fn local(name: TypeName) -> Self {
        Self {
            module: EcoVec::new(),
            name,
        }
    }

    pub fn new(module: EcoVec<ModuleName>, name: TypeName) -> Self {
        Self { module, name }
    }

    /// The segments before the name, outermost first. Empty for a local name.
    pub fn module(&self) -> &[ModuleName] {
        &self.module
    }

    pub fn name(&self) -> &TypeName {
        &self.name
    }

    /// Whether this path reaches outside the file that wrote it, which is
    /// what `modules` adds.
    pub fn is_qualified(&self) -> bool {
        !self.module.is_empty()
    }
}

impl fmt::Display for TypePath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for segment in &self.module {
            write!(f, "{segment}::")?;
        }
        write!(f, "{}", self.name)
    }
}

/// One segment of a path as it was written.
///
/// Case decides the kind, so the parser classifies a segment without
/// resolving it: lowercase names a module, capitalised names a type or a tag.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize)]
pub enum PathSegment {
    /// `self`: the module the enclosing path names.
    Zelf,
    Module(ModuleName),
    Name(TypeName),
}

impl PathSegment {
    /// The segment as written, which is what a diagnostic should print.
    pub fn as_str(&self) -> &str {
        match self {
            Self::Zelf => "self",
            Self::Module(name) => name.as_str(),
            Self::Name(name) => name.as_str(),
        }
    }
}

impl fmt::Display for PathSegment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// What follows a path in a `use`.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize)]
pub enum ImportKind {
    /// `use a::b` — the path names the item itself.
    Item,
    /// `use a::*`
    Glob,
    /// `use a::{…}`, each entry a path in its own right.
    Group(EcoVec<UseTree>),
}

/// One `use`, as written.
///
/// The type is recursive because a group entry is itself a `use` relative to
/// the enclosing path, which is what lets `use a::{self, b::{C, D}}` be one
/// shape rather than three special cases.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize)]
pub struct UseTree {
    path: EcoVec<PathSegment>,
    kind: ImportKind,
    span: Span,
}

impl UseTree {
    pub fn new(path: EcoVec<PathSegment>, kind: ImportKind, span: Span) -> Self {
        Self { path, kind, span }
    }

    pub fn path(&self) -> &[PathSegment] {
        &self.path
    }

    pub fn kind(&self) -> &ImportKind {
        &self.kind
    }

    pub fn span(&self) -> Span {
        self.span
    }

    /// The single segment this path names, when it names exactly one.
    ///
    /// `use experimental::{…}` is told apart from an item import this way,
    /// and nothing else may be spelled with one segment and a group.
    pub fn sole_segment(&self) -> Option<&PathSegment> {
        match self.path.as_slice() {
            [segment] => Some(segment),
            _ => None,
        }
    }

    /// Whether this is the header line that configures the file rather than
    /// an import of an item, which decides where it may appear (S9).
    ///
    /// The first segment decides, so `use experimental::tagged_unions` and
    /// `use experimental::{…}` are both header lines.
    pub fn is_experimental(&self) -> bool {
        matches!(self.path.first(), Some(PathSegment::Module(name)) if name.as_str() == "experimental")
    }
}

impl fmt::Display for UseTree {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut first = true;
        for segment in &self.path {
            if !first {
                write!(f, "::")?;
            }
            write!(f, "{segment}")?;
            first = false;
        }
        match &self.kind {
            ImportKind::Item => Ok(()),
            ImportKind::Glob => write!(f, "::*"),
            ImportKind::Group(items) => {
                write!(f, "::{{")?;
                for (index, item) in items.iter().enumerate() {
                    if index > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{item}")?;
                }
                write!(f, "}}")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use test_log::test;

    fn module(segments: &[&str]) -> EcoVec<ModuleName> {
        segments
            .iter()
            .map(|s| ModuleName::new(*s).expect("a lowercase segment"))
            .collect()
    }

    fn name(s: &str) -> TypeName {
        TypeName::new(s).expect("a type name")
    }

    #[test]
    fn a_local_name_has_no_module_and_prints_bare() {
        let path = TypePath::local(name("Kind"));
        assert!(!path.is_qualified());
        assert!(path.module().is_empty());
        assert_eq!(path.to_string(), "Kind");
    }

    #[test]
    fn a_qualified_name_prints_every_segment_in_order() {
        let path = TypePath::new(module(&["lib", "opt"]), name("Option"));
        assert!(path.is_qualified());
        assert_eq!(path.to_string(), "lib::opt::Option");
    }

    #[test]
    fn a_module_segment_must_be_lowercase() {
        assert!(ModuleName::new("other").is_ok());
        assert!(ModuleName::new("_private").is_ok());
        assert!(ModuleName::new("Other").is_err());
    }
}
