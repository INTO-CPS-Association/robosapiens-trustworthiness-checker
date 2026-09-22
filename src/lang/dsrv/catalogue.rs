//! Modules compiled into the checker.
//!
//! A catalogue registers **package roots**, such as `std`, and the modules
//! under them. A root owns its whole absolute prefix: every module path that
//! begins with it is either one the catalogue holds or none at all. So an
//! application may not declare a module there, and an import of an owned
//! path the catalogue does not hold is an error where it is imported, rather
//! than a file looked for on disk.
//!
//! An embedded module is activated by an ordinary `use` of it; there is no
//! `mod` for it and no prelude. It is read under its own header alone, never
//! under settings requested for the application, and it may import only
//! other modules of its catalogue.

use crate::lang::dsrv::path::ModuleName;

/// One module compiled into the checker.
#[derive(Debug)]
pub struct EmbeddedModule {
    /// The module's absolute path, one segment per entry.
    pub path: &'static [&'static str],
    /// The logical file diagnostics name it by.
    pub file: &'static str,
    pub source: &'static str,
}

impl EmbeddedModule {
    fn is(&self, path: &[ModuleName]) -> bool {
        self.path.len() == path.len()
            && self
                .path
                .iter()
                .zip(path)
                .all(|(segment, name)| *segment == name.as_str())
    }
}

/// The package roots a program's module paths may reach without a `mod`,
/// and the modules under them.
#[derive(Debug)]
pub struct Catalogue {
    pub roots: &'static [&'static str],
    pub modules: &'static [EmbeddedModule],
}

impl Catalogue {
    /// The modules every program may import.
    pub const STANDARD: Self = Self {
        roots: &["std"],
        modules: &[EmbeddedModule {
            path: &["std", "option"],
            file: "std/option.dsrv",
            source: include_str!("std/option.dsrv"),
        }],
    };

    /// The registered root `path` begins with, if any.
    pub fn owner(&self, path: &[ModuleName]) -> Option<&'static str> {
        let first = path.first()?;
        self.roots
            .iter()
            .copied()
            .find(|root| *root == first.as_str())
    }

    /// The module this catalogue holds at `path`.
    pub fn get(&self, path: &[ModuleName]) -> Option<&'static EmbeddedModule> {
        self.modules.iter().find(|module| module.is(path))
    }
}
