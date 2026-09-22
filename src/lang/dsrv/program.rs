//! Shared filesystem boundary for loading and expanding complete DSRV programs.

use std::path::Path;

use anyhow::Context;

use super::{
    ast::DsrvSpecification,
    expand,
    expand::language::LanguageRequest,
    modules::{ModuleCollector, ModuleSources, module_file, show_path},
    path::ModuleName,
    source_map::SourceLabel,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ModuleOrigin {
    Filesystem(String),
    Embedded,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActivatedModule {
    pub path: String,
    pub origin: ModuleOrigin,
}

/// A program read from disk and expanded under the standard catalogue policy.
pub struct LoadedProgram {
    pub specification: DsrvSpecification,
    pub root_source: String,
    pub modules: Vec<ActivatedModule>,
}

/// Read a program and every filesystem module it declares.
pub async fn collect_modules_from_file(file: &str) -> anyhow::Result<ModuleSources> {
    let directory = Path::new(file)
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .to_owned();
    let root = smol::fs::read_to_string(file)
        .await
        .with_context(|| format!("reading {file}"))?;
    let mut collector = ModuleCollector::with_label(&root, SourceLabel::Path(file.into()))?;
    while let Some(path) = collector.next_request().map(<[ModuleName]>::to_vec) {
        let location = directory.join(module_file(&path));
        let source = smol::fs::read_to_string(&location).await.with_context(|| {
            format!(
                "reading module {} from {}",
                show_path(&path),
                location.display()
            )
        })?;
        collector.supply_labelled(
            &source,
            SourceLabel::Path(location.display().to_string().into()),
        )?;
    }
    Ok(collector.finish()?)
}

/// Load and expand the complete global root program without checking it.
pub async fn load_program_file(
    file: &str,
    request: LanguageRequest,
) -> anyhow::Result<LoadedProgram> {
    let sources = collect_modules_from_file(file).await?;
    let directory = Path::new(file).parent().unwrap_or_else(|| Path::new("."));
    let modules = sources
        .paths()
        .filter(|path| !path.is_empty())
        .map(|path| ActivatedModule {
            path: show_path(path),
            origin: if sources.is_embedded(path) {
                ModuleOrigin::Embedded
            } else {
                ModuleOrigin::Filesystem(directory.join(module_file(path)).display().to_string())
            },
        })
        .collect();
    let root_source = sources.root_source().to_owned();
    Ok(LoadedProgram {
        specification: expand::expand_program(sources, request)?,
        root_source,
        modules,
    })
}
