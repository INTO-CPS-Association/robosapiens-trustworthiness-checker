//! Target-indexed correspondence between two compiled dataflow definitions.
//!
//! This module only analyses immutable programs. It does not own evaluator state or choose a
//! transfer policy.

use std::collections::BTreeMap;

use super::environment::EnvironmentSlot;
use super::stream_id::StreamId;

use super::program::DataflowProgram;
use super::reconfiguration::DefinitionKey;

/// The source correspondence for one target stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::dataflow) enum StreamMapping {
    /// The source and target stream definitions are exact matches.
    Exact(StreamId),
    /// The target stream has no exact transferable source stream.
    Unmapped,
}

impl StreamMapping {
    #[inline]
    pub(in crate::dataflow) fn source(&self) -> Option<StreamId> {
        match self {
            Self::Exact(source) => Some(*source),
            Self::Unmapped => None,
        }
    }
}

/// The source slot for one target environment slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::dataflow) struct EnvironmentMapping {
    source: Option<EnvironmentSlot>,
}

impl EnvironmentMapping {
    #[inline]
    fn mapped(source: EnvironmentSlot) -> Self {
        Self {
            source: Some(source),
        }
    }

    #[inline]
    fn unmapped() -> Self {
        Self { source: None }
    }

    #[inline]
    pub(in crate::dataflow) fn source(self) -> Option<EnvironmentSlot> {
        self.source
    }
}

/// A target-indexed correspondence between a source and target definition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ReconfigurationMapping {
    source_definition: DefinitionKey,
    target_definition: DefinitionKey,
    streams: Box<[StreamMapping]>,
    environments: Box<[EnvironmentMapping]>,
}

impl ReconfigurationMapping {
    /// Analyse two immutable programs. Collections are ordered by target dense identity.
    pub(in crate::dataflow) fn between(source: &DataflowProgram, target: &DataflowProgram) -> Self {
        let source_layout = source.environment_layout();
        let target_layout = target.environment_layout();
        let source_streams = source
            .stream_vars()
            .iter()
            .enumerate()
            .map(|(index, name)| (name, StreamId::new(index)))
            .collect::<BTreeMap<_, _>>();

        let streams = target
            .stream_vars()
            .iter()
            .enumerate()
            .map(|(target_index, target_name)| {
                let target_stream = StreamId::new(target_index);
                let Some(&source_stream) = source_streams.get(target_name) else {
                    return StreamMapping::Unmapped;
                };
                let source_program = &source.stream_programs()[source_stream.index()];
                let target_program = &target.stream_programs()[target_stream.index()];
                if source_program.state_key() == target_program.state_key() {
                    StreamMapping::Exact(source_stream)
                } else {
                    StreamMapping::Unmapped
                }
            })
            .collect::<Vec<_>>();

        let environments = (0..target_layout.len())
            .map(|index| {
                let target_slot = EnvironmentSlot::new(index);
                target_layout
                    .variable(target_slot)
                    .and_then(|variable| source_layout.slot(variable))
                    .filter(|&source_slot| {
                        target_layout.stream_type(target_slot)
                            == source_layout.stream_type(source_slot)
                    })
                    .map_or_else(EnvironmentMapping::unmapped, EnvironmentMapping::mapped)
            })
            .collect::<Vec<_>>();
        Self {
            source_definition: source.definition_key().clone(),
            target_definition: target.definition_key().clone(),
            streams: streams.into_boxed_slice(),
            environments: environments.into_boxed_slice(),
        }
    }

    /// Validate the structural invariants of a mapping against its source and target programs.
    #[cfg(test)]
    pub(in crate::dataflow) fn validate(
        &self,
        source: &DataflowProgram,
        target: &DataflowProgram,
    ) -> Result<(), ReconfigurationMappingError> {
        if self.source_definition != *source.definition_key() {
            return Err(ReconfigurationMappingError::DefinitionKeyMismatch {
                side: "source",
                mapping: self.source_definition.clone(),
                program: source.definition_key().clone(),
            });
        }
        if self.target_definition != *target.definition_key() {
            return Err(ReconfigurationMappingError::DefinitionKeyMismatch {
                side: "target",
                mapping: self.target_definition.clone(),
                program: target.definition_key().clone(),
            });
        }

        validate_target_count("stream", target.stream_vars().len(), self.streams.len())?;
        validate_target_count(
            "environment",
            target.environment_layout().len(),
            self.environments.len(),
        )?;

        let source_layout = source.environment_layout();
        let target_layout = target.environment_layout();
        let mut used_source_streams = vec![false; source.stream_vars().len()];
        for (target_index, mapping) in self.streams.iter().enumerate() {
            let target_stream = StreamId::new(target_index);
            let target_name = &target.stream_vars()[target_index];
            let Some(source_stream) = mapping.source() else {
                continue;
            };
            validate_source_id(
                "stream",
                source_stream.index(),
                source.stream_vars().len(),
                &mut used_source_streams,
            )?;
            if source.stream_vars()[source_stream.index()] != *target_name {
                return Err(ReconfigurationMappingError::StreamVariableMismatch {
                    target: target_stream,
                    source_stream,
                });
            }

            let source_program = &source.stream_programs()[source_stream.index()];
            let target_program = &target.stream_programs()[target_stream.index()];
            match mapping {
                StreamMapping::Exact(_)
                    if source_program.state_key() != target_program.state_key() =>
                {
                    return Err(ReconfigurationMappingError::ExactStreamMismatch {
                        target: target_stream,
                    });
                }
                StreamMapping::Exact(_) | StreamMapping::Unmapped => {}
            }
        }

        let mut used_source_environment = vec![false; source_layout.len()];
        for (target_index, mapping) in self.environments.iter().enumerate() {
            let Some(source_slot) = mapping.source() else {
                continue;
            };
            let target_slot = EnvironmentSlot::new(target_index);
            validate_source_id(
                "environment slot",
                source_slot.index(),
                source_layout.len(),
                &mut used_source_environment,
            )?;
            if target_layout.variable(target_slot) != source_layout.variable(source_slot)
                || target_layout.stream_type(target_slot) != source_layout.stream_type(source_slot)
            {
                return Err(ReconfigurationMappingError::EnvironmentVariableMismatch {
                    target: target_slot,
                    source_slot,
                });
            }
        }

        Ok(())
    }

    #[inline]
    pub(in crate::dataflow) fn streams(&self) -> &[StreamMapping] {
        &self.streams
    }

    #[inline]
    pub(in crate::dataflow) fn environments(&self) -> &[EnvironmentMapping] {
        &self.environments
    }

    #[inline]
    pub(in crate::dataflow) fn stream(&self, target: StreamId) -> Option<&StreamMapping> {
        self.streams.get(target.index())
    }

    #[inline]
    pub(in crate::dataflow) fn environment(
        &self,
        target: EnvironmentSlot,
    ) -> Option<EnvironmentMapping> {
        self.environments.get(target.index()).copied()
    }
}

/// A structural error in a target-indexed reconfiguration mapping.
#[cfg(test)]
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub(in crate::dataflow) enum ReconfigurationMappingError {
    #[error("{side} definition key in mapping ({mapping}) does not match program key ({program})")]
    DefinitionKeyMismatch {
        side: &'static str,
        mapping: DefinitionKey,
        program: DefinitionKey,
    },
    #[error("target {entity} mapping is incomplete: expected {expected} entries, got {actual}")]
    TargetIncomplete {
        entity: &'static str,
        expected: usize,
        actual: usize,
    },

    #[error(
        "{entity} mapping refers to source id {source_id}, but the source has {source_count} entries"
    )]
    SourceOutOfBounds {
        entity: &'static str,
        source_id: usize,
        source_count: usize,
    },
    #[error(
        "{entity} mapping is not source-injective: source id {source_id} is used more than once"
    )]
    SourceNotInjective {
        entity: &'static str,
        source_id: usize,
    },
    #[error(
        "target stream {target:?} maps to source stream {source_stream:?} with a different name"
    )]
    StreamVariableMismatch {
        target: StreamId,
        source_stream: StreamId,
    },
    #[error("target stream {target:?} is marked exact but its bound semantics differ")]
    ExactStreamMismatch { target: StreamId },
    #[error("target environment slot {target:?} maps to a source slot with a different variable")]
    EnvironmentVariableMismatch {
        target: EnvironmentSlot,
        source_slot: EnvironmentSlot,
    },
}

#[cfg(test)]
fn validate_target_count(
    entity: &'static str,
    expected: usize,
    actual: usize,
) -> Result<(), ReconfigurationMappingError> {
    (expected == actual)
        .then_some(())
        .ok_or(ReconfigurationMappingError::TargetIncomplete {
            entity,
            expected,
            actual,
        })
}

#[cfg(test)]
fn validate_source_id(
    entity: &'static str,
    source: usize,
    source_count: usize,
    seen: &mut [bool],
) -> Result<(), ReconfigurationMappingError> {
    if source >= source_count {
        return Err(ReconfigurationMappingError::SourceOutOfBounds {
            entity,
            source_id: source,
            source_count,
        });
    }
    if seen[source] {
        return Err(ReconfigurationMappingError::SourceNotInjective {
            entity,
            source_id: source,
        });
    }
    seen[source] = true;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::VarName;

    fn compile(source: &str) -> DataflowProgram {
        let specification = source
            .parse::<crate::lang::dsrv::ast::DsrvSpecification>()
            .expect("test specification must parse");
        DataflowProgram::compile_untyped(specification).expect("test specification must compile")
    }

    #[test]
    fn exact_streams_need_no_owner_enumeration() {
        let source = compile("in x\nout z\nz = x + 1");
        let target = compile("in x\nout z\nz = x + 1");
        let mapping = ReconfigurationMapping::between(&source, &target);

        assert!(matches!(
            mapping.stream(StreamId::new(0)),
            Some(StreamMapping::Exact(stream)) if *stream == StreamId::new(0)
        ));

        assert_eq!(
            mapping
                .environment(EnvironmentSlot::new(0))
                .unwrap()
                .source(),
            Some(EnvironmentSlot::new(0))
        );
        assert_eq!(
            mapping
                .environment(EnvironmentSlot::new(1))
                .unwrap()
                .source(),
            Some(EnvironmentSlot::new(1))
        );
    }

    #[test]
    fn changed_streams_are_unmapped_and_start_cold() {
        let source = compile("in x\nout z\nz = x + 1");
        let target = compile("in x\nout z\nz = x + 2");
        let mapping = ReconfigurationMapping::between(&source, &target);

        assert!(matches!(
            mapping.stream(StreamId::new(0)),
            Some(StreamMapping::Unmapped)
        ));
    }

    #[test]
    fn streams_are_matched_by_name_across_added_removed_and_reordered_streams() {
        let source = compile("in x\nout a\nout b\na = x + 1\nb = x + 2");
        let target = compile("in x\nout b\nout c\nb = x + 2\nc = x + 3");
        let mapping = ReconfigurationMapping::between(&source, &target);

        let source_b = source
            .stream_vars()
            .iter()
            .position(|name| name == &VarName::new("b"))
            .unwrap();
        let target_b = target
            .stream_vars()
            .iter()
            .position(|name| name == &VarName::new("b"))
            .unwrap();
        let target_c = target
            .stream_vars()
            .iter()
            .position(|name| name == &VarName::new("c"))
            .unwrap();

        assert!(matches!(
            mapping.stream(StreamId::new(target_b)),
            Some(StreamMapping::Exact(source)) if *source == StreamId::new(source_b)
        ));
        assert!(matches!(
            mapping.stream(StreamId::new(target_c)),
            Some(StreamMapping::Unmapped)
        ));
        assert!(
            source
                .stream_vars()
                .iter()
                .any(|name| name == &VarName::new("a"))
        );
        assert!(
            !target
                .stream_vars()
                .iter()
                .any(|name| name == &VarName::new("a"))
        );
    }

    #[test]
    fn environment_slots_are_remapped_by_variable_name() {
        let source = compile("in x\nout z\nz = x + 1");
        let target = compile("in a\nin x\nout z\nz = x + 1");
        let mapping = ReconfigurationMapping::between(&source, &target);
        let source_x = source
            .environment_layout()
            .slot(&VarName::new("x"))
            .unwrap();
        let target_a = target
            .environment_layout()
            .slot(&VarName::new("a"))
            .unwrap();
        let target_x = target
            .environment_layout()
            .slot(&VarName::new("x"))
            .unwrap();

        assert_eq!(mapping.environment(target_a).unwrap().source(), None);
        assert_eq!(
            mapping.environment(target_x).unwrap().source(),
            Some(source_x)
        );
        assert!(matches!(
            mapping.stream(StreamId::new(0)),
            Some(StreamMapping::Exact(_))
        ));
    }

    #[test]
    fn changed_dynamic_and_deferred_outer_streams_are_conservative() {
        for kind in ["dynamic", "defer"] {
            let source = compile(&format!(
                "in source: Str\nout z: Int\nz = {kind}(source: Int)"
            ));
            let target = compile(&format!(
                "in source: Str\nout z: Int\nz = {kind}(source: Int) + 1"
            ));
            let mapping = ReconfigurationMapping::between(&source, &target);

            assert!(matches!(
                mapping.stream(StreamId::new(0)),
                Some(StreamMapping::Unmapped)
            ));
        }
    }

    #[test]
    fn validation_checks_definition_keys_and_source_injectivity() {
        let source = compile("in x\nout z\nz = x + 1");
        let target = compile("in x\nout z\nz = x + 2");
        let mut key_mismatch = ReconfigurationMapping::between(&source, &target);
        key_mismatch.source_definition = DefinitionKey::from_canonical("wrong");
        assert!(matches!(
            key_mismatch.validate(&source, &target),
            Err(ReconfigurationMappingError::DefinitionKeyMismatch { side: "source", .. })
        ));

        let source = compile("in x\nout a\nout b\na = x + 1\nb = x + 2");
        let target = compile("in x\nout a\nout b\na = x + 1\nb = x + 2");
        let mut duplicate = ReconfigurationMapping::between(&source, &target);
        duplicate.streams[1] = StreamMapping::Exact(StreamId::new(0));
        assert_eq!(
            duplicate.validate(&source, &target).unwrap_err(),
            ReconfigurationMappingError::SourceNotInjective {
                entity: "stream",
                source_id: 0,
            }
        );
    }
}
