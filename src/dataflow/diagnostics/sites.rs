//! Source attribution carried beside evaluation graphs.
//!
//! Lowering records, for every graph node, the span and [`NodeOrigin`] of the frontend expression
//! that produced it. The record is a sidecar: [`NodeSites`] compares equal to every other
//! [`NodeSites`], is never written into a canonical graph descriptor, and so never changes graph
//! equality, stream state keys, definition fingerprints, scheduling or cache identity. Without the
//! `diagnostics` feature it is zero-sized and lowering records nothing.
//!
//! A node's *call site* is its primary span: for code inlined from another module's `def`, that is
//! the call. Its *definition site* is where inlined code was written, when the frontend recorded
//! one. Inlining removes the call boundary from the graph, so a definition site attributes nodes,
//! not a separately timed function invocation. Programmatically constructed and generated nodes
//! carry no origin and stay explicitly unlocated.

#[cfg(feature = "diagnostics")]
mod enabled {
    use crate::lang::dsrv::ast::CheckedExprRef;
    use crate::lang::dsrv::source_map::NodeOrigin;
    use crate::lang::dsrv::span::Span;

    #[derive(Clone, Copy, Debug)]
    pub(in crate::dataflow) struct NodeSite {
        pub(in crate::dataflow) span: Span,
        pub(in crate::dataflow) origin: NodeOrigin,
    }

    /// One optional site per graph node, index-aligned with the graph's nodes.
    ///
    /// Lowering is post-order, so a node is attributed by the first enclosing expression that
    /// finishes lowering after it was pushed. `unassigned` lists the indices still waiting, in
    /// push order: attributing an expression pops only the suffix it owns, so each node is
    /// touched once however deeply expressions nest.
    #[derive(Clone, Default)]
    pub(in crate::dataflow) struct NodeSites {
        sites: Vec<Option<NodeSite>>,
        unassigned: Vec<usize>,
    }

    impl NodeSites {
        #[inline]
        pub(in crate::dataflow) fn push_unassigned(&mut self) {
            self.unassigned.push(self.sites.len());
            self.sites.push(None);
        }

        /// Attribute every node pushed since `start` that has no site yet to `expr`. A
        /// subexpression has already claimed its own nodes, so only this expression's remain.
        #[inline]
        pub(in crate::dataflow) fn assign_from(&mut self, start: usize, expr: CheckedExprRef<'_>) {
            if self.unassigned.last().is_none_or(|&last| last < start) {
                return;
            }
            let expr = expr.expr();
            let site = NodeSite {
                span: expr.span(),
                origin: expr.origin(),
            };
            while let Some(&index) = self.unassigned.last() {
                if index < start {
                    break;
                }
                self.sites[index] = Some(site);
                self.unassigned.pop();
            }
        }

        pub(in crate::dataflow) fn get(&self, node: usize) -> Option<NodeSite> {
            self.sites.get(node).copied().flatten()
        }
    }

    /// Debug metadata never takes part in graph equality.
    impl PartialEq for NodeSites {
        fn eq(&self, _: &Self) -> bool {
            true
        }
    }

    impl std::fmt::Debug for NodeSites {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(formatter, "NodeSites({})", self.sites.len())
        }
    }

    /// The source archive a compiled program was expanded from, kept for diagnostics only.
    #[derive(Clone, Default)]
    pub(in crate::dataflow) struct DebugSources(
        Option<crate::lang::dsrv::ast::AstShared<crate::lang::dsrv::source_map::SourceArchive>>,
    );

    impl DebugSources {
        pub(in crate::dataflow) fn of(
            specification: &crate::lang::dsrv::ElaboratedDsrvSpecification,
        ) -> Self {
            Self(Some(specification.checked().unchecked().sources().clone()))
        }

        pub(in crate::dataflow) fn archive(
            &self,
        ) -> Option<&crate::lang::dsrv::source_map::SourceArchive> {
            self.0.as_deref()
        }
    }
}

#[cfg(not(feature = "diagnostics"))]
mod disabled {
    use crate::lang::dsrv::ast::CheckedExprRef;

    #[derive(Clone, Default, PartialEq)]
    pub(in crate::dataflow) struct NodeSites;

    impl NodeSites {
        #[inline(always)]
        pub(in crate::dataflow) fn push_unassigned(&mut self) {}

        #[inline(always)]
        pub(in crate::dataflow) fn assign_from(
            &mut self,
            _start: usize,
            _expr: CheckedExprRef<'_>,
        ) {
        }
    }

    impl std::fmt::Debug for NodeSites {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("NodeSites")
        }
    }

    #[derive(Clone, Default)]
    pub(in crate::dataflow) struct DebugSources;

    impl DebugSources {
        #[inline(always)]
        pub(in crate::dataflow) fn of(
            _specification: &crate::lang::dsrv::ElaboratedDsrvSpecification,
        ) -> Self {
            Self
        }
    }
}

#[cfg(not(feature = "diagnostics"))]
pub(in crate::dataflow) use disabled::{DebugSources, NodeSites};
#[cfg(feature = "diagnostics")]
pub(in crate::dataflow) use enabled::{DebugSources, NodeSite, NodeSites};

#[cfg(feature = "diagnostics")]
pub(in crate::dataflow) use report::{SourcePoint, SourceResolver, StreamSites};

#[cfg(feature = "diagnostics")]
mod report {
    use serde::Serialize;

    use std::collections::HashMap;

    use super::NodeSite;
    use crate::dataflow::ir::{BoundEvaluationGraph, BoundOp, BoundRef, StreamOp};
    use crate::lang::dsrv::source_map::{SourceArchive, SourceId};
    use crate::lang::dsrv::span::Span;

    const SNIPPET_CHARS: usize = 60;
    /// Most characters of a span examined when building its snippet, so a span covering a huge
    /// run of whitespace or text costs a constant amount however large it is.
    const SNIPPET_SCAN_CHARS: usize = 8 * SNIPPET_CHARS;
    /// Longest line prefix whose characters are counted for one column.
    const MAX_COLUMN_SCAN: usize = 4 << 10;
    /// Most characters of an operation label's variable parts (names, keys, targets).
    const LABEL_CHARS: usize = 48;

    /// A resolved source location.
    #[derive(Clone, Debug, PartialEq, Eq, Serialize)]
    pub(in crate::dataflow) struct SourcePoint {
        pub(in crate::dataflow) file: String,
        pub(in crate::dataflow) line: Option<usize>,
        pub(in crate::dataflow) column: Option<usize>,
        pub(in crate::dataflow) start: u32,
        pub(in crate::dataflow) end: u32,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(in crate::dataflow) snippet: Option<String>,
        /// The line, the column or both are absent because the capture's source-scan budget (or a
        /// per-line scan bound) was spent, not because the span lies outside the file. The file,
        /// span and snippet are still reported.
        #[serde(skip_serializing_if = "std::ops::Not::not")]
        pub(in crate::dataflow) position_truncated: bool,
    }

    /// Resolves spans to [`SourcePoint`]s for one definition capture.
    ///
    /// Line numbers come from a per-file index of line starts built once on first use, so
    /// resolving a span costs a binary search plus a bounded column scan instead of a scan of the
    /// file up to the span, which would make a capture quadratic in the file size. All indexing
    /// and column scanning of one resolver draws on one total budget, `limits.max_source_scan_bytes`: the
    /// bytes indexed for line starts plus the line-prefix bytes counted for columns.
    pub(in crate::dataflow) struct SourceResolver<'a> {
        archive: Option<&'a SourceArchive>,
        line_starts: HashMap<SourceId, Option<Vec<u32>>>,
        scan_left: usize,
    }

    impl<'a> SourceResolver<'a> {
        /// A resolver that scans at most `scan_budget` bytes of source in total.
        pub(in crate::dataflow) fn new(
            archive: Option<&'a SourceArchive>,
            scan_budget: usize,
        ) -> Self {
            Self {
                archive,
                line_starts: HashMap::new(),
                scan_left: scan_budget,
            }
        }

        fn resolve(&mut self, source: SourceId, span: Span) -> Option<SourcePoint> {
            let file = self.archive?.file(source)?;
            let text = file.text();
            let (start, end) = (span.start as usize, span.end as usize);
            let valid = start <= end
                && end <= text.len()
                && text.is_char_boundary(start)
                && text.is_char_boundary(end);
            let position = if valid {
                self.position(source, text, start)
            } else {
                None
            };
            let (line, column) = match position {
                Some((line, column)) => (line, column),
                None => (None, None),
            };
            let snippet = valid.then(|| bounded_snippet(&text[start..end]));
            Some(SourcePoint {
                file: file.label().to_string(),
                line,
                column,
                start: span.start,
                end: span.end,
                snippet,
                position_truncated: valid && (line.is_none() || column.is_none()),
            })
        }

        /// The one-based line and the Unicode-scalar column at which `start` lies. Either is
        /// `None` when the scan budget does not cover the work; the whole result is `None` only
        /// when nothing could be attempted.
        fn position(
            &mut self,
            source: SourceId,
            text: &str,
            start: usize,
        ) -> Option<(Option<usize>, Option<usize>)> {
            if !self.line_starts.contains_key(&source) {
                let starts = (text.len() <= self.scan_left).then(|| {
                    self.scan_left -= text.len();
                    std::iter::once(0)
                        .chain(
                            text.bytes()
                                .enumerate()
                                .filter(|(_, byte)| *byte == b'\n')
                                .map(|(index, _)| (index + 1) as u32),
                        )
                        .collect::<Vec<u32>>()
                });
                self.line_starts.insert(source, starts);
            }
            let Some(starts) = self.line_starts[&source].as_deref() else {
                return Some((None, None));
            };
            let line = starts.partition_point(|&line_start| line_start as usize <= start);
            let line_start = starts[line - 1] as usize;
            let prefix = start - line_start;
            let column = (prefix <= MAX_COLUMN_SCAN && prefix <= self.scan_left).then(|| {
                self.scan_left -= prefix;
                text[line_start..start].chars().count() + 1
            });
            Some((Some(line), column))
        }
    }

    /// A span's text with whitespace runs collapsed and at most [`SNIPPET_CHARS`] characters. Only
    /// the first [`SNIPPET_SCAN_CHARS`] characters of the span are examined.
    fn bounded_snippet(text: &str) -> String {
        let mut snippet = String::new();
        let mut emitted = 0;
        let mut gap = false;
        let mut truncated = false;
        for (scanned, character) in text.chars().enumerate() {
            if scanned == SNIPPET_SCAN_CHARS {
                truncated = true;
                break;
            }
            if character.is_whitespace() {
                gap = emitted > 0;
                continue;
            }
            if emitted + usize::from(gap) >= SNIPPET_CHARS {
                truncated = true;
                break;
            }
            if gap {
                snippet.push(' ');
                emitted += 1;
                gap = false;
            }
            snippet.push(character);
            emitted += 1;
        }
        if truncated {
            snippet.push('…');
        }
        snippet
    }

    /// One graph node of one stream, with its attribution.
    #[derive(Clone, Debug, Serialize)]
    pub(in crate::dataflow) struct NodeRecord {
        /// Position inside the stream: `3` is top-level node 3, `3/then/1` node 1 of node 3's
        /// `then` branch, `5/fn/0` node 0 of the function bound at node 5.
        pub(in crate::dataflow) path: String,
        pub(in crate::dataflow) op: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(in crate::dataflow) call_site: Option<SourcePoint>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(in crate::dataflow) definition_site: Option<SourcePoint>,
    }

    /// Attribution of one stream's graph.
    ///
    /// Capture visits at most as many nodes as the shared node budget leaves, and formats and
    /// resolves only the nodes it lists: once the budget is spent traversal stops, so the work is
    /// bounded by the budget however large the program is.
    #[derive(Clone, Debug, Default, Serialize)]
    pub(in crate::dataflow) struct StreamSites {
        /// The site of the node that produces the stream's value, when it is a located node.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(in crate::dataflow) site: Option<SourcePoint>,
        /// Nodes visited and listed. With `nodes_truncated` it is a lower bound on the graph.
        pub(in crate::dataflow) node_count: usize,
        /// Visited nodes with no call or definition site.
        pub(in crate::dataflow) unlocated_nodes: usize,
        /// The node budget was reached and completeness is not established: later nodes, if any,
        /// are neither counted nor listed. Set conservatively whenever the budget is spent, even
        /// if the last node visited happened to be the final one.
        #[serde(skip_serializing_if = "std::ops::Not::not")]
        pub(in crate::dataflow) nodes_truncated: bool,
        pub(in crate::dataflow) nodes: Vec<NodeRecord>,
    }

    impl StreamSites {
        pub(in crate::dataflow) fn capture(
            graph: &BoundEvaluationGraph,
            resolver: &mut SourceResolver<'_>,
            budget: &mut usize,
        ) -> Self {
            let mut sites = Self::default();
            if *budget == 0 {
                sites.nodes_truncated = true;
                return sites;
            }
            if let BoundRef::Node(node) = &graph.output {
                sites.site = graph
                    .sites
                    .get(node.index())
                    .and_then(|site| resolve_call_site(resolver, site));
            }
            sites.walk(graph, resolver, "", budget);
            sites
        }

        fn walk(
            &mut self,
            graph: &BoundEvaluationGraph,
            resolver: &mut SourceResolver<'_>,
            prefix: &str,
            budget: &mut usize,
        ) {
            for (index, op) in graph.nodes.iter().enumerate() {
                if *budget == 0 {
                    self.nodes_truncated = true;
                    return;
                }
                *budget -= 1;
                let path = format!("{prefix}{index}");
                let site = graph.sites.get(index);
                let call_site = site.and_then(|site| resolve_call_site(resolver, site));
                let definition_site = site.and_then(|site| {
                    let definition = site.origin.definition?;
                    resolver.resolve(definition.source, definition.span)
                });
                self.node_count += 1;
                if call_site.is_none() && definition_site.is_none() {
                    self.unlocated_nodes += 1;
                }
                self.nodes.push(NodeRecord {
                    path: path.clone(),
                    op: op_label(op),
                    call_site,
                    definition_site,
                });
                // Conservative: reaching the budget stops the walk and flags it, without a second
                // walk to learn whether anything was left. Completeness is not established.
                if *budget == 0 {
                    self.nodes_truncated = true;
                    return;
                }
                let proceeded = match op {
                    StreamOp::If {
                        then_branch,
                        else_branch,
                        ..
                    } => {
                        self.descend(then_branch, resolver, budget, || format!("{path}/then/"))
                            && self
                                .descend(else_branch, resolver, budget, || format!("{path}/else/"))
                    }
                    StreamOp::Function { func }
                    | StreamOp::DirectApply { func, .. }
                    | StreamOp::RecursiveApply { func, .. } => {
                        self.descend(&func.program.graph, resolver, budget, || {
                            format!("{path}/fn/")
                        })
                    }
                    StreamOp::Match { arms, .. } => arms.iter().enumerate().all(|(index, arm)| {
                        arm.guard.iter().all(|guard| {
                            self.descend(&guard.program.graph, resolver, budget, || {
                                format!("{path}/arm{index}.guard/")
                            })
                        }) && self.descend(&arm.body.program.graph, resolver, budget, || {
                            format!("{path}/arm{index}/")
                        })
                    }),
                    _ => true,
                };
                if !proceeded {
                    return;
                }
            }
        }

        /// Walk a nested graph, or report that the budget stops the capture here. A graph with no
        /// nodes costs nothing; with none left, no path is formatted and nothing is entered.
        fn descend(
            &mut self,
            nested: &BoundEvaluationGraph,
            resolver: &mut SourceResolver<'_>,
            budget: &mut usize,
            prefix: impl FnOnce() -> String,
        ) -> bool {
            if nested.nodes.is_empty() {
                return true;
            }
            if *budget == 0 {
                self.nodes_truncated = true;
                return false;
            }
            self.walk(nested, resolver, &prefix(), budget);
            // The nested walk ran out partway if it flagged truncation.
            !self.nodes_truncated
        }
    }

    fn resolve_call_site(resolver: &mut SourceResolver<'_>, site: NodeSite) -> Option<SourcePoint> {
        resolver.resolve(site.origin.source?, site.span)
    }

    /// A short, stable name for one operation. Names, keys, targets and function displays are
    /// clipped to [`LABEL_CHARS`] characters, so no listed node can emit unbounded text.
    pub(in crate::dataflow) fn op_label(op: &BoundOp) -> String {
        match op {
            StreamOp::Unary { op, .. } => format!("unary {op:?}"),
            StreamOp::Ascribe { target, .. } => format!("ascribe {}", clip(target)),
            StreamOp::Newtype { .. } => "newtype".into(),
            StreamOp::Binary { op, .. } => format!("binary {op:?}"),
            StreamOp::If { .. } => "if".into(),
            StreamOp::Delay { offset, .. } => format!("sindex[{offset}]"),
            StreamOp::RecursiveDelay { offset } => format!("sindex[{offset}] (recursive)"),
            StreamOp::Default { .. } => "default".into(),
            StreamOp::Init { .. } => "init".into(),
            StreamOp::IsDefined { .. } => "is_defined".into(),
            StreamOp::When { .. } => "when".into(),
            StreamOp::Update { .. } => "update".into(),
            StreamOp::Latch { .. } => "latch".into(),
            StreamOp::List(items) => format!("list[{}]", items.len()),
            StreamOp::Tuple(items) => format!("tuple[{}]", items.len()),
            StreamOp::Match { arms, .. } => format!("match[{}]", arms.len()),
            StreamOp::Constructor { tag, .. } => format!("constructor {}", clip(tag)),
            StreamOp::Map(fields) => format!("map[{}]", fields.len()),
            StreamOp::LIndex { .. } => "list.get".into(),
            StreamOp::LAppend { .. } => "list.append".into(),
            StreamOp::LConcat { .. } => "list.concat".into(),
            StreamOp::LHead { .. } => "list.head".into(),
            StreamOp::LTail { .. } => "list.tail".into(),
            StreamOp::LLen { .. } => "list.len".into(),
            StreamOp::MGet { key, .. } => format!("map.get {}", clip(key)),
            StreamOp::MRemove { key, .. } => format!("map.remove {}", clip(key)),
            StreamOp::MInsert { key, .. } => format!("map.insert {}", clip(key)),
            StreamOp::MHasKey { key, .. } => format!("map.has_key {}", clip(key)),
            StreamOp::TGet { index, .. } => format!("tuple.{index}"),
            StreamOp::Reconfigurable(spec) => format!("{:?}", spec.kind).to_lowercase(),
            StreamOp::Function { func } => format!("function {}", clip(&func.display)),
            StreamOp::Apply { args, .. } => format!("apply/{}", args.len()),
            StreamOp::DirectApply { func, .. } => format!("call {}", clip(&func.display)),
            StreamOp::RecursiveApply { func, .. } => {
                format!("recursive call {}", clip(&func.display))
            }
            StreamOp::RecursiveCall { args } => format!("self call/{}", args.len()),
            StreamOp::Partial { display, .. } => format!("partial {}", clip(display)),
            StreamOp::Fix { display, .. } => format!("fix {}", clip(display)),
            StreamOp::ListMap { .. } => "list.map".into(),
            StreamOp::ListFilter { .. } => "list.filter".into(),
            StreamOp::ListFold { .. } => "list.fold".into(),
        }
    }

    /// `value` displayed with at most [`LABEL_CHARS`] characters, ending in an ellipsis when cut.
    /// Formatting stops as soon as the limit is reached.
    fn clip(value: &dyn std::fmt::Display) -> String {
        use std::fmt::Write as _;

        struct Bounded {
            text: String,
            chars: usize,
            truncated: bool,
        }
        impl std::fmt::Write for Bounded {
            fn write_str(&mut self, part: &str) -> std::fmt::Result {
                for character in part.chars() {
                    if self.chars == LABEL_CHARS {
                        self.truncated = true;
                        return Err(std::fmt::Error);
                    }
                    self.text.push(character);
                    self.chars += 1;
                }
                Ok(())
            }
        }
        let mut bounded = Bounded {
            text: String::new(),
            chars: 0,
            truncated: false,
        };
        let _ = write!(bounded, "{value}");
        if bounded.truncated {
            bounded.text.push('…');
        }
        bounded.text
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn snippets_collapse_whitespace_and_stop_at_the_limit() {
            assert_eq!(bounded_snippet("a   b\n\t c"), "a b c");
            assert_eq!(bounded_snippet("  padded  "), "padded");
            let exact = "a".repeat(SNIPPET_CHARS);
            assert_eq!(bounded_snippet(&exact), exact);
            let longer = "a".repeat(SNIPPET_CHARS + 1);
            assert_eq!(
                bounded_snippet(&longer),
                format!("{}…", "a".repeat(SNIPPET_CHARS))
            );
            // Whitespace between words counts toward the limit like any other character.
            let spaced = "ab ".repeat(40);
            assert!(bounded_snippet(&spaced).chars().count() <= SNIPPET_CHARS + 1);
        }

        #[test]
        fn operation_labels_clip_long_names() {
            let long = "n".repeat(1 << 20);
            let started = std::time::Instant::now();
            let clipped = clip(&long.as_str());
            assert_eq!(clipped.chars().count(), LABEL_CHARS + 1);
            assert!(clipped.ends_with('…'));
            assert_eq!(clip(&"short"), "short");
            assert!(started.elapsed() < std::time::Duration::from_secs(1));
        }

        #[test]
        fn a_huge_span_costs_a_bounded_scan() {
            let started = std::time::Instant::now();
            // Nothing but whitespace is scanned only as far as the scan limit.
            let blank = " ".repeat(64 << 20) + "x";
            assert_eq!(bounded_snippet(&blank), "…");
            let long = "word ".repeat(4 << 20);
            let snippet = bounded_snippet(&long);
            assert!(snippet.ends_with('…') && snippet.chars().count() <= SNIPPET_CHARS + 1);
            assert!(started.elapsed() < std::time::Duration::from_secs(2));
        }
    }
}
