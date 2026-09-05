"use strict";

const RECONFIGURATION_PERCENTAGES = [1, 10, 50, 100];
const RECONFIGURATION_RUNTIMES = [
    ["SemiSync", "semisync"],
    ["Dataflow untyped runtime", "dataflow_untyped"],
    ["Dataflow checked canonical runtime", "dataflow"],
    ["Dataflow checked quickened runtime", "dataflow_quickened"],
];

const RUNTIME_DATAFLOW_LABELS = {
    untyped: "Dataflow untyped runtime pipeline",
    canonical: "Dataflow checked canonical runtime pipeline",
    quickened: "Dataflow checked quickened runtime pipeline",
    jit: "Dataflow checked JIT runtime pipeline (includes warmup and compilation)",
};

const SUSTAINED_FIXTURES = [
    ["Scalar arithmetic", "arithmetic"],
    ["32-node chain", "chain32"],
    ["Conditional expression", "conditional"],
    ["Threshold property", "threshold"],
    ["Three-step temporal window", "window3"],
    ["Stateful accumulator", "accumulator"],
];

function sustainedMonitorCard([title, fixture]) {
    const prefix = `jit/sustained/${fixture}`;
    return {
        title,
        description:
            "Sustained execution over 100,000 timed events after 10,000 untimed setup events. Value-interface routes include Value-row dispatch and conversion; direct routes use the typed monitor interface. The warmed route is verified native before timing.",
        series: [
            { label: "Untyped value interface", name: `${prefix}/untyped_value` },
            {
                label: "Checked canonical value interface",
                name: `${prefix}/checked_canonical_value`,
            },
            {
                label: "Checked quickened value interface",
                name: `${prefix}/checked_quickened_value`,
            },
            { label: "Value JIT — steady state", name: `${prefix}/native_value_eager` },
            { label: "Native typed interface — eager", name: `${prefix}/native_direct_eager` },
            {
                label: "Native typed interface — warmed after 1,024 events",
                name: `${prefix}/native_direct_warmed`,
            },
        ],
    };
}

function movingAverageReconfigurationCard(percent) {
    return {
        title: `Stateful reconfiguration — moving average, ${percent}% reconfiguration`,
        description:
            "End-to-end runtime reconfiguration over 1,000 inputs, comparing execution tiers with and without compatible context transfer.",
        series: RECONFIGURATION_RUNTIMES.flatMap(([label, name]) =>
            ["on", "off"].map((contextTransfer) => ({
                label: `${label} — context transfer ${contextTransfer}`,
                name: `rec_moving_average/${name}_reconf_ct_${contextTransfer}_percent_${percent}/1000`,
            })),
        ),
    };
}

const IMPORTANT_SECTIONS = [
    {
        title: "Overall runtime",
        cards: [
            {
                title: "MAPLE sequence — 25,000 inputs",
                description: "End-to-end execution of the MAPLE sequence monitor.",
                series: [
                    {
                        label: RUNTIME_DATAFLOW_LABELS.untyped,
                        name: "maple_sequence/maple_sequence_dataflow_untyped/25000",
                    },
                    {
                        label: RUNTIME_DATAFLOW_LABELS.canonical,
                        name: "maple_sequence/maple_sequence_dataflow/25000",
                    },
                    {
                        label: RUNTIME_DATAFLOW_LABELS.quickened,
                        name: "maple_sequence/maple_sequence_dataflow_quickened/25000",
                    },
                    {
                        label: RUNTIME_DATAFLOW_LABELS.jit,
                        name: "maple_sequence/maple_sequence_dataflow_jit/25000",
                    },
                    {
                        label: "SemiSync",
                        name: "maple_sequence/maple_sequence_untyped_semisync/25000",
                    },
                    {
                        label: "Async stream runtime",
                        name: "maple_sequence/maple_sequence_untyped_async/25000",
                    },
                ],
            },
            {
                title: "Arithmetic-heavy pipeline — 64 stages, 25,000 inputs",
                description:
                    "End-to-end execution of a bounded integer pipeline containing addition, multiplication, subtraction and modulo operations.",
                series: [
                    {
                        label: RUNTIME_DATAFLOW_LABELS.untyped,
                        name: "arithmetic_heavy/dataflow_untyped/25000",
                    },
                    {
                        label: RUNTIME_DATAFLOW_LABELS.canonical,
                        name: "arithmetic_heavy/dataflow/25000",
                    },
                    {
                        label: RUNTIME_DATAFLOW_LABELS.quickened,
                        name: "arithmetic_heavy/dataflow_quickened/25000",
                    },
                    {
                        label: RUNTIME_DATAFLOW_LABELS.jit,
                        name: "arithmetic_heavy/dataflow_jit/25000",
                    },
                    {
                        label: "SemiSync untyped",
                        name: "arithmetic_heavy/semisync_untyped/25000",
                    },
                    {
                        label: "SemiSync typed",
                        name: "arithmetic_heavy/semisync_typed/25000",
                    },
                ],
            },
            {
                title: "Dynamic expression workload — 50% dynamic, 100,000 inputs",
                description:
                    "Paper workload exercising deferred expression parsing, compilation and evaluation over a large stream.",
                series: [
                    {
                        label: RUNTIME_DATAFLOW_LABELS.untyped,
                        name: "dyn_paper/dyn_paper_50_dataflow_untyped/100000",
                    },
                    {
                        label: RUNTIME_DATAFLOW_LABELS.canonical,
                        name: "dyn_paper/dyn_paper_50_dataflow/100000",
                    },
                    {
                        label: RUNTIME_DATAFLOW_LABELS.quickened,
                        name: "dyn_paper/dyn_paper_50_dataflow_quickened/100000",
                    },
                    {
                        label: RUNTIME_DATAFLOW_LABELS.jit,
                        name: "dyn_paper/dyn_paper_50_dataflow_jit/100000",
                    },
                    {
                        label: "SemiSync",
                        name: "dyn_paper/dyn_paper_50_semisync/100000",
                    },
                    {
                        label: "Async stream runtime",
                        name: "dyn_paper/dyn_paper_50/100000",
                    },
                ],
            },
            {
                title: "Hard dynamic/defer — automatic scope, 1,024 inputs",
                description:
                    "End-to-end execution of 32 defer and 32 dynamic nodes sharing an automatically scoped environment. Dynamic dependencies change every 64 ticks, with temporal reads and sparse inputs.",
                series: [
                    {
                        label: RUNTIME_DATAFLOW_LABELS.untyped,
                        name: "hard_dynamic_defer/automatic_scope_dataflow_untyped/1024",
                    },
                    {
                        label: RUNTIME_DATAFLOW_LABELS.canonical,
                        name: "hard_dynamic_defer/automatic_scope_dataflow/1024",
                    },
                    {
                        label: RUNTIME_DATAFLOW_LABELS.quickened,
                        name: "hard_dynamic_defer/automatic_scope_dataflow_quickened/1024",
                    },
                    {
                        label: RUNTIME_DATAFLOW_LABELS.jit,
                        name: "hard_dynamic_defer/automatic_scope_dataflow_jit/1024",
                    },
                    {
                        label: "SemiSync",
                        name: "hard_dynamic_defer/automatic_scope_semisync/1024",
                    },
                ],
            },
            {
                title: "Hard dynamic/defer — four explicit components, 1,024 inputs",
                description:
                    "Execution over four connected static components feeding 32 defer and 32 dynamic expressions with narrow explicit scopes and periodic dependency changes.",
                series: [
                    {
                        label: RUNTIME_DATAFLOW_LABELS.untyped,
                        name: "hard_dynamic_defer/explicit_components_dataflow_untyped/1024",
                    },
                    {
                        label: RUNTIME_DATAFLOW_LABELS.canonical,
                        name: "hard_dynamic_defer/explicit_components_dataflow/1024",
                    },
                    {
                        label: RUNTIME_DATAFLOW_LABELS.quickened,
                        name: "hard_dynamic_defer/explicit_components_dataflow_quickened/1024",
                    },
                    {
                        label: RUNTIME_DATAFLOW_LABELS.jit,
                        name: "hard_dynamic_defer/explicit_components_dataflow_jit/1024",
                    },
                    {
                        label: "SemiSync",
                        name: "hard_dynamic_defer/explicit_components_semisync/1024",
                    },
                ],
            },
            {
                title: "Deferred expression — 25,000 inputs",
                description:
                    "End-to-end evaluation of an expression deferred until runtime, comparing all three local execution engines.",
                series: [
                    {
                        label: RUNTIME_DATAFLOW_LABELS.untyped,
                        name: "dup_defer/dup_defer_dataflow_untyped/25000",
                    },
                    {
                        label: RUNTIME_DATAFLOW_LABELS.canonical,
                        name: "dup_defer/dup_defer_dataflow/25000",
                    },
                    {
                        label: RUNTIME_DATAFLOW_LABELS.quickened,
                        name: "dup_defer/dup_defer_dataflow_quickened/25000",
                    },
                    {
                        label: RUNTIME_DATAFLOW_LABELS.jit,
                        name: "dup_defer/dup_defer_dataflow_jit/25000",
                    },
                    {
                        label: "SemiSync",
                        name: "dup_defer/dup_defer_untyped_semisync/25000",
                    },
                    {
                        label: "Async stream runtime",
                        name: "dup_defer/dup_defer_untyped_async/25000",
                    },
                ],
            },
            ...RECONFIGURATION_PERCENTAGES.map(movingAverageReconfigurationCard),
            {
                title: "Stateful runtime steady state — moving average, 1,000 inputs",
                description:
                    "No-reconfiguration baseline separating per-tick runtime cost from cutover cost. This workload ends before native activation, so its JIT route measures the pre-activation tier and hotness checks.",
                series: [
                    {
                        label: "SemiSync",
                        name: "rec_moving_average/semisync_reconf_ct_on_percent_0/1000",
                    },
                    {
                        label: RUNTIME_DATAFLOW_LABELS.untyped,
                        name: "rec_moving_average/dataflow_untyped_reconf_ct_on_percent_0/1000",
                    },
                    {
                        label: RUNTIME_DATAFLOW_LABELS.canonical,
                        name: "rec_moving_average/dataflow_reconf_ct_on_percent_0/1000",
                    },
                    {
                        label: RUNTIME_DATAFLOW_LABELS.quickened,
                        name: "rec_moving_average/dataflow_quickened_reconf_ct_on_percent_0/1000",
                    },
                    {
                        label: RUNTIME_DATAFLOW_LABELS.jit,
                        name: "rec_moving_average/dataflow_jit_reconf_ct_on_percent_0/1000",
                    },
                ],
            },
            {
                title: "Dataflow reconfiguration internals",
                description:
                    "Monitor-level compilation, state transfer and dynamic schedule-repair signals without transport or asynchronous output noise.",
                series: [
                    {
                        label: "Compile candidate and transfer — 16 streams",
                        name: "dataflow/runtime_reconfiguration/candidate_compile_and_transfer/16",
                    },
                    {
                        label: "State transfer only — 16 streams",
                        name: "dataflow/runtime_reconfiguration/state_transfer_only/16",
                    },
                    {
                        label: "Dynamic schedule repair",
                        name: "dataflow/dynamic_transitions/schedule_repair",
                    },
                ],
            },
            {
                title: "Time-dependent property — 10,000 inputs",
                description: "Runtime evaluation using stream history, defaults and a three-step temporal window.",
                series: [
                    {
                        label: RUNTIME_DATAFLOW_LABELS.untyped,
                        name: "time_dependent_property/dsrv_default_window_dataflow_untyped/10000",
                    },
                    {
                        label: RUNTIME_DATAFLOW_LABELS.canonical,
                        name: "time_dependent_property/dsrv_default_window_dataflow/10000",
                    },
                    {
                        label: RUNTIME_DATAFLOW_LABELS.quickened,
                        name: "time_dependent_property/dsrv_default_window_dataflow_quickened/10000",
                    },
                    {
                        label: RUNTIME_DATAFLOW_LABELS.jit,
                        name: "time_dependent_property/dsrv_default_window_dataflow_jit/10000",
                    },
                    {
                        label: "SemiSync",
                        name: "time_dependent_property/dsrv_default_window_semisync/10000",
                    },
                ],
            },
        ],
    },
    {
        title: "Sustained Dataflow monitor execution",
        description:
            "Monitor-level routes isolate steady-state evaluation from the asynchronous runtime pipeline.",
        cards: SUSTAINED_FIXTURES.map(sustainedMonitorCard),
    },
    {
        title: "Parsing and compilation",
        cards: [
            {
                title: "LALR parser — 10,000 varied expressions",
                description:
                    "Parses a deterministic specification containing varied arithmetic, Boolean, string, collection, dynamic and stream-index expressions.",
                series: [
                    {
                        label: "LALR parser",
                        name: "parse_small_varied_inputs/parsing_lalrpop/10000",
                    },
                ],
            },
            {
                kind: "stacked-pipeline",
                fullWidth: true,
                title: "Typed compilation pipeline — 1,024 assignments",
                description:
                    "Stacked area showing staged compilation time over benchmark history. The total explicitly runs parsing, type checking, dependency graph construction and dataflow compilation; the remaining compilation segment is derived after subtracting the preceding phases.",
                phases: [
                    {
                        label: "LALR parsing and validation",
                        name: "compilation_phases/parse_and_validate_specification/1024",
                    },
                    {
                        label: "Strict type checking",
                        name: "compilation_phases/strict_type_check/1024",
                    },
                    {
                        label: "Typed dependency graph",
                        name: "compilation_phases/typed_dependency_graph/1024",
                    },
                ],
                total: {
                    label: "Complete typed pipeline",
                    name: "compilation_phases/parse_typecheck_dependency_compile_typed/1024",
                },
            },
        ],
    },
    {
        title: "Reference comparisons",
        cards: [
            {
                title: "Threshold runtime pipelines — 10,000 inputs",
                description: "End-to-end runtime comparison for a simple threshold property.",
                series: [
                    {
                        label: RUNTIME_DATAFLOW_LABELS.untyped,
                        name: "threshold_property/dsrv_dataflow_untyped/10000",
                    },
                    {
                        label: RUNTIME_DATAFLOW_LABELS.canonical,
                        name: "threshold_property/dsrv_dataflow/10000",
                    },
                    {
                        label: RUNTIME_DATAFLOW_LABELS.quickened,
                        name: "threshold_property/dsrv_dataflow_quickened/10000",
                    },
                    {
                        label: RUNTIME_DATAFLOW_LABELS.jit,
                        name: "threshold_property/dsrv_dataflow_jit/10000",
                    },
                    {
                        label: "DSRV SemiSync",
                        name: "threshold_property/dsrv_semisync/10000",
                    },
                    {
                        label: "MSTLO runtime qualitative",
                        name: "threshold_property/mstlo_runtime_qual/10000",
                    },
                ],
            },
            {
                title: "Threshold direct monitor — 10,000 inputs",
                description:
                    "Direct MSTLO monitor evaluation without the DSRV or MSTLO runtime pipeline.",
                series: [
                    {
                        label: "MSTLO direct qualitative",
                        name: "threshold_property/mstlo_direct_qual/10000",
                    },
                ],
            },
            {
                title: "Temporal runtime pipelines — 10,000 inputs",
                description: "End-to-end runtime comparison for a bounded globally property.",
                series: [
                    {
                        label: RUNTIME_DATAFLOW_LABELS.untyped,
                        name: "time_dependent_property/dsrv_default_window_dataflow_untyped/10000",
                    },
                    {
                        label: RUNTIME_DATAFLOW_LABELS.canonical,
                        name: "time_dependent_property/dsrv_default_window_dataflow/10000",
                    },
                    {
                        label: RUNTIME_DATAFLOW_LABELS.quickened,
                        name: "time_dependent_property/dsrv_default_window_dataflow_quickened/10000",
                    },
                    {
                        label: RUNTIME_DATAFLOW_LABELS.jit,
                        name: "time_dependent_property/dsrv_default_window_dataflow_jit/10000",
                    },
                    {
                        label: "DSRV SemiSync",
                        name: "time_dependent_property/dsrv_default_window_semisync/10000",
                    },
                    {
                        label: "MSTLO runtime qualitative",
                        name: "time_dependent_property/mstlo_globally_window_qual/10000",
                    },
                ],
            },
            {
                title: "Temporal direct monitor — 10,000 inputs",
                description:
                    "Direct MSTLO monitor evaluation without the DSRV or MSTLO runtime pipeline.",
                series: [
                    {
                        label: "MSTLO direct qualitative",
                        name: "time_dependent_property/mstlo_direct_globally_window_qual/10000",
                    },
                ],
            },
        ],
    },
];

const BENCHMARK_ALIASES = {
    "compilation_phases/parse_and_validate_specification/1024": [
        "compilation_phases/lalr_parse/1024",
    ],
    ...Object.fromEntries(
        SUSTAINED_FIXTURES.map(([_label, fixture]) => [
            `jit/sustained/${fixture}/native_value_eager`,
            [
                `jit/sustained/${fixture}/all_no_hotness`,
                `jit/sustained/${fixture}/all_with_hotness`,
            ],
        ]),
    ),
};

// Source history gives the plain threshold/time-dependent `dataflow` ID three meanings:
// untyped before 242b5bd9, checked quickened from 242b5bd9 (where explicit `_untyped`
// was added) through the specialised-name era, and checked canonical from 33fa9871.
// Use commit timestamps because backfilled measurements retain their source commit but can
// have a later collection date.
const CHECKED_QUICKENED_EPOCH_START = Date.parse("2026-07-29T17:54:52+02:00");
const CHECKED_CANONICAL_EPOCH_START = Date.parse("2026-08-23T11:34:17+02:00");
const EPOCH_CLASSIFIED_ROUTE_PATTERNS = [
    [/^(threshold_property\/dsrv_dataflow)(?:_(untyped|quickened))?\/([^/]+)$/, "three-tier"],
    [/^(time_dependent_property\/dsrv_default_window_dataflow)(?:_(untyped|quickened))?\/([^/]+)$/, "three-tier"],
    [/^(dyn_paper\/dyn_paper_(?:0|25|50|75|100)_dataflow)(?:_(untyped|quickened))?\/([^/]+)$/, "canonical"],
    [/^(hard_dynamic_defer\/(?:automatic_scope|explicit_components)_dataflow)(?:_(untyped|quickened))?\/([^/]+)$/, "canonical"],
];

const COLORS = ["#0969da", "#cf222e", "#1a7f37", "#8250df", "#bc4c00", "#0550ae"];
const AREA_COLORS = [
    "rgba(9, 105, 218, 0.45)",
    "rgba(207, 34, 46, 0.45)",
    "rgba(26, 127, 55, 0.45)",
    "rgba(130, 80, 223, 0.45)",
];
const TIME_FACTORS = { ns: 1, us: 1e3, "µs": 1e3, ms: 1e6, s: 1e9 };
// First comparable post-redesign benchmark run, at commit 867f298ac7.
const CURRENT_ASYNC_EPOCH_START = Date.parse("2026-07-07T11:02:02Z");
const IS_BROWSER = typeof window !== "undefined" && typeof document !== "undefined";
const chartsElement = IS_BROWSER ? document.getElementById("charts") : null;
const emptyState = IS_BROWSER ? document.getElementById("empty-state") : null;
const metadataElement = IS_BROWSER ? document.getElementById("metadata") : null;
const page = IS_BROWSER ? document.documentElement.dataset.page : null;
const benchmarkData = IS_BROWSER ? window.BENCHMARK_DATA : null;
const chartInstances = [];
// Loaded benchmark runs are immutable snapshots. Cache their canonical grouping so every chart
// lookup does not repeatedly classify all raw benchmark IDs in the run.
const canonicalRunIndexes = new WeakMap();
const query = new URLSearchParams(IS_BROWSER ? window.location.search : "");
let includeSuperseded = query.get("include-superseded") === "true";

function benchmarkRuns() {
    return Object.values(benchmarkData.entries).flat();
}

function allBenchmarkNames(runs) {
    return [...new Set(runs.flatMap((run) => [...canonicalIndexFor(run).keys()]))].sort((a, b) =>
        a.localeCompare(b),
    );
}

function canonicalIndexFor(run) {
    const cached = canonicalRunIndexes.get(run);
    if (cached) return cached;

    const index = new Map();
    for (const benchmark of run.benches) {
        const canonical = classifiedBenchmarkName(run, benchmark.name);
        if (canonical === null) continue;
        if (!index.has(canonical)) index.set(canonical, []);
        index.get(canonical).push(benchmark);
    }
    canonicalRunIndexes.set(run, index);
    return index;
}

function classifiedBenchmarkName(run, name) {
    const route = classifiedDataflowRoute(name);
    if (route && name === route.plain) {
        const commitTimestamp = Date.parse(run.commit.timestamp);
        if (route.history === "three-tier") {
            if (commitTimestamp < CHECKED_QUICKENED_EPOCH_START) return route.untyped;
            if (commitTimestamp < CHECKED_CANONICAL_EPOCH_START) return route.quickened;
        } else if (commitTimestamp < CHECKED_CANONICAL_EPOCH_START) {
            return route.untyped;
        }
        return route.plain;
    }
    const legacyUntyped = canonicalUntypedName(name);
    if (legacyUntyped) return legacyUntyped;
    const legacyMapleQuickened = canonicalMapleQuickenedName(run, name);
    if (legacyMapleQuickened) return legacyMapleQuickened;
    for (const [canonical, aliases] of Object.entries(BENCHMARK_ALIASES)) {
        if (!aliases.includes(name)) continue;
        return canonical;
    }
    return name;
}

function classifiedDataflowRoute(name) {
    for (const [pattern, history] of EPOCH_CLASSIFIED_ROUTE_PATTERNS) {
        const match = name.match(pattern);
        if (!match) continue;
        const [, prefix, tier, size] = match;
        return {
            plain: `${prefix}/${size}`,
            untyped: `${prefix}_untyped/${size}`,
            quickened: `${prefix}_quickened/${size}`,
            tier: tier || "plain",
            history,
        };
    }
    return null;
}

function canonicalUntypedName(name) {
    const match = name.match(
        /^(maple_sequence\/maple_sequence|dup_defer\/dup_defer)_untyped_dataflow(\/[^/]+)$/,
    );
    return match ? `${match[1]}_dataflow_untyped${match[2]}` : null;
}

function canonicalMapleQuickenedName(run, name) {
    const match = name.match(/^(maple_sequence\/maple_sequence)_typed_dataflow(\/[^/]+)$/);
    if (!match || Date.parse(run.commit.timestamp) < CHECKED_QUICKENED_EPOCH_START) return null;
    return `${match[1]}_dataflow_quickened${match[2]}`;
}

function benchmarkFor(run, name) {
    const matching = canonicalIndexFor(run).get(name) || [];
    const exact = matching.find((benchmark) => benchmark.name === name);
    if (exact) return exact;
    for (const alias of BENCHMARK_ALIASES[name] || []) {
        const aliased = matching.find((benchmark) => benchmark.name === alias);
        if (aliased) return aliased;
    }
    return matching[0];
}

function valueInNanoseconds(benchmark) {
    const unit = benchmark.unit.split("/")[0].toLowerCase();
    return benchmark.value * (TIME_FACTORS[unit] || 1);
}

function readableDuration(nanoseconds) {
    if (nanoseconds === null || nanoseconds === undefined) return "No measurement";
    if (nanoseconds >= 1e9) return `${(nanoseconds / 1e9).toFixed(3)} s`;
    if (nanoseconds >= 1e6) return `${(nanoseconds / 1e6).toFixed(3)} ms`;
    if (nanoseconds >= 1e3) return `${(nanoseconds / 1e3).toFixed(3)} µs`;
    return `${nanoseconds.toFixed(1)} ns`;
}

function dateAndCommit(run) {
    const date = new Intl.DateTimeFormat(undefined, {
        dateStyle: "medium",
        timeStyle: "short",
        timeZone: "UTC",
    }).format(new Date(run.date));
    return `${date} UTC · ${run.commit.id.slice(0, 8)}`;
}

function isAsyncRuntimeBenchmark(name) {
    return (
        name.includes("_async/") ||
        /^dyn_paper\/dyn_paper_(?:0|25|50|75|100)\//.test(name)
    );
}

function isSupersededMeasurement(run, name) {
    return isAsyncRuntimeBenchmark(name) && run.date < CURRENT_ASYNC_EPOCH_START;
}

function benchmarkValue(run, name) {
    const benchmark = benchmarkFor(run, name);
    if (!benchmark || (!includeSuperseded && isSupersededMeasurement(run, name))) return null;
    return valueInNanoseconds(benchmark);
}

function axisDate(run, includeDay) {
    return new Intl.DateTimeFormat(undefined, {
        day: includeDay ? "numeric" : undefined,
        month: "short",
        year: "numeric",
        timeZone: "UTC",
    }).format(new Date(run.date));
}

function selectedTickIndices(runs, limit = 8) {
    if (runs.length <= limit) return new Set(runs.map((_run, index) => index));

    const monthBoundaries = [0];
    let previousMonth = "";
    runs.forEach((run, index) => {
        const date = new Date(run.date);
        const month = `${date.getUTCFullYear()}-${date.getUTCMonth()}`;
        if (index > 0 && month !== previousMonth) monthBoundaries.push(index);
        previousMonth = month;
    });
    monthBoundaries.push(runs.length - 1);

    const candidates = [...new Set(monthBoundaries)];
    if (candidates.length <= limit) return new Set(candidates);

    return new Set(
        Array.from({ length: limit }, (_unused, index) =>
            candidates[Math.round((index * (candidates.length - 1)) / (limit - 1))],
        ),
    );
}

function xAxisOptions(runs) {
    const visibleTicks = selectedTickIndices(runs);
    return {
        gridLines: { drawOnChartArea: false },
        ticks: {
            autoSkip: false,
            callback: (_value, index) =>
                visibleTicks.has(index) ? axisDate(runs[index], index === 0 || index === runs.length - 1) : "",
            maxRotation: 0,
            minRotation: 0,
        },
    };
}

function cardSources(cardDefinition) {
    if (cardDefinition.kind === "stacked-pipeline") {
        return [...cardDefinition.phases, cardDefinition.total];
    }
    return cardDefinition.series;
}

function cardHasData(cardDefinition, availableNames) {
    const sources = cardSources(cardDefinition);
    if (cardDefinition.kind === "stacked-pipeline") {
        return sources.every(({ name }) => availableNames.has(name));
    }
    return sources.some(({ name }) => availableNames.has(name));
}

function destroyCharts() {
    chartInstances.splice(0).forEach((chart) => chart.destroy());
    chartsElement.replaceChildren();
}

function createCard(cardDefinition, sources) {
    const card = document.createElement("article");
    card.className = "chart-card";
    if (cardDefinition.fullWidth) card.classList.add("full-width");

    const title = document.createElement("h3");
    title.textContent = cardDefinition.title;
    card.append(title);

    if (cardDefinition.description) {
        const description = document.createElement("p");
        description.className = "chart-description";
        description.textContent = cardDefinition.description;
        card.append(description);
    }

    const container = document.createElement("div");
    container.className = "chart-container";
    const canvas = document.createElement("canvas");
    container.append(canvas);
    card.append(container);

    const names = document.createElement("dl");
    names.className = "benchmark-series";
    sources.forEach(({ label, name }) => {
        const seriesLabel = document.createElement("dt");
        seriesLabel.textContent = label;
        const benchmarkName = document.createElement("dd");
        const code = document.createElement("code");
        code.textContent = name;
        benchmarkName.append(code);
        names.append(seriesLabel, benchmarkName);
    });
    card.append(names);
    chartsElement.append(card);

    return canvas;
}

function interactionOptions(relevantRuns) {
    return {
        hover: {
            onHover: (event, elements) => {
                event.target.style.cursor = elements.length > 0 ? "pointer" : "default";
            },
        },
        onClick: (_event, elements) => {
            if (elements.length > 0) {
                window.open(relevantRuns[elements[0]._index].commit.url, "_blank", "noopener");
            }
        },
        tooltips: {
            mode: "index",
            intersect: false,
            callbacks: {
                title: (items) => dateAndCommit(relevantRuns[items[0].index]),
                label: (item, data) =>
                    `${data.datasets[item.datasetIndex].label}: ${readableDuration(item.yLabel)}`,
                afterBody: (items) => relevantRuns[items[0].index].commit.message.split("\n")[0],
            },
        },
    };
}

function renderLineCard(cardDefinition, runs, availableNames) {
    const series = cardDefinition.series.filter(({ name }) => availableNames.has(name));
    if (series.length === 0) return false;

    const relevantRuns = runs.filter((run) => series.some(({ name }) => benchmarkFor(run, name)));
    const canvas = createCard(cardDefinition, series);
    const interaction = interactionOptions(relevantRuns);

    chartInstances.push(
        new Chart(canvas.getContext("2d"), {
            type: "line",
            data: {
                labels: relevantRuns.map(dateAndCommit),
                datasets: series.map(({ label, name }, index) => ({
                    label,
                    data: relevantRuns.map((run) => {
                        return benchmarkValue(run, name);
                    }),
                    backgroundColor: "transparent",
                    borderColor: COLORS[index % COLORS.length],
                    borderWidth: 2,
                    fill: false,
                    lineTension: 0,
                    pointBackgroundColor: COLORS[index % COLORS.length],
                    pointHitRadius: 8,
                    pointRadius: 2.5,
                    spanGaps: false,
                })),
            },
            options: {
                maintainAspectRatio: false,
                legend: { display: series.length > 1, position: "bottom" },
                hover: interaction.hover,
                onClick: interaction.onClick,
                scales: {
                    xAxes: [xAxisOptions(relevantRuns)],
                    yAxes: [
                        {
                            scaleLabel: { display: true, labelString: "Time per iteration" },
                            ticks: { beginAtZero: false, callback: readableDuration },
                        },
                    ],
                },
                tooltips: interaction.tooltips,
            },
        }),
    );
    return true;
}

function renderStackedPipeline(cardDefinition, runs) {
    const sources = cardSources(cardDefinition);
    const relevantRuns = runs.filter((run) =>
        sources.every(({ name }) => benchmarkFor(run, name) !== undefined),
    );
    if (relevantRuns.length === 0) return false;

    const canvas = createCard(cardDefinition, sources);
    const interaction = interactionOptions(relevantRuns);
    const phaseValues = cardDefinition.phases.map(({ name }) =>
        relevantRuns.map((run) => valueInNanoseconds(benchmarkFor(run, name))),
    );
    const remainingValues = relevantRuns.map((run, runIndex) => {
        const total = valueInNanoseconds(benchmarkFor(run, cardDefinition.total.name));
        const measuredPhases = phaseValues.reduce((sum, values) => sum + values[runIndex], 0);
        return Math.max(0, total - measuredPhases);
    });
    const datasets = cardDefinition.phases.map(({ label }, index) => ({
        label,
        data: phaseValues[index],
        backgroundColor: AREA_COLORS[index % AREA_COLORS.length],
        borderColor: COLORS[index % COLORS.length],
        borderWidth: 1.5,
        fill: true,
        lineTension: 0,
        pointHitRadius: 8,
        pointHoverRadius: 4,
        pointRadius: relevantRuns.length <= 20 ? 2 : 0,
        spanGaps: false,
    }));
    datasets.push({
        label: "Remaining dataflow compilation (derived)",
        data: remainingValues,
        backgroundColor: AREA_COLORS[cardDefinition.phases.length % AREA_COLORS.length],
        borderColor: COLORS[cardDefinition.phases.length % COLORS.length],
        borderWidth: 2,
        fill: true,
        lineTension: 0,
        pointHitRadius: 8,
        pointHoverRadius: 4,
        pointRadius: relevantRuns.length <= 20 ? 2 : 0,
        spanGaps: false,
    });

    interaction.tooltips.callbacks.label = (item, data) => {
        const value = Number(item.yLabel);
        const total = valueInNanoseconds(
            benchmarkFor(relevantRuns[item.index], cardDefinition.total.name),
        );
        const percentage = total > 0 ? ` (${((value / total) * 100).toFixed(1)}%)` : "";
        return `${data.datasets[item.datasetIndex].label}: ${readableDuration(value)}${percentage}`;
    };
    interaction.tooltips.callbacks.afterBody = (items) => {
        const run = relevantRuns[items[0].index];
        const total = valueInNanoseconds(benchmarkFor(run, cardDefinition.total.name));
        return [`Complete typed pipeline: ${readableDuration(total)}`, run.commit.message.split("\n")[0]];
    };

    chartInstances.push(
        new Chart(canvas.getContext("2d"), {
            type: "line",
            data: {
                labels: relevantRuns.map(dateAndCommit),
                datasets,
            },
            options: {
                maintainAspectRatio: false,
                legend: { display: true, position: "bottom" },
                hover: interaction.hover,
                onClick: interaction.onClick,
                scales: {
                    xAxes: [xAxisOptions(relevantRuns)],
                    yAxes: [
                        {
                            stacked: true,
                            scaleLabel: { display: true, labelString: "End-to-end compilation time" },
                            ticks: { beginAtZero: true, callback: readableDuration },
                        },
                    ],
                },
                tooltips: interaction.tooltips,
            },
        }),
    );
    return true;
}

function renderCard(cardDefinition, runs, availableNames) {
    if (cardDefinition.kind === "stacked-pipeline") {
        return renderStackedPipeline(cardDefinition, runs);
    }
    return renderLineCard(cardDefinition, runs, availableNames);
}

function renderImportant(runs, availableNames) {
    destroyCharts();
    let renderedCards = 0;
    IMPORTANT_SECTIONS.forEach((section) => {
        const availableCards = section.cards.filter((card) => cardHasData(card, availableNames));
        if (availableCards.length === 0) return;

        const heading = document.createElement("h2");
        heading.className = "section-title";
        heading.textContent = section.title;
        chartsElement.append(heading);
        availableCards.forEach((card) => {
            if (renderCard(card, runs, availableNames)) renderedCards += 1;
        });
    });
    emptyState.hidden = renderedCards !== 0;
}

function renderAll(names, runs, availableNames) {
    destroyCharts();
    names.forEach((name) =>
        renderCard(
            {
                title: name,
                series: [{ label: "Benchmark ID", name }],
            },
            runs,
            availableNames,
        ),
    );
    emptyState.hidden = names.length !== 0;
}

if (IS_BROWSER && (!benchmarkData || !benchmarkData.entries)) {
    emptyState.hidden = false;
    metadataElement.textContent = "Benchmark data could not be loaded.";
} else if (IS_BROWSER) {
    const runs = benchmarkRuns().sort((a, b) => a.date - b.date);
    const availableNames = allBenchmarkNames(runs);
    const available = new Set(availableNames);
    const latest = new Date(benchmarkData.lastUpdate);
    metadataElement.textContent = `${runs.length} recorded runs · ${availableNames.length} benchmarks · updated ${latest.toLocaleString()}`;

    const viewOptions = document.getElementById("view-options");
    const supersededToggle = document.getElementById("include-superseded");
    const hasSupersededMeasurements = runs.some((run) =>
        run.benches.some((benchmark) => isSupersededMeasurement(run, benchmark.name)),
    );
    viewOptions.hidden = !hasSupersededMeasurements;
    supersededToggle.checked = includeSuperseded;

    const rerender = () => {
        if (page === "important") {
            renderImportant(runs, available);
        } else {
            const filter = document.getElementById("benchmark-filter");
            const filterQuery = filter.value.trim().toLowerCase();
            renderAll(
                filterQuery
                    ? availableNames.filter((name) => name.toLowerCase().includes(filterQuery))
                    : availableNames,
                runs,
                available,
            );
        }
    };

    supersededToggle.addEventListener("change", () => {
        includeSuperseded = supersededToggle.checked;
        if (includeSuperseded) query.set("include-superseded", "true");
        else query.delete("include-superseded");
        const queryString = query.toString();
        const nextUrl = `${window.location.pathname}${queryString ? `?${queryString}` : ""}${window.location.hash}`;
        window.history.replaceState(null, "", nextUrl);
        rerender();
    });

    if (page !== "important") {
        document.getElementById("benchmark-filter").addEventListener("input", rerender);
    }
    rerender();
}

if (typeof module !== "undefined") {
    module.exports = {
        allBenchmarkNames,
        benchmarkFor,
        cardHasData,
        classifiedBenchmarkName,
    };
}
