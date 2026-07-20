"use strict";

// Ordered by user impact and regression risk. Missing series are ignored, so
// pipeline and frame-prototype cards appear after their first recorded run.
const IMPORTANT_SECTIONS = [
  {
    title: "Dataflow and SemiSync runtimes",
    cards: [
      {
        title: "Recursive typed semantics",
        series: [
          ["Dataflow", "dataflow_semantics_recursive/dataflow_typed_runtime/50000"],
          ["SemiSync", "dataflow_semantics_recursive/semisync_typed_runtime/50000"],
        ],
      },
      {
        title: "Stream-dependency typed semantics",
        series: [
          ["Dataflow", "dataflow_semantics_stream_dependency/dataflow_typed_runtime/50000"],
          ["SemiSync", "dataflow_semantics_stream_dependency/semisync_typed_runtime/50000"],
        ],
      },
      {
        title: "Time-dependent property",
        series: [
          ["Dataflow", "time_dependent_property/dsrv_default_window_dataflow/10000"],
          ["SemiSync", "time_dependent_property/dsrv_default_window_semisync/10000"],
        ],
      },
      {
        title: "Threshold property",
        series: [
          ["Dataflow", "threshold_property/dsrv_dataflow/10000"],
          ["SemiSync", "threshold_property/dsrv_semisync/10000"],
        ],
      },
      {
        title: "Deferred evaluation",
        series: [
          ["Dataflow", "dup_defer/dup_defer_untyped_dataflow/25000"],
          ["SemiSync", "dup_defer/dup_defer_untyped_semisync/25000"],
        ],
      },
      {
        title: "Arithmetic typed semantics",
        series: [
          ["Dataflow", "dataflow_semantics_arithmetic/dataflow_typed_runtime/50000"],
          ["SemiSync", "dataflow_semantics_arithmetic/semisync_typed_runtime/50000"],
        ],
      },
      {
        title: "Simple runtime baseline",
        series: [
          ["Dataflow", "simple_add/simple_add_untyped_dataflow/25000"],
          ["SemiSync", "simple_add/simple_add_untyped_little/25000"],
        ],
      },
    ],
  },
  {
    title: "MSTLO",
    cards: [
      {
        title: "Threshold monitoring",
        series: [
          ["Runtime qualitative", "threshold_property/mstlo_runtime_qual/10000"],
          ["Runtime quantitative", "threshold_property/mstlo_runtime_quant/10000"],
          ["Direct qualitative", "threshold_property/mstlo_direct_qual/10000"],
          ["Direct quantitative", "threshold_property/mstlo_direct_quant/10000"],
        ],
      },
      {
        title: "Time-dependent monitoring",
        series: [
          ["Runtime qualitative", "time_dependent_property/mstlo_globally_window_qual/10000"],
          ["Runtime quantitative", "time_dependent_property/mstlo_globally_window_quant/10000"],
          ["Direct qualitative", "time_dependent_property/mstlo_direct_globally_window_qual/10000"],
          ["Direct quantitative", "time_dependent_property/mstlo_direct_globally_window_quant/10000"],
        ],
      },
      {
        title: "Output-stream scaling",
        series: [
          ["Runtime (8 outputs)", "threshold_property_diagnostics/mstlo_runtime_output_streams/8"],
          ["Direct (8 outputs)", "threshold_property_diagnostics/mstlo_direct_output_streams/8"],
        ],
      },
    ],
  },
  {
    title: "Compilation pipeline",
    cards: [
      {
        title: "Parsing",
        series: [["LALR parsing", "compilation_phases/lalr_parse/1024"]],
      },
      {
        title: "Type checking",
        series: [["Strict type checking", "compilation_phases/strict_type_check/1024"]],
      },
      {
        title: "Dependency-graph construction",
        series: [
          ["Untyped", "compilation_phases/untyped_dependency_graph/1024"],
          ["Typed", "compilation_phases/typed_dependency_graph/1024"],
        ],
      },
      {
        title: "Dataflow compilation",
        series: [
          ["Untyped", "compilation_phases/dataflow_compile_untyped/1024"],
          ["Typed", "compilation_phases/dataflow_compile_typed/1024"],
        ],
      },
      {
        title: "Complete typed pipeline",
        series: [["Parse + type check + compile", "compilation_phases/parse_typecheck_compile_typed/1024"]],
      },
    ],
  },
  {
    title: "Frame and dynamic-expression safeguards",
    cards: [
      {
        title: "Production-frame execution",
        series: [["Evaluate plan", "indexed_arena_production_comparison/evaluate_production_arena_plan/8191"]],
      },
      {
        title: "Production-frame compilation",
        series: [["Compile arena", "indexed_arena_production_comparison/compile_production_arena/8191"]],
      },
      {
        title: "Dynamic-expression lifecycle",
        series: [
          ["Dynamic first compile", "dataflow_dynamic_phases/dynamic_first_compile"],
          ["Dynamic cached tick", "dataflow_dynamic_phases/dynamic_cached_tick"],
          ["Defer first compile", "dataflow_dynamic_phases/defer_first_compile"],
          ["Defer cached tick", "dataflow_dynamic_phases/defer_cached_tick"],
        ],
      },
    ],
  },
];

const COLORS = ["#0969da", "#cf222e", "#1a7f37", "#8250df"];
const TIME_FACTORS = { ns: 1, us: 1e3, "µs": 1e3, ms: 1e6, s: 1e9 };
const chartsElement = document.getElementById("charts");
const emptyState = document.getElementById("empty-state");
const metadataElement = document.getElementById("metadata");
const page = document.documentElement.dataset.page;
const benchmarkData = window.BENCHMARK_DATA;
const chartInstances = [];

function benchmarkRuns() {
  return Object.values(benchmarkData.entries).flat();
}

function allBenchmarkNames(runs) {
  return [...new Set(runs.flatMap((run) => run.benches.map((bench) => bench.name)))].sort((a, b) =>
    a.localeCompare(b),
  );
}

function benchmarkFor(run, name) {
  return run.benches.find((benchmark) => benchmark.name === name);
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
  const date = new Date(run.date).toISOString().slice(0, 10);
  return `${date} · ${run.commit.id.slice(0, 8)}`;
}

function destroyCharts() {
  chartInstances.splice(0).forEach((chart) => chart.destroy());
  chartsElement.replaceChildren();
}

function renderCard(cardDefinition, runs, availableNames) {
  const series = cardDefinition.series
    .map(([label, name]) => ({ label, name }))
    .filter(({ name }) => availableNames.has(name));
  if (series.length === 0) return false;

  const relevantRuns = runs.filter((run) => series.some(({ name }) => benchmarkFor(run, name)));
  const card = document.createElement("article");
  card.className = "chart-card";
  const title = document.createElement("h3");
  title.textContent = cardDefinition.title;
  const container = document.createElement("div");
  container.className = "chart-container";
  const canvas = document.createElement("canvas");
  container.append(canvas);

  const history = document.createElement("div");
  history.className = "commit-history";
  history.setAttribute("aria-label", "Benchmark commits");
  relevantRuns.forEach((run) => {
    const link = document.createElement("a");
    link.href = run.commit.url;
    link.target = "_blank";
    link.rel = "noopener";
    link.textContent = dateAndCommit(run);
    history.append(link);
  });
  card.append(title, container, history);
  chartsElement.append(card);

  chartInstances.push(
    new Chart(canvas.getContext("2d"), {
      type: "line",
      data: {
        labels: relevantRuns.map(dateAndCommit),
        datasets: series.map(({ label, name }, index) => ({
          label,
          data: relevantRuns.map((run) => {
            const benchmark = benchmarkFor(run, name);
            return benchmark ? valueInNanoseconds(benchmark) : null;
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
        scales: {
          xAxes: [{ ticks: { autoSkip: true, maxTicksLimit: 8, maxRotation: 0 } }],
          yAxes: [
            {
              scaleLabel: { display: true, labelString: "Time per iteration" },
              ticks: { beginAtZero: false, callback: readableDuration },
            },
          ],
        },
        tooltips: {
          mode: "index",
          intersect: false,
          callbacks: {
            title: (items) => dateAndCommit(relevantRuns[items[0].index]),
            label: (item, data) => `${data.datasets[item.datasetIndex].label}: ${readableDuration(item.yLabel)}`,
            afterBody: (items) => relevantRuns[items[0].index].commit.message.split("\n")[0],
          },
        },
      },
    }),
  );
  return true;
}

function renderImportant(runs, availableNames) {
  destroyCharts();
  let renderedCards = 0;
  IMPORTANT_SECTIONS.forEach((section) => {
    const availableCards = section.cards.filter((card) =>
      card.series.some(([_label, name]) => availableNames.has(name)),
    );
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
  names.forEach((name) => renderCard({ title: name, series: [[name, name]] }, runs, availableNames));
  emptyState.hidden = names.length !== 0;
}

if (!benchmarkData || !benchmarkData.entries) {
  emptyState.hidden = false;
  metadataElement.textContent = "Benchmark data could not be loaded.";
} else {
  const runs = benchmarkRuns().sort((a, b) => a.date - b.date);
  const availableNames = allBenchmarkNames(runs);
  const available = new Set(availableNames);
  const latest = new Date(benchmarkData.lastUpdate);
  metadataElement.textContent = `${runs.length} recorded runs · ${availableNames.length} benchmarks · updated ${latest.toLocaleString()}`;

  if (page === "important") {
    renderImportant(runs, available);
  } else {
    const filter = document.getElementById("benchmark-filter");
    const applyFilter = () => {
      const query = filter.value.trim().toLowerCase();
      renderAll(
        query ? availableNames.filter((name) => name.toLowerCase().includes(query)) : availableNames,
        runs,
        available,
      );
    };
    filter.addEventListener("input", applyFilter);
    applyFilter();
  }
}
