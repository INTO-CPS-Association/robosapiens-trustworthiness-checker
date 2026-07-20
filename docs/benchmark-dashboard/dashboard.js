"use strict";

// Ordered by user impact and regression risk. Entries not present in data.js are
// ignored, so benchmarks from sexpr-frame-prototype appear after its first run.
const IMPORTANT_BENCHMARKS = [
  "dataflow_semantics_function_evaluation/dataflow_typed_recursive_if_function_monitor/5000",
  "dataflow_semantics_function_evaluation/dataflow_typed_function_monitor/5000",
  "dataflow_semantics_stream_dependency/dataflow_typed_runtime/50000",
  "dataflow_semantics_recursive/dataflow_typed_runtime/50000",

  // sexpr-frame-prototype: execution and compilation of the production arena.
  "indexed_arena_production_comparison/evaluate_production_arena_plan/8191",
  "indexed_arena_production_comparison/compile_production_arena/8191",
  "compilation_phases/dataflow_compile_typed/1024",
  "compilation_phases/parse_typecheck_compile_typed/1024",
  "function_call_binding/checked/512",
  "dataflow_dynamic_phases/dynamic_cached_tick",
  "dataflow_dynamic_phases/dynamic_first_compile",
  "ast_traversal/postorder/65535",
  "ast_traversal/free_variables/65535",

  "time_dependent_property/dsrv_default_window_dataflow/10000",
  "time_dependent_property/mstlo_globally_window_quant/10000",
  "threshold_property/dsrv_dataflow/10000",
  "threshold_property/mstlo_runtime_quant/10000",

  "sindex100/reconf_ct_on_percent_20/100",
  "sindex100/reconf_ct_off_percent_20/100",

  "simple_add/simple_add_typed_dataflow/25000",
  "simple_add/simple_add_untyped_dataflow/25000",
  "simple_add/simple_add_untyped_little/25000",
];

const TIME_FACTORS = {
  ns: 1,
  us: 1e3,
  "µs": 1e3,
  ms: 1e6,
  s: 1e9,
};

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

function valueInNanoseconds(bench) {
  const unit = bench.unit.split("/")[0].toLowerCase();
  return bench.value * (TIME_FACTORS[unit] || 1);
}

function readableDuration(nanoseconds) {
  if (nanoseconds >= 1e9) return `${(nanoseconds / 1e9).toFixed(3)} s`;
  if (nanoseconds >= 1e6) return `${(nanoseconds / 1e6).toFixed(3)} ms`;
  if (nanoseconds >= 1e3) return `${(nanoseconds / 1e3).toFixed(3)} µs`;
  return `${nanoseconds.toFixed(1)} ns`;
}

function seriesFor(name, runs) {
  return runs.flatMap((run) => {
    const bench = run.benches.find((candidate) => candidate.name === name);
    if (!bench) return [];
    return [
      {
        commit: run.commit,
        date: run.date,
        value: valueInNanoseconds(bench),
      },
    ];
  });
}

function destroyCharts() {
  chartInstances.splice(0).forEach((chart) => chart.destroy());
  chartsElement.replaceChildren();
}

function render(names, runs) {
  destroyCharts();
  emptyState.hidden = names.length !== 0;

  names.forEach((name) => {
    const series = seriesFor(name, runs);
    if (series.length === 0) return;

    const card = document.createElement("section");
    card.className = "chart-card";
    const title = document.createElement("h2");
    title.textContent = name;
    const container = document.createElement("div");
    container.className = "chart-container";
    const canvas = document.createElement("canvas");
    container.append(canvas);
    card.append(title, container);
    chartsElement.append(card);

    chartInstances.push(
      new Chart(canvas.getContext("2d"), {
        type: "line",
        data: {
          labels: series.map((point) => point.commit.id.slice(0, 8)),
          datasets: [
            {
              data: series.map((point) => point.value),
              backgroundColor: "rgba(9, 105, 218, 0.10)",
              borderColor: "#0969da",
              borderWidth: 2,
              fill: false,
              lineTension: 0,
              pointHitRadius: 8,
              pointRadius: 2.5,
            },
          ],
        },
        options: {
          maintainAspectRatio: false,
          legend: { display: false },
          onClick: (_event, elements) => {
            if (elements.length > 0) {
              window.open(series[elements[0]._index].commit.url, "_blank", "noopener");
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
            callbacks: {
              title: (items) => {
                const point = series[items[0].index];
                return `${point.commit.id.slice(0, 12)} · ${new Date(point.date).toLocaleDateString()}`;
              },
              label: (item) => readableDuration(series[item.index].value),
              afterLabel: (item) => series[item.index].commit.message.split("\n")[0],
            },
          },
        },
      }),
    );
  });
}

if (!benchmarkData || !benchmarkData.entries) {
  emptyState.hidden = false;
  metadataElement.textContent = "Benchmark data could not be loaded.";
} else {
  const runs = benchmarkRuns().sort((a, b) => a.date - b.date);
  const availableNames = allBenchmarkNames(runs);
  const latest = new Date(benchmarkData.lastUpdate);
  metadataElement.textContent = `${runs.length} recorded runs · ${availableNames.length} benchmarks · updated ${latest.toLocaleString()}`;

  if (page === "important") {
    const available = new Set(availableNames);
    render(IMPORTANT_BENCHMARKS.filter((name) => available.has(name)), runs);
  } else {
    const filter = document.getElementById("benchmark-filter");
    const applyFilter = () => {
      const query = filter.value.trim().toLowerCase();
      render(
        query ? availableNames.filter((name) => name.toLowerCase().includes(query)) : availableNames,
        runs,
      );
    };
    filter.addEventListener("input", applyFilter);
    applyFilter();
  }
}
