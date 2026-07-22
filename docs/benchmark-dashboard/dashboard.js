"use strict";

const IMPORTANT_SECTIONS = [
  {
    title: "Overall runtime",
    cards: [
      {
        title: "MAPLE sequence — 25,000 inputs",
        description: "End-to-end execution of the MAPLE sequence monitor.",
        series: [
          {
            label: "Dataflow",
            name: "maple_sequence/maple_sequence_untyped_dataflow/25000",
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
        title: "Dynamic expression workload — 50% dynamic, 100,000 inputs",
        description:
          "Paper workload exercising deferred expression parsing, compilation and evaluation over a large stream.",
        series: [
          {
            label: "Dataflow",
            name: "dyn_paper/dyn_paper_50_dataflow/100000",
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
        title: "Deferred expression — 25,000 inputs",
        description:
          "End-to-end evaluation of an expression deferred until runtime, comparing all three local execution engines.",
        series: [
          {
            label: "Dataflow",
            name: "dup_defer/dup_defer_untyped_dataflow/25000",
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
      {
        title: "Stateful reconfiguration — moving average, 10% reconfiguration",
        description:
          "End-to-end runtime reconfiguration between recursive moving-average windows, with and without context transfer.",
        series: [
          {
            label: "Context transfer enabled",
            name: "rec_moving_average/reconf_ct_on_percent_10/100",
          },
          {
            label: "Context transfer disabled",
            name: "rec_moving_average/reconf_ct_off_percent_10/100",
          },
        ],
      },
      {
        title: "Time-dependent property — 10,000 inputs",
        description: "Runtime evaluation using stream history, defaults and a three-step temporal window.",
        series: [
          {
            label: "Dataflow",
            name: "time_dependent_property/dsrv_default_window_dataflow/10000",
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
            label: "LALR parsing",
            name: "compilation_phases/lalr_parse/1024",
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
        title: "Threshold monitoring — 10,000 inputs",
        description: "Complete runtime and direct-monitor comparison for a simple threshold property.",
        series: [
          {
            label: "DSRV dataflow",
            name: "threshold_property/dsrv_dataflow/10000",
          },
          {
            label: "DSRV SemiSync",
            name: "threshold_property/dsrv_semisync/10000",
          },
          {
            label: "MSTLO runtime qualitative",
            name: "threshold_property/mstlo_runtime_qual/10000",
          },
          {
            label: "MSTLO direct qualitative",
            name: "threshold_property/mstlo_direct_qual/10000",
          },
        ],
      },
      {
        title: "Temporal monitoring — 10,000 inputs",
        description: "Runtime and direct-monitor comparison for a bounded globally property.",
        series: [
          {
            label: "DSRV dataflow",
            name: "time_dependent_property/dsrv_default_window_dataflow/10000",
          },
          {
            label: "DSRV SemiSync",
            name: "time_dependent_property/dsrv_default_window_semisync/10000",
          },
          {
            label: "MSTLO runtime qualitative",
            name: "time_dependent_property/mstlo_globally_window_qual/10000",
          },
          {
            label: "MSTLO direct qualitative",
            name: "time_dependent_property/mstlo_direct_globally_window_qual/10000",
          },
        ],
      },
    ],
  },
];

const COLORS = ["#0969da", "#cf222e", "#1a7f37", "#8250df"];
const AREA_COLORS = [
  "rgba(9, 105, 218, 0.45)",
  "rgba(207, 34, 46, 0.45)",
  "rgba(26, 127, 55, 0.45)",
  "rgba(130, 80, 223, 0.45)",
];
const TIME_FACTORS = { ns: 1, us: 1e3, "µs": 1e3, ms: 1e6, s: 1e9 };
// First comparable post-redesign benchmark run, at commit 867f298ac7.
const CURRENT_ASYNC_EPOCH_START = Date.parse("2026-07-07T11:02:02Z");
const chartsElement = document.getElementById("charts");
const emptyState = document.getElementById("empty-state");
const metadataElement = document.getElementById("metadata");
const page = document.documentElement.dataset.page;
const benchmarkData = window.BENCHMARK_DATA;
const chartInstances = [];
const query = new URLSearchParams(window.location.search);
let includeSuperseded = query.get("include-superseded") === "true";

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

if (!benchmarkData || !benchmarkData.entries) {
  emptyState.hidden = false;
  metadataElement.textContent = "Benchmark data could not be loaded.";
} else {
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
