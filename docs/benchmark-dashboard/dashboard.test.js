"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const {
    allBenchmarkNames,
    benchmarkFor,
    cardHasData,
} = require("./dashboard.js");

const THRESHOLD_UNTYPED = "threshold_property/dsrv_dataflow_untyped/10000";
const THRESHOLD_CANONICAL = "threshold_property/dsrv_dataflow/10000";
const THRESHOLD_QUICKENED = "threshold_property/dsrv_dataflow_quickened/10000";

function run(timestamp, names, date = Date.parse(timestamp)) {
    return {
        commit: { timestamp },
        date,
        benches: names.map((name, value) => ({ name, value })),
    };
}

test("classifies the legacy plain threshold route as untyped", () => {
    const legacy = run("2026-07-01T12:00:00Z", [THRESHOLD_CANONICAL]);

    assert.deepEqual(allBenchmarkNames([legacy]), [THRESHOLD_UNTYPED]);
    assert.equal(benchmarkFor(legacy, THRESHOLD_UNTYPED).name, THRESHOLD_CANONICAL);
    assert.equal(benchmarkFor(legacy, THRESHOLD_CANONICAL), undefined);
});

test("classifies the 242b5bd9 plain route as quickened and retains explicit untyped", () => {
    const transitional = run("2026-07-29T17:54:52+02:00", [
        THRESHOLD_CANONICAL,
        THRESHOLD_UNTYPED,
    ], Date.parse("2026-09-01T12:00:00Z"));

    assert.deepEqual(allBenchmarkNames([transitional]), [
        THRESHOLD_QUICKENED,
        THRESHOLD_UNTYPED,
    ]);
    assert.equal(benchmarkFor(transitional, THRESHOLD_UNTYPED).name, THRESHOLD_UNTYPED);
    assert.equal(benchmarkFor(transitional, THRESHOLD_QUICKENED).name, THRESHOLD_CANONICAL);
    assert.equal(benchmarkFor(transitional, THRESHOLD_CANONICAL), undefined);
});

test("prefers an explicit quickened route over its transitional plain ID", () => {
    const transitional = run("2026-08-01T12:00:00+02:00", [
        THRESHOLD_CANONICAL,
        THRESHOLD_UNTYPED,
        THRESHOLD_QUICKENED,
    ]);

    assert.equal(benchmarkFor(transitional, THRESHOLD_QUICKENED).name, THRESHOLD_QUICKENED);
});

test("classifies non-key sizes with source commit time rather than collection date", () => {
    const plain = "threshold_property/dsrv_dataflow/1000";
    const untyped = "threshold_property/dsrv_dataflow_untyped/1000";
    const quickened = "threshold_property/dsrv_dataflow_quickened/1000";
    const backfill = run(
        "2026-07-29T17:54:52+02:00",
        [plain, untyped],
        Date.parse("2026-09-05T12:00:00Z"),
    );

    assert.deepEqual(allBenchmarkNames([backfill]), [quickened, untyped]);
    assert.equal(benchmarkFor(backfill, quickened).name, plain);
    assert.equal(benchmarkFor(backfill, untyped).name, untyped);
});

test("classifies surviving pre-33fa dynamic plain IDs as untyped", () => {
    const plain = "dyn_paper/dyn_paper_50_dataflow/100000";
    const untyped = "dyn_paper/dyn_paper_50_dataflow_untyped/100000";
    const legacy = run("2026-07-01T12:00:00Z", [plain]);
    const current = run("2026-08-23T11:34:17+02:00", [plain, untyped]);

    assert.deepEqual(allBenchmarkNames([legacy]), [untyped]);
    assert.equal(benchmarkFor(legacy, untyped).name, plain);
    assert.deepEqual(allBenchmarkNames([current]), [untyped, plain]);
});

test("canonicalizes proven MAPLE and defer legacy route names for every size", () => {
    const mapleOld = "maple_sequence/maple_sequence_untyped_dataflow/1000";
    const mapleNew = "maple_sequence/maple_sequence_dataflow_untyped/1000";
    const deferOld = "dup_defer/dup_defer_untyped_dataflow/250";
    const deferNew = "dup_defer/dup_defer_dataflow_untyped/250";
    const history = run("2026-07-29T17:54:52+02:00", [mapleOld, deferOld]);

    assert.deepEqual(allBenchmarkNames([history]), [deferNew, mapleNew]);
    assert.equal(benchmarkFor(history, mapleNew).name, mapleOld);
    assert.equal(benchmarkFor(history, deferNew).name, deferOld);
});

test("classifies the 242b5bd9 MAPLE typed route as quickened", () => {
    const oldName = "maple_sequence/maple_sequence_typed_dataflow/25000";
    const quickened = "maple_sequence/maple_sequence_dataflow_quickened/25000";
    const transitional = run("2026-07-29T17:54:52+02:00", [oldName]);

    assert.deepEqual(allBenchmarkNames([transitional]), [quickened]);
    assert.equal(benchmarkFor(transitional, quickened).name, oldName);
});

test("keeps post-epoch checked canonical and untyped routes distinct", () => {
    const current = run("2026-08-23T11:34:17+02:00", [
        THRESHOLD_CANONICAL,
        THRESHOLD_UNTYPED,
    ]);

    assert.deepEqual(allBenchmarkNames([current]), [THRESHOLD_UNTYPED, THRESHOLD_CANONICAL]);
    assert.equal(benchmarkFor(current, THRESHOLD_CANONICAL).name, THRESHOLD_CANONICAL);
    assert.equal(benchmarkFor(current, THRESHOLD_UNTYPED).name, THRESHOLD_UNTYPED);
});

test("modern canonical data does not advertise an absent untyped series", () => {
    const current = run("2026-08-23T11:34:17+02:00", [THRESHOLD_CANONICAL]);
    const available = new Set(allBenchmarkNames([current]));

    assert.equal(
        cardHasData({ series: [{ label: "Untyped", name: THRESHOLD_UNTYPED }] }, available),
        false,
    );
    assert.equal(
        cardHasData({ series: [{ label: "Canonical", name: THRESHOLD_CANONICAL }] }, available),
        true,
    );
});

test("collapses a true legacy alias in list and key views", () => {
    const canonical = "compilation_phases/parse_and_validate_specification/1024";
    const alias = "compilation_phases/lalr_parse/1024";
    const history = run("2026-07-01T12:00:00Z", [alias, canonical]);

    assert.deepEqual(allBenchmarkNames([history]), [canonical]);
    assert.equal(benchmarkFor(history, canonical).name, canonical);
});

test("collapses both legacy sustained Value-JIT routes and prefers the current ID", () => {
    const current = "jit/sustained/arithmetic/native_value_eager";
    const eagerLegacy = "jit/sustained/arithmetic/all_no_hotness";
    const warmedLegacy = "jit/sustained/arithmetic/all_with_hotness";
    const history = run("2026-09-01T12:00:00Z", [warmedLegacy, eagerLegacy, current]);

    assert.deepEqual(allBenchmarkNames([history]), [current]);
    assert.equal(benchmarkFor(history, current).name, current);
    assert.equal(benchmarkFor(run("2026-08-01T12:00:00Z", [eagerLegacy]), current).name, eagerLegacy);
    assert.equal(benchmarkFor(run("2026-08-01T12:00:00Z", [warmedLegacy]), current).name, warmedLegacy);
});
