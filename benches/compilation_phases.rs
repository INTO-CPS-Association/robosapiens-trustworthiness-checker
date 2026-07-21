use std::collections::{BTreeMap, BTreeSet};
use std::hint::black_box;

use contiguous_tree::TreeCursorExt;
use std::time::Duration;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use trustworthiness_checker::Value;
use trustworthiness_checker::core::StreamType;
use trustworthiness_checker::dataflow::DataflowMonitor;
use trustworthiness_checker::lang::core::dependency_graph::{
    DependencyGraphRoots, DependencyGraphSpec,
};

use trustworthiness_checker::lang::dsrv::parser::{parse_sexpr, parse_str};
use trustworthiness_checker::lang::dsrv::type_checker::type_check;
use trustworthiness_checker::{DsrvSpecification, Specification, VarName};

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn compilation_input(assignments: usize) -> String {
    assert!(assignments > 0);
    let mut source = String::from("in input: Int\n");
    for index in 0..assignments {
        let previous = if index == 0 {
            "input".to_owned()
        } else {
            format!("value{}", index - 1)
        };
        source.push_str(&format!("aux value{index}: Int\n"));
        source.push_str(&format!(
            "value{index} = ({previous} + {index}) * 3 - ({previous} % 7)\n"
        ));
    }
    source.push_str("out result: Int\n");
    source.push_str(&format!("result = value{} + 1\n", assignments - 1));
    source
}

fn lexical_binding_input(bindings: usize) -> String {
    assert!(bindings > 0);
    let params = (0..bindings)
        .map(|index| format!("p{index}: Int"))
        .collect::<Vec<_>>()
        .join(", ");
    let body = (0..bindings)
        .map(|index| format!("p{index}"))
        .collect::<Vec<_>>()
        .join(" + ");
    let args = (0..bindings)
        .map(|index| index.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    format!("out result: Int\nresult = (\\{params} -> {body})({args})\n")
}

fn compilation_phases(c: &mut Criterion) {
    let mut group = c.benchmark_group("compilation_phases");
    group.sample_size(20);
    group.warm_up_time(Duration::from_millis(750));
    group.measurement_time(Duration::from_secs(3));

    for assignments in [32, 256, 1024] {
        let source = compilation_input(assignments);
        let parsed = parse_str(&source).expect("benchmark input should parse");
        let typed = type_check(parsed.clone(), false).expect("benchmark input should type check");
        DataflowMonitor::try_compile_untyped(parsed.clone())
            .expect("benchmark input should compile untyped");
        DataflowMonitor::try_compile_checked(typed.clone())
            .expect("benchmark input should compile typed");

        group.throughput(Throughput::Elements(assignments as u64));
        group.bench_with_input(
            BenchmarkId::new("lalr_parse", assignments),
            &source,
            |b, source| b.iter(|| black_box(parse_str(black_box(source)).unwrap())),
        );
        group.bench_function(BenchmarkId::new("strict_type_check", assignments), |b| {
            b.iter_batched(
                || parse_str(&source).expect("benchmark source should parse"),
                |spec| black_box(type_check(spec, false).unwrap()),
                BatchSize::SmallInput,
            )
        });
        group.bench_function(
            BenchmarkId::new("untyped_dependency_graph", assignments),
            |b| {
                b.iter(|| {
                    black_box(
                        black_box(&parsed).dependency_graph_for(DependencyGraphRoots::AllStreams),
                    )
                })
            },
        );
        group.bench_function(
            BenchmarkId::new("typed_dependency_graph", assignments),
            |b| {
                b.iter(|| {
                    black_box(
                        black_box(&typed).dependency_graph_for(DependencyGraphRoots::AllStreams),
                    )
                })
            },
        );
        group.bench_function(
            BenchmarkId::new("dataflow_compile_untyped", assignments),
            |b| {
                b.iter_batched(
                    || parsed.clone(),
                    |spec| black_box(DataflowMonitor::try_compile_untyped(spec).unwrap()),
                    BatchSize::SmallInput,
                )
            },
        );
        group.bench_function(
            BenchmarkId::new("dataflow_compile_typed", assignments),
            |b| {
                b.iter_batched(
                    || typed.clone(),
                    |spec| black_box(DataflowMonitor::try_compile_checked(spec).unwrap()),
                    BatchSize::SmallInput,
                )
            },
        );
        group.bench_with_input(
            BenchmarkId::new("parse_typecheck_compile_typed", assignments),
            &source,
            |b, source| {
                b.iter(|| {
                    let parsed = parse_str(black_box(source)).unwrap();
                    let typed = type_check(parsed, false).unwrap();
                    black_box(DataflowMonitor::try_compile_checked(typed).unwrap())
                })
            },
        );
    }
    group.finish();

    let mut group = c.benchmark_group("lexical_binding_typecheck");
    group.sample_size(20);
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(2));
    for bindings in [8, 64, 256] {
        let source = lexical_binding_input(bindings);
        let parsed = parse_str(&source).expect("lexical binding input should parse");
        type_check(parsed.clone(), false).expect("lexical binding input should type check");
        group.throughput(Throughput::Elements(bindings as u64));
        group.bench_function(BenchmarkId::from_parameter(bindings), |b| {
            b.iter_batched(
                || parsed.clone(),
                |spec| black_box(type_check(spec, false).unwrap()),
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

fn balanced_scalar_source(depth: u32) -> String {
    fn expression(depth: u32) -> String {
        if depth == 0 {
            return "(x + 1)".to_owned();
        }
        let child = expression(depth - 1);
        format!("({child} + {child})")
    }
    format!("in x: Int\nout z: Int\nz = {}", expression(depth))
}

/// Uses balanced scalar trees to expose scaling costs independently of
/// collection and dynamic-expression features.
fn indexed_arena_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("indexed_arena_production_comparison");
    group.sample_size(15);
    group.warm_up_time(Duration::from_millis(300));
    group.measurement_time(Duration::from_secs(2));

    for depth in [5_u32, 8, 11] {
        let nodes = (1_u64 << (depth + 2)) - 1;
        let source = balanced_scalar_source(depth);
        let typed = type_check(
            parse_str(&source).expect("balanced scalar source should parse"),
            false,
        )
        .expect("balanced scalar source should type check");
        let mut monitor = DataflowMonitor::try_compile_checked(typed.clone())
            .expect("balanced scalar source should compile");

        group.throughput(Throughput::Elements(nodes));
        group.bench_with_input(
            BenchmarkId::new("parse_production_arena", nodes),
            &source,
            |b, source| b.iter(|| black_box(parse_str(black_box(source)).unwrap())),
        );
        group.bench_function(BenchmarkId::new("typecheck_production_arena", nodes), |b| {
            b.iter_batched(
                || parse_str(&source).expect("benchmark source should parse"),
                |spec| black_box(type_check(spec, false).unwrap()),
                BatchSize::SmallInput,
            )
        });
        group.bench_function(BenchmarkId::new("compile_production_arena", nodes), |b| {
            b.iter_batched(
                || typed.clone(),
                |spec| black_box(DataflowMonitor::try_compile_checked(spec).unwrap()),
                BatchSize::SmallInput,
            )
        });
        group.bench_function(
            BenchmarkId::new("evaluate_production_arena_plan", nodes),
            |b| {
                let mut output = vec![Value::NoVal];
                b.iter(|| {
                    monitor
                        .evaluate(black_box(&[Value::Int(2)]), black_box(&mut output))
                        .unwrap()
                })
            },
        );
    }
    group.finish();
}

fn specification_import(c: &mut Criterion) {
    let mut group = c.benchmark_group("specification_import");
    group.sample_size(20);
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(3));

    for (assignments, depth) in [(32_usize, 5_u32), (256, 5), (32, 9)] {
        let fixture = balanced_scalar_source(depth);
        let expression = fixture
            .lines()
            .last()
            .expect("fixture has an assignment")
            .strip_prefix("z = ")
            .expect("fixture assignment has expected form");
        let roots = (0..assignments)
            .map(|index| {
                let expr = parse_sexpr(expression).expect("fixture expression should parse");
                (VarName::from(format!("value{index}")), expr)
            })
            .collect::<BTreeMap<_, _>>();
        let outputs = roots.keys().cloned().collect::<BTreeSet<_>>();
        let annotations = roots
            .keys()
            .cloned()
            .map(|name| (name, StreamType::Int))
            .collect::<BTreeMap<_, _>>();
        let nodes = assignments as u64 * ((1_u64 << (depth + 2)) - 1);
        group.throughput(Throughput::Elements(nodes));
        group.bench_function(
            BenchmarkId::new(format!("{assignments}_roots"), depth),
            |b| {
                b.iter(|| {
                    black_box(DsrvSpecification::new(
                        BTreeSet::from([VarName::new("x")]),
                        outputs.clone(),
                        roots.clone(),
                        annotations.clone(),
                        BTreeSet::new(),
                    ))
                })
            },
        );
    }
    group.finish();
}

fn ast_traversal(c: &mut Criterion) {
    let mut group = c.benchmark_group("ast_traversal");
    group.sample_size(30);
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(3));

    for depth in [8_u32, 11, 14] {
        let nodes = (1_u64 << (depth + 2)) - 1;
        let source = balanced_scalar_source(depth);
        let spec = parse_str(&source).expect("balanced scalar source should parse");
        let expr = spec
            .var_expr(&VarName::new("z"))
            .expect("benchmark fixture has output z");

        group.throughput(Throughput::Elements(nodes));
        group.bench_function(BenchmarkId::new("postorder", nodes), |b| {
            b.iter(|| {
                black_box(expr.as_ref()).postorder().for_each(|node| {
                    black_box(node);
                })
            })
        });
        group.bench_function(BenchmarkId::new("fold_node_count", nodes), |b| {
            b.iter(|| {
                black_box(expr.as_ref())
                    .fold(|node| 1_usize + node.children().copied().sum::<usize>())
            })
        });
        group.bench_function(BenchmarkId::new("variable_references", nodes), |b| {
            b.iter(|| black_box(expr.variable_references().count()))
        });
        group.bench_function(BenchmarkId::new("free_variables", nodes), |b| {
            b.iter(|| black_box(expr.free_variables()))
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    compilation_phases,
    indexed_arena_comparison,
    specification_import,
    ast_traversal
);
criterion_main!(benches);
