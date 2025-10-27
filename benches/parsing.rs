use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use trustworthiness_checker::lang::dynamic_lola::lalr_parser::parse_str;
use trustworthiness_checker::lola_specification;

#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

// Create many small terms separated by &&
fn create_small_varied_expressions(rng: &mut StdRng, size: usize) -> String {
    let expressions = [
        // Arithmetic / comparisons
        "(42)",
        "(1 - 2 * 3)",
        "((1 + 2) * (3 - 4))",
        "(x / y + z)",
        "(1 + 2 <= 3)",
        "(42 % 9)",
        "(1.0 - 2.31231 * 3.4124)",
        "((1.9421 + 2.4123) * (3.49123 - 4.9213))",
        "(1.15145 + 2.1513 >= 3.46515)",
        // Booleans / logical
        "(true)",
        "(false)",
        "(!true)",
        "(!false || true)",
        "(a && (b || c))",
        "(if x == y then true else false)",
        // Strings
        r#"("hello" ++ " world")"#,
        r#"(s1 ++ s2 ++ "!")"#,
        // Lists and maps
        "(List(1, 2, 3))",
        "(List())",
        r#"(Map("x": 1, "y": 2))"#,
        "(List.get(xs, 0))",
        // Special keywords
        "(dynamic(a + b))",
        "(eval(1 + 2))",
        "(defer(x))",
        "(update(x, y))",
        "(default(a, b))",
        // "(isDefined(x))",
        // "(when(y))",
        // ---- Lists ----
        "(List(1, 2, 3))",
        "(List.get(xs, 0))",
        "(List.append(List(1), 2))",
        "(List.concat(List(1,2), List(3)))",
        "(List.head(List(1,2,3)))",
        "(List.tail(List(1,2,3)))",
        "(List.len(List(1,2,3)))",
        // ---- Maps ----
        r#"(Map("x": 1, "y": 2))"#,
        r#"(Map.get(Map("x": 1), "x"))"#,
        r#"(Map.insert(Map(), "k", 42))"#,
        r#"(Map.remove(Map("a": 1), "a"))"#,
        r#"(Map.has_key(Map("a": 1), "a"))"#,
        // Stream index:
        "(x[1])",
        // Var:
        "(longName)",
        // Mixed / nested
        "(if a then List(1, 2) else List(3))",
        "(!(x < y) && (z == 0))",
    ];
    let exprs: Vec<&str> = (0..size)
        .map(|_| {
            let idx = rng.random_range(0..expressions.len());
            expressions[idx]
        })
        .collect();
    // Might not semantically make sense but it should parse
    "out z\nz = ".to_owned() + &exprs.join(" && ")
}

// Create a single expression that grows in size
fn create_growing_complexity_expression(rng: &mut StdRng, size: usize) -> String {
    // Set of atomic building blocks (inspired by Tobias and Amalie's project)
    let atoms = [
        "(x)".to_string(),
        "(y)".to_string(),
        "((0.1) * sin(a))".to_string(),
        "((0.153) * cos(a))".to_string(),
        "(((-0.181) * sin(a)) + ((-0.153) * cos(a)))".to_string(),
        "((0.1) * sin(3.14))".to_string(),
        "((0.153) * cos(3.14))".to_string(),
    ];

    let mut expr = atoms[0].clone();

    for _ in 1..size {
        let idx = rng.random_range(0..atoms.len());
        let atom = &atoms[idx];

        // alternate how we combine them so it grows but stays varied
        let idx = rng.random_range(0..4);
        expr = match idx % 4 {
            0 => format!("({expr} + {atom})"),
            1 => format!("({expr} - {atom})"),
            2 => format!("({expr} * {atom})"),
            _ => format!("(if {expr} <= {atom} then 1 else 0)"),
        };
    }

    "out z\nz = ".to_owned() + &expr
}

fn parse_small_varied_inputs(c: &mut Criterion) {
    let mut rng = StdRng::seed_from_u64(42);
    let mut group = c.benchmark_group("parse_small_varied_inputs");

    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(5));

    let sizes = [10, 100, 500, 1000, 2000, 5000, 10000];
    for size in sizes {
        let input = create_small_varied_expressions(&mut rng, size);
        // pre-validate that all parsers accept the input
        assert!(parse_str(input.as_str()).is_ok());
        let inp = &mut input.as_str();
        assert!(lola_specification(inp).is_ok());

        group.bench_with_input(
            BenchmarkId::new("parsing_winnow", size),
            &input,
            |b, input| {
                b.iter(|| {
                    let inp = &mut input.as_str();
                    let _ = lola_specification(inp);
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("parsing_lalrpop", size),
            &input,
            |b, input| {
                b.iter(|| {
                    let _ = parse_str(input.as_str());
                })
            },
        );
    }
    group.finish();
}

fn parse_growing_complexity_input(c: &mut Criterion) {
    const FAST_VERSION: bool = true;
    let mut rng = StdRng::seed_from_u64(42);
    let name = if FAST_VERSION {
        "DRAFT_DO_NOT_PUBLISH_parse_growing_complexity_input"
    } else {
        "parse_growing_complexity_input"
    };
    let mut group = c.benchmark_group(name);

    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(5));

    // Fast but unscientific version:
    let sizes = if FAST_VERSION {
        // Only really shows how Winnow's growth
        vec![1, 2, 4, 6, 8, 10, 12, 14, 16]
    } else {
        // Slow growth initially for Winnow and then faster growth for LALRPOP:
        vec![
            1, 2, 4, 8, 12, 16, 20, 24, 28, 32, 48, 64, 96, 128, 192, 256, 384, 512, 768, 1024,
            1536, 2048,
        ]
    };
    for size in sizes {
        let input = create_growing_complexity_expression(&mut rng, size);
        // pre-validate that all parsers accept the input
        assert!(parse_str(input.as_str()).is_ok());
        if size <= 16 {
            let inp = &mut input.as_str();
            assert!(lola_specification(inp).is_ok());
        }

        // parsing with winnow gets very slow for complex inputs
        if size <= 16 {
            group.bench_with_input(
                BenchmarkId::new("parsing_winnow", size),
                &input,
                |b, input| {
                    b.iter(|| {
                        let inp = &mut input.as_str();
                        let _ = lola_specification(inp);
                    })
                },
            );
        }
        group.bench_with_input(
            BenchmarkId::new("parsing_lalrpop", size),
            &input,
            |b, input| {
                b.iter(|| {
                    let _ = parse_str(input.as_str());
                })
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    parse_small_varied_inputs,
    parse_growing_complexity_input
);
criterion_main!(benches);
