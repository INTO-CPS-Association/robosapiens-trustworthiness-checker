//! List-operation workloads shared by the `list_operations` timing benchmark and the
//! `list_operation_allocations` counting benchmark.
//!
//! Each workload evaluates one list operation per tick over an input list whose elements are
//! integers or eight-field records. The input row keeps its list, and operand lifting retains
//! it, so every operation sees a shared list, as specifications do.

use std::collections::BTreeMap;
use std::hint::black_box;

use ecow::{EcoString, EcoVec};
use trustworthiness_checker::core::Semantics;
use trustworthiness_checker::dataflow::DataflowMonitor;
use trustworthiness_checker::{TypeCheckOptions, Value, VarName};

const RECORD: &str = "Struct<key: Int, label: Str, ...>";

#[derive(Clone, Copy)]
enum Element {
    Int,
    Record,
}

impl Element {
    fn name(self) -> &'static str {
        match self {
            Element::Int => "int",
            Element::Record => "record",
        }
    }

    fn type_name(self) -> &'static str {
        match self {
            Element::Int => "Int",
            Element::Record => RECORD,
        }
    }

    /// The expression reading an element's key.
    fn key(self, element: &str) -> String {
        match self {
            Element::Int => element.to_owned(),
            Element::Record => format!("{element}.key"),
        }
    }

    fn value(self, key: i64) -> Value {
        match self {
            Element::Int => Value::Int(key),
            Element::Record => {
                let mut fields = BTreeMap::from([
                    (EcoString::from("key"), Value::Int(key)),
                    (
                        EcoString::from("label"),
                        Value::Str("a label longer than inline storage".into()),
                    ),
                ]);
                for field in 0..6 {
                    fields.insert(
                        format!("field_{field}").into(),
                        Value::Int(key * 10 + field),
                    );
                }
                Value::from(fields)
            }
        }
    }
}

/// Each workload's name, output type and the expression computing output `z` from the inputs
/// `xs`, `one`, `empty`, `x` and `k`; `z[1]` is the output's own previous value.
fn expressions(element: Element, len: usize) -> Vec<(&'static str, String, String)> {
    let t = element.type_name();
    let list = format!("List<{t}>");
    let key = element.key("r");
    let filter = |predicate: &str| format!("List.filter(\\r: {t} -> {predicate}, xs)");
    vec![
        ("filter_none", list.clone(), filter("false")),
        ("filter_one", list.clone(), filter(&format!("{key} == k"))),
        (
            "filter_half",
            list.clone(),
            filter(&format!("{key} % 2 == 0")),
        ),
        (
            "filter_all_but_one",
            list.clone(),
            filter(&format!("{key} != k")),
        ),
        ("filter_all", list.clone(), filter(&format!("{key} >= 0"))),
        (
            "map_key",
            "List<Int>".into(),
            format!("List.map(\\r: {t} -> {key} + 1, xs)"),
        ),
        (
            "fold_count",
            "Int".into(),
            format!("List.fold(\\n: Int, r: {t} -> n + 1, 0, xs)"),
        ),
        (
            "concat_empty_left",
            list.clone(),
            "List.concat(empty, xs)".into(),
        ),
        ("concat_one", list.clone(), "List.concat(xs, one)".into()),
        ("append", list.clone(), "List.append(xs, x)".into()),
        ("tail", list.clone(), "List.tail(xs)".into()),
        ("head", t.into(), "List.head(xs)".into()),
        (
            "window",
            list,
            format!(
                "if List.len(default(z[1], xs)) < {len} \
                 then List.append(default(z[1], xs), x) \
                 else List.append(List.tail(default(z[1], xs)), x)"
            ),
        ),
    ]
}

/// One compiled workload with its input row.
pub struct Workload {
    pub label: serde_json::Value,
    monitor: DataflowMonitor,
    input: Vec<Value>,
    output: Vec<Value>,
}

impl Workload {
    fn new(element: Element, len: usize, name: &str, output: &str, expression: &str) -> Self {
        let t = element.type_name();
        let source = format!(
            "in xs: List<{t}>\nin one: List<{t}>\nin empty: List<{t}>\nin x: {t}\nin k: Int\n\
             out z: {output}\nz = {expression}\n"
        );
        let spec = trustworthiness_checker::dsrv_fixtures::elaborated_with(
            &source,
            TypeCheckOptions::GRADUAL,
        );
        let monitor = DataflowMonitor::compile_with_semantics(spec, Semantics::Untimed)
            .unwrap_or_else(|error| panic!("{name} should compile: {error:?}"));
        let xs: EcoVec<Value> = (0..len as i64).map(|key| element.value(key)).collect();
        let values = [
            ("xs", Value::List(xs)),
            (
                "one",
                Value::List(EcoVec::from([element.value(len as i64)])),
            ),
            ("empty", Value::List(EcoVec::new())),
            ("x", element.value(len as i64)),
            ("k", Value::Int(len as i64 / 2)),
        ];
        let input = monitor
            .input_vars()
            .iter()
            .map(|var| {
                values
                    .iter()
                    .find_map(|(name, value)| (var == &VarName::new(name)).then(|| value.clone()))
                    .unwrap_or_else(|| panic!("missing list benchmark input `{var}`"))
            })
            .collect();
        let output = vec![Value::NoVal; monitor.output_vars().len()];
        let mut workload = Self {
            label: serde_json::json!({
                "workload": name,
                "element": element.name(),
                "len": len,
            }),
            monitor,
            input,
            output,
        };
        // Warm lifting state and fill a bounded window before measuring.
        for _ in 0..len + 2 {
            workload.tick();
        }
        workload
    }

    pub fn tick(&mut self) {
        self.monitor
            .evaluate(black_box(&self.input), &mut self.output)
            .unwrap();
        black_box(&self.output);
    }
}

/// The workloads whose `element/name/len` label contains any command-line filter, or all of
/// them without filters. Each is compiled only when the iterator reaches it.
pub fn selected() -> impl Iterator<Item = Workload> {
    let filters: Vec<String> = std::env::args()
        .skip(1)
        .filter(|argument| !argument.starts_with("--"))
        .collect();
    [Element::Int, Element::Record]
        .into_iter()
        .flat_map(|element| [4_usize, 32, 128].map(|len| (element, len)))
        .flat_map(|(element, len)| {
            expressions(element, len)
                .into_iter()
                .map(move |(name, output, expression)| (element, len, name, output, expression))
        })
        .filter(move |(element, len, name, _, _)| {
            let label = format!("{}/{name}/{len}", element.name());
            filters.is_empty() || filters.iter().any(|filter| label.contains(filter))
        })
        .map(|(element, len, name, output, expression)| {
            Workload::new(element, len, name, &output, &expression)
        })
}

/// The runtime layouts of a value and of a list's payload.
pub fn layout() -> serde_json::Value {
    serde_json::json!({
        "size_of_value": std::mem::size_of::<Value>(),
        "size_of_value_list_payload_ecovec": std::mem::size_of::<EcoVec<Value>>(),
    })
}
