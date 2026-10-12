//! Library coherence checked against fixed timelines and direct recurrences.
use crate::dataflow::DataflowMonitor;
use crate::dsrv_fixtures::WithoutWarnings;
use crate::lang::dsrv::TypeCheckOptions;
use crate::lang::dsrv::ast::DsrvSpecification;
use crate::lang::dsrv::expand::DsrvExpandError;
use crate::lang::dsrv::modules::{ModuleCollector, show_path};
use crate::lang::dsrv::path::ModuleName;
use crate::lang::dsrv::source_map::SourceLabel;
use crate::{Value, VarName};
use std::collections::BTreeMap;

const BOTH: [TypeCheckOptions; 2] = [TypeCheckOptions::STRICT, TypeCheckOptions::GRADUAL];
const HEADER: &str = "use experimental::{modules, functions, generics, local_streams, tagged_unions, pattern_matching, newtypes}\n";

fn expand_with(root: &str, modules: &[(&str, &str)]) -> Result<DsrvSpecification, DsrvExpandError> {
    let label = |name: &str| SourceLabel::Path(format!("{name}.dsrv").into());
    let mut collector = ModuleCollector::with_label(root, label("root")).expect("a parsable root");
    while let Some(module) = collector.next_request().map(<[ModuleName]>::to_vec) {
        let wanted = show_path(&module);
        let text = modules
            .iter()
            .find(|(name, _)| *name == wanted)
            .unwrap_or_else(|| panic!("no source for {wanted}"))
            .1;
        collector
            .supply_labelled(text, label(&wanted))
            .expect("a parsable module");
    }
    crate::lang::dsrv::expand::expand_program(collector.finish().expect("collected"), None)
}

fn expand(body: &str) -> Result<DsrvSpecification, DsrvExpandError> {
    expand_with(&format!("{HEADER}{body}"), &[])
}
fn program(body: &str) -> DsrvSpecification {
    expand(body).unwrap_or_else(|error| panic!("{body}: {error}"))
}
fn run(spec: &DsrvSpecification, rows: &[&[(&str, Value)]]) -> Vec<BTreeMap<String, Value>> {
    let mut agreed: Option<Vec<BTreeMap<String, Value>>> = None;
    for options in BOTH {
        let elaborated = spec
            .clone()
            .check_and_elaborate(options)
            .without_warnings()
            .unwrap_or_else(|errors| panic!("{:?}: {errors:?}\n{spec}", options.mode));
        let monitors = [
            DataflowMonitor::compile_checked(elaborated.clone()).unwrap(),
            DataflowMonitor::compile_with_semantics(elaborated, crate::core::Semantics::Untimed)
                .unwrap(),
        ];
        for mut monitor in monitors {
            let inputs = monitor.input_vars().to_vec();
            let outputs = monitor.output_vars().to_vec();
            let results = rows
                .iter()
                .map(|row| {
                    let input: Vec<Value> = inputs
                        .iter()
                        .map(|name| {
                            row.iter()
                                .find(|(written, _)| VarName::from(*written) == *name)
                                .map_or(Value::NoVal, |(_, value)| value.clone())
                        })
                        .collect();
                    let mut output = vec![Value::NoVal; outputs.len()];
                    monitor.evaluate(&input, &mut output).unwrap();
                    outputs
                        .iter()
                        .map(|name| name.name())
                        .zip(output)
                        .collect::<BTreeMap<_, _>>()
                })
                .collect::<Vec<_>>();
            match &agreed {
                Some(agreed) => assert_eq!(&results, agreed, "{:?}", options.mode),
                None => agreed = Some(results),
            }
        }
    }
    agreed.expect("at least one monitor")
}

/// One output over every row.
fn column(results: &[BTreeMap<String, Value>], name: &str) -> Vec<Value> {
    results.iter().map(|row| row[name].clone()).collect()
}

#[test]
fn hold_and_count_match_direct_recurrences_including_reset_priority() {
    let spec = program(
        r#"
use std::stream
in updating: Bool
in reset: Bool
in x: Int
aux held = std::stream::hold_when(7, updating, x)
aux counted = std::stream::count_when(reset, updating)
aux direct_previous: Int = default(direct[1],7)
aux direct: Int = if updating then x else direct_previous
aux count_previous: Int = default(count[1],0)
aux count: Int = if reset then 0 else if updating then count_previous + 1 else count_previous
out same: Bool = held == direct && counted == count
out h: Int = held
out c: Int = counted
"#,
    );
    let rows: Vec<Vec<(&str, Value)>> = [
        (false, false, 2),
        (true, false, 4),
        (false, false, 8),
        (true, true, 9),
        (true, false, 3),
    ]
    .into_iter()
    .map(|(updating, reset, x)| {
        vec![
            ("updating", Value::Bool(updating)),
            ("reset", Value::Bool(reset)),
            ("x", Value::Int(x)),
        ]
    })
    .collect();
    let refs: Vec<_> = rows.iter().map(Vec::as_slice).collect();
    let result = run(&spec, &refs);
    assert_eq!(column(&result, "same"), vec![Value::Bool(true); 5]);
    assert_eq!(column(&result, "h"), [7, 4, 4, 9, 3].map(Value::Int));
    assert_eq!(column(&result, "c"), [0, 1, 1, 0, 1].map(Value::Int));
}

#[test]
fn recent_preserves_occurrences_before_after_and_reset_priority() {
    let spec = program(
        r#"
use std::stream
use std::option::{Option}
in present: Bool
in reset: Bool
in x: Int
aux sample: Option<Int> = if present then Some(x) else None
aux history = std::stream::recent::<3>(sample, reset)
aux zero = std::stream::recent::<0>(sample, reset)
aux negative = std::stream::recent::<-2>(sample, reset)
out before: List<Int> = history.before
out after: List<Int> = history.after
out empty: Bool = List.len(zero.after) == 0 && List.len(negative.after) == 0
"#,
    );
    let rows: Vec<Vec<(&str, Value)>> = [
        (true, false, 1),
        (true, false, 1),
        (false, false, 9),
        (true, false, 2),
        (true, false, 3),
        (true, true, 4),
        (false, false, 0),
    ]
    .into_iter()
    .map(|(present, reset, x)| {
        vec![
            ("present", Value::Bool(present)),
            ("reset", Value::Bool(reset)),
            ("x", Value::Int(x)),
        ]
    })
    .collect();
    let refs: Vec<_> = rows.iter().map(Vec::as_slice).collect();
    let result = run(&spec, &refs);
    let list = |xs: &[i64]| Value::List(xs.iter().copied().map(Value::Int).collect());
    let after = vec![
        list(&[1]),
        list(&[1, 1]),
        list(&[1, 1]),
        list(&[2, 1, 1]),
        list(&[3, 2, 1]),
        list(&[]),
        list(&[]),
    ];
    let mut before = vec![list(&[])];
    before.extend(after[..6].iter().cloned());
    assert_eq!(column(&result, "after"), after);
    assert_eq!(column(&result, "before"), before);
    assert_eq!(column(&result, "empty"), vec![Value::Bool(true); 7]);
}

#[test]
fn boolean_overdue_is_one_shot_at_a_strict_boundary() {
    let spec = program(
        r#"
use std::option::{Option}
use std::time::{Instant, Duration}
use std::timing
newtype Domain = Unit
in start: Bool
in cancel: Bool
in finish: Bool
in seconds: Float
aux key: Option<Int> = if start then Some(1) else None
aux withdrawn: Option<Int> = if cancel then Some(1) else None
aux response: Option<Int> = if finish then Some(1) else None
aux now: Option<Instant<Domain>> = Some(std::time::instant(seconds))
aux event: Option<Instant<Domain>> = if start || cancel || finish then now else None
aux rich = std::timing::bounded_response(key,response,withdrawn,now,event,std::time::seconds(2.0))
aux breach = std::timing::response_overdue(key,response,withdrawn,now,event,std::time::seconds(2.0))
out same: Bool = breach == (List.len(rich.overdue)>0)
out late: Bool = breach
"#,
    );
    let rows: Vec<Vec<(&str, Value)>> = [
        (true, false, false, 0.0),
        (true, false, false, 1.0),
        (false, false, false, 2.0),
        (false, false, false, 2.1),
        (false, false, false, 3.0),
        (false, false, true, 4.0),
        (true, false, false, 5.0),
        (false, true, false, 6.0),
        (false, false, false, 9.0),
    ]
    .into_iter()
    .map(|(start, cancel, finish, seconds)| {
        vec![
            ("start", Value::Bool(start)),
            ("cancel", Value::Bool(cancel)),
            ("finish", Value::Bool(finish)),
            ("seconds", Value::Float(seconds)),
        ]
    })
    .collect();
    let refs: Vec<_> = rows.iter().map(Vec::as_slice).collect();
    let result = run(&spec, &refs);
    assert_eq!(column(&result, "same"), vec![Value::Bool(true); 9]);
    assert_eq!(
        column(&result, "late"),
        [false, false, false, true, false, false, false, false, false].map(Value::Bool)
    );
}

#[test]
fn maplek_queries_distinguish_transition_evidence_from_accepted_state() {
    let spec = program(
        r#"
use rpio::journal::view::maplek::{self, Episode, EpisodeStep}
use rpio::journal::view::{Resolved}
use std::option::{Option}
use std::time::{Instant}
newtype Domain = Unit
aux at: Instant<Domain> = std::time::instant(1.0)
aux evidence: Resolved<Int> = {ref:{store_epoch:"epoch",global_write_sequence:2,key:"plan"},value:42,written_by:Node({run_id:"fixture-run",name:"writer"})}
aux validation: Resolved<Int> = {ref:{store_epoch:"epoch",global_write_sequence:3,key:"validation"},value:17,written_by:Node({run_id:"fixture-run",name:"validator"})}
aux active: Episode<Int,Domain> = Active({analysis_activation:{epoch:"epoch",start_seq:"1"},opened:at,since:at,last_plan_rejection:None,plan:Some({activation:{epoch:"epoch",start_seq:"2"},plan:evidence,accepted:None})})
aux accepted: Episode<Int,Domain> = Active({analysis_activation:{epoch:"epoch",start_seq:"1"},opened:at,since:at,last_plan_rejection:None,plan:Some({activation:{epoch:"epoch",start_seq:"2"},plan:evidence,accepted:Some({legitimate_activation:{epoch:"epoch",start_seq:"3"},legitimate_result:validation})})})
aux generated: EpisodeStep<Int,Domain> = Moved({from:EpisodeIdle,by:Generated({plan_activation:{epoch:"epoch",start_seq:"2"},plan:evidence}),to:active})
aux validated: EpisodeStep<Int,Domain> = Moved({from:active,by:Accepted({legitimate_activation:{epoch:"epoch",start_seq:"3"},legitimate_result:validation}),to:accepted})
aux rejected: EpisodeStep<Int,Domain> = Moved({from:active,by:Rejected({legitimate_activation:{epoch:"epoch",start_seq:"3"},legitimate_result:validation}),to:EpisodeIdle})
aux applied: EpisodeStep<Int,Domain> = Closed({from:accepted,how:Applied({effect:evidence}),at})
aux idle: EpisodeStep<Int,Domain> = Stayed
aux idle_episode: Episode<Int,Domain> = EpisodeIdle
out correct: Bool = rpio::journal::view::maplek::generated_plan(generated)==Some(evidence)
 && rpio::journal::view::maplek::validation_result(validated)==Some(validation)
 && rpio::journal::view::maplek::validation_result(rejected)==Some(validation)
 && rpio::journal::view::maplek::applied_effect(applied)==Some(evidence)
 && rpio::journal::view::maplek::pending_plan(active)==Some(evidence)
 && rpio::journal::view::maplek::pending_plan(accepted)==None
 && rpio::journal::view::maplek::accepted_plan(active)==None
 && rpio::journal::view::maplek::accepted_plan(accepted)==Some(evidence)
 && rpio::journal::view::maplek::accepted_validation(accepted)==Some(validation)
 && rpio::journal::view::maplek::generated_plan(idle)==None
 && rpio::journal::view::maplek::validation_result(generated)==None
 && rpio::journal::view::maplek::applied_effect(validated)==None
 && rpio::journal::view::maplek::pending_plan(idle_episode)==None
"#,
    );
    assert_eq!(
        column(&run(&spec, &[&[]]), "correct"),
        vec![Value::Bool(true)]
    );
}

#[test]
fn knowledge_latest_retains_writer_version_and_selected_value() {
    let spec = program(
        r#"
use rpio::journal::view::{self, Record, Resolved, Activation}
use rpio::journal::view::timing::{Observation}
use rpio::journal::view::knowledge::{Access}
use std::option::{Option}
use std::time
newtype Domain = Unit
type K = Union<Chosen:Int, Other:Int>
type E = Unit
type M = Unit
in tick: Int
aux activation: Activation = {epoch:"run",start_seq:"1",producer:"Plan",role:Plan,loop:["l"],caused_by:None}
aux epoch: Str = if tick == 4 then "new" else "old"
aux seq: Int = match(tick){0->2,1->1,2->2,_->0}
aux evidence: Resolved<K> = {ref:{store_epoch:epoch,global_write_sequence:seq,key:"chosen"},value:if tick==6 then Other(99) else Chosen(tick+10),written_by:Node({run_id:"fixture-run",name:epoch})}
// Tick 7 reads nothing: an absent Read names no version.
aux result: Option<Resolved<K>> = if tick==7 then None else Some(evidence)
aux slots: List<view::Slot<K>> = match(result){Some(r)->[{name:"value",value:r}],_->List.filter(\s->false,[{name:"value",value:evidence}])}
aux record: Option<Record<K,E,M>> = if tick==3 || tick==5 then None else Some(Phase({position:{epoch:"run",seq:tick+2},arrival:{journal_time:0.0,journal_clock:"c"},activation,step:Read({key:"chosen",value:result}),evidence:slots}))
aux observation: Observation<K,E,M,Domain> = {record:match(record){Some(r)->Some({time:std::time::instant(0.0),value:r}),None->None},clock:None}
def choose(k:K)->Option<Int> = match(k){Chosen(n)->Some(n),_->None}
aux state = rpio::journal::view::knowledge::latest::<choose>(observation)
aux access = rpio::journal::view::knowledge::last_access::<view::Role::Plan,Access::Read,choose>(observation)
out value: Int = match(rpio::journal::view::knowledge::value(state.latest)){Some(n)->n,_->-1}
out sequence: Int = match(rpio::journal::view::knowledge::version(state.latest)){Some(v)->v.global_write_sequence,_->-1}
out writer: Str = match(state.latest){Some({written_by:Node({name,..}),..})->name,_->"none"}
out access_writer: Str = match(access){Some({written_by:Node({name,..}),..})->name,_->"none"}
out updated: Bool = state.updated
out access_value: Int = match(rpio::journal::view::knowledge::value(access)){Some(n)->n,_->-1}
aux nothing: Option<Resolved<Int>> = None
out absent: Bool = rpio::journal::view::knowledge::value(nothing)==None && rpio::journal::view::knowledge::version(nothing)==None
"#,
    );
    let rows: Vec<_> = (0..8).map(|n| vec![("tick", Value::Int(n))]).collect();
    let refs: Vec<_> = rows.iter().map(Vec::as_slice).collect();
    let result = run(&spec, &refs);
    assert_eq!(
        column(&result, "value"),
        [10, 10, 10, 10, 14, 14, 14, 14].map(Value::Int)
    );
    assert_eq!(
        column(&result, "sequence"),
        [2, 2, 2, 2, 0, 0, 0, 0].map(Value::Int)
    );
    assert_eq!(
        column(&result, "updated"),
        [true, false, false, false, true, false, false, false].map(Value::Bool)
    );
    assert_eq!(
        column(&result, "writer"),
        ["old", "old", "old", "old", "new", "new", "new", "new"].map(|s| Value::Str(s.into()))
    );
    assert_eq!(
        column(&result, "access_value"),
        [10, 11, 12, 12, 14, 14, 14, 14].map(Value::Int)
    );
    assert_eq!(
        column(&result, "access_writer"),
        ["old", "old", "old", "old", "new", "new", "new", "new"].map(|s| Value::Str(s.into()))
    );
    assert_eq!(column(&result, "absent"), vec![Value::Bool(true); 8]);
}

#[test]
fn record_phase_and_held_clock_times_are_distinct() {
    let spec = program(
        r#"
use rpio::journal::view::{self, Record, Activation, Resolved}
use rpio::journal::view::timing::{self, Observation}
use std::option::{Option}
use std::time::{Instant}
newtype Domain = Unit
in tick: Int
aux activation: Activation = {epoch:"e",start_seq:"2",producer:"Plan",role:Plan,loop:["l"],caused_by:None}
aux evidence: Resolved<Int> = {ref:{store_epoch:"e",global_write_sequence:1,key:"k"},value:1,written_by:Node({run_id:"fixture-run",name:"writer"})}
aux none: List<view::Slot<Int>> = List.filter(\s->false,[{name:"value",value:evidence}])
def at(seq: Int, time: Float) -> view::Position = {epoch:"e",seq}
def arrived(time: Float) -> view::Arrival = {journal_time:time,journal_clock:"c"}
aux record: Option<Record<Int,Unit,Unit>> = match(tick){
  0->Some(Phase({position:at(3,8.0),arrival:arrived(8.0),activation,step:Read({key:"k",value:Some(evidence)}),evidence:[{name:"value",value:evidence}]})),
  1->Some(Knowledge({position:at(4,9.0),arrival:arrived(9.0),version:evidence})),
  2->Some(Knowledge({position:at(5,10.0),arrival:arrived(10.0),version:evidence})),
  4->Some(Phase({position:at(6,7.0),arrival:arrived(7.0),activation,step:Read({key:"k",value:None}),evidence:none})),
  _->None,
}
aux observation: Observation<Int,Unit,Unit,Domain> = {
 record:match(record){Some(r)->Some({time:std::time::instant(rpio::journal::view::arrival(r).journal_time),value:r}),None->None},
 clock:if tick==3 then Some({clock:"test",time:std::time::instant(12.0),origin:"Replay"}) else None
}
aux now = rpio::journal::view::timing::now(observation)
aux summary = rpio::journal::view::timing::tick(observation)
aux expected_record: Option<Instant<Domain>> = match(tick){0->Some(std::time::instant(8.0)),1->Some(std::time::instant(9.0)),2->Some(std::time::instant(10.0)),4->Some(std::time::instant(7.0)),_->None}
aux expected_phase: Option<Instant<Domain>> = match(tick){0->Some(std::time::instant(8.0)),4->Some(std::time::instant(7.0)),_->None}
// Held "now" is the latest instant of every delivered record, Knowledge
// included, and every released clock.
aux expected_now: Option<Instant<Domain>> = match(tick){0->Some(std::time::instant(8.0)),1->Some(std::time::instant(9.0)),2->Some(std::time::instant(10.0)),3->Some(std::time::instant(12.0)),4->Some(std::time::instant(12.0)),_->None}
aux expected_previous: Option<Instant<Domain>> = if tick==0 then None else Some(std::time::instant(8.0))
out correct: Bool = rpio::journal::view::timing::record_time(observation)==expected_record && rpio::journal::view::timing::phase_time(observation)==expected_phase && now==expected_now && summary.previous_phase_time==expected_previous
out same: Bool = summary.record_time == rpio::journal::view::timing::record_time(observation)
"#,
    );
    let rows: Vec<_> = (0..5).map(|n| vec![("tick", Value::Int(n))]).collect();
    let refs: Vec<_> = rows.iter().map(Vec::as_slice).collect();
    let result = run(&spec, &refs);
    assert_eq!(column(&result, "correct"), vec![Value::Bool(true); 5]);
    assert_eq!(column(&result, "same"), vec![Value::Bool(true); 5]);
}

#[test]
fn missing_inputs_match_direct_ordinary_recurrences() {
    let helper = program(
        r#"
use std::stream
in updating: Bool
in reset: Bool
in x: Int
out h = std::stream::hold_when(7,updating,x)
out c = std::stream::count_when(reset,updating)
"#,
    );
    let direct = program(
        r#"
in updating: Bool
in reset: Bool
in x: Int
aux previous_h: Int = default(h[1],7)
out h: Int = if updating then x else previous_h
aux previous_c: Int = default(c[1],0)
out c: Int = if reset then 0 else if updating then previous_c+1 else previous_c
"#,
    );
    let rows: &[&[(&str, Value)]] = &[
        &[
            ("updating", Value::Bool(true)),
            ("reset", Value::Bool(false)),
            ("x", Value::Int(2)),
        ],
        &[],
        &[
            ("updating", Value::Bool(false)),
            ("reset", Value::Bool(false)),
        ],
        &[
            ("updating", Value::Bool(true)),
            ("reset", Value::Bool(false)),
            ("x", Value::Int(4)),
        ],
    ];
    assert_eq!(run(&helper, rows), run(&direct, rows));
}

#[test]
fn capacity_one_recent_matches_flat_recurrence_on_missing_ticks() {
    let helper = program(
        r#"
use std::stream
use std::option::{Option}
in present: Bool
in reset: Bool
in x: Int
aux sample: Option<Int> = if present then Some(x) else None
aux history = std::stream::recent::<1>(sample,reset)
out before: List<Int> = history.before
out after: List<Int> = history.after
"#,
    );
    let flat = program(
        r#"
use std::option::{Option}
in present: Bool
in reset: Bool
in x: Int
aux sample: Option<Int> = if present then Some(x) else None
out before: List<Int> = default(after[1],[])
out after: List<Int> = if reset then [] else match(sample){Some(n)->[n],None->before}
"#,
    );
    let rows: &[&[(&str, Value)]] = &[
        &[
            ("present", Value::Bool(true)),
            ("reset", Value::Bool(false)),
            ("x", Value::Int(2)),
        ],
        &[],
        &[
            ("present", Value::Bool(false)),
            ("reset", Value::Bool(false)),
            ("x", Value::Int(9)),
        ],
        &[
            ("present", Value::Bool(true)),
            ("reset", Value::Bool(false)),
            ("x", Value::Int(2)),
        ],
        &[
            ("present", Value::Bool(true)),
            ("reset", Value::Bool(true)),
            ("x", Value::Int(3)),
        ],
        &[
            ("present", Value::Bool(false)),
            ("reset", Value::Bool(false)),
        ],
    ];
    assert_eq!(run(&helper, rows), run(&flat, rows));
}
