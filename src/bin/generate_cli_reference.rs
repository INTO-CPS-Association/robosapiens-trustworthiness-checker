use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    fmt::Write as FmtWrite,
    fs,
    path::PathBuf,
};

use clap::{Arg, ArgAction, Command, CommandFactory};
use trustworthiness_checker::cli::args::{CLI_ARGUMENT_REQUIREMENTS, Cli};

const DEFAULT_OUTPUT: &str = "docs/src/reference/cli.md";
const SOURCE_COMMAND: &str =
    "cargo run --quiet --bin generate_cli_reference -- --output docs/src/reference/cli.md";

const GROUPS: &[&str] = &[
    "Model, language, and semantics",
    "Finite file processing",
    "Live input sources",
    "Output destinations and routing",
    "Runtime and execution policy",
    "Input windows and tick composition",
    "Reconfiguration",
    "Distributed monitoring and scheduling",
    "MQTT, Redis, and ROS transport settings",
    "Logging and process operation",
];

// Keep this list keyed by the stable Clap argument ID. There is deliberately no
// fallback group: adding a public argument requires an intentional documentation
// decision here, and removing an argument requires removing its classification.
const ARGUMENT_CLASSIFICATIONS: &[(&str, &str, Option<&str>)] = &[
    ("model", "Model, language, and semantics", None),
    ("language", "Model, language, and semantics", None),
    ("semantics", "Model, language, and semantics", None),
    ("input_file", "Finite file processing", None),
    ("input_mqtt_file", "Live input sources", None),
    ("mqtt_input", "Live input sources", None),
    (
        "input_redis_file",
        "Live input sources",
        Some("Requires the Cargo feature `redis`."),
    ),
    (
        "redis_input",
        "Live input sources",
        Some("Requires the Cargo feature `redis`."),
    ),
    (
        "redis_knowledge_input",
        "Live input sources",
        Some("Requires the Cargo feature `redis`."),
    ),
    (
        "input_ros_file",
        "Live input sources",
        Some("Requires the Cargo feature `ros`."),
    ),
    ("input_config", "Live input sources", None),
    ("output_stdout", "Output destinations and routing", None),
    (
        "mqtt_output",
        "Output destinations and routing",
        Some("Requires the Cargo feature `mqtt`."),
    ),
    (
        "output_mqtt_file",
        "Output destinations and routing",
        Some("Requires the Cargo feature `mqtt`."),
    ),
    (
        "redis_output",
        "Output destinations and routing",
        Some("Requires the Cargo feature `redis`."),
    ),
    (
        "output_redis_file",
        "Output destinations and routing",
        Some("Requires the Cargo feature `redis`."),
    ),
    (
        "output_ros_file",
        "Output destinations and routing",
        Some("Requires the Cargo feature `ros`."),
    ),
    ("output_config", "Output destinations and routing", None),
    ("runtime", "Runtime and execution policy", None),
    ("execution_policy", "Runtime and execution policy", None),
    ("mstlo_algorithm", "Runtime and execution policy", None),
    (
        "mstlo_synchronization",
        "Runtime and execution policy",
        None,
    ),
    ("mstlo_vars", "Runtime and execution policy", None),
    (
        "input_window_ms",
        "Input windows and tick composition",
        None,
    ),
    (
        "input_window_mode",
        "Input windows and tick composition",
        None,
    ),
    (
        "input_window_update_limit",
        "Input windows and tick composition",
        None,
    ),
    ("reconf_topic", "Reconfiguration", None),
    ("no_context_transfer", "Reconfiguration", None),
    ("centralised", "Distributed monitoring and scheduling", None),
    (
        "distribution_graph",
        "Distributed monitoring and scheduling",
        None,
    ),
    (
        "local_topics",
        "Distributed monitoring and scheduling",
        None,
    ),
    (
        "mqtt_centralised_distributed",
        "Distributed monitoring and scheduling",
        Some("Requires the Cargo feature `mqtt`."),
    ),
    (
        "mqtt_randomized_distributed",
        "Distributed monitoring and scheduling",
        Some("Requires the Cargo feature `mqtt`."),
    ),
    (
        "mqtt_static_optimized",
        "Distributed monitoring and scheduling",
        Some("Requires the Cargo feature `mqtt`; optimized modes also need constraints."),
    ),
    (
        "mqtt_dynamic_optimized",
        "Distributed monitoring and scheduling",
        Some("Requires the Cargo feature `mqtt`; optimized modes also need constraints."),
    ),
    (
        "ros_centralised_distributed",
        "Distributed monitoring and scheduling",
        Some("Requires the Cargo feature `ros`."),
    ),
    (
        "ros_randomized_distributed",
        "Distributed monitoring and scheduling",
        Some("Requires the Cargo feature `ros`."),
    ),
    (
        "ros_static_optimized",
        "Distributed monitoring and scheduling",
        Some("Requires the Cargo feature `ros`; optimized modes also need constraints."),
    ),
    (
        "ros_dynamic_optimized",
        "Distributed monitoring and scheduling",
        Some("Requires the Cargo feature `ros`; optimized modes also need constraints."),
    ),
    (
        "distributed_work",
        "Distributed monitoring and scheduling",
        None,
    ),
    ("local_node", "Distributed monitoring and scheduling", None),
    (
        "scheduling_mode",
        "Distributed monitoring and scheduling",
        Some("The `ros` value requires the Cargo feature `ros`."),
    ),
    (
        "distribution_constraints",
        "Distributed monitoring and scheduling",
        None,
    ),
    (
        "dist_constraint_solver",
        "Distributed monitoring and scheduling",
        Some("The `sat` value requires the Cargo feature `sat`."),
    ),
    (
        "scheduler_ros_node_name",
        "Distributed monitoring and scheduling",
        Some("Used by ROS scheduling; ROS operation requires the Cargo feature `ros`."),
    ),
    (
        "scheduler_reconf_topic",
        "Distributed monitoring and scheduling",
        Some("Used by ROS scheduling; ROS operation requires the Cargo feature `ros`."),
    ),
    ("mqtt_port", "MQTT, Redis, and ROS transport settings", None),
    (
        "mqtt_paho",
        "MQTT, Redis, and ROS transport settings",
        Some("Requires the Cargo feature `mqtt`."),
    ),
    (
        "mqtt_rumqttc",
        "MQTT, Redis, and ROS transport settings",
        None,
    ),
    (
        "redis_port",
        "MQTT, Redis, and ROS transport settings",
        None,
    ),
    (
        "redis_knowledge_keys",
        "MQTT, Redis, and ROS transport settings",
        Some("Requires the Cargo feature `redis`."),
    ),
    (
        "redis_knowledge_database",
        "MQTT, Redis, and ROS transport settings",
        Some("Requires the Cargo feature `redis`."),
    ),
    (
        "redis_knowledge_publish_initial",
        "MQTT, Redis, and ROS transport settings",
        Some("Requires the Cargo feature `redis`."),
    ),
    (
        "redis_knowledge_no_initial",
        "MQTT, Redis, and ROS transport settings",
        Some("Requires the Cargo feature `redis`."),
    ),
    (
        "redis_knowledge_retry_max_attempts",
        "MQTT, Redis, and ROS transport settings",
        Some("Requires the Cargo feature `redis`."),
    ),
    (
        "redis_knowledge_retry_forever",
        "MQTT, Redis, and ROS transport settings",
        Some("Requires the Cargo feature `redis`."),
    ),
    (
        "redis_knowledge_retry_initial_delay_ms",
        "MQTT, Redis, and ROS transport settings",
        Some("Requires the Cargo feature `redis`."),
    ),
    (
        "redis_knowledge_retry_max_delay_ms",
        "MQTT, Redis, and ROS transport settings",
        Some("Requires the Cargo feature `redis`."),
    ),
    (
        "redis_knowledge_source",
        "MQTT, Redis, and ROS transport settings",
        Some("Requires the Cargo feature `redis`."),
    ),
    (
        "ros_dist_graph_topic",
        "MQTT, Redis, and ROS transport settings",
        Some("Used by ROS distribution graph providers; requires the Cargo feature `ros`."),
    ),
    ("log_file", "Logging and process operation", None),
    ("help", "Logging and process operation", None),
];

#[derive(Clone, Copy)]
struct Classification {
    group_index: usize,
    group: &'static str,
    feature_note: Option<&'static str>,
}

struct Options {
    output: PathBuf,
    check: bool,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("generate_cli_reference: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let options = parse_options()?;
    let mut command = Cli::command();
    command.build();
    let classifications = validate_classifications(&command)?;
    let requirements = validate_requirements(&command)?;
    let generated = render_reference(&command, &classifications, &requirements);

    if options.check {
        let current = fs::read_to_string(&options.output).map_err(|error| {
            format!(
                "could not read {} for stale-output check: {error}",
                options.output.display()
            )
        })?;
        if current != generated {
            return Err(format!(
                "{} is stale; run `{SOURCE_COMMAND}`",
                options.output.display()
            ));
        }
        println!("CLI reference is current: {}", options.output.display());
        return Ok(());
    }

    if let Some(parent) = options.output.parent() {
        fs::create_dir_all(parent).map_err(|error| {
            format!(
                "could not create output directory {}: {error}",
                parent.display()
            )
        })?;
    }
    fs::write(&options.output, generated)
        .map_err(|error| format!("could not write {}: {error}", options.output.display()))?;
    println!("Wrote CLI reference: {}", options.output.display());
    Ok(())
}

fn parse_options() -> Result<Options, String> {
    let mut output = PathBuf::from(DEFAULT_OUTPUT);
    let mut check = false;
    let mut arguments = env::args_os();
    arguments.next();

    while let Some(argument) = arguments.next() {
        if argument == "--check" {
            check = true;
        } else if argument == "--output" {
            let value = arguments
                .next()
                .ok_or_else(|| "--output requires a path".to_owned())?;
            output = PathBuf::from(value);
        } else if let Some(value) = argument.to_string_lossy().strip_prefix("--output=") {
            if value.is_empty() {
                return Err("--output requires a path".to_owned());
            }
            output = PathBuf::from(value);
        } else if argument == "--help" || argument == "-h" {
            println!(
                "Usage: generate_cli_reference [--check] [--output PATH]\n\n\
                 Write the generated CLI reference, or check a checked-in file for staleness.\n\
                 Default output: {DEFAULT_OUTPUT}"
            );
            std::process::exit(0);
        } else {
            return Err(format!("unknown argument `{}`", argument.to_string_lossy()));
        }
    }

    Ok(Options { output, check })
}

fn validate_classifications(command: &Command) -> Result<BTreeMap<String, Classification>, String> {
    let group_indexes = GROUPS
        .iter()
        .enumerate()
        .map(|(index, group)| (*group, index))
        .collect::<BTreeMap<_, _>>();
    let mut classifications = BTreeMap::new();
    let mut duplicate_ids = BTreeSet::new();
    let mut unknown_groups = BTreeSet::new();

    for &(id, group, feature_note) in ARGUMENT_CLASSIFICATIONS {
        if !group_indexes.contains_key(group) {
            unknown_groups.insert(group);
        }
        let classification = Classification {
            group_index: group_indexes.get(group).copied().unwrap_or(usize::MAX),
            group,
            feature_note,
        };
        if classifications
            .insert(id.to_owned(), classification)
            .is_some()
        {
            duplicate_ids.insert(id);
        }
    }

    let command_ids = command
        .get_arguments()
        .map(|argument| argument.get_id().to_string())
        .collect::<BTreeSet<_>>();
    let classification_ids = classifications.keys().cloned().collect::<BTreeSet<_>>();
    let removed_ids = classification_ids
        .difference(&command_ids)
        .cloned()
        .collect::<Vec<_>>();
    let unclassified_ids = command_ids
        .difference(&classification_ids)
        .cloned()
        .collect::<Vec<_>>();

    if duplicate_ids.is_empty()
        && unknown_groups.is_empty()
        && removed_ids.is_empty()
        && unclassified_ids.is_empty()
    {
        return Ok(classifications);
    }

    let mut errors = Vec::new();
    if !duplicate_ids.is_empty() {
        errors.push(format!(
            "duplicate classification IDs: {}",
            join_strings(duplicate_ids.into_iter().map(str::to_owned))
        ));
    }
    if !unknown_groups.is_empty() {
        errors.push(format!(
            "classification uses unknown groups: {}",
            join_strings(unknown_groups.into_iter().map(str::to_owned))
        ));
    }
    if !removed_ids.is_empty() {
        errors.push(format!(
            "classification IDs refer to removed arguments: {}",
            removed_ids.join(", ")
        ));
    }
    if !unclassified_ids.is_empty() {
        errors.push(format!(
            "public arguments are unclassified: {}",
            unclassified_ids.join(", ")
        ));
    }
    Err(errors.join("; "))
}

fn validate_requirements(command: &Command) -> Result<BTreeMap<String, Vec<String>>, String> {
    let command_ids = command
        .get_arguments()
        .map(|argument| argument.get_id().to_string())
        .collect::<BTreeSet<_>>();
    let mut requirements = BTreeMap::<String, Vec<String>>::new();
    let mut pairs = BTreeSet::new();
    let mut duplicate_pairs = BTreeSet::new();
    let mut unknown_ids = BTreeSet::new();

    for &(dependent, required) in CLI_ARGUMENT_REQUIREMENTS {
        if !command_ids.contains(dependent) {
            unknown_ids.insert(dependent);
        }
        if !command_ids.contains(required) {
            unknown_ids.insert(required);
        }
        if !pairs.insert((dependent, required)) {
            duplicate_pairs.insert((dependent, required));
        }
        requirements
            .entry(dependent.to_owned())
            .or_default()
            .push(required.to_owned());
    }

    if !unknown_ids.is_empty() {
        return Err(format!(
            "requirement metadata refers to removed arguments: {}",
            join_strings(unknown_ids.into_iter().map(str::to_owned))
        ));
    }
    if !duplicate_pairs.is_empty() {
        return Err(format!(
            "duplicate requirement metadata: {}",
            join_strings(
                duplicate_pairs
                    .into_iter()
                    .map(|(dependent, required)| format!("{dependent} -> {required}"))
            )
        ));
    }

    for required in requirements.values_mut() {
        required.sort();
        required.dedup();
    }
    Ok(requirements)
}

fn render_reference(
    command: &Command,
    classifications: &BTreeMap<String, Classification>,
    requirements: &BTreeMap<String, Vec<String>>,
) -> String {
    let mut output = String::new();
    output.push_str(
        "<!-- @generated by src/bin/generate_cli_reference.rs; do not edit by hand. -->\n",
    );
    output.push_str("<!-- Source command: ");
    output.push_str(SOURCE_COMMAND);
    output.push_str(" -->\n\n");
    output.push_str("# CLI argument reference (generated)\n\n");
    output.push_str(
        "This fragment is generated from `trustworthiness_checker::cli::args::Cli::command()`; ",
    );
    output.push_str("the hand-written semantic notes are in [CLI notes](cli-notes.md) and are included below.\n\n");

    output.push_str("## Clap argument groups\n\n");
    output.push_str(
        "These groups are the parser-level requirements and mutual-exclusion boundaries exposed by Clap. "
    );
    output.push_str(
        "Additional cross-option validation is described in the surrounding reference pages.\n\n",
    );
    output.push_str("| Group ID | Members | Required | Multiple |\n");
    output.push_str("| --- | --- | --- | --- |\n");
    let mut groups = command
        .get_groups()
        .map(|group| {
            let mut group = group.clone();
            let mut members = group
                .get_args()
                .map(ToString::to_string)
                .collect::<Vec<_>>();
            members.sort();
            (
                group.get_id().to_string(),
                members,
                group.is_required_set(),
                group.is_multiple(),
            )
        })
        .collect::<Vec<_>>();
    groups.sort_by(|left, right| left.0.cmp(&right.0));
    for (id, members, required, multiple) in groups {
        let _ = writeln!(
            output,
            "| `{}` | {} | {} | {} |",
            markdown_cell(&id),
            markdown_cell(
                &members
                    .into_iter()
                    .map(|member| format!("`{member}`"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            if required { "yes" } else { "no" },
            if multiple { "yes" } else { "no" },
        );
    }
    output.push('\n');

    let mut arguments = command
        .get_arguments()
        .map(|argument| {
            let id = argument.get_id().to_string();
            let classification = classifications
                .get(&id)
                .expect("classification validation should cover every argument");
            (argument, id, *classification)
        })
        .collect::<Vec<_>>();
    arguments.sort_by(|left, right| {
        left.2
            .group_index
            .cmp(&right.2.group_index)
            .then_with(|| left.1.cmp(&right.1))
            .then_with(|| argument_spelling(left.0).cmp(&argument_spelling(right.0)))
    });

    let mut current_group = None;
    for (argument, id, classification) in arguments {
        if current_group != Some(classification.group_index) {
            if current_group.is_some() {
                output.push('\n');
            }
            current_group = Some(classification.group_index);
            let _ = writeln!(
                output,
                "## {}. {}\n\n",
                classification.group_index + 1,
                classification.group
            );
            output.push_str(
                "| Argument ID | Spelling | Value names | Multiplicity | Required | Requires | Possible values | Clap default | Conflicts | Help | Feature note |\n",
            );
            output
                .push_str("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |\n");
        }

        let required_arguments = requirements
            .get(&id)
            .map(|required| {
                required
                    .iter()
                    .map(|required| format!("`{required}`"))
                    .collect::<Vec<_>>()
                    .join(", ")
            })
            .unwrap_or_else(|| "—".to_owned());
        let possible_values = possible_values(argument);
        let possible_values = if possible_values.is_empty() {
            "—".to_owned()
        } else {
            possible_values
                .into_iter()
                .map(|(name, help)| match help {
                    Some(help) => format!("`{name}` — {help}"),
                    None => format!("`{name}`"),
                })
                .collect::<Vec<_>>()
                .join("<br>")
        };
        let defaults = argument
            .get_default_values()
            .iter()
            .map(|value| value.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        let defaults = if defaults.is_empty() {
            "—".to_owned()
        } else {
            defaults.join(", ")
        };
        let conflicts = argument_conflicts(command, argument);
        let conflicts = if conflicts.is_empty() {
            "—".to_owned()
        } else {
            conflicts
                .into_iter()
                .map(|conflict| format!("`{conflict}`"))
                .collect::<Vec<_>>()
                .join(", ")
        };
        let help = argument
            .get_help()
            .map(ToString::to_string)
            .unwrap_or_else(|| "—".to_owned());
        let feature_note = classification.feature_note.unwrap_or("—");

        let _ = writeln!(
            output,
            "| `{}` | `{}` | {} | {} | {} | {} | {} | {} | {} | {} | {} |",
            markdown_cell(&id),
            markdown_cell(&argument_spelling(argument)),
            markdown_cell(&value_names(argument)),
            markdown_cell(&multiplicity(argument)),
            if argument.is_required_set() {
                "yes"
            } else {
                "no"
            },
            markdown_cell(&required_arguments),
            markdown_cell(&possible_values),
            markdown_cell(&defaults),
            markdown_cell(&conflicts),
            markdown_cell(&help),
            markdown_cell(feature_note),
        );
    }

    output.push_str("\n## Semantic notes\n\n");
    output.push_str("{{#include cli-notes.md}}\n");
    output
}

fn argument_spelling(argument: &Arg) -> String {
    let names = value_names_vec(argument);
    let value_suffix = if names.is_empty() {
        String::new()
    } else {
        let suffix = argument
            .get_num_args()
            .map(|range| range.max_values() == usize::MAX || range.max_values() > 1)
            .unwrap_or(false);
        format!(
            " {}{}",
            names
                .into_iter()
                .map(|name| format!("<{name}>"))
                .collect::<Vec<_>>()
                .join(" "),
            if suffix { "..." } else { "" }
        )
    };

    if argument.is_positional() {
        let positional_suffix = argument
            .get_num_args()
            .map(|range| range.max_values() == usize::MAX || range.max_values() > 1)
            .unwrap_or(false);
        format!(
            "<{}>{}",
            value_names(argument),
            if positional_suffix { "..." } else { "" }
        )
    } else {
        let option = match (argument.get_short(), argument.get_long()) {
            (Some(short), Some(long)) => format!("-{short}, --{long}"),
            (Some(short), None) => format!("-{short}"),
            (None, Some(long)) => format!("--{long}"),
            (None, None) => argument.get_id().to_string(),
        };
        format!("{option}{value_suffix}")
    }
}

fn value_names(argument: &Arg) -> String {
    value_names_vec(argument).join(", ")
}

fn takes_value(argument: &Arg) -> bool {
    !matches!(
        argument.get_action(),
        ArgAction::SetTrue
            | ArgAction::SetFalse
            | ArgAction::Count
            | ArgAction::Help
            | ArgAction::HelpShort
            | ArgAction::HelpLong
            | ArgAction::Version
    )
}

fn value_names_vec(argument: &Arg) -> Vec<String> {
    if !takes_value(argument) {
        return Vec::new();
    }
    if let Some(names) = argument.get_value_names() {
        return names.iter().map(ToString::to_string).collect();
    }
    vec![argument.get_id().to_string().to_ascii_uppercase()]
}

fn multiplicity(argument: &Arg) -> String {
    if !takes_value(argument) {
        return "flag (no value)".to_owned();
    }
    let Some(range) = argument.get_num_args() else {
        let repeatable = matches!(argument.get_action(), ArgAction::Append);
        return format!("1 value(s){}", if repeatable { "; repeatable" } else { "" });
    };
    let max = if range.max_values() == usize::MAX {
        "unbounded".to_owned()
    } else {
        range.max_values().to_string()
    };
    let range = if range.min_values() == range.max_values() {
        range.min_values().to_string()
    } else {
        format!("{}–{max}", range.min_values())
    };
    let delimiter = argument
        .get_value_delimiter()
        .map(|delimiter| format!("; delimiter `{delimiter}`"))
        .unwrap_or_default();
    let repeatable = matches!(argument.get_action(), ArgAction::Append);
    format!(
        "{range} value(s){}{}",
        if repeatable { "; repeatable" } else { "" },
        delimiter
    )
}

fn possible_values(argument: &Arg) -> Vec<(String, Option<String>)> {
    if !takes_value(argument) {
        return Vec::new();
    }
    let mut values = argument
        .get_possible_values()
        .into_iter()
        .map(|value| {
            (
                value.get_name().to_owned(),
                value.get_help().map(ToString::to_string),
            )
        })
        .collect::<Vec<_>>();
    values.sort_by(|left, right| left.0.cmp(&right.0));
    values
}

fn argument_conflicts(command: &Command, argument: &Arg) -> Vec<String> {
    let mut conflicts = command
        .get_arg_conflicts_with(argument)
        .into_iter()
        .map(|conflict| conflict.get_id().to_string())
        .collect::<Vec<_>>();
    conflicts.sort();
    conflicts.dedup();
    conflicts
}

fn markdown_cell(value: &str) -> String {
    value
        .replace('|', "\\|")
        .replace('\n', " ")
        .replace('\r', " ")
}

fn join_strings<I>(values: I) -> String
where
    I: IntoIterator<Item = String>,
{
    values.into_iter().collect::<Vec<_>>().join(", ")
}
