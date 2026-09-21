pub mod asynchronous;
pub mod builder;
pub mod dataflow;
pub mod distributed;
pub use builder::GeneralRuntimeBuilder;
pub use builder::RuntimeBuilder;
pub mod mstlo;
mod output_utils;
pub mod reconfigurable_semi_sync;
pub mod semi_sync;

/// Turns the text of a live replacement into the specification a
/// reconfigurable runtime runs, or says why it cannot. The application owns
/// it and may capture whatever it needs, such as where to present a
/// replacement's warnings; the runtime sees only the outcome.
pub type ReplacementPreparation<Spec> = std::rc::Rc<dyn Fn(&str) -> anyhow::Result<Spec>>;
