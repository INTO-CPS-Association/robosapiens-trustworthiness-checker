mod manual_output_handler;
pub use manual_output_handler::ManualOutputHandler;
mod null_output_handler;
pub use null_output_handler::{LimitedNullOutputHandler, NullOutputHandler};
mod manual_input;
pub(crate) use manual_input::from_streams;
pub use manual_input::{ManualInputController, channel};

/// Construct a rebuildable manual input for reconfiguration tests and benchmarks.
pub fn input_factory(
    fanouts: std::collections::BTreeMap<
        crate::VarName,
        std::rc::Rc<crate::stream_utils::Fanout<crate::Value>>,
    >,
) -> crate::io::InputStreamFactory {
    crate::io::InputStreamFactory::manual(fanouts)
}
