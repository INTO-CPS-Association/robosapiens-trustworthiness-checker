mod manual_output_handler;
pub use manual_output_handler::ManualOutputHandler;
mod null_output_handler;
pub use null_output_handler::{LimitedNullOutputHandler, NullOutputHandler};
mod manual_input;
pub(crate) use manual_input::from_streams;
pub use manual_input::{ManualInputController, channel};

/// Construct a reusable manual step source for tests and benchmarks.
pub fn input_source(
    fanouts: std::collections::BTreeMap<
        crate::VarName,
        std::rc::Rc<crate::stream_utils::Fanout<crate::Value>>,
    >,
) -> crate::io::InputSource {
    crate::io::InputSource::<crate::Value>::manual(fanouts)
}

/// Construct a manual step source with a separate control-plane fanout.
pub fn input_source_with_control(
    fanouts: std::collections::BTreeMap<
        crate::VarName,
        std::rc::Rc<crate::stream_utils::Fanout<crate::Value>>,
    >,
    control: std::rc::Rc<crate::stream_utils::Fanout<crate::Value>>,
) -> crate::io::InputSource {
    crate::io::InputSource::<crate::Value>::manual_with_control(fanouts, Some(control))
}
