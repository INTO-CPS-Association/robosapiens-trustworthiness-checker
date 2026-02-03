pub mod variables;
pub use variables::*;
pub mod interfaces;
pub use interfaces::*;
pub mod values;
pub use values::*;
pub mod stream_casting;
pub use stream_casting::*;

pub const MQTT_HOSTNAME: &str = "localhost";
pub const REDIS_HOSTNAME: &str = "localhost";
