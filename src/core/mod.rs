pub mod variables;
pub use variables::*;
pub mod interfaces;
pub use interfaces::*;
pub mod values;
pub use values::*;
pub mod stream_converstions;
pub use stream_converstions::*;

pub const MQTT_HOSTNAME: &str = "localhost";
pub const REDIS_HOSTNAME: &str = "localhost";
