//! Reading an RPIO MAPLE-K observation journal.
//!
//! A producer publishes two streams: the phase records of each activation it
//! ran, and the versions it wrote to Knowledge. [`json`] decodes exactly what
//! the producer wrote, byte for byte, so a value's digest can be checked
//! against the text it was computed over, and [`wire`] reads each record as
//! the value `rpio::journal` declares. A record names Knowledge rather than
//! carrying it, so [`knowledge_resolver`] joins each named version with the
//! value written under it. [`time`] reads the admission times and clock
//! labels the journal carries, which the observation-plus-clock source binds
//! to a time domain.

pub(crate) mod admission;
pub mod journal_view;
pub mod json;
pub mod knowledge_resolver;
pub mod time;
pub mod wire;

pub use journal_view::{JournalCapability, JournalReading, Unrecorded};
pub use json::{CODEC_ID, JsonError, JsonNumber, JsonValue};
pub use knowledge_resolver::{KnowledgeResolver, ResolverError};
pub use time::{JournalArrival, JournalEnvelope};
pub use wire::{WireDecoder, WireError};

pub mod declarations;
pub use declarations::ObservationSchema;
