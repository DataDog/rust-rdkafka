//! A wrapper module to export logging functionality from
//! [`log`] or [`tracing`] depending on the `tracing` feature.
//!
//! [`log`]: https://docs.rs/log
//! [`tracing`]: https://docs.rs/tracing
//!
use crate::config::RDKafkaLogLevel;
use std::str::Split;

#[cfg(not(feature = "tracing"))]
pub use log::Level::{Debug as DEBUG, Info as INFO, Warn as WARN};
#[cfg(not(feature = "tracing"))]
pub use log::{debug, error, info, log_enabled, trace, warn};

#[cfg(feature = "tracing")]
pub use tracing::{debug, enabled as log_enabled, error, info, trace, warn};
#[cfg(feature = "tracing")]
pub const DEBUG: tracing::Level = tracing::Level::DEBUG;
#[cfg(feature = "tracing")]
pub const INFO: tracing::Level = tracing::Level::INFO;
#[cfg(feature = "tracing")]
pub const WARN: tracing::Level = tracing::Level::WARN;

///A record containing all of the logging metadata from librdkafka
#[derive(Debug, Clone)]
pub struct LogRecord {
    level: RDKafkaLogLevel,
    fac: String,
    log_message: String,
    contexts: String,
}

impl LogRecord {
    /// Build a new LogRecord
    pub fn new(level: RDKafkaLogLevel, fac: String, log_message: String, contexts: String) -> Self {
        LogRecord {
            level,
            fac,
            log_message,
            contexts,
        }
    }

    /// The librdkafka log level for this record
    pub fn level(&self) -> RDKafkaLogLevel {
        self.level
    }

    /// The librdkafka facility for this record
    pub fn facility(&self) -> &str {
        &self.fac
    }

    /// The message for this record
    pub fn log_message(&self) -> &str {
        &self.log_message
    }

    /// The set of debug contexts for this record
    pub fn contexts(&self) -> &str {
        &self.contexts
    }

    /// An iterator over the CSV context items
    pub fn split_contexts(&self) -> Split<'_, &str> {
        self.contexts.split(",")
    }
}
