//! A zero-copy of kafka Ack used by the `BaseEventProducer`.

use std::fmt;
use std::{ffi::CStr, ptr::NonNull, sync::Arc};

use rdkafka_sys as rdsys;
use rdkafka_sys::{RDKafkaMessage, RDKafkaRespErr};
use rdsys::RDKafkaErrorCode;

use crate::Timestamp;
use crate::{admin::NativeEvent, error::KafkaError, message::BorrowedHeaders, util, Message};

/// A zero-copy of a Kafka ack record.
///
/// `Ack`s are removed from the producer event queue in librdkafka
/// once they have been fully consumed by the `Producer` object in rust.
/// Holding references to too many ack messages will cause the memory of
/// the event queue to fill up and the producer to block until
/// some of the `Ack`s are dropped.
///
/// `Ack` is only used when `DeliveryReportMode::EVENT` is set.
///
// On this mode, we can't use a `BorrowedMessage`. This is due delivery
// report events are collected with `rd_kafka_event_message_next`.
// In this case messages will be freed automatically when the event is
// destroyed and the app MUST NOT call `rd_kafka_message_destroy`.
// A `BorrowedMessage` will call `rd_kafka_message_destroy` when it is
// dropped.
//
// An `Ack` is dropped when the rdkafka event is dropped.
#[derive(Clone)]
pub struct Ack {
    ptr: NonNull<RDKafkaMessage>,
    _owner: Arc<NativeEvent>,
}

impl Ack {
    pub(crate) fn new(ptr: *mut RDKafkaMessage, _owner: Arc<NativeEvent>) -> Self {
        Ack {
            ptr: NonNull::new(ptr).unwrap(),
            _owner,
        }
    }

    pub fn ptr(&self) -> *mut RDKafkaMessage {
        self.ptr.as_ptr()
    }

    /// Returns the ack error (if any)
    pub fn error(&self) -> Option<KafkaError> {
        let kafka_err = unsafe { (*self.ptr()).err };
        if kafka_err != RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR {
            Some(KafkaError::MessageProduction(kafka_err.into()))
        } else {
            None
        }
    }
}

impl Message for Ack {
    type Headers = BorrowedHeaders;

    fn key(&self) -> Option<&[u8]> {
        unsafe { util::ptr_to_opt_slice((*self.ptr()).key, (*self.ptr()).key_len) }
    }

    fn payload(&self) -> Option<&[u8]> {
        unsafe { util::ptr_to_opt_slice((*self.ptr()).payload, (*self.ptr()).len) }
    }

    unsafe fn payload_mut(&mut self) -> Option<&mut [u8]> {
        util::ptr_to_opt_mut_slice((*self.ptr()).payload, (*self.ptr()).len)
    }

    fn topic(&self) -> &str {
        unsafe {
            CStr::from_ptr(rdsys::rd_kafka_topic_name((*self.ptr()).rkt))
                .to_str()
                .expect("Topic name is not valid UTF-8")
        }
    }

    fn partition(&self) -> i32 {
        unsafe { (*self.ptr()).partition }
    }

    fn offset(&self) -> i64 {
        unsafe { (*self.ptr()).offset }
    }

    fn timestamp(&self) -> Timestamp {
        let mut timestamp_type = rdsys::rd_kafka_timestamp_type_t::RD_KAFKA_TIMESTAMP_NOT_AVAILABLE;
        let timestamp =
            unsafe { rdsys::rd_kafka_message_timestamp(self.ptr(), &mut timestamp_type) };
        if timestamp == -1 {
            Timestamp::NotAvailable
        } else {
            match timestamp_type {
                rdsys::rd_kafka_timestamp_type_t::RD_KAFKA_TIMESTAMP_NOT_AVAILABLE => {
                    Timestamp::NotAvailable
                }
                rdsys::rd_kafka_timestamp_type_t::RD_KAFKA_TIMESTAMP_CREATE_TIME => {
                    Timestamp::CreateTime(timestamp)
                }
                rdsys::rd_kafka_timestamp_type_t::RD_KAFKA_TIMESTAMP_LOG_APPEND_TIME => {
                    Timestamp::LogAppendTime(timestamp)
                }
            }
        }
    }

    fn headers(&self) -> Option<&BorrowedHeaders> {
        let mut native_headers_ptr = std::ptr::null_mut();
        unsafe {
            let err = rdsys::rd_kafka_message_headers(self.ptr(), &mut native_headers_ptr);
            match err.into() {
                RDKafkaErrorCode::NoError => {
                    Some(BorrowedHeaders::from_native_ptr(self, native_headers_ptr))
                }
                RDKafkaErrorCode::NoEnt => None,
                _ => None,
            }
        }
    }
}

unsafe impl Send for Ack {}
unsafe impl Sync for Ack {}

impl fmt::Debug for Ack {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ptr: {:?}, Topic: {}, Partition: {}, error: {:?}",
            self.ptr,
            self.topic(),
            self.partition(),
            self.error(),
        )
    }
}
