//! Event based Kafka Producer
//! It leverages the rdkafka Event Interface rather than the callback one.

use std::{
    convert::TryFrom,
    ffi::CStr,
    mem,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use crate::{
    ack::Ack,
    admin::NativeEvent,
    client::Client,
    config::{FromClientConfig, FromClientConfigAndContext},
    consumer::ConsumerGroupMetadata,
    error::{KafkaError, KafkaResult},
    message::ToBytes,
    util::Timeout,
    ClientConfig, TopicPartitionList,
};
use log::{trace, warn};
use rdkafka_sys as rdsys;
use rdsys::{RDKafkaMessage, RDKafkaRespErr};

use super::{BaseProducer, BaseRecord, DefaultProducerContext, Producer, ProducerContext};

/// The `BaseEventProducer` uses the Event Interface provided by librdkafka.
/// Delivery report events are not handled via callbacks.
pub struct BaseEventProducer<C>
where
    C: ProducerContext + 'static,
{
    producer: BaseProducer<C>,
    should_stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
    acks_recv: crossbeam_channel::Receiver<Ack>,
}

impl FromClientConfig for BaseEventProducer<DefaultProducerContext> {
    fn from_config(
        config: &ClientConfig,
    ) -> KafkaResult<BaseEventProducer<DefaultProducerContext>> {
        BaseEventProducer::from_config_and_context(config, DefaultProducerContext)
    }
}

impl<C> FromClientConfigAndContext<C> for BaseEventProducer<C>
where
    C: ProducerContext + 'static,
{
    fn from_config_and_context(
        config: &ClientConfig,
        context: C,
    ) -> KafkaResult<BaseEventProducer<C>> {
        let producer = BaseProducer::from_config_and_context(config, context)?;
        let should_stop = Arc::new(AtomicBool::new(false));
        // TODO: make this configurable
        let (sender, recv) = crossbeam_channel::bounded::<Ack>(100_000);
        let thread = BaseEventProducer::spawn_polling_thread_event_mode(
            producer.clone(),
            should_stop.clone(),
            sender.clone(),
        );
        Ok(BaseEventProducer {
            producer,
            should_stop,
            handle: Some(thread),
            acks_recv: recv,
        })
    }
}

impl<C> BaseEventProducer<C>
where
    C: ProducerContext + 'static,
{
    fn spawn_polling_thread_event_mode(
        producer: BaseProducer<C>,
        should_stop: Arc<AtomicBool>,
        acks_sender: crossbeam_channel::Sender<Ack>,
    ) -> JoinHandle<()> {
        let rd_main_queue = producer.client().main_queue();
        let handle = thread::Builder::new()
            .name("producer polling thread event-mode".to_string())
            .spawn(move || {
                trace!("Polling thread loop started");
                loop {
                    let ev = unsafe {
                        NativeEvent::from_ptr(rd_main_queue.poll(Duration::from_millis(100)))
                    };
                    if ev.is_none() {
                        if should_stop.load(Ordering::Relaxed) {
                            break;
                        }
                        continue;
                    }

                    let ev = Arc::new(ev.unwrap());
                    let evtype = unsafe { rdsys::rd_kafka_event_type(ev.ptr()) };
                    match evtype as u32 {
                        rdsys::RD_KAFKA_EVENT_STATS => {
                            let json = unsafe {
                                let c_json = rdsys::rd_kafka_event_stats(ev.ptr());
                                CStr::from_ptr(c_json).to_string_lossy()
                            };
                            producer.client().context().stats_raw(json.as_bytes());
                        }
                        rdsys::RD_KAFKA_EVENT_ERROR => {
                            let c_err = unsafe { rdsys::rd_kafka_event_error(ev.ptr()) };
                            let err = RDKafkaRespErr::try_from(c_err)
                                .expect("global error not an rd_kafka_resp_err_t");
                            let error = KafkaError::Global(err.into());
                            let c_reason = unsafe { rdsys::rd_kafka_event_error_string(ev.ptr()) };
                            let reason = unsafe { CStr::from_ptr(c_reason).to_string_lossy() };
                            producer.client().context().error(error, reason.trim());
                        }
                        rdsys::RD_KAFKA_EVENT_DR => {
                            let max_messages =
                                unsafe { rdsys::rd_kafka_event_message_count(ev.ptr()) };
                            let raw_messages: Vec<*const RDKafkaMessage> =
                                Vec::with_capacity(max_messages);

                            let mut raw_messages = mem::ManuallyDrop::new(raw_messages);
                            let msgs_ptr = raw_messages.as_mut_ptr();
                            let raw_messages = unsafe {
                                let extracted_messages = rdsys::rd_kafka_event_message_array(
                                    ev.ptr(),
                                    msgs_ptr,
                                    max_messages,
                                );
                                Vec::from_raw_parts(msgs_ptr, extracted_messages, max_messages)
                            };

                            for raw_msg in raw_messages {
                                let ack = Ack::new(raw_msg as *mut RDKafkaMessage, ev.clone());
                                acks_sender.send(ack).unwrap();
                            }
                        }
                        _ => {
                            let buf = unsafe {
                                let evname = rdsys::rd_kafka_event_name(ev.ptr());
                                CStr::from_ptr(evname).to_bytes()
                            };
                            let evname = String::from_utf8(buf.to_vec()).unwrap();
                            warn!("Ignored event {}", evname);
                        }
                    }
                }
            })
            .expect("Failed to start polling thread event-mode");
        handle
    }

    /// Sends a message to Kafka.
    ///
    /// See the documentation for [`BaseProducer::send`] for details.
    pub fn send<'a, K, P>(
        &self,
        record: BaseRecord<'a, K, P, C::DeliveryOpaque>,
    ) -> Result<(), (KafkaError, BaseRecord<'a, K, P, C::DeliveryOpaque>)>
    where
        K: ToBytes + ?Sized,
        P: ToBytes + ?Sized,
    {
        self.producer.send(record)
    }

    /// Returns the receiver end of a channel for getting the acks
    /// The idea is to avoid copying the acks into an intermediate queue
    /// and allow the caller to directly receive the Acks.
    pub fn acks_recv(&self) -> crossbeam_channel::Receiver<Ack> {
        self.acks_recv.clone()
    }
}

impl<C> Producer<C> for BaseEventProducer<C>
where
    C: ProducerContext + 'static,
{
    fn client(&self) -> &Client<C> {
        self.producer.client()
    }

    fn flush<T: Into<Timeout>>(&self, timeout: T) {
        self.producer.flush(timeout);
    }

    fn in_flight_count(&self) -> i32 {
        self.producer.in_flight_count()
    }

    fn init_transactions<T: Into<Timeout>>(&self, timeout: T) -> KafkaResult<()> {
        self.producer.init_transactions(timeout)
    }

    fn begin_transaction(&self) -> KafkaResult<()> {
        self.producer.begin_transaction()
    }

    fn send_offsets_to_transaction<T: Into<Timeout>>(
        &self,
        offsets: &TopicPartitionList,
        cgm: &ConsumerGroupMetadata,
        timeout: T,
    ) -> KafkaResult<()> {
        self.producer
            .send_offsets_to_transaction(offsets, cgm, timeout)
    }

    fn commit_transaction<T: Into<Timeout>>(&self, timeout: T) -> KafkaResult<()> {
        self.producer.commit_transaction(timeout)
    }

    fn abort_transaction<T: Into<Timeout>>(&self, timeout: T) -> KafkaResult<()> {
        self.producer.abort_transaction(timeout)
    }
}

impl<C> Drop for BaseEventProducer<C>
where
    C: ProducerContext + 'static,
{
    fn drop(&mut self) {
        trace!("Destroy BaseEventProducer");
        if let Some(handle) = self.handle.take() {
            trace!("Stopping polling");
            self.should_stop.store(true, Ordering::Relaxed);
            trace!("Waiting for polling thread termination");
            match handle.join() {
                Ok(()) => trace!("Polling stopped"),
                Err(e) => warn!("Failure while terminating thread: {:?}", e),
            };
        }
        trace!("BaseEventProducer destroyed");
    }
}
