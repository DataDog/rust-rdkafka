//! Low-level consumers.

use std::ffi::{CStr, CString};
use std::mem::ManuallyDrop;
use std::os::raw::c_void;
use std::ptr;
use std::sync::Arc;
use std::time::Duration;

use log::warn;
use rdkafka_sys as rdsys;
use rdkafka_sys::types::*;

use crate::client::{Client, NativeQueue};
use crate::config::{
    ClientConfig, FromClientConfig, FromClientConfigAndContext, NativeClientConfig,
};
use crate::consumer::{
    CommitMode, Consumer, ConsumerContext, ConsumerGroupMetadata, DefaultConsumerContext,
    RebalanceProtocol,
};
use crate::error::{IsError, KafkaError, KafkaResult, RDKafkaError};
use crate::groups::GroupList;
use crate::log::trace;
use crate::message::{BorrowedMessage, Message};
use crate::metadata::Metadata;
use crate::topic_partition_list::{Offset, TopicPartitionList};
use crate::util::{cstr_to_owned, NativePtr, Timeout};

/// A low-level consumer that requires manual polling.
///
/// This consumer must be periodically polled to make progress on rebalancing,
/// callbacks and to receive messages.
pub struct BaseConsumer<C = DefaultConsumerContext>
where
    C: ConsumerContext,
{
    client: Client<C>,
    queue: Arc<NativeQueue>,
    static_member: bool,
}

impl FromClientConfig for BaseConsumer {
    fn from_config(config: &ClientConfig) -> KafkaResult<BaseConsumer> {
        BaseConsumer::from_config_and_context(config, DefaultConsumerContext)
    }
}

/// Creates a new `BaseConsumer` starting from a `ClientConfig`.
impl<C: ConsumerContext> FromClientConfigAndContext<C> for BaseConsumer<C> {
    fn from_config_and_context(config: &ClientConfig, context: C) -> KafkaResult<BaseConsumer<C>> {
        BaseConsumer::new(config, config.create_native_config()?, context)
    }
}

impl<C> BaseConsumer<C>
where
    C: ConsumerContext,
{
    pub(crate) fn new(
        config: &ClientConfig,
        native_config: NativeClientConfig,
        context: C,
    ) -> KafkaResult<BaseConsumer<C>> {
        let mut static_member = false;
        if let Some(group_instance_id) = config.get("group.instance.id") {
            if !group_instance_id.is_empty() {
                static_member = true;
            }
        }
        unsafe {
            rdsys::rd_kafka_conf_set_events(
                native_config.ptr(),
                rdsys::RD_KAFKA_EVENT_REBALANCE
                    | rdsys::RD_KAFKA_EVENT_OFFSET_COMMIT
                    | rdsys::RD_KAFKA_EVENT_STATS
                    | rdsys::RD_KAFKA_EVENT_ERROR
                    | rdsys::RD_KAFKA_EVENT_OAUTHBEARER_TOKEN_REFRESH,
            )
        };
        let client = Client::new(
            config,
            native_config,
            RDKafkaType::RD_KAFKA_CONSUMER,
            context,
        )?;

        // Redirect rdkafka's main queue to the consumer queue so that we only
        // need to listen to the consumer queue to observe events like
        // rebalancings and stats.
        unsafe { rdsys::rd_kafka_poll_set_consumer(client.native_ptr()) };
        let queue = Arc::new(client.consumer_queue().ok_or(KafkaError::ClientCreation(
            "rdkafka consumer queue not available".to_string(),
        ))?);
        Ok(BaseConsumer {
            client,
            queue,
            static_member,
        })
    }

    /// Polls the consumer for new messages.
    ///
    /// It won't block for more than the specified timeout. Use zero `Duration` for non-blocking
    /// call. With no timeout it blocks until an event is received.
    ///
    /// This method should be called at regular intervals, even if no message is expected,
    /// to serve any queued events waiting to be handled. This is especially important for
    /// automatic consumer rebalance, as the rebalance function will be executed by the thread
    /// calling the poll() function.
    ///
    /// # Lifetime
    ///
    /// The returned message lives in the memory of the consumer and cannot outlive it.
    pub fn poll<T: Into<Timeout>>(&self, timeout: T) -> Option<KafkaResult<BorrowedMessage<'_>>> {
        let event = Arc::new(
            self.client()
                .poll_event(self.queue.clone(), timeout.into())?,
        );
        let evtype = unsafe { rdsys::rd_kafka_event_type(event.ptr()) };
        match evtype {
            rdsys::RD_KAFKA_EVENT_FETCH => self.handle_fetch_event(event.clone()),
            rdsys::RD_KAFKA_EVENT_REBALANCE => {
                self.handle_rebalance_event(event.clone());
                None
            }
            rdsys::RD_KAFKA_EVENT_OFFSET_COMMIT => {
                self.handle_offset_commit_event(event.clone());
                None
            }
            _ => {
                let buf = unsafe {
                    let evname = rdsys::rd_kafka_event_name(event.ptr());
                    CStr::from_ptr(evname).to_bytes()
                };
                let evname = String::from_utf8(buf.to_vec()).unwrap();
                warn!("Ignored event '{}' on consumer poll", evname);
                None
            }
        }
    }

    fn handle_fetch_event(
        &self,
        event: Arc<NativePtr<RDKafkaEvent>>,
    ) -> Option<KafkaResult<BorrowedMessage<'_>>> {
        unsafe {
            NativePtr::from_ptr(rdsys::rd_kafka_event_message_next(event.ptr()) as *mut _)
                .map(|ptr| BorrowedMessage::from_fetch_event(ptr, event.clone()))
        }
    }

    fn handle_rebalance_event(&self, event: Arc<NativePtr<RDKafkaEvent>>) {
        let err = unsafe { rdsys::rd_kafka_event_error(event.ptr()) };
        match err {
            rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS
            | rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS => {
                let tpl = unsafe {
                    let native_tpl = rdsys::rd_kafka_event_topic_partition_list(event.ptr());
                    TopicPartitionList::from_ptr(native_tpl)
                };
                let mut tpl = ManuallyDrop::new(tpl);
                self.context()
                    .rebalance(self.client.native_client(), err, &mut tpl);
            }
            _ => {
                let buf = unsafe {
                    let err_name =
                        rdsys::rd_kafka_err2name(rdsys::rd_kafka_event_error(event.ptr()));
                    CStr::from_ptr(err_name).to_bytes()
                };
                let err = String::from_utf8(buf.to_vec()).unwrap();
                warn!("invalid rebalance event: {:?}", err);
            }
        }
    }

    fn handle_offset_commit_event(&self, event: Arc<NativePtr<RDKafkaEvent>>) {
        let err = unsafe { rdsys::rd_kafka_event_error(event.ptr()) };
        let commit_error = if err.is_error() {
            Err(KafkaError::ConsumerCommit(err.into()))
        } else {
            Ok(())
        };

        let offsets = unsafe { rdsys::rd_kafka_event_topic_partition_list(event.ptr()) };
        if offsets.is_null() {
            let tpl = TopicPartitionList::new();
            self.context().commit_callback(commit_error, &tpl);
        } else {
            let tpl = ManuallyDrop::new(unsafe { TopicPartitionList::from_ptr(offsets) });
            self.context().commit_callback(commit_error, &tpl);
        }
    }

    /// Returns an iterator over the available messages.
    ///
    /// It repeatedly calls [`poll`](#method.poll) with no timeout.
    ///
    /// Note that it's also possible to iterate over the consumer directly.
    ///
    /// # Examples
    ///
    /// All these are equivalent and will receive messages without timing out.
    ///
    /// ```rust,no_run
    /// # let consumer: rdkafka::consumer::BaseConsumer<_> = rdkafka::ClientConfig::new()
    /// #    .create()
    /// #    .unwrap();
    /// #
    /// loop {
    ///   let message = consumer.poll(None);
    ///   // Handle the message
    /// }
    /// ```
    ///
    /// ```rust,no_run
    /// # let consumer: rdkafka::consumer::BaseConsumer<_> = rdkafka::ClientConfig::new()
    /// #    .create()
    /// #    .unwrap();
    /// #
    /// for message in consumer.iter() {
    ///   // Handle the message
    /// }
    /// ```
    ///
    /// ```rust,no_run
    /// # let consumer: rdkafka::consumer::BaseConsumer<_> = rdkafka::ClientConfig::new()
    /// #    .create()
    /// #    .unwrap();
    /// #
    /// for message in &consumer {
    ///   // Handle the message
    /// }
    /// ```
    pub fn iter(&self) -> Iter<'_, C> {
        Iter(self)
    }

    pub(crate) fn get_queue(&self) -> Arc<NativeQueue> {
        self.queue.clone()
    }

    /// Splits messages for the specified partition into their own queue.
    ///
    /// If the `topic` or `partition` is invalid, returns `None`.
    ///
    /// After calling this method, newly-fetched messages for the specified
    /// partition will be returned via [`PartitionQueue::poll`] rather than
    /// [`BaseConsumer::poll`]. Note that there may be buffered messages for the
    /// specified partition that will continue to be returned by
    /// `BaseConsumer::poll`. For best results, call `split_partition_queue`
    /// before the first call to `BaseConsumer::poll`.
    ///
    /// You must continue to call `BaseConsumer::poll`, even if no messages are
    /// expected, to serve callbacks.
    ///
    /// Note that calling [`Consumer::assign`] will deactivate any existing
    /// partition queues. You will need to call this method for every partition
    /// that should be split after every call to `assign`.
    ///
    /// Beware that this method is implemented for `&Arc<Self>`, not `&self`.
    /// You will need to wrap your consumer in an `Arc` in order to call this
    /// method. This design permits moving the partition queue to another thread
    /// while ensuring the partition queue does not outlive the consumer.
    pub fn split_partition_queue(
        self: &Arc<Self>,
        topic: &str,
        partition: i32,
    ) -> Option<PartitionQueue<C>> {
        let topic = match CString::new(topic) {
            Ok(topic) => topic,
            Err(_) => return None,
        };
        let queue = unsafe {
            NativeQueue::from_ptr(rdsys::rd_kafka_queue_get_partition(
                self.client.native_ptr(),
                topic.as_ptr(),
                partition,
            ))
        };
        queue.map(|queue| {
            unsafe { rdsys::rd_kafka_queue_forward(queue.ptr(), ptr::null_mut()) }
            PartitionQueue::new(self.clone(), queue)
        })
    }
}

impl<C> Consumer<C> for BaseConsumer<C>
where
    C: ConsumerContext,
{
    fn client(&self) -> &Client<C> {
        &self.client
    }

    fn group_metadata(&self) -> Option<ConsumerGroupMetadata> {
        let ptr = unsafe {
            NativePtr::from_ptr(rdsys::rd_kafka_consumer_group_metadata(
                self.client.native_ptr(),
            ))
        }?;
        Some(ConsumerGroupMetadata(ptr))
    }

    fn subscribe(&self, topics: &[&str]) -> KafkaResult<()> {
        let mut tpl = TopicPartitionList::new();
        for topic in topics {
            tpl.add_topic_unassigned(topic);
        }
        let ret_code = unsafe { rdsys::rd_kafka_subscribe(self.client.native_ptr(), tpl.ptr()) };
        if ret_code.is_error() {
            let error = unsafe { cstr_to_owned(rdsys::rd_kafka_err2str(ret_code)) };
            return Err(KafkaError::Subscription(error));
        };
        Ok(())
    }

    fn unsubscribe(&self) {
        unsafe { rdsys::rd_kafka_unsubscribe(self.client.native_ptr()) };
    }

    fn assign(&self, assignment: &TopicPartitionList) -> KafkaResult<()> {
        let ret_code =
            unsafe { rdsys::rd_kafka_assign(self.client.native_ptr(), assignment.ptr()) };
        if ret_code.is_error() {
            let error = unsafe { cstr_to_owned(rdsys::rd_kafka_err2str(ret_code)) };
            return Err(KafkaError::Subscription(error));
        };
        Ok(())
    }

    fn unassign(&self) -> KafkaResult<()> {
        // Passing null to assign clears the current static assignments list
        let ret_code = unsafe { rdsys::rd_kafka_assign(self.client.native_ptr(), ptr::null()) };
        if ret_code.is_error() {
            let error = unsafe { cstr_to_owned(rdsys::rd_kafka_err2str(ret_code)) };
            return Err(KafkaError::Subscription(error));
        };
        Ok(())
    }

    fn incremental_assign(&self, assignment: &TopicPartitionList) -> KafkaResult<()> {
        let ret = unsafe {
            RDKafkaError::from_ptr(rdsys::rd_kafka_incremental_assign(
                self.client.native_ptr(),
                assignment.ptr(),
            ))
        };
        if ret.is_error() {
            let error = ret.name();
            return Err(KafkaError::Subscription(error));
        };
        Ok(())
    }

    fn incremental_unassign(&self, assignment: &TopicPartitionList) -> KafkaResult<()> {
        let ret = unsafe {
            RDKafkaError::from_ptr(rdsys::rd_kafka_incremental_unassign(
                self.client.native_ptr(),
                assignment.ptr(),
            ))
        };
        if ret.is_error() {
            let error = ret.name();
            return Err(KafkaError::Subscription(error));
        };
        Ok(())
    }

    fn seek<T: Into<Timeout>>(
        &self,
        topic: &str,
        partition: i32,
        offset: Offset,
        timeout: T,
    ) -> KafkaResult<()> {
        let topic = self.client.native_topic(topic)?;
        let ret_code = match offset.to_raw() {
            Some(offset) => unsafe {
                rdsys::rd_kafka_seek(topic.ptr(), partition, offset, timeout.into().as_millis())
            },
            None => return Err(KafkaError::Seek("Local: Unrepresentable offset".into())),
        };
        if ret_code.is_error() {
            let error = unsafe { cstr_to_owned(rdsys::rd_kafka_err2str(ret_code)) };
            return Err(KafkaError::Seek(error));
        };
        Ok(())
    }

    fn seek_partitions<T: Into<Timeout>>(
        &self,
        topic_partition_list: TopicPartitionList,
        timeout: T,
    ) -> KafkaResult<TopicPartitionList> {
        let ret = unsafe {
            RDKafkaError::from_ptr(rdsys::rd_kafka_seek_partitions(
                self.client.native_ptr(),
                topic_partition_list.ptr(),
                timeout.into().as_millis(),
            ))
        };
        if ret.is_error() {
            let error = ret.name();
            return Err(KafkaError::Seek(error));
        }
        Ok(topic_partition_list)
    }

    fn commit(
        &self,
        topic_partition_list: &TopicPartitionList,
        mode: CommitMode,
    ) -> KafkaResult<()> {
        let error = unsafe {
            rdsys::rd_kafka_commit(
                self.client.native_ptr(),
                topic_partition_list.ptr(),
                mode as i32,
            )
        };
        if error.is_error() {
            Err(KafkaError::ConsumerCommit(error.into()))
        } else {
            Ok(())
        }
    }

    fn commit_consumer_state(&self, mode: CommitMode) -> KafkaResult<()> {
        let error = unsafe {
            rdsys::rd_kafka_commit(self.client.native_ptr(), ptr::null_mut(), mode as i32)
        };
        if error.is_error() {
            Err(KafkaError::ConsumerCommit(error.into()))
        } else {
            Ok(())
        }
    }

    fn commit_message(&self, message: &BorrowedMessage<'_>, mode: CommitMode) -> KafkaResult<()> {
        let error = unsafe {
            rdsys::rd_kafka_commit_message(self.client.native_ptr(), message.ptr(), mode as i32)
        };
        if error.is_error() {
            Err(KafkaError::ConsumerCommit(error.into()))
        } else {
            Ok(())
        }
    }

    fn store_offset(&self, topic: &str, partition: i32, offset: i64) -> KafkaResult<()> {
        let topic = self.client.native_topic(topic)?;
        let error = unsafe { rdsys::rd_kafka_offset_store(topic.ptr(), partition, offset) };
        if error.is_error() {
            Err(KafkaError::StoreOffset(error.into()))
        } else {
            Ok(())
        }
    }

    fn store_offset_from_message(&self, message: &BorrowedMessage<'_>) -> KafkaResult<()> {
        let error = unsafe {
            rdsys::rd_kafka_offset_store(message.topic_ptr(), message.partition(), message.offset())
        };
        if error.is_error() {
            Err(KafkaError::StoreOffset(error.into()))
        } else {
            Ok(())
        }
    }

    fn store_offsets(&self, tpl: &TopicPartitionList) -> KafkaResult<()> {
        let error = unsafe { rdsys::rd_kafka_offsets_store(self.client.native_ptr(), tpl.ptr()) };
        if error.is_error() {
            Err(KafkaError::StoreOffset(error.into()))
        } else {
            Ok(())
        }
    }

    fn subscription(&self) -> KafkaResult<TopicPartitionList> {
        let mut tpl_ptr = ptr::null_mut();
        let error = unsafe { rdsys::rd_kafka_subscription(self.client.native_ptr(), &mut tpl_ptr) };

        if error.is_error() {
            Err(KafkaError::MetadataFetch(error.into()))
        } else {
            Ok(unsafe { TopicPartitionList::from_ptr(tpl_ptr) })
        }
    }

    fn assignment(&self) -> KafkaResult<TopicPartitionList> {
        let mut tpl_ptr = ptr::null_mut();
        let error = unsafe { rdsys::rd_kafka_assignment(self.client.native_ptr(), &mut tpl_ptr) };

        if error.is_error() {
            Err(KafkaError::MetadataFetch(error.into()))
        } else {
            Ok(unsafe { TopicPartitionList::from_ptr(tpl_ptr) })
        }
    }

    fn assignment_lost(&self) -> bool {
        unsafe { rdsys::rd_kafka_assignment_lost(self.client.native_ptr()) == 1 }
    }

    fn committed<T: Into<Timeout>>(&self, timeout: T) -> KafkaResult<TopicPartitionList> {
        let mut tpl_ptr = ptr::null_mut();
        let assignment_error =
            unsafe { rdsys::rd_kafka_assignment(self.client.native_ptr(), &mut tpl_ptr) };
        if assignment_error.is_error() {
            return Err(KafkaError::MetadataFetch(assignment_error.into()));
        }

        self.committed_offsets(unsafe { TopicPartitionList::from_ptr(tpl_ptr) }, timeout)
    }

    fn committed_offsets<T: Into<Timeout>>(
        &self,
        tpl: TopicPartitionList,
        timeout: T,
    ) -> KafkaResult<TopicPartitionList> {
        let committed_error = unsafe {
            rdsys::rd_kafka_committed(
                self.client.native_ptr(),
                tpl.ptr(),
                timeout.into().as_millis(),
            )
        };

        if committed_error.is_error() {
            Err(KafkaError::MetadataFetch(committed_error.into()))
        } else {
            Ok(tpl)
        }
    }

    fn offsets_for_timestamp<T: Into<Timeout>>(
        &self,
        timestamp: i64,
        timeout: T,
    ) -> KafkaResult<TopicPartitionList> {
        let mut tpl_ptr = ptr::null_mut();
        let assignment_error =
            unsafe { rdsys::rd_kafka_assignment(self.client.native_ptr(), &mut tpl_ptr) };
        if assignment_error.is_error() {
            return Err(KafkaError::MetadataFetch(assignment_error.into()));
        }
        let mut tpl = unsafe { TopicPartitionList::from_ptr(tpl_ptr) };

        // Set the timestamp we want in the offset field for every partition as
        // librdkafka expects.
        tpl.set_all_offsets(Offset::Offset(timestamp))?;

        self.offsets_for_times(tpl, timeout)
    }

    // `timestamps` is a `TopicPartitionList` with timestamps instead of
    // offsets.
    fn offsets_for_times<T: Into<Timeout>>(
        &self,
        timestamps: TopicPartitionList,
        timeout: T,
    ) -> KafkaResult<TopicPartitionList> {
        // This call will then put the offset in the offset field of this topic
        // partition list.
        let offsets_for_times_error = unsafe {
            rdsys::rd_kafka_offsets_for_times(
                self.client.native_ptr(),
                timestamps.ptr(),
                timeout.into().as_millis(),
            )
        };

        if offsets_for_times_error.is_error() {
            Err(KafkaError::MetadataFetch(offsets_for_times_error.into()))
        } else {
            Ok(timestamps)
        }
    }

    fn position(&self) -> KafkaResult<TopicPartitionList> {
        let tpl = self.assignment()?;
        let error = unsafe { rdsys::rd_kafka_position(self.client.native_ptr(), tpl.ptr()) };
        if error.is_error() {
            Err(KafkaError::MetadataFetch(error.into()))
        } else {
            Ok(tpl)
        }
    }

    fn fetch_metadata<T: Into<Timeout>>(
        &self,
        topic: Option<&str>,
        timeout: T,
    ) -> KafkaResult<Metadata> {
        self.client.fetch_metadata(topic, timeout)
    }

    fn fetch_watermarks<T: Into<Timeout>>(
        &self,
        topic: &str,
        partition: i32,
        timeout: T,
    ) -> KafkaResult<(i64, i64)> {
        self.client.fetch_watermarks(topic, partition, timeout)
    }

    fn fetch_group_list<T: Into<Timeout>>(
        &self,
        group: Option<&str>,
        timeout: T,
    ) -> KafkaResult<GroupList> {
        self.client.fetch_group_list(group, timeout)
    }

    fn pause(&self, partitions: &TopicPartitionList) -> KafkaResult<()> {
        let ret_code =
            unsafe { rdsys::rd_kafka_pause_partitions(self.client.native_ptr(), partitions.ptr()) };
        if ret_code.is_error() {
            let error = unsafe { cstr_to_owned(rdsys::rd_kafka_err2str(ret_code)) };
            return Err(KafkaError::PauseResume(error));
        };
        Ok(())
    }

    fn resume(&self, partitions: &TopicPartitionList) -> KafkaResult<()> {
        let ret_code = unsafe {
            rdsys::rd_kafka_resume_partitions(self.client.native_ptr(), partitions.ptr())
        };
        if ret_code.is_error() {
            let error = unsafe { cstr_to_owned(rdsys::rd_kafka_err2str(ret_code)) };
            return Err(KafkaError::PauseResume(error));
        };
        Ok(())
    }

    fn rebalance_protocol(&self) -> RebalanceProtocol {
        self.client.native_client().rebalance_protocol()
    }
}

impl<C> Drop for BaseConsumer<C>
where
    C: ConsumerContext,
{
    fn drop(&mut self) {
        trace!("Destroying consumer: {:?}", self.client.native_ptr());
        // If this consumer is configured for static membership, do not explicitly unsubscribe from
        // the group. Note that static members will *not* receive a final revoke event when they
        // shutdown.
        if !self.static_member {
            // We use the Event API rather than the Callback API.
            // As we don't register a rebalance_cb, rd_kafka_consumer_close()
            // will shortcut the rebalance_cb and do a forced unassign.
            // This is undesired as the application might need the final
            // revoke events before shutting down. Hence, we trigger
            // an Unsubscribe() first, wait for that to propagate, and then
            // close the consumer.
            unsafe { rdsys::rd_kafka_unsubscribe(self.client.native_ptr()) };

            // Poll for rebalance events
            loop {
                self.poll(Duration::from_secs(5));
                let qlen = unsafe { rdsys::rd_kafka_queue_length(self.queue.ptr()) };
                if qlen == 0 {
                    break;
                }
            }
        }

        // TODO(sam): do we need to destroy the queue before calling close?
        unsafe { rdsys::rd_kafka_consumer_close(self.client.native_ptr()) };
        trace!("Consumer destroyed: {:?}", self.client.native_ptr());
    }
}

/// A convenience iterator over the messages in a [`BaseConsumer`].
///
/// Each call to [`Iter::next`] simply calls [`BaseConsumer::poll`] with an
/// infinite timeout.
pub struct Iter<'a, C>(&'a BaseConsumer<C>)
where
    C: ConsumerContext;

impl<'a, C> Iterator for Iter<'a, C>
where
    C: ConsumerContext,
{
    type Item = KafkaResult<BorrowedMessage<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(item) = self.0.poll(None) {
                return Some(item);
            }
        }
    }
}

impl<'a, C> IntoIterator for &'a BaseConsumer<C>
where
    C: ConsumerContext,
{
    type Item = KafkaResult<BorrowedMessage<'a>>;
    type IntoIter = Iter<'a, C>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// A message queue for a single partition.
pub struct PartitionQueue<C>
where
    C: ConsumerContext,
{
    consumer: Arc<BaseConsumer<C>>,
    queue: NativeQueue,
    nonempty_callback: Option<Box<Box<dyn Fn() + Send + Sync>>>,
}

impl<C> PartitionQueue<C>
where
    C: ConsumerContext,
{
    pub(crate) fn new(consumer: Arc<BaseConsumer<C>>, queue: NativeQueue) -> Self {
        PartitionQueue {
            consumer,
            queue,
            nonempty_callback: None,
        }
    }

    /// Polls the partition for new messages.
    ///
    /// The `timeout` parameter controls how long to block if no messages are
    /// available.
    ///
    /// Remember that you must also call [`BaseConsumer::poll`] on the
    /// associated consumer regularly, even if no messages are expected, to
    /// serve events.
    pub fn poll<T: Into<Timeout>>(&self, timeout: T) -> Option<KafkaResult<BorrowedMessage<'_>>> {
        unsafe {
            NativePtr::from_ptr(rdsys::rd_kafka_consume_queue(
                self.queue.ptr(),
                timeout.into().as_millis(),
            ))
        }
        .map(|ptr| unsafe { BorrowedMessage::from_consumer(ptr, &self.consumer) })
    }

    /// Sets a callback that will be invoked whenever the queue becomes
    /// nonempty.
    pub fn set_nonempty_callback<F>(&mut self, f: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        // SAFETY: we keep `F` alive until the next call to
        // `rd_kafka_queue_cb_event_enable`. That might be the next call to
        // `set_nonempty_callback` or it might be when the queue is dropped. The
        // double indirection is required because `&dyn Fn` is a fat pointer.

        unsafe extern "C" fn native_message_queue_nonempty_cb(
            _: *mut RDKafka,
            opaque_ptr: *mut c_void,
        ) {
            let f = opaque_ptr as *const *const (dyn Fn() + Send + Sync);
            (**f)();
        }

        let f: Box<Box<dyn Fn() + Send + Sync>> = Box::new(Box::new(f));
        unsafe {
            rdsys::rd_kafka_queue_cb_event_enable(
                self.queue.ptr(),
                Some(native_message_queue_nonempty_cb),
                &*f as *const _ as *mut c_void,
            )
        }
        self.nonempty_callback = Some(f);
    }
}

impl<C> Drop for PartitionQueue<C>
where
    C: ConsumerContext,
{
    fn drop(&mut self) {
        unsafe { rdsys::rd_kafka_queue_cb_event_enable(self.queue.ptr(), None, ptr::null_mut()) }
    }
}
