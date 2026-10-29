//! Common client functionality.
//!
//! In librdkafka parlance, a client is either a consumer or a producer. This
//! module's [`Client`] type provides the functionality that is common to both
//! consumers and producers.
//!
//! Typically you will not want to construct a client directly. Construct one of
//! the consumers in the [`consumer`] module or one of the producers in the
//! [`producer`] modules instead.
//!
//! [`consumer`]: crate::consumer
//! [`producer`]: crate::producer
use std::error::Error;
use std::ffi::{CStr, CString};
use std::mem::ManuallyDrop;
use std::os::raw::c_char;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;
use std::string::ToString;
use std::sync::Arc;

use libc::c_void;
use rdkafka_sys as rdsys;
use rdkafka_sys::types::*;

use crate::admin::NativeEvent;
use crate::config::DEBUG_CONTEXTS_BYTE_LENGTH;
use crate::config::{ClientConfig, NativeClientConfig, RDKafkaLogLevel};
use crate::consumer::RebalanceProtocol;
use crate::error::{IsError, KafkaError, KafkaResult, RDKafkaError};
use crate::groups::GroupList;
use crate::log::{debug, error, info, trace, warn, LogRecord};
use crate::metadata::Metadata;
use crate::mocking::MockCluster;
use crate::util::{self, ErrBuf, KafkaDrop, NativePtr, Timeout};
use crate::Statistics;

/// Client-level context.
///
/// Each client (consumers and producers included) has a context object that can
/// be used to customize its behavior. Implementing `ClientContext` enables the
/// customization of methods common to all clients, while [`ProducerContext`]
/// and [`ConsumerContext`] are specific to producers and consumers. Refer to
/// the list of methods to see which callbacks can currently be overridden.
///
/// **Important**: implementations of `ClientContext` must be thread safe, as
/// they might be shared between multiple threads.
///
/// [`ConsumerContext`]: crate::consumer::ConsumerContext
/// [`ProducerContext`]: crate::producer::ProducerContext
pub trait ClientContext: Send + Sync {
    /// Whether to periodically refresh the SASL `OAUTHBEARER` token
    /// by calling [`ClientContext::generate_oauth_token`].
    ///
    /// If disabled, librdkafka's default token refresh callback is used
    /// instead.
    ///
    /// This parameter is only relevant when using the `OAUTHBEARER` SASL
    /// mechanism. When enabled, BaseConsumer and StreamConsumer service this
    /// callback on librdkafka's background SASL queue, independently of application
    /// polling, unless librdkafka's built-in OIDC or unsecure JWT token source is
    /// configured. The callback must not destroy the client and should bound any
    /// blocking work.
    const ENABLE_REFRESH_OAUTH_TOKEN: bool = false;

    /// Receives log lines from librdkafka.
    ///
    /// The default implementation forwards the log lines to the appropriate
    /// [`log`] crate macro. Consult the [`RDKafkaLogLevel`] documentation for
    /// details about the log level mapping.
    ///
    /// [`log`]: https://docs.rs/log
    fn log(&self, record: LogRecord) {
        match record.level() {
            RDKafkaLogLevel::Emerg
            | RDKafkaLogLevel::Alert
            | RDKafkaLogLevel::Critical
            | RDKafkaLogLevel::Error => {
                error!(target: "librdkafka", "librdkafka: {} {}", record.facility(), record.log_message())
            }
            RDKafkaLogLevel::Warning => {
                warn!(target: "librdkafka", "librdkafka: {} {}", record.facility(), record.log_message())
            }
            RDKafkaLogLevel::Notice | RDKafkaLogLevel::Info => {
                info!(target: "librdkafka", "librdkafka: {} {}", record.facility(), record.log_message())
            }
            RDKafkaLogLevel::Debug => {
                debug!(target: "librdkafka", "librdkafka: {} {}", record.facility(), record.log_message())
            }
        }
    }

    /// Receives the decoded statistics of the librdkafka client. To enable, the
    /// `statistics.interval.ms` configuration parameter must be specified.
    ///
    /// The default implementation logs the statistics at the `info` log level.
    fn stats(&self, statistics: Statistics) {
        info!("Client stats: {:?}", statistics);
    }

    // Keep the typed statistics view API unhooked for now. The active stats
    // delivery path remains the historical JSON callback via `stats_raw`.

    /// Receives the JSON-encoded statistics of the librdkafka client. To
    /// enable, the `statistics.interval.ms` configuration parameter must be
    /// specified.
    ///
    /// The default implementation calls [`ClientContext::stats`] with the
    /// decoded statistics, logging an error if the decoding fails.
    fn stats_raw(&self, statistics: &[u8]) {
        match serde_json::from_slice(statistics) {
            Ok(stats) => self.stats(stats),
            Err(e) => error!("Could not parse statistics JSON: {}", e),
        }
    }

    /// Receives global errors from the librdkafka client.
    ///
    /// The default implementation logs the error at the `error` log level.
    fn error(&self, error: KafkaError, reason: &str) {
        error!("librdkafka: {}: {}", error, reason);
    }

    /// Generates an OAuth token from the provided configuration.
    ///
    /// Override with an appropriate implementation when using the `OAUTHBEARER`
    /// SASL authentication mechanism. For this method to be called, you must
    /// also set [`ClientContext::ENABLE_REFRESH_OAUTH_TOKEN`] to true.
    ///
    /// The `fmt::Display` implementation of the returned error must not
    /// generate a message with an embedded null character.
    ///
    /// The default implementation always returns an error and is meant to
    /// be overridden.
    fn generate_oauth_token(
        &self,
        _oauthbearer_config: Option<&str>,
    ) -> Result<OAuthToken, Box<dyn Error>> {
        Err("Default implementation of generate_oauth_token must be overridden".into())
    }

    // NOTE: when adding a new method, remember to add it to the
    // FutureProducerContext as well.
    // https://github.com/rust-lang/rfcs/pull/1406 will maybe help in the
    // future.
}

/// An empty [`ClientContext`] that can be used when no customizations are
/// needed.
///
/// Uses the default callback implementations provided by `ClientContext`.
#[derive(Clone, Debug, Default)]
pub struct DefaultClientContext;

impl ClientContext for DefaultClientContext {}

//
// ********** CLIENT **********
//

/// A native rdkafka-sys client. This struct shouldn't be used directly. Use
/// higher level `Client` or producers and consumers.
// TODO(benesch): this should be `pub(crate)`.
pub struct NativeClient {
    ptr: NativePtr<RDKafka>,
}

unsafe impl KafkaDrop for RDKafka {
    const TYPE: &'static str = "client";
    const DROP: unsafe extern "C" fn(*mut Self) = rdsys::rd_kafka_destroy;
}

// The library is completely thread safe, according to the documentation.
unsafe impl Sync for NativeClient {}
unsafe impl Send for NativeClient {}

impl NativeClient {
    /// Wraps a pointer to an RDKafka object and returns a new NativeClient.
    pub(crate) unsafe fn from_ptr(ptr: *mut RDKafka) -> NativeClient {
        NativeClient {
            ptr: NativePtr::from_ptr(ptr).unwrap(),
        }
    }

    /// Returns the wrapped pointer to RDKafka.
    pub fn ptr(&self) -> *mut RDKafka {
        self.ptr.ptr()
    }

    pub(crate) fn rebalance_protocol(&self) -> RebalanceProtocol {
        let protocol = unsafe { rdsys::rd_kafka_rebalance_protocol(self.ptr()) };
        if protocol.is_null() {
            RebalanceProtocol::None
        } else {
            let protocol = unsafe { CStr::from_ptr(protocol) };
            match protocol.to_bytes() {
                b"NONE" => RebalanceProtocol::None,
                b"EAGER" => RebalanceProtocol::Eager,
                b"COOPERATIVE" => RebalanceProtocol::Cooperative,
                _ => unreachable!(),
            }
        }
    }
}

pub(crate) enum EventPollResult<T> {
    None,
    EventConsumed,
    Event(T),
}

impl<T> From<EventPollResult<T>> for Option<T> {
    fn from(val: EventPollResult<T>) -> Self {
        match val {
            EventPollResult::None | EventPollResult::EventConsumed => None,
            EventPollResult::Event(evt) => Some(evt),
        }
    }
}

// The opaque pointer is backed by Client.context. NativeClient is dropped first,
// and rd_kafka_destroy joins the background thread before that context is freed.
// The static bound also prevents dangling borrowed data if safe code leaks a client.
unsafe extern "C" fn background_oauth_refresh<C: ClientContext + 'static>(
    native: *mut RDKafka,
    config: *const c_char,
    opaque: *mut c_void,
) {
    // Never unwind through C. Report callback panics as refresh failures so
    // librdkafka can schedule a retry using its normal OAuth refresh policy.
    if let Err(payload) = catch_unwind(AssertUnwindSafe(|| {
        let context = unsafe { &*(opaque as *const C) };
        Client::<C>::refresh_oauth_token(native, context, config);
    })) {
        let message = oauth_refresh_panic_message(payload.as_ref());
        // librdkafka copies the full string, but its emitted error event uses a
        // 2048-byte buffer (including its prefix and trailing NUL).
        unsafe {
            rdsys::rd_kafka_oauthbearer_set_token_failure(native, message.as_ptr());
        }
    }
}

fn oauth_refresh_panic_message(payload: &(dyn std::any::Any + Send)) -> CString {
    let reason = payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("non-string panic payload");
    // Panic messages can contain NULs, unlike C strings. Preserve them visibly
    // without panicking again while handling an unwind at the FFI boundary.
    CString::new(format!("OAuth refresh callback panicked: {}", reason).replace('\0', "\\0"))
        .expect("embedded NULs have been escaped")
}

fn uses_context_oauth_refresh(config: &NativeClientConfig) -> KafkaResult<bool> {
    let protocol = config.get("security.protocol")?;
    // OIDC and unsecure JWT supply their own tokens. A context refresh callback
    // would override built-in OIDC and is mutually exclusive with unsecure JWT.
    Ok((protocol.eq_ignore_ascii_case("sasl_ssl")
        || protocol.eq_ignore_ascii_case("sasl_plaintext"))
        && config
            .get("sasl.mechanisms")?
            .eq_ignore_ascii_case("OAUTHBEARER")
        && config.get("sasl.oauthbearer.method")? != "oidc"
        && config.get("enable.sasl.oauthbearer.unsecure.jwt")? != "true")
}

/// A low-level rdkafka client.
///
/// This type is the basis of the consumers and producers in the [`consumer`]
/// and [`producer`] modules, respectively.
///
/// Typically you do not want to construct a `Client` directly, but instead
/// construct a consumer or producer. A `Client` can be used, however, when
/// only access to cluster metadata and watermarks is required.
///
/// [`consumer`]: crate::consumer
/// [`producer`]: crate::producer
pub struct Client<C: ClientContext = DefaultClientContext> {
    native: NativeClient,
    context: Arc<C>,
}

impl<C: ClientContext> Client<C> {
    /// Creates a new `Client` given a configuration, a client type and a context.
    pub fn new(
        config: &ClientConfig,
        native_config: NativeClientConfig,
        rd_kafka_type: RDKafkaType,
        context: C,
    ) -> KafkaResult<Client<C>> {
        Self::new_context_arc(config, native_config, rd_kafka_type, Arc::new(context))
    }

    /// Creates a new `Client` given a configuration, a client type and a context.
    pub(crate) fn new_context_arc(
        config: &ClientConfig,
        native_config: NativeClientConfig,
        rd_kafka_type: RDKafkaType,
        context: Arc<C>,
    ) -> KafkaResult<Client<C>> {
        let mut err_buf = ErrBuf::new();
        unsafe {
            rdsys::rd_kafka_conf_set_opaque(
                native_config.ptr(),
                Arc::as_ptr(&context) as *mut c_void,
            )
        };
        native_config.set("log.queue", "true")?;

        let client_ptr = unsafe {
            let native_config = ManuallyDrop::new(native_config);
            rdsys::rd_kafka_new(
                rd_kafka_type,
                native_config.ptr(),
                err_buf.as_mut_ptr(),
                err_buf.capacity(),
            )
        };
        trace!("Create new librdkafka client {:p}", client_ptr);

        if client_ptr.is_null() {
            return Err(KafkaError::ClientCreation(err_buf.to_string()));
        }

        // Own the handle before any fallible setup.
        let native = unsafe { NativeClient::from_ptr(client_ptr) };

        let ret = unsafe {
            rdsys::rd_kafka_set_log_queue(client_ptr, rdsys::rd_kafka_queue_get_main(client_ptr))
        };
        if ret.is_error() {
            return Err(KafkaError::Global(ret.into()));
        }
        unsafe { rdsys::rd_kafka_set_log_level(client_ptr, config.log_level as i32) };

        Ok(Client { native, context })
    }

    // Only consumer construction enables background OAuth callbacks. Keep the
    // static bound on this path, without changing producer or admin contexts.
    pub(crate) fn new_consumer(
        config: &ClientConfig,
        native_config: NativeClientConfig,
        context: C,
    ) -> KafkaResult<Client<C>>
    where
        C: 'static,
    {
        let background_oauth =
            C::ENABLE_REFRESH_OAUTH_TOKEN && uses_context_oauth_refresh(&native_config)?;
        if background_oauth {
            // Route only SASL refresh work off the application queue. Messages,
            // rebalances, commits and other callbacks retain their normal path.
            unsafe {
                rdsys::rd_kafka_conf_enable_sasl_queue(native_config.ptr(), 1);
                rdsys::rd_kafka_conf_set_oauthbearer_token_refresh_cb(
                    native_config.ptr(),
                    Some(background_oauth_refresh::<C>),
                );
            }
        }
        let client = Self::new(
            config,
            native_config,
            RDKafkaType::RD_KAFKA_CONSUMER,
            context,
        )?;
        // On an enablement error, Client drops the native handle (joining its
        // threads) before releasing the context backing the opaque pointer.
        if background_oauth {
            let error = unsafe {
                RDKafkaError::from_ptr(rdsys::rd_kafka_sasl_background_callbacks_enable(
                    client.native_ptr(),
                ))
            };
            if error.is_error() {
                return Err(KafkaError::ClientCreation(error.string()));
            }
        }

        Ok(client)
    }

    /// Returns a reference to the native rdkafka-sys client.
    pub fn native_client(&self) -> &NativeClient {
        &self.native
    }

    /// Returns a pointer to the native rdkafka-sys client.
    pub fn native_ptr(&self) -> *mut RDKafka {
        self.native.ptr.ptr()
    }

    /// Returns a reference to the context.
    pub fn context(&self) -> &Arc<C> {
        &self.context
    }

    pub(crate) fn poll_event<T: Into<Timeout>>(
        &self,
        queue: &NativeQueue,
        timeout: T,
    ) -> EventPollResult<NativeEvent> {
        let event = unsafe { NativeEvent::from_ptr(queue.poll(timeout)) };
        if let Some(ev) = event {
            let evtype = unsafe { rdsys::rd_kafka_event_type(ev.ptr()) };
            match evtype {
                rdsys::RD_KAFKA_EVENT_LOG => {
                    self.handle_log_event(ev.ptr());
                    return EventPollResult::EventConsumed;
                }
                rdsys::RD_KAFKA_EVENT_STATS => {
                    self.handle_stats_event(ev.ptr());
                    return EventPollResult::EventConsumed;
                }
                rdsys::RD_KAFKA_EVENT_ERROR => {
                    // rdkafka reports consumer errors via RD_KAFKA_EVENT_ERROR but producer errors gets
                    // embedded on the ack returned via RD_KAFKA_EVENT_DR. Hence we need to return this event
                    // for the consumer case in order to return the error to the user.
                    self.handle_error_event(ev.ptr());
                    return EventPollResult::Event(ev);
                }
                rdsys::RD_KAFKA_EVENT_OAUTHBEARER_TOKEN_REFRESH => {
                    if C::ENABLE_REFRESH_OAUTH_TOKEN {
                        self.handle_oauth_refresh_event(ev.ptr());
                    }
                    return EventPollResult::EventConsumed;
                }
                _ => {
                    return EventPollResult::Event(ev);
                }
            }
        }
        EventPollResult::None
    }

    fn handle_log_event(&self, event: *mut RDKafkaEvent) {
        let mut fac: *const c_char = std::ptr::null();
        let mut str_: *const c_char = std::ptr::null();
        let mut level: i32 = 0;
        let result = unsafe { rdsys::rd_kafka_event_log(event, &mut fac, &mut str_, &mut level) };
        if result == 0 {
            let fac = unsafe { CStr::from_ptr(fac).to_string_lossy() };
            let log_message = unsafe { CStr::from_ptr(str_).to_string_lossy() };
            // Allocate a string big enough to hold all contexts if necessary
            let mut raw_contents = vec![0u8; *DEBUG_CONTEXTS_BYTE_LENGTH + 1];
            let result = unsafe {
                rdsys::rd_kafka_event_debug_contexts(
                    event,
                    raw_contents.as_mut_ptr() as *mut c_char,
                    *DEBUG_CONTEXTS_BYTE_LENGTH,
                )
            };
            let contexts = if result == 0 {
                let cstr = CStr::from_bytes_until_nul(&raw_contents)
                    .expect("raw_contents always contains a NULL byte");
                cstr.to_string_lossy().into_owned()
            } else {
                String::new()
            };
            self.context().log(LogRecord::new(
                level.into(),
                fac.trim().into(),
                log_message.trim().into(),
                contexts,
            ));
        }
    }

    fn handle_stats_event(&self, event: *mut RDKafkaEvent) {
        // Keep stats delivery on the historical JSON callback path so binaries
        // do not gain a hard dependency on newer librdkafka symbols.
        let json = unsafe { CStr::from_ptr(rdsys::rd_kafka_event_stats(event)) };
        self.context().stats_raw(json.to_bytes());
    }

    fn handle_error_event(&self, event: *mut RDKafkaEvent) {
        let rdkafka_err = unsafe { rdsys::rd_kafka_event_error(event) };
        let error = KafkaError::Global(rdkafka_err.into());
        let reason =
            unsafe { CStr::from_ptr(rdsys::rd_kafka_event_error_string(event)).to_string_lossy() };
        self.context().error(error, reason.trim());
    }

    fn handle_oauth_refresh_event(&self, event: *mut RDKafkaEvent) {
        let oauthbearer_config = unsafe { rdsys::rd_kafka_event_config_string(event) };
        Self::refresh_oauth_token(self.native_ptr(), self.context(), oauthbearer_config);
    }

    fn refresh_oauth_token(native: *mut RDKafka, context: &C, oauthbearer_config: *const c_char) {
        let res: Result<_, Box<dyn Error>> = (|| {
            let oauthbearer_config = match oauthbearer_config.is_null() {
                true => None,
                false => unsafe { Some(util::cstr_to_owned(oauthbearer_config)) },
            };
            let token_info = context.generate_oauth_token(oauthbearer_config.as_deref())?;
            let token = CString::new(token_info.token)?;
            let principal_name = CString::new(token_info.principal_name)?;
            Ok((token, principal_name, token_info.lifetime_ms))
        })();
        match res {
            Ok((token, principal_name, lifetime_ms)) => {
                let mut err_buf = ErrBuf::new();
                let code = unsafe {
                    rdkafka_sys::rd_kafka_oauthbearer_set_token(
                        native,
                        token.as_ptr(),
                        lifetime_ms,
                        principal_name.as_ptr(),
                        ptr::null_mut(),
                        0,
                        err_buf.as_mut_ptr(),
                        err_buf.capacity(),
                    )
                };
                if code == RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR {
                    debug!("successfully set refreshed OAuth token");
                } else {
                    debug!(
                        "failed to set refreshed OAuth token (code {:?}): {}",
                        code, err_buf
                    );
                    unsafe {
                        rdkafka_sys::rd_kafka_oauthbearer_set_token_failure(
                            native,
                            err_buf.as_mut_ptr(),
                        )
                    };
                }
            }
            Err(e) => {
                debug!("failed to refresh OAuth token: {}", e);
                let message = match CString::new(e.to_string()) {
                    Ok(message) => message,
                    Err(e) => {
                        error!(
                            "error message generated while refreshing OAuth token has embedded null character: {}",
                            e
                        );
                        CString::new(
                            "error while refreshing OAuth token has embedded null character",
                        )
                        .expect("known to be a valid CString")
                    }
                };
                unsafe {
                    rdkafka_sys::rd_kafka_oauthbearer_set_token_failure(native, message.as_ptr())
                };
            }
        }
    }

    /// Returns the metadata information for the specified topic, or for all topics in the cluster
    /// if no topic is specified.
    pub fn fetch_metadata<T: Into<Timeout>>(
        &self,
        topic: Option<&str>,
        timeout: T,
    ) -> KafkaResult<Metadata> {
        let mut metadata_ptr: *const RDKafkaMetadata = ptr::null_mut();
        let (flag, native_topic) = if let Some(topic_name) = topic {
            (0, Some(self.native_topic(topic_name)?))
        } else {
            (1, None)
        };
        trace!("Starting metadata fetch");
        let ret = unsafe {
            rdsys::rd_kafka_metadata(
                self.native_ptr(),
                flag,
                native_topic.map(|t| t.ptr()).unwrap_or_else(ptr::null_mut),
                &mut metadata_ptr as *mut *const RDKafkaMetadata,
                timeout.into().as_millis(),
            )
        };
        trace!("Metadata fetch completed");
        if ret.is_error() {
            return Err(KafkaError::MetadataFetch(ret.into()));
        }

        Ok(unsafe { Metadata::from_ptr(metadata_ptr) })
    }

    /// Returns high and low watermark for the specified topic and partition.
    pub fn fetch_watermarks<T: Into<Timeout>>(
        &self,
        topic: &str,
        partition: i32,
        timeout: T,
    ) -> KafkaResult<(i64, i64)> {
        let mut low = -1;
        let mut high = -1;
        let topic_c = CString::new(topic.to_string())?;
        let ret = unsafe {
            rdsys::rd_kafka_query_watermark_offsets(
                self.native_ptr(),
                topic_c.as_ptr(),
                partition,
                &mut low as *mut i64,
                &mut high as *mut i64,
                timeout.into().as_millis(),
            )
        };
        if ret.is_error() {
            return Err(KafkaError::MetadataFetch(ret.into()));
        }
        Ok((low, high))
    }

    /// Returns the cluster identifier option or None if the cluster identifier is null
    pub fn fetch_cluster_id<T: Into<Timeout>>(&self, timeout: T) -> Option<String> {
        let cluster_id =
            unsafe { rdsys::rd_kafka_clusterid(self.native_ptr(), timeout.into().as_millis()) };
        if cluster_id.is_null() {
            return None;
        }
        let buf = unsafe { CStr::from_ptr(cluster_id).to_bytes() };
        String::from_utf8(buf.to_vec()).ok()
    }

    /// Returns the group membership information for the given group. If no group is
    /// specified, all groups will be returned.
    pub fn fetch_group_list<T: Into<Timeout>>(
        &self,
        group: Option<&str>,
        timeout: T,
    ) -> KafkaResult<GroupList> {
        // Careful with group_c getting freed before time
        let group_c = CString::new(group.map_or("".to_string(), ToString::to_string))?;
        let group_c_ptr = if group.is_some() {
            group_c.as_ptr()
        } else {
            ptr::null_mut()
        };
        let mut group_list_ptr: *const RDKafkaGroupList = ptr::null_mut();
        trace!("Starting group list fetch");
        let ret = unsafe {
            rdsys::rd_kafka_list_groups(
                self.native_ptr(),
                group_c_ptr,
                &mut group_list_ptr as *mut *const RDKafkaGroupList,
                timeout.into().as_millis(),
            )
        };
        trace!("Group list fetch completed");
        if ret.is_error() {
            return Err(KafkaError::GroupListFetch(ret.into()));
        }

        Ok(unsafe { GroupList::from_ptr(group_list_ptr) })
    }

    /// Returns the first fatal error set on this client instance, or `None` if
    /// no fatal error has occurred.
    ///
    /// This function is intended to be used with idempotent producers, where
    /// some errors must logically be considered fatal to retain consistency.
    pub fn fatal_error(&self) -> Option<(RDKafkaErrorCode, String)> {
        let mut err_buf = ErrBuf::new();
        let code = unsafe {
            rdsys::rd_kafka_fatal_error(self.native_ptr(), err_buf.as_mut_ptr(), err_buf.capacity())
        };
        if code == RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR {
            None
        } else {
            Some((code.into(), err_buf.to_string()))
        }
    }

    /// If this client was configured with `test.mock.num.brokers`,
    /// this will return a [`MockCluster`] instance associated with this client,
    /// otherwise `None` is returned.
    ///
    /// [`MockCluster`]: crate::mocking::MockCluster
    pub fn mock_cluster(&self) -> Option<MockCluster<'_, C>> {
        MockCluster::from_client(self)
    }

    /// Returns a NativeTopic from the current client. The NativeTopic shouldn't outlive the client
    /// it was generated from.
    pub(crate) fn native_topic(&self, topic: &str) -> KafkaResult<NativeTopic> {
        let topic_c = CString::new(topic.to_string())?;
        Ok(unsafe {
            NativeTopic::from_ptr(rdsys::rd_kafka_topic_new(
                self.native_ptr(),
                topic_c.as_ptr(),
                ptr::null_mut(),
            ))
            .unwrap()
        })
    }

    /// Returns a NativeQueue from the current client. The NativeQueue shouldn't
    /// outlive the client it was generated from.
    pub(crate) fn new_native_queue(&self) -> NativeQueue {
        unsafe { NativeQueue::from_ptr(rdsys::rd_kafka_queue_new(self.native_ptr())).unwrap() }
    }

    pub(crate) fn consumer_queue(&self) -> Option<NativeQueue> {
        unsafe { NativeQueue::from_ptr(rdsys::rd_kafka_queue_get_consumer(self.native_ptr())) }
    }

    /// Returns a NativeQueue for the main librdkafka event queue from the current client.
    pub(crate) fn main_queue(&self) -> NativeQueue {
        unsafe { NativeQueue::from_ptr(rdsys::rd_kafka_queue_get_main(self.native_ptr())).unwrap() }
    }
}

pub(crate) type NativeTopic = NativePtr<RDKafkaTopic>;

unsafe impl KafkaDrop for RDKafkaTopic {
    const TYPE: &'static str = "native topic";
    const DROP: unsafe extern "C" fn(*mut Self) = rdsys::rd_kafka_topic_destroy;
}

unsafe impl Send for NativeTopic {}
unsafe impl Sync for NativeTopic {}

pub(crate) type NativeQueue = NativePtr<RDKafkaQueue>;

unsafe impl KafkaDrop for RDKafkaQueue {
    const TYPE: &'static str = "queue";
    const DROP: unsafe extern "C" fn(*mut Self) = rdsys::rd_kafka_queue_destroy;
}

// The library is completely thread safe, according to the documentation.
unsafe impl Sync for NativeQueue {}
unsafe impl Send for NativeQueue {}

impl NativeQueue {
    pub fn poll<T: Into<Timeout>>(&self, t: T) -> *mut RDKafkaEvent {
        unsafe { rdsys::rd_kafka_queue_poll(self.ptr(), t.into().as_millis()) }
    }
}

/// A generated OAuth token and its associated metadata.
///
/// When using the `OAUTHBEARER` SASL authentication method, this type is
/// returned from [`ClientContext::generate_oauth_token`]. The token and
/// principal name must not contain embedded null characters.
///
/// Specifying SASL extensions is not currently supported.
pub struct OAuthToken {
    /// The token value to set.
    pub token: String,
    /// The Kafka principal name associated with the token.
    pub principal_name: String,
    /// When the token expires, in number of milliseconds since the Unix epoch.
    pub lifetime_ms: i64,
}

#[cfg(test)]
mod tests {
    // Just call everything to test there no panics by default, behavior
    // is tested in the integrations tests.

    use super::*;
    use crate::config::ClientConfig;
    use crate::consumer::{BaseConsumer, ConsumerContext};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::mpsc::{self, Sender};
    use std::thread;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    struct BackgroundOAuthContext {
        calls: Sender<thread::ThreadId>,
        errors: Option<Sender<String>>,
        attempts: AtomicUsize,
        first_failure: &'static str,
        dropped: Arc<AtomicBool>,
    }

    impl ClientContext for BackgroundOAuthContext {
        const ENABLE_REFRESH_OAUTH_TOKEN: bool = true;

        fn error(&self, _: KafkaError, reason: &str) {
            if let Some(errors) = &self.errors {
                let _ = errors.send(reason.to_owned());
            }
        }

        fn generate_oauth_token(&self, _: Option<&str>) -> Result<OAuthToken, Box<dyn Error>> {
            self.calls.send(thread::current().id()).unwrap();
            if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                match self.first_failure {
                    "error" => return Err("temporary token fetch failure".into()),
                    "panic" => panic!("test callback panic"),
                    _ => {}
                }
            }
            Ok(OAuthToken {
                token: "test-token".into(),
                principal_name: "test-principal".into(),
                lifetime_ms: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64
                    + 2000,
            })
        }
    }

    impl ConsumerContext for BackgroundOAuthContext {}

    impl Drop for BackgroundOAuthContext {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

    #[test]
    fn oauth_panic_messages_preserve_payloads() {
        for (payload, expected) in [
            (
                Box::new("static panic") as Box<dyn std::any::Any + Send>,
                "static panic",
            ),
            (Box::new(String::from("owned panic")), "owned panic"),
            (Box::new("before\0after"), "before\\0after"),
            (Box::new(42), "non-string panic payload"),
        ] {
            assert_eq!(
                oauth_refresh_panic_message(payload.as_ref())
                    .to_str()
                    .unwrap(),
                format!("OAuth refresh callback panicked: {}", expected)
            );
        }
        // The token-failure API accepts the full message even though native
        // error events truncate it later.
        let long_message = "x".repeat(4096);
        assert_eq!(
            oauth_refresh_panic_message(&long_message).to_str().unwrap(),
            format!("OAuth refresh callback panicked: {}", long_message)
        );
    }

    #[test]
    #[cfg(feature = "ssl")]
    fn background_oauth_reports_panic_message() {
        let (sender, _receiver) = mpsc::channel();
        let (errors, received_errors) = mpsc::channel();
        let consumer: BaseConsumer<BackgroundOAuthContext> = ClientConfig::new()
            .set("security.protocol", "SASL_PLAINTEXT")
            .set("sasl.mechanisms", "OAUTHBEARER")
            .create_with_context(BackgroundOAuthContext {
                calls: sender,
                errors: Some(errors),
                attempts: AtomicUsize::new(0),
                first_failure: "panic",
                dropped: Arc::new(AtomicBool::new(false)),
            })
            .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let _ = consumer.poll(Duration::from_millis(100));
            if let Some(reason) = received_errors
                .try_iter()
                .find(|reason| reason.contains("OAuth refresh callback panicked"))
            {
                assert!(reason.ends_with("OAuth refresh callback panicked: test callback panic"));
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "panic error was not delivered"
            );
        }
    }

    #[cfg(feature = "ssl")]
    fn assert_builtin_oauth_source(config: &ClientConfig) {
        let (sender, receiver) = mpsc::channel();
        let consumer: BaseConsumer<BackgroundOAuthContext> = config
            .create_with_context(BackgroundOAuthContext {
                calls: sender,
                errors: None,
                attempts: AtomicUsize::new(0),
                first_failure: "none",
                dropped: Arc::new(AtomicBool::new(false)),
            })
            .unwrap();
        // Construction and the initial poll must leave the explicitly configured
        // native token source in control.
        assert!(receiver.recv_timeout(Duration::from_millis(100)).is_err());
        let _ = consumer.poll(Duration::from_millis(100));
        assert!(receiver.try_recv().is_err());
        drop(consumer);
    }

    #[test]
    #[cfg(feature = "ssl")]
    fn background_oauth_preserves_unsecure_jwt() {
        assert_builtin_oauth_source(
            ClientConfig::new()
                .set("security.protocol", "SASL_PLAINTEXT")
                .set("sasl.mechanisms", "OAUTHBEARER")
                .set("enable.sasl.oauthbearer.unsecure.jwt", "true")
                .set("sasl.oauthbearer.config", "principal=test-principal"),
        );
    }

    #[test]
    #[cfg(all(feature = "ssl", feature = "curl"))]
    fn background_oauth_preserves_oidc() {
        assert_builtin_oauth_source(
            ClientConfig::new()
                .set("security.protocol", "SASL_PLAINTEXT")
                .set("sasl.mechanisms", "OAUTHBEARER")
                .set("sasl.oauthbearer.method", "oidc")
                .set("sasl.oauthbearer.client.id", "test-client")
                .set("sasl.oauthbearer.client.secret", "test-secret")
                .set(
                    "sasl.oauthbearer.token.endpoint.url",
                    "http://127.0.0.1:1/token",
                ),
        );
    }

    #[test]
    #[cfg(feature = "ssl")]
    fn background_oauth_refreshes_and_retries_without_polling() {
        for first_failure in ["none", "error", "panic"] {
            let (sender, receiver) = mpsc::channel();
            let dropped = Arc::new(AtomicBool::new(false));
            let consumer: BaseConsumer<BackgroundOAuthContext> = ClientConfig::new()
                .set("security.protocol", "SASL_PLAINTEXT")
                .set("sasl.mechanisms", "OAUTHBEARER")
                .create_with_context(BackgroundOAuthContext {
                    calls: sender,
                    errors: None,
                    attempts: AtomicUsize::new(0),
                    first_failure,
                    dropped: Arc::clone(&dropped),
                })
                .unwrap();
            // Never poll the consumer: initialization, expiry refresh and error
            // recovery must all progress while the application is backpressured.
            assert_ne!(
                receiver.recv_timeout(Duration::from_secs(5)).unwrap(),
                thread::current().id()
            );
            // librdkafka schedules a failed refresh retry after ten seconds.
            assert_ne!(
                receiver.recv_timeout(Duration::from_secs(15)).unwrap(),
                thread::current().id()
            );
            assert!(!dropped.load(Ordering::SeqCst));
            drop(consumer);
            assert!(dropped.load(Ordering::SeqCst));
        }
    }

    #[test]
    fn background_oauth_does_not_run_for_plaintext_consumers() {
        let (sender, receiver) = mpsc::channel();
        let consumer: BaseConsumer<BackgroundOAuthContext> = ClientConfig::new()
            .create_with_context(BackgroundOAuthContext {
                calls: sender,
                errors: None,
                attempts: AtomicUsize::new(0),
                first_failure: "none",
                dropped: Arc::new(AtomicBool::new(false)),
            })
            .unwrap();
        assert!(receiver.recv_timeout(Duration::from_millis(100)).is_err());
        drop(consumer);
    }

    #[test]
    fn producer_and_admin_still_accept_borrowed_contexts() {
        use crate::admin::AdminClient;
        use crate::producer::{BaseProducer, DeliveryResult, Producer, ProducerContext};

        struct BorrowedContext<'a>(&'a str);
        impl ClientContext for BorrowedContext<'_> {}
        impl ProducerContext for BorrowedContext<'_> {
            type DeliveryOpaque = ();
            fn delivery(&self, _: &DeliveryResult<'_>, _: ()) {}
        }

        let data = String::from("borrowed context");
        let producer: BaseProducer<BorrowedContext<'_>> = ClientConfig::new()
            .create_with_context(BorrowedContext(&data))
            .unwrap();
        let admin: AdminClient<BorrowedContext<'_>> = ClientConfig::new()
            .create_with_context(BorrowedContext(&data))
            .unwrap();
        assert_eq!(producer.client().context().0, data);
        assert_eq!(admin.inner().context().0, data);
    }

    #[test]
    fn test_client() {
        let config = ClientConfig::new();
        let native_config = config.create_native_config().unwrap();
        let client = Client::new(
            &config,
            native_config,
            RDKafkaType::RD_KAFKA_PRODUCER,
            DefaultClientContext,
        )
        .unwrap();
        assert!(!client.native_ptr().is_null());
    }
}
