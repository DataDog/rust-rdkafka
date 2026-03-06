//! Client and broker statistics.
//!
//! These statistics are collected automatically by librdkafka when the client
//! is configured with a non-zero `statistics.interval.ms`. They are made
//! available via the [`ClientContext::stats`] callback.
//!
//! Refer to the [librdkafka statistics documentation][librdkafka-stats] for
//! details.
//!
//! [`ClientContext::stats`]: crate::ClientContext::stats
//! [librdkafka-stats]: https://github.com/edenhill/librdkafka/blob/master/STATISTICS.md

use std::collections::HashMap;
use std::os::raw::c_char;

use rdkafka_sys as rdsys;
use serde::{Deserialize, Serialize};

/// Overall statistics.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct Statistics {
    /// The name of the librdkafka handle.
    pub name: String,
    /// The configured `client.id`.
    pub client_id: String,
    /// The instance type (producer or consumer).
    #[serde(rename = "type")]
    pub client_type: String,
    /// The current value of librdkafka's internal monotonic clock, in
    // microseconds since start.
    pub ts: i64,
    /// Wall clock time, in seconds since the Unix epoch.
    pub time: i64,
    /// Time since this client instance was created, in microseconds.
    pub age: i64,
    /// The number of operations (callbacks, events, etc.) waiting in queue.
    pub replyq: i64,
    /// The current number of messages in producer queues.
    pub msg_cnt: u64,
    /// The current total size of messages in producer queues.
    pub msg_size: u64,
    /// The maximum number of messages allowed in the producer queues.
    pub msg_max: u64,
    /// The maximum total size of messages allowed in the producer queues.
    pub msg_size_max: u64,
    /// The total number of requests sent to brokers.
    pub tx: i64,
    /// The total number of bytes transmitted to brokers.
    pub tx_bytes: i64,
    /// The total number of responses received from brokers.
    pub rx: i64,
    /// The total number of bytes received from brokers.
    pub rx_bytes: i64,
    /// The total number of messages transmitted (produced) to brokers.
    pub txmsgs: i64,
    /// The total number of bytes transmitted (produced) to brokers.
    pub txmsg_bytes: i64,
    /// The total number of messages consumed from brokers, not including
    /// ignored messages.
    pub rxmsgs: i64,
    /// The total number of bytes (including framing) consumed from brokers.
    pub rxmsg_bytes: i64,
    /// Internal tracking of legacy vs. new consumer API state.
    pub simple_cnt: i64,
    /// Number of topics in the metadata cache.
    pub metadata_cache_cnt: i64,
    /// Per-broker statistics.
    pub brokers: HashMap<String, Broker>,
    /// Per-topic statistics.
    pub topics: HashMap<String, Topic>,
    /// Consumer group statistics.
    pub cgrp: Option<ConsumerGroup>,
    /// Exactly-once semantics and idempotent producer statistics.
    pub eos: Option<ExactlyOnceSemantics>,
}

/// Per-broker statistics.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct Broker {
    /// The broker hostname, port, and ID, in the form `HOSTNAME:PORT/ID`.
    pub name: String,
    /// The broker ID (-1 for bootstraps).
    pub nodeid: i32,
    /// The broker hostname and port.
    pub nodename: String,
    /// The broker source (learned, configured, internal, or logical).
    pub source: String,
    /// The broker state (INIT, DOWN, CONNECT, AUTH, APIVERSION_QUERY,
    /// AUTH_HANDSHAKE, UP, UPDATE).
    pub state: String,
    /// The time since the last broker state change, in microseconds.
    pub stateage: i64,
    /// The number of requests awaiting transmission to the broker.
    pub outbuf_cnt: i64,
    /// The number of messages awaiting transmission to the broker.
    pub outbuf_msg_cnt: i64,
    /// The number of requests in-flight to the broker that are awaiting a
    /// response.
    pub waitresp_cnt: i64,
    /// The number of messages in-flight to the broker that are awaiting a
    /// response.
    pub waitresp_msg_cnt: i64,
    /// The total number of requests sent to the broker.
    pub tx: u64,
    /// The total number of bytes sent to the broker.
    pub txbytes: u64,
    /// The total number of transmission errors.
    pub txerrs: u64,
    /// The total number of request retries.
    pub txretries: u64,
    /// Microseconds since last socket send, or -1 if no sends yet for the
    /// current connection.
    pub txidle: i64,
    /// The total number of requests that timed out.
    pub req_timeouts: u64,
    /// The total number of responses received from the broker.
    pub rx: u64,
    /// The total number of bytes received from the broker.
    pub rxbytes: u64,
    /// The total number of receive errors.
    pub rxerrs: u64,
    /// The number of unmatched correlation IDs in response, typically for
    /// timed out requests.
    pub rxcorriderrs: u64,
    /// The total number of partial message sets received. The broker may return
    /// partial responses if the full message set could not fit in the remaining
    /// fetch response size.
    pub rxpartial: u64,
    /// Microseconds since last socket receive, or -1 if no receives yet for the
    /// current connection.
    pub rxidle: i64,
    /// Request type counters. The object key is the name of the request type
    /// and the value is the number of requests of that type that have been
    /// sent.
    pub req: HashMap<String, i64>,
    /// The total number of decompression buffer size increases.
    pub zbuf_grow: u64,
    /// The total number of buffer size increases (deprecated and unused).
    pub buf_grow: u64,
    /// The number of broker thread poll wakeups.
    pub wakeups: Option<u64>,
    /// The number of connection attempts, including successful and failed
    /// attempts, and name resolution failures.
    pub connects: Option<i64>,
    /// The number of disconnections, whether triggered by the broker, the
    /// network, the load balancer, or something else.
    pub disconnects: Option<i64>,
    /// Rolling window statistics for the internal producer queue latency, in
    /// microseconds.
    pub int_latency: Option<Window>,
    /// Rolling window statistics for the internal request queue latency, in
    /// microseconds.
    ///
    /// This is the time between when a request is enqueued on the transmit
    /// (outbuf) queue and the time the request is written to the TCP socket.
    /// Additional buffering and latency may be incurred by the TCP stack and
    /// network.
    pub outbuf_latency: Option<Window>,
    /// Rolling window statistics for the broker latency/round-trip time,
    /// in microseconds.
    pub rtt: Option<Window>,
    /// Rolling window statistics for the broker throttling time, in
    /// milliseconds.
    pub throttle: Option<Window>,
    /// The partitions that are handled by this broker handle.
    pub toppars: HashMap<String, TopicPartition>,

    // The following produce statistics are available as of librdkafka 2.10.3
    /// Rolling window statistics for partitions per ProduceRequest
    pub produce_partitions: Option<Window>,
    /// Rolling window statistics for messages per ProduceRequest
    pub produce_messages: Option<Window>,
    /// Rolling window statistics for bytes per produce_request
    pub produce_reqsize: Option<Window>,
    /// Rolling window statistics for ProduceRequest fill ratio (permille)
    pub produce_fill: Option<Window>,
    /// Rolling window statistics for how long it takes for a batch to go
    /// from ready to the xmit_queue
    pub batch_wait: Option<Window>,

    /// Adaptive batching statistics (only present when adaptive batching is enabled)
    pub adaptive: Option<AdaptiveBatching>,
}

/// Adaptive batching statistics.
///
/// These statistics are only populated when adaptive batching is enabled
/// (`adaptive.batching.enable = true`).
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct AdaptiveBatching {
    /// Whether adaptive batching is enabled.
    pub enabled: bool,
    /// Current adaptive linger time in microseconds.
    pub linger_us: i64,
    /// Current adaptive batch max bytes.
    pub batch_max_bytes: i64,
    /// Combined congestion score (0.0 = no congestion).
    pub congestion: f64,
    /// RTT-based congestion component (Vegas-style).
    pub rtt_congestion: f64,
    /// Internal latency congestion component.
    pub int_lat_congestion: f64,
    /// RTT baseline in microseconds (minimum observed).
    pub rtt_base_us: i64,
    /// Current smoothed RTT in microseconds.
    pub rtt_current_us: i64,
    /// Internal latency baseline in microseconds.
    pub int_lat_base_us: i64,
    /// Current smoothed internal latency in microseconds.
    pub int_lat_current_us: i64,
    /// Count of slow-down adjustments (congestion detected).
    pub adjustments_up: i64,
    /// Count of speed-up adjustments (congestion cleared).
    pub adjustments_down: i64,
    /// Count of backlog drain cycles (speeding up due to queue backlog).
    pub backlog_drain_events: i64,
}

/// Rolling window statistics.
///
/// These values are not exact; they are sampled estimates maintained by an
/// HDR histogram in librdkafka.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct Window {
    /// The smallest value.
    pub min: i64,
    /// The largest value.
    pub max: i64,
    /// The mean value.
    pub avg: i64,
    /// The sum of all values.
    pub sum: i64,
    /// The total number of values.
    pub cnt: i64,
    /// The standard deviation.
    pub stddev: i64,
    /// The memory size of the underlying HDR histogram.
    pub hdrsize: i64,
    /// The 50th percentile.
    pub p50: i64,
    /// The 75th percentile.
    pub p75: i64,
    /// The 90th percentile.
    pub p90: i64,
    /// The 95th percentile.
    pub p95: i64,
    /// The 99th percentile.
    pub p99: i64,
    /// The 99.99th percentile.
    pub p99_99: i64,
    /// The number of values not included in the underlying histogram because
    /// they were out of range.
    pub outofrange: i64,
}

/// A topic and partition specifier.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct TopicPartition {
    /// The name of the topic.
    pub topic: String,
    /// The ID of the partition.
    pub partition: i32,
}

/// Per-topic statistics.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct Topic {
    /// The name of the topic.
    pub topic: String,
    /// The age of the client's metadata for this topic, in milliseconds.
    pub metadata_age: i64,
    /// Rolling window statistics for batch sizes, in bytes.
    pub batchsize: Window,
    /// Rolling window statistics for batch message counts.
    pub batchcnt: Window,
    /// Per-partition statistics.
    pub partitions: HashMap<i32, Partition>,
}

/// Per-partition statistics.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct Partition {
    /// The partition ID.
    pub partition: i32,
    /// The ID of the broker from which messages are currently being fetched.
    pub broker: i32,
    /// The broker ID of the leader.
    pub leader: i32,
    /// Whether the partition is explicitly desired by the application.
    pub desired: bool,
    /// Whether the partition is not seen in the topic metadata from the broker.
    pub unknown: bool,
    /// The number of messages waiting to be produced in the first-level queue.
    pub msgq_cnt: i64,
    /// The number of bytes waiting to be produced in the first-level queue.
    pub msgq_bytes: u64,
    /// The number of messages ready to be produced in the transmit queue.
    pub xmit_msgq_cnt: i64,
    /// The number of bytes ready to be produced in the transmit queue.
    pub xmit_msgq_bytes: u64,
    /// The number of prefetched messages in the fetch queue.
    pub fetchq_cnt: i64,
    /// The number of bytes in the fetch queue.
    pub fetchq_size: u64,
    /// The consumer fetch state for this partition (none, stopping, stopped,
    /// offset-query, offset-wait, active).
    pub fetch_state: String,
    /// The current/last logical offset query.
    pub query_offset: i64,
    /// The next offset to fetch.
    pub next_offset: i64,
    /// The offset of the last message passed to the application, plus one.
    pub app_offset: i64,
    /// The offset to be committed.
    pub stored_offset: i64,
    /// The last committed offset.
    pub committed_offset: i64,
    /// The last offset for which partition EOF was signaled.
    pub eof_offset: i64,
    /// The low watermark offset on the broker.
    pub lo_offset: i64,
    /// The high watermark offset on the broker.
    pub hi_offset: i64,
    /// The last stable offset on the broker.
    pub ls_offset: i64,
    /// The difference between `hi_offset` and `committed_offset`.
    pub consumer_lag: i64,
    /// The difference between `hi_offset` and `stored_offset`.
    pub consumer_lag_stored: i64,
    /// The total number of messages transmitted (produced).
    pub txmsgs: u64,
    /// The total number of bytes transmitted (produced).
    pub txbytes: u64,
    /// The total number of messages consumed, not included ignored messages.
    pub rxmsgs: u64,
    /// The total bytes consumed.
    pub rxbytes: u64,
    /// The total number of messages received, for consumers, or the total
    /// number of messages produced, for producers.
    pub msgs: u64,
    /// The number of dropped outdated messages.
    pub rx_ver_drops: u64,
    /// The current number of messages in flight to or from the broker.
    pub msgs_inflight: i64,
    /// The next expected acked sequence number, for idempotent producers.
    pub next_ack_seq: i64,
    /// The next expected errored sequence number, for idempotent producers.
    pub next_err_seq: i64,
    /// The last acked internal message ID, for idempotent producers.
    pub acked_msgid: u64,
}

/// Consumer group manager statistics.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct ConsumerGroup {
    /// The local consumer group handler's state.
    pub state: String,
    /// The time elapsed since the last state change, in milliseconds.
    pub stateage: i64,
    /// The local consumer group hander's join state.
    pub join_state: String,
    /// The time elapsed since the last rebalance (assign or revoke), in
    /// milliseconds.
    pub rebalance_age: i64,
    /// The total number of rebalances (assign or revoke).
    pub rebalance_cnt: i64,
    /// The reason for the last rebalance.
    ///
    /// This string will be empty if no rebalances have occurred.
    pub rebalance_reason: String,
    /// The partition count for the current assignment.
    pub assignment_size: i32,
}

/// Exactly-once semantics statistics.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct ExactlyOnceSemantics {
    /// The current idempotent producer state.
    pub idemp_state: String,
    /// THe time elapsed since the last idempotent producer state change, in
    /// milliseconds.
    pub idemp_stateage: i64,
    /// The current transactional producer state.
    pub txn_state: String,
    /// The time elapsed since the last transactional producer state change, in
    /// milliseconds.
    pub txn_stateage: i64,
    /// Whether the transactional state allows enqueing (producing) new
    /// messages.
    pub txn_may_enq: bool,
    /// The currently assigned producer ID, or -1.
    pub producer_id: i64,
    /// The current epoch, or -1.
    pub producer_epoch: i64,
    /// The number of producer ID assignments.
    pub epoch_cnt: i64,
}

// ============================================================================
// Native (C struct) to Rust conversions
// ============================================================================

/// Helper to convert a C char array to a Rust String.
/// Safely handles non-null-terminated arrays by using the array length as max.
fn c_char_array_to_string<const N: usize>(arr: &[c_char; N]) -> String {
    // Safety: We're treating the c_char array as bytes
    let bytes: &[u8] = unsafe { std::slice::from_raw_parts(arr.as_ptr() as *const u8, N) };

    // Find the null terminator or use the full length
    let len = bytes.iter().position(|&b| b == 0).unwrap_or(N);

    String::from_utf8_lossy(&bytes[..len]).into_owned()
}

/// Broker state integer to string mapping.
/// Uses the rd_kafka_broker_state_names array exposed by librdkafka.
fn broker_state_to_string(state: i32) -> String {
    if state < 0 || state >= rdsys::RD_KAFKA_BROKER_STATE_COUNT {
        return "UNKNOWN".to_owned();
    }
    unsafe {
        let base = std::ptr::addr_of!(rdsys::rd_kafka_broker_state_names) as *const *const c_char;
        let ptr = *base.add(state as usize);
        if ptr.is_null() {
            return "UNKNOWN".to_owned();
        }
        std::ffi::CStr::from_ptr(ptr)
            .to_str()
            .unwrap_or("UNKNOWN")
            .to_owned()
    }
}

/// Partition fetch state integer to string mapping.
/// Uses the rd_kafka_fetch_states array exposed by librdkafka.
fn fetch_state_to_string(state: i32) -> String {
    if state < 0 || state >= rdsys::RD_KAFKA_FETCH_STATE_COUNT {
        return "unknown".to_owned();
    }
    unsafe {
        let base = std::ptr::addr_of!(rdsys::rd_kafka_fetch_states) as *const *const c_char;
        let ptr = *base.add(state as usize);
        if ptr.is_null() {
            return "unknown".to_owned();
        }
        std::ffi::CStr::from_ptr(ptr)
            .to_str()
            .unwrap_or("unknown")
            .to_owned()
    }
}

/// Client type integer to string mapping.
fn client_type_to_string(type_: i32) -> String {
    match type_ {
        0 => "producer",
        1 => "consumer",
        _ => "unknown",
    }
    .to_string()
}

/// Consumer group state to string.
fn cgrp_state_to_string(state: i32) -> String {
    match state {
        0 => "init",
        1 => "term",
        2 => "query-coord",
        3 => "wait-coord",
        4 => "wait-broker",
        5 => "wait-broker-transport",
        6 => "up",
        _ => "unknown",
    }
    .to_string()
}

/// Consumer group join state to string.
fn cgrp_join_state_to_string(state: i32) -> String {
    match state {
        0 => "init",
        1 => "wait-join",
        2 => "wait-metadata",
        3 => "wait-sync",
        4 => "wait-unassign",
        5 => "wait-unassign-call",
        6 => "wait-assign-call",
        7 => "wait-rebalance-cb",
        8 => "wait-change",
        9 => "steady",
        _ => "unknown",
    }
    .to_string()
}

/// Idempotent producer state to string.
fn idemp_state_to_string(state: i32) -> String {
    match state {
        0 => "Init",
        1 => "WaitTransport",
        2 => "WaitPID",
        3 => "Assigned",
        4 => "DrainReset",
        5 => "DrainBump",
        6 => "WaitPIDRotate",
        7 => "Term",
        _ => "Unknown",
    }
    .to_string()
}

/// Transactional producer state to string.
fn txn_state_to_string(state: i32) -> String {
    match state {
        0 => "Init",
        1 => "WaitPID",
        2 => "Ready",
        3 => "InTransaction",
        4 => "BeginCommit",
        5 => "CommittingTransaction",
        6 => "BeginAbort",
        7 => "AbortingTransaction",
        _ => "Unknown",
    }
    .to_string()
}

impl Window {
    /// Convert from native rd_kafka_avg_stats_t.
    pub fn from_native(avg: &rdsys::rd_kafka_avg_stats_t) -> Self {
        Window {
            min: avg.min,
            max: avg.max,
            avg: avg.avg,
            sum: avg.sum,
            cnt: avg.cnt,
            stddev: avg.stddev,
            hdrsize: avg.hdrsize as i64,
            p50: avg.p50,
            p75: avg.p75,
            p90: avg.p90,
            p95: avg.p95,
            p99: avg.p99,
            p99_99: avg.p99_99,
            outofrange: avg.oor,
        }
    }
}

impl TopicPartition {
    /// Convert from native rd_kafka_broker_toppar_ref_t.
    pub fn from_native(tp: &rdsys::rd_kafka_broker_toppar_ref_t) -> Self {
        TopicPartition {
            topic: c_char_array_to_string(&tp.topic),
            partition: tp.partition,
        }
    }
}

impl Partition {
    /// Convert from native rd_kafka_partition_stats_t.
    pub fn from_native(p: &rdsys::rd_kafka_partition_stats_t) -> Self {
        Partition {
            partition: p.partition,
            broker: p.broker_id,
            leader: p.leader,
            desired: p.desired != 0,
            unknown: p.unknown != 0,
            msgq_cnt: p.msgq_cnt as i64,
            msgq_bytes: p.msgq_bytes as u64,
            xmit_msgq_cnt: p.xmit_msgq_cnt as i64,
            xmit_msgq_bytes: p.xmit_msgq_bytes as u64,
            fetchq_cnt: p.fetchq_cnt as i64,
            fetchq_size: p.fetchq_size as u64,
            fetch_state: fetch_state_to_string(p.fetch_state),
            query_offset: p.query_offset,
            next_offset: p.next_offset,
            app_offset: p.app_offset,
            stored_offset: p.stored_offset,
            committed_offset: p.committed_offset,
            eof_offset: p.eof_offset,
            lo_offset: p.lo_offset,
            hi_offset: p.hi_offset,
            ls_offset: p.ls_offset,
            consumer_lag: p.consumer_lag,
            consumer_lag_stored: p.consumer_lag_stored,
            txmsgs: p.txmsgs as u64,
            txbytes: p.txbytes as u64,
            rxmsgs: p.rxmsgs as u64,
            rxbytes: p.rxbytes as u64,
            msgs: p.msgs as u64,
            rx_ver_drops: p.rx_ver_drops as u64,
            msgs_inflight: p.msgs_inflight,
            next_ack_seq: p.next_ack_seq,
            next_err_seq: p.next_err_seq,
            acked_msgid: p.acked_msgid as u64,
        }
    }
}

impl Topic {
    /// Convert from native rd_kafka_topic_stats_t.
    ///
    /// # Safety
    /// The `partitions` pointer must be valid for `partition_cnt` elements.
    pub unsafe fn from_native(t: &rdsys::rd_kafka_topic_stats_t) -> Self {
        let mut partitions = HashMap::new();

        if !t.partitions.is_null() {
            for i in 0..t.partition_cnt as usize {
                let p = &*t.partitions.add(i);
                partitions.insert(p.partition, Partition::from_native(p));
            }
        }

        Topic {
            topic: c_char_array_to_string(&t.name),
            metadata_age: t.metadata_age_us / 1000, // Convert us to ms
            batchsize: Window::from_native(&t.batchsize),
            batchcnt: Window::from_native(&t.batchcnt),
            partitions,
        }
    }
}

impl Broker {
    /// Convert from native rd_kafka_broker_stats_t.
    ///
    /// # Safety
    /// The `toppars` pointer must be valid for `toppar_cnt` elements.
    pub unsafe fn from_native(b: &rdsys::rd_kafka_broker_stats_t) -> Self {
        let mut toppars = HashMap::new();

        if !b.toppars.is_null() {
            for i in 0..b.toppar_cnt as usize {
                let tp = &*b.toppars.add(i);
                let key = format!("{}-{}", c_char_array_to_string(&tp.topic), tp.partition);
                toppars.insert(key, TopicPartition::from_native(tp));
            }
        }

        // Build request type counts HashMap from pre-populated reqs array
        let mut req = HashMap::new();
        if !b.reqs.is_null() {
            for i in 0..b.req_cnt as usize {
                let r = &*b.reqs.add(i);
                let name = c_char_array_to_string(&r.name);
                if r.count > 0 {
                    req.insert(name, r.count);
                }
            }
        }

        Broker {
            name: c_char_array_to_string(&b.name),
            nodeid: b.nodeid,
            nodename: c_char_array_to_string(&b.nodename),
            source: c_char_array_to_string(&b.source),
            state: broker_state_to_string(b.state),
            stateage: b.stateage_us,
            outbuf_cnt: b.outbuf_cnt as i64,
            outbuf_msg_cnt: b.outbuf_msg_cnt as i64,
            waitresp_cnt: b.waitresp_cnt as i64,
            waitresp_msg_cnt: b.waitresp_msg_cnt as i64,
            tx: b.tx as u64,
            txbytes: b.tx_bytes as u64,
            txerrs: b.tx_errs as u64,
            txretries: b.tx_retries as u64,
            txidle: b.tx_idle_us,
            req_timeouts: b.req_timeouts as u64,
            rx: b.rx as u64,
            rxbytes: b.rx_bytes as u64,
            rxerrs: b.rx_errs as u64,
            rxcorriderrs: b.rx_corriderrs as u64,
            rxpartial: b.rx_partial as u64,
            rxidle: b.rx_idle_us,
            req,
            zbuf_grow: b.zbuf_grow as u64,
            buf_grow: b.buf_grow as u64,
            wakeups: Some(b.wakeups as u64),
            connects: Some(b.connects),
            disconnects: Some(b.disconnects),
            int_latency: Some(Window::from_native(&b.int_latency)),
            outbuf_latency: Some(Window::from_native(&b.outbuf_latency)),
            rtt: Some(Window::from_native(&b.rtt)),
            throttle: Some(Window::from_native(&b.throttle)),
            toppars,
            produce_partitions: Some(Window::from_native(&b.produce_partitions)),
            produce_messages: Some(Window::from_native(&b.produce_messages)),
            produce_reqsize: Some(Window::from_native(&b.produce_reqsize)),
            produce_fill: Some(Window::from_native(&b.produce_fill)),
            batch_wait: Some(Window::from_native(&b.batch_wait)),
            adaptive: if b.adaptive_enabled != 0 {
                Some(AdaptiveBatching {
                    enabled: true,
                    linger_us: b.adaptive_linger_us,
                    batch_max_bytes: b.adaptive_batch_max_bytes,
                    congestion: b.adaptive_congestion,
                    rtt_congestion: b.adaptive_rtt_congestion,
                    int_lat_congestion: b.adaptive_int_lat_congestion,
                    rtt_base_us: b.adaptive_rtt_base_us,
                    rtt_current_us: b.adaptive_rtt_current_us,
                    int_lat_base_us: b.adaptive_int_lat_base_us,
                    int_lat_current_us: b.adaptive_int_lat_current_us,
                    adjustments_up: b.adaptive_adjustments_up,
                    adjustments_down: b.adaptive_adjustments_down,
                    backlog_drain_events: b.adaptive_backlog_drain_events,
                })
            } else {
                None
            },
        }
    }
}

impl ConsumerGroup {
    /// Convert from native rd_kafka_cgrp_stats_t.
    pub fn from_native(cg: &rdsys::rd_kafka_cgrp_stats_t) -> Self {
        ConsumerGroup {
            state: cgrp_state_to_string(cg.state),
            stateage: cg.stateage_us / 1000, // Convert us to ms
            join_state: cgrp_join_state_to_string(cg.join_state),
            rebalance_age: cg.rebalance_age_us / 1000, // Convert us to ms
            rebalance_cnt: cg.rebalance_cnt as i64,
            rebalance_reason: c_char_array_to_string(&cg.rebalance_reason),
            assignment_size: cg.assignment_size,
        }
    }
}

impl ExactlyOnceSemantics {
    /// Convert from native rd_kafka_eos_stats_t.
    pub fn from_native(eos: &rdsys::rd_kafka_eos_stats_t) -> Self {
        ExactlyOnceSemantics {
            idemp_state: idemp_state_to_string(eos.idemp_state),
            idemp_stateage: eos.idemp_stateage_us / 1000, // Convert us to ms
            txn_state: txn_state_to_string(eos.txn_state),
            txn_stateage: eos.txn_stateage_us / 1000, // Convert us to ms
            txn_may_enq: eos.txn_may_enq != 0,
            producer_id: eos.producer_id,
            producer_epoch: eos.producer_epoch as i64,
            epoch_cnt: eos.epoch_cnt as i64,
        }
    }
}

impl Statistics {
    /// Convert from native rd_kafka_stats_t.
    ///
    /// # Safety
    /// The `brokers` and `topics` pointers must be valid for their respective counts.
    pub unsafe fn from_native(stats: &rdsys::rd_kafka_stats_t) -> Self {
        let mut brokers = HashMap::new();
        let mut topics = HashMap::new();

        // Convert brokers
        if !stats.brokers.is_null() {
            for i in 0..stats.broker_cnt as usize {
                let b = &*stats.brokers.add(i);
                let name = c_char_array_to_string(&b.name);
                brokers.insert(name, Broker::from_native(b));
            }
        }

        // Convert topics
        if !stats.topics.is_null() {
            for i in 0..stats.topic_cnt as usize {
                let t = &*stats.topics.add(i);
                let name = c_char_array_to_string(&t.name);
                topics.insert(name, Topic::from_native(t));
            }
        }

        // Convert optional consumer group stats
        let cgrp = if stats.has_cgrp != 0 {
            Some(ConsumerGroup::from_native(&stats.cgrp))
        } else {
            None
        };

        // Convert optional EOS stats
        let eos = if stats.has_eos != 0 {
            Some(ExactlyOnceSemantics::from_native(&stats.eos))
        } else {
            None
        };

        Statistics {
            name: c_char_array_to_string(&stats.name),
            client_id: c_char_array_to_string(&stats.client_id),
            client_type: client_type_to_string(stats.type_),
            ts: stats.ts_us,
            time: stats.time_sec,
            age: stats.age_us,
            replyq: stats.replyq as i64,
            msg_cnt: stats.msg_cnt as u64,
            msg_size: stats.msg_size,
            msg_max: stats.msg_max as u64,
            msg_size_max: stats.msg_size_max,
            tx: stats.tx,
            tx_bytes: stats.tx_bytes,
            rx: stats.rx,
            rx_bytes: stats.rx_bytes,
            txmsgs: stats.txmsgs,
            txmsg_bytes: stats.txmsg_bytes,
            rxmsgs: stats.rxmsgs,
            rxmsg_bytes: stats.rxmsg_bytes,
            simple_cnt: stats.simple_cnt as i64,
            metadata_cache_cnt: stats.metadata_cache_cnt as i64,
            brokers,
            topics,
            cgrp,
            eos,
        }
    }
}

#[cfg(test)]
mod tests {
    use maplit::hashmap;

    use super::*;

    #[test]
    fn test_statistics() {
        let stats: Statistics = serde_json::from_str(EXAMPLE).unwrap();

        assert_eq!(stats.name, "rdkafka#producer-1");
        assert_eq!(stats.client_type, "producer");
        assert_eq!(stats.ts, 1163982743268);
        assert_eq!(stats.time, 1589652530);
        assert_eq!(stats.replyq, 0);
        assert_eq!(stats.msg_cnt, 320);
        assert_eq!(stats.msg_size, 9920);
        assert_eq!(stats.msg_max, 500000);
        assert_eq!(stats.msg_size_max, 1073741824);
        assert_eq!(stats.simple_cnt, 0);

        assert_eq!(stats.brokers.len(), 1);

        let broker = stats.brokers.values().collect::<Vec<_>>()[0];

        assert_eq!(
            broker.req,
            hashmap! {
                "Produce".to_string() => 31307,
                "Offset".to_string() => 0,
                "Metadata".to_string() => 2,
                "FindCoordinator".to_string() => 0,
                "SaslHandshake".to_string() => 0,
                "ApiVersion".to_string() => 2,
                "InitProducerId".to_string() => 0,
                "AddPartitionsToTxn".to_string() => 0,
                "AddOffsetsToTxn".to_string() => 0,
                "EndTxn".to_string() => 0,
                "TxnOffsetCommit".to_string() => 0,
                "SaslAuthenticate".to_string() => 0,
            }
        );

        // Verify new produce statistics fields
        let produce_partitions = broker.produce_partitions.as_ref().unwrap();
        assert_eq!(produce_partitions.min, 1);
        assert_eq!(produce_partitions.max, 3);
        assert_eq!(produce_partitions.avg, 2);
        assert_eq!(produce_partitions.cnt, 4739);

        let produce_messages = broker.produce_messages.as_ref().unwrap();
        assert_eq!(produce_messages.min, 1);
        assert_eq!(produce_messages.max, 18483);
        assert_eq!(produce_messages.avg, 912);
        assert_eq!(produce_messages.cnt, 4739);

        let produce_reqsize = broker.produce_reqsize.as_ref().unwrap();
        assert_eq!(produce_reqsize.min, 99);
        assert_eq!(produce_reqsize.max, 720828);
        assert_eq!(produce_reqsize.avg, 35613);
        assert_eq!(produce_reqsize.cnt, 4739);

        let produce_fill = broker.produce_fill.as_ref().unwrap();
        assert_eq!(produce_fill.min, 0);
        assert_eq!(produce_fill.max, 720);
        assert_eq!(produce_fill.avg, 35);
        assert_eq!(produce_fill.cnt, 4739);

        assert_eq!(stats.topics.len(), 1);
    }

    // Example from https://github.com/edenhill/librdkafka/wiki/Statistics
    const EXAMPLE: &str = r#"
      {
        "name": "rdkafka#producer-1",
        "client_id": "rdkafka",
        "type": "producer",
        "ts": 1163982743268,
        "time": 1589652530,
        "age": 5,
        "replyq": 0,
        "msg_cnt": 320,
        "msg_size": 9920,
        "msg_max": 500000,
        "msg_size_max": 1073741824,
        "simple_cnt": 0,
        "metadata_cache_cnt": 1,
        "brokers": {
          "localhost:9092/0": {
            "name": "localhost:9092/0",
            "nodeid": 0,
            "nodename": "localhost:9092",
            "source": "configured",
            "state": "UP",
            "stateage": 8005652,
            "outbuf_cnt": 0,
            "outbuf_msg_cnt": 0,
            "waitresp_cnt": 1,
            "waitresp_msg_cnt": 126,
            "tx": 31311,
            "txbytes": 463869957,
            "txerrs": 0,
            "txretries": 0,
            "txidle": 5,
            "req_timeouts": 0,
            "rx": 31310,
            "rxbytes": 1753668,
            "rxerrs": 0,
            "rxcorriderrs": 0,
            "rxpartial": 0,
            "rxidle": 5,
            "zbuf_grow": 0,
            "buf_grow": 0,
            "wakeups": 131568,
            "connects": 1,
            "disconnects": 0,
            "int_latency": {
              "min": 2,
              "max": 9193,
              "avg": 605,
              "sum": 874202325,
              "stddev": 1080,
              "p50": 319,
              "p75": 481,
              "p90": 1135,
              "p95": 3023,
              "p99": 5919,
              "p99_99": 9087,
              "outofrange": 0,
              "hdrsize": 15472,
              "cnt": 1443154
            },
            "outbuf_latency": {
              "min": 1,
              "max": 308,
              "avg": 22,
              "sum": 107311,
              "stddev": 21,
              "p50": 22,
              "p75": 29,
              "p90": 36,
              "p95": 44,
              "p99": 111,
              "p99_99": 309,
              "outofrange": 0,
              "hdrsize": 11376,
              "cnt": 4740
            },
            "rtt": {
              "min": 94,
              "max": 3279,
              "avg": 237,
              "sum": 1124867,
              "stddev": 198,
              "p50": 193,
              "p75": 245,
              "p90": 329,
              "p95": 393,
              "p99": 1183,
              "p99_99": 3279,
              "outofrange": 0,
              "hdrsize": 13424,
              "cnt": 4739
            },
            "throttle": {
              "min": 0,
              "max": 0,
              "avg": 0,
              "sum": 0,
              "stddev": 0,
              "p50": 0,
              "p75": 0,
              "p90": 0,
              "p95": 0,
              "p99": 0,
              "p99_99": 0,
              "outofrange": 0,
              "hdrsize": 17520,
              "cnt": 4739
            },
            "req": {
              "Produce": 31307,
              "Offset": 0,
              "Metadata": 2,
              "FindCoordinator": 0,
              "SaslHandshake": 0,
              "ApiVersion": 2,
              "InitProducerId": 0,
              "AddPartitionsToTxn": 0,
              "AddOffsetsToTxn": 0,
              "EndTxn": 0,
              "TxnOffsetCommit": 0,
              "SaslAuthenticate": 0
            },
            "toppars": {
              "test-0": {
                "topic": "test",
                "partition": 0
              },
              "test-1": {
                "topic": "test",
                "partition": 1
              },
              "test-2": {
                "topic": "test",
                "partition": 2
              }
            },
            "produce_partitions": {
              "min": 1,
              "max": 3,
              "avg": 2,
              "sum": 9478,
              "stddev": 1,
              "p50": 2,
              "p75": 3,
              "p90": 3,
              "p95": 3,
              "p99": 3,
              "p99_99": 3,
              "outofrange": 0,
              "hdrsize": 8304,
              "cnt": 4739
            },
            "produce_messages": {
              "min": 1,
              "max": 18483,
              "avg": 912,
              "sum": 4322068,
              "stddev": 1008,
              "p50": 801,
              "p75": 891,
              "p90": 987,
              "p95": 1059,
              "p99": 5541,
              "p99_99": 18495,
              "outofrange": 0,
              "hdrsize": 11376,
              "cnt": 4739
            },
            "produce_reqsize": {
              "min": 99,
              "max": 720828,
              "avg": 35613,
              "sum": 168781107,
              "stddev": 39411,
              "p50": 31293,
              "p75": 34749,
              "p90": 38397,
              "p95": 41469,
              "p99": 216573,
              "p99_99": 721919,
              "outofrange": 0,
              "hdrsize": 14448,
              "cnt": 4739
            },
            "produce_fill": {
              "min": 0,
              "max": 720,
              "avg": 35,
              "sum": 165865,
              "stddev": 39,
              "p50": 31,
              "p75": 34,
              "p90": 38,
              "p95": 41,
              "p99": 216,
              "p99_99": 721,
              "outofrange": 0,
              "hdrsize": 8304,
              "cnt": 4739
            }
          }
        },
        "topics": {
          "test": {
            "topic": "test",
            "metadata_age": 7014,
            "batchsize": {
              "min": 99,
              "max": 240276,
              "avg": 11871,
              "sum": 56260370,
              "stddev": 13137,
              "p50": 10431,
              "p75": 11583,
              "p90": 12799,
              "p95": 13823,
              "p99": 72191,
              "p99_99": 240639,
              "outofrange": 0,
              "hdrsize": 14448,
              "cnt": 4739
            },
            "batchcnt": {
              "min": 1,
              "max": 6161,
              "avg": 304,
              "sum": 1442353,
              "stddev": 336,
              "p50": 267,
              "p75": 297,
              "p90": 329,
              "p95": 353,
              "p99": 1847,
              "p99_99": 6175,
              "outofrange": 0,
              "hdrsize": 8304,
              "cnt": 4739
            },
            "partitions": {
              "0": {
                "partition": 0,
                "broker": 0,
                "leader": 0,
                "desired": false,
                "unknown": false,
                "msgq_cnt": 845,
                "msgq_bytes": 26195,
                "xmit_msgq_cnt": 0,
                "xmit_msgq_bytes": 0,
                "fetchq_cnt": 0,
                "fetchq_size": 0,
                "fetch_state": "none",
                "query_offset": -1001,
                "next_offset": 0,
                "app_offset": -1001,
                "stored_offset": -1001,
                "commited_offset": -1001,
                "committed_offset": -1001,
                "eof_offset": -1001,
                "lo_offset": -1001,
                "hi_offset": -1001,
                "ls_offset": -1001,
                "consumer_lag": -1,
                "consumer_lag_stored": 0,
                "txmsgs": 3950967,
                "txbytes": 122479977,
                "rxmsgs": 0,
                "rxbytes": 0,
                "msgs": 3951812,
                "rx_ver_drops": 0,
                "msgs_inflight": 1067,
                "next_ack_seq": 0,
                "next_err_seq": 0,
                "acked_msgid": 0
              },
              "1": {
                "partition": 1,
                "broker": 0,
                "leader": 0,
                "desired": false,
                "unknown": false,
                "msgq_cnt": 229,
                "msgq_bytes": 7099,
                "xmit_msgq_cnt": 0,
                "xmit_msgq_bytes": 0,
                "fetchq_cnt": 0,
                "fetchq_size": 0,
                "fetch_state": "none",
                "query_offset": -1001,
                "next_offset": 0,
                "app_offset": -1001,
                "stored_offset": -1001,
                "commited_offset": -1001,
                "committed_offset": -1001,
                "eof_offset": -1001,
                "lo_offset": -1001,
                "hi_offset": -1001,
                "ls_offset": -1001,
                "consumer_lag": -1,
                "consumer_lag_stored": 0,
                "txmsgs": 3950656,
                "txbytes": 122470336,
                "rxmsgs": 0,
                "rxbytes": 0,
                "msgs": 3952618,
                "rx_ver_drops": 0,
                "msgs_inflight": 0,
                "next_ack_seq": 0,
                "next_err_seq": 0,
                "acked_msgid": 0
              },
              "2": {
                "partition": 2,
                "broker": 0,
                "leader": 0,
                "desired": false,
                "unknown": false,
                "msgq_cnt": 1816,
                "msgq_bytes": 56296,
                "xmit_msgq_cnt": 0,
                "xmit_msgq_bytes": 0,
                "fetchq_cnt": 0,
                "fetchq_size": 0,
                "fetch_state": "none",
                "query_offset": -1001,
                "next_offset": 0,
                "app_offset": -1001,
                "stored_offset": -1001,
                "commited_offset": -1001,
                "committed_offset": -1001,
                "eof_offset": -1001,
                "lo_offset": -1001,
                "hi_offset": -1001,
                "ls_offset": -1001,
                "consumer_lag": -1,
                "consumer_lag_stored": 0,
                "txmsgs": 3952027,
                "txbytes": 122512837,
                "rxmsgs": 0,
                "rxbytes": 0,
                "msgs": 3953855,
                "rx_ver_drops": 0,
                "msgs_inflight": 0,
                "next_ack_seq": 0,
                "next_err_seq": 0,
                "acked_msgid": 0
              },
              "-1": {
                "partition": -1,
                "broker": -1,
                "leader": -1,
                "desired": false,
                "unknown": false,
                "msgq_cnt": 0,
                "msgq_bytes": 0,
                "xmit_msgq_cnt": 0,
                "xmit_msgq_bytes": 0,
                "fetchq_cnt": 0,
                "fetchq_size": 0,
                "fetch_state": "none",
                "query_offset": -1001,
                "next_offset": 0,
                "app_offset": -1001,
                "stored_offset": -1001,
                "commited_offset": -1001,
                "committed_offset": -1001,
                "eof_offset": -1001,
                "lo_offset": -1001,
                "hi_offset": -1001,
                "ls_offset": -1001,
                "consumer_lag": -1,
                "consumer_lag_stored": 0,
                "txmsgs": 0,
                "txbytes": 0,
                "rxmsgs": 0,
                "rxbytes": 0,
                "msgs": 500000,
                "rx_ver_drops": 0,
                "msgs_inflight": 0,
                "next_ack_seq": 0,
                "next_err_seq": 0,
                "acked_msgid": 0
              }
            }
          }
        },
        "tx": 31311,
        "tx_bytes": 463869957,
        "rx": 31310,
        "rx_bytes": 1753668,
        "txmsgs": 11853650,
        "txmsg_bytes": 367463150,
        "rxmsgs": 0,
        "rxmsg_bytes": 0
      }"#;
}
