//! Client and broker statistics.
//!
//! These statistics are collected automatically by librdkafka when the client
//! is configured with a non-zero `statistics.interval.ms`. They are made
//! available via the [`ClientContext::stats_view`] callback.
//!
//! Refer to the [librdkafka statistics documentation][librdkafka-stats] for
//! details.
//!
//! [`ClientContext::stats_view`]: crate::ClientContext::stats_view
//! [librdkafka-stats]: https://github.com/edenhill/librdkafka/blob/master/STATISTICS.md

use std::borrow::Cow;
use std::os::raw::c_char;

use rdkafka_sys as rdsys;

#[inline]
unsafe fn slice_from_ptr_count<'a, T>(ptr: *const T, count: usize) -> &'a [T] {
    if ptr.is_null() || count == 0 {
        &[]
    } else {
        std::slice::from_raw_parts(ptr, count)
    }
}

#[inline]
fn c_char_array_to_cow<'a, const N: usize>(arr: &'a [c_char; N]) -> Cow<'a, str> {
    let bytes: &'a [u8] = unsafe { std::slice::from_raw_parts(arr.as_ptr() as *const u8, N) };
    let len = bytes.iter().position(|&b| b == 0).unwrap_or(N);
    String::from_utf8_lossy(&bytes[..len])
}

#[doc(hidden)]
#[derive(Copy, Clone)]
pub struct StatsView<'a> {
    raw: &'a rdsys::rd_kafka_stats_t,
    brokers: &'a [rdsys::rd_kafka_broker_stats_t],
    topics: &'a [rdsys::rd_kafka_topic_stats_t],
}

#[doc(hidden)]
#[derive(Copy, Clone)]
pub struct BrokerView<'a> {
    raw: &'a rdsys::rd_kafka_broker_stats_t,
    reqs: &'a [rdsys::rd_kafka_req_count_t],
}

#[doc(hidden)]
#[derive(Copy, Clone)]
pub struct TopicView<'a> {
    raw: &'a rdsys::rd_kafka_topic_stats_t,
    partitions: &'a [rdsys::rd_kafka_partition_stats_t],
}

#[doc(hidden)]
#[derive(Copy, Clone)]
pub struct PartitionView<'a> {
    raw: &'a rdsys::rd_kafka_partition_stats_t,
}

#[doc(hidden)]
#[derive(Copy, Clone)]
pub struct ConsumerGroupView<'a> {
    raw: &'a rdsys::rd_kafka_cgrp_stats_t,
}

#[doc(hidden)]
#[derive(Copy, Clone)]
pub struct ExactlyOnceSemanticsView<'a> {
    raw: &'a rdsys::rd_kafka_eos_stats_t,
}

#[doc(hidden)]
#[derive(Copy, Clone)]
pub struct WindowView<'a> {
    raw: &'a rdsys::rd_kafka_avg_stats_t,
}

#[doc(hidden)]
#[derive(Copy, Clone)]
pub struct BrokerRequestView<'a> {
    raw: &'a rdsys::rd_kafka_req_count_t,
}

#[doc(hidden)]
impl<'a> StatsView<'a> {
    /// Safety: `raw` and all nested pointers must remain valid for `'a`.
    pub unsafe fn new(raw: &'a rdsys::rd_kafka_stats_t) -> Self {
        Self {
            raw,
            brokers: slice_from_ptr_count(raw.brokers, raw.broker_cnt as usize),
            topics: slice_from_ptr_count(raw.topics, raw.topic_cnt as usize),
        }
    }

    pub fn brokers(&self) -> impl ExactSizeIterator<Item = BrokerView<'a>> + 'a {
        self.brokers.iter().map(BrokerView::from_raw)
    }

    pub fn topics(&self) -> impl ExactSizeIterator<Item = TopicView<'a>> + 'a {
        self.topics.iter().map(TopicView::from_raw)
    }

    pub fn cgrp(&self) -> Option<ConsumerGroupView<'a>> {
        (self.raw.has_cgrp != 0).then_some(ConsumerGroupView {
            raw: &self.raw.cgrp,
        })
    }

    pub fn eos(&self) -> Option<ExactlyOnceSemanticsView<'a>> {
        (self.raw.has_eos != 0).then_some(ExactlyOnceSemanticsView { raw: &self.raw.eos })
    }
}

#[doc(hidden)]
impl<'a> BrokerView<'a> {
    fn from_raw(raw: &'a rdsys::rd_kafka_broker_stats_t) -> Self {
        let reqs = unsafe { slice_from_ptr_count(raw.reqs, raw.req_cnt as usize) };
        Self { raw, reqs }
    }

    pub fn requests(&self) -> impl Iterator<Item = BrokerRequestView<'a>> + 'a {
        self.reqs
            .iter()
            .filter(|req| req.count > 0)
            .map(|raw| BrokerRequestView { raw })
    }
}

#[doc(hidden)]
impl<'a> TopicView<'a> {
    fn from_raw(raw: &'a rdsys::rd_kafka_topic_stats_t) -> Self {
        let partitions =
            unsafe { slice_from_ptr_count(raw.partitions, raw.partition_cnt as usize) };
        Self { raw, partitions }
    }

    pub fn partitions(&self) -> impl ExactSizeIterator<Item = PartitionView<'a>> + 'a {
        self.partitions.iter().map(PartitionView::from_raw)
    }
}

#[doc(hidden)]
impl<'a> PartitionView<'a> {
    fn from_raw(raw: &'a rdsys::rd_kafka_partition_stats_t) -> Self {
        Self { raw }
    }
}

#[doc(hidden)]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub enum BrokerState {
    Init,
    Down,
    Connect,
    Auth,
    ApiVersionQuery,
    AuthHandshake,
    Up,
    Update,
    Unknown(i32),
}

#[doc(hidden)]
impl BrokerState {
    #[inline]
    pub fn from_raw(v: i32) -> Self {
        match v {
            0 => Self::Init,
            1 => Self::Down,
            2 => Self::Connect,
            3 => Self::Auth,
            4 => Self::ApiVersionQuery,
            5 => Self::AuthHandshake,
            6 => Self::Up,
            7 => Self::Update,
            other => Self::Unknown(other),
        }
    }

    #[inline]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Init => "INIT",
            Self::Down => "DOWN",
            Self::Connect => "CONNECT",
            Self::Auth => "AUTH",
            Self::ApiVersionQuery => "APIVERSION_QUERY",
            Self::AuthHandshake => "AUTH_HANDSHAKE",
            Self::Up => "UP",
            Self::Update => "UPDATE",
            Self::Unknown(_) => "UNKNOWN",
        }
    }
}

#[doc(hidden)]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub enum ClientType {
    Producer,
    Consumer,
    Unknown(i32),
}

#[doc(hidden)]
impl ClientType {
    #[inline]
    pub fn from_raw(v: i32) -> Self {
        match v {
            0 => Self::Producer,
            1 => Self::Consumer,
            o => Self::Unknown(o),
        }
    }

    #[inline]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Producer => "producer",
            Self::Consumer => "consumer",
            Self::Unknown(_) => "unknown",
        }
    }
}

#[doc(hidden)]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub enum FetchState {
    None,
    Stopping,
    Stopped,
    OffsetQuery,
    OffsetWait,
    ValidateEpochWait,
    Active,
    Unknown(i32),
}

#[doc(hidden)]
impl FetchState {
    #[inline]
    pub fn from_raw(v: i32) -> Self {
        match v {
            0 => Self::None,
            1 => Self::Stopping,
            2 => Self::Stopped,
            3 => Self::OffsetQuery,
            4 => Self::OffsetWait,
            5 => Self::ValidateEpochWait,
            6 => Self::Active,
            other => Self::Unknown(other),
        }
    }

    #[inline]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Stopping => "stopping",
            Self::Stopped => "stopped",
            Self::OffsetQuery => "offset-query",
            Self::OffsetWait => "offset-wait",
            Self::ValidateEpochWait => "validate-epoch-wait",
            Self::Active => "active",
            Self::Unknown(_) => "unknown",
        }
    }
}

#[doc(hidden)]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub enum ConsumerGroupState {
    Init,
    Term,
    QueryCoord,
    WaitCoord,
    WaitBroker,
    WaitBrokerTransport,
    Up,
    Unknown(i32),
}

#[doc(hidden)]
impl ConsumerGroupState {
    #[inline]
    pub fn from_raw(v: i32) -> Self {
        match v {
            0 => Self::Init,
            1 => Self::Term,
            2 => Self::QueryCoord,
            3 => Self::WaitCoord,
            4 => Self::WaitBroker,
            5 => Self::WaitBrokerTransport,
            6 => Self::Up,
            other => Self::Unknown(other),
        }
    }

    #[inline]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Init => "init",
            Self::Term => "term",
            Self::QueryCoord => "query-coord",
            Self::WaitCoord => "wait-coord",
            Self::WaitBroker => "wait-broker",
            Self::WaitBrokerTransport => "wait-broker-transport",
            Self::Up => "up",
            Self::Unknown(_) => "unknown",
        }
    }
}

#[doc(hidden)]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub enum ConsumerGroupJoinState {
    Init,
    WaitJoin,
    WaitMetadata,
    WaitSync,
    WaitUnassign,
    WaitUnassignCall,
    WaitAssignCall,
    WaitRebalanceCb,
    WaitChange,
    Steady,
    Unknown(i32),
}

#[doc(hidden)]
impl ConsumerGroupJoinState {
    #[inline]
    pub fn from_raw(v: i32) -> Self {
        match v {
            0 => Self::Init,
            1 => Self::WaitJoin,
            2 => Self::WaitMetadata,
            3 => Self::WaitSync,
            4 => Self::WaitUnassign,
            5 => Self::WaitUnassignCall,
            6 => Self::WaitAssignCall,
            7 => Self::WaitRebalanceCb,
            8 => Self::WaitChange,
            9 => Self::Steady,
            other => Self::Unknown(other),
        }
    }

    #[inline]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Init => "init",
            Self::WaitJoin => "wait-join",
            Self::WaitMetadata => "wait-metadata",
            Self::WaitSync => "wait-sync",
            Self::WaitUnassign => "wait-unassign",
            Self::WaitUnassignCall => "wait-unassign-call",
            Self::WaitAssignCall => "wait-assign-call",
            Self::WaitRebalanceCb => "wait-rebalance-cb",
            Self::WaitChange => "wait-change",
            Self::Steady => "steady",
            Self::Unknown(_) => "unknown",
        }
    }
}

#[doc(hidden)]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub enum IdempotentState {
    Init,
    WaitTransport,
    WaitPid,
    Assigned,
    DrainReset,
    DrainBump,
    WaitPidRotate,
    Term,
    Unknown(i32),
}

#[doc(hidden)]
impl IdempotentState {
    #[inline]
    pub fn from_raw(v: i32) -> Self {
        match v {
            0 => Self::Init,
            1 => Self::WaitTransport,
            2 => Self::WaitPid,
            3 => Self::Assigned,
            4 => Self::DrainReset,
            5 => Self::DrainBump,
            6 => Self::WaitPidRotate,
            7 => Self::Term,
            other => Self::Unknown(other),
        }
    }

    #[inline]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Init => "Init",
            Self::WaitTransport => "WaitTransport",
            Self::WaitPid => "WaitPID",
            Self::Assigned => "Assigned",
            Self::DrainReset => "DrainReset",
            Self::DrainBump => "DrainBump",
            Self::WaitPidRotate => "WaitPIDRotate",
            Self::Term => "Term",
            Self::Unknown(_) => "Unknown",
        }
    }
}

#[doc(hidden)]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub enum TransactionState {
    Init,
    WaitPid,
    Ready,
    InTransaction,
    BeginCommit,
    CommittingTransaction,
    BeginAbort,
    AbortingTransaction,
    Unknown(i32),
}

#[doc(hidden)]
impl TransactionState {
    #[inline]
    pub fn from_raw(v: i32) -> Self {
        match v {
            0 => Self::Init,
            1 => Self::WaitPid,
            2 => Self::Ready,
            3 => Self::InTransaction,
            4 => Self::BeginCommit,
            5 => Self::CommittingTransaction,
            6 => Self::BeginAbort,
            7 => Self::AbortingTransaction,
            other => Self::Unknown(other),
        }
    }

    #[inline]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Init => "Init",
            Self::WaitPid => "WaitPID",
            Self::Ready => "Ready",
            Self::InTransaction => "InTransaction",
            Self::BeginCommit => "BeginCommit",
            Self::CommittingTransaction => "CommittingTransaction",
            Self::BeginAbort => "BeginAbort",
            Self::AbortingTransaction => "AbortingTransaction",
            Self::Unknown(_) => "Unknown",
        }
    }
}

#[doc(hidden)]
impl<'a> StatsView<'a> {
    pub fn name(&self) -> Cow<'a, str> {
        c_char_array_to_cow(&self.raw.name)
    }
    pub fn client_id(&self) -> Cow<'a, str> {
        c_char_array_to_cow(&self.raw.client_id)
    }
    pub fn client_type(&self) -> ClientType {
        ClientType::from_raw(self.raw.type_)
    }

    pub fn msg_cnt(&self) -> u64 {
        self.raw.msg_cnt as u64
    }
    pub fn msg_size(&self) -> u64 {
        self.raw.msg_size
    }
    pub fn msg_max(&self) -> u64 {
        self.raw.msg_max as u64
    }
    pub fn msg_size_max(&self) -> u64 {
        self.raw.msg_size_max
    }
    pub fn txmsgs(&self) -> i64 {
        self.raw.txmsgs
    }
}

#[doc(hidden)]
impl<'a> BrokerView<'a> {
    pub fn name(&self) -> Cow<'a, str> {
        c_char_array_to_cow(&self.raw.name)
    }
    pub fn nodeid(&self) -> i32 {
        self.raw.nodeid
    }
    pub fn nodename(&self) -> Cow<'a, str> {
        c_char_array_to_cow(&self.raw.nodename)
    }
    pub fn source(&self) -> Cow<'a, str> {
        c_char_array_to_cow(&self.raw.source)
    }

    pub fn state(&self) -> BrokerState {
        BrokerState::from_raw(self.raw.state)
    }
    pub fn stateage(&self) -> i64 {
        self.raw.stateage_us
    }

    pub fn waitresp_cnt(&self) -> i64 {
        self.raw.waitresp_cnt as i64
    }
    pub fn req_timeouts(&self) -> u64 {
        self.raw.req_timeouts as u64
    }
    pub fn txretries(&self) -> u64 {
        self.raw.tx_retries as u64
    }
    pub fn txerrs(&self) -> u64 {
        self.raw.tx_errs as u64
    }
    pub fn connects(&self) -> Option<i64> {
        Some(self.raw.connects)
    }
    pub fn disconnects(&self) -> Option<i64> {
        Some(self.raw.disconnects)
    }
    pub fn rx(&self) -> u64 {
        self.raw.rx as u64
    }
    pub fn tx(&self) -> u64 {
        self.raw.tx as u64
    }
    pub fn txidle(&self) -> i64 {
        self.raw.tx_idle_us
    }
    pub fn rxidle(&self) -> i64 {
        self.raw.rx_idle_us
    }
    pub fn wakeups(&self) -> Option<u64> {
        Some(self.raw.wakeups as u64)
    }
    pub fn outbuf_cnt(&self) -> i64 {
        self.raw.outbuf_cnt as i64
    }

    pub fn int_latency(&self) -> Option<WindowView<'a>> {
        Some(WindowView {
            raw: &self.raw.int_latency,
        })
    }

    pub fn outbuf_latency(&self) -> Option<WindowView<'a>> {
        Some(WindowView {
            raw: &self.raw.outbuf_latency,
        })
    }

    pub fn rtt(&self) -> Option<WindowView<'a>> {
        Some(WindowView { raw: &self.raw.rtt })
    }

    pub fn produce_reqsize(&self) -> Option<WindowView<'a>> {
        Some(WindowView {
            raw: &self.raw.produce_reqsize,
        })
    }

    pub fn produce_partitions(&self) -> Option<WindowView<'a>> {
        Some(WindowView {
            raw: &self.raw.produce_partitions,
        })
    }

    pub fn produce_messages(&self) -> Option<WindowView<'a>> {
        Some(WindowView {
            raw: &self.raw.produce_messages,
        })
    }

    pub fn produce_fill(&self) -> Option<WindowView<'a>> {
        Some(WindowView {
            raw: &self.raw.produce_fill,
        })
    }

    pub fn batch_wait(&self) -> Option<WindowView<'a>> {
        Some(WindowView {
            raw: &self.raw.batch_wait,
        })
    }
}

#[doc(hidden)]
impl<'a> TopicView<'a> {
    pub fn name(&self) -> Cow<'a, str> {
        c_char_array_to_cow(&self.raw.name)
    }
    pub fn metadata_age(&self) -> i64 {
        self.raw.metadata_age_us / 1000
    }
    pub fn batchsize(&self) -> WindowView<'a> {
        WindowView {
            raw: &self.raw.batchsize,
        }
    }
    pub fn batchcnt(&self) -> WindowView<'a> {
        WindowView {
            raw: &self.raw.batchcnt,
        }
    }
}

#[doc(hidden)]
impl<'a> PartitionView<'a> {
    pub fn partition(&self) -> i32 {
        self.raw.partition
    }
    pub fn broker(&self) -> i32 {
        self.raw.broker_id
    }
    pub fn leader(&self) -> i32 {
        self.raw.leader
    }

    pub fn msgq_cnt(&self) -> i64 {
        self.raw.msgq_cnt as i64
    }
    pub fn msgq_bytes(&self) -> u64 {
        self.raw.msgq_bytes as u64
    }
    pub fn xmit_msgq_cnt(&self) -> i64 {
        self.raw.xmit_msgq_cnt as i64
    }
    pub fn xmit_msgq_bytes(&self) -> u64 {
        self.raw.xmit_msgq_bytes as u64
    }
    pub fn fetchq_cnt(&self) -> i64 {
        self.raw.fetchq_cnt as i64
    }
    pub fn fetchq_size(&self) -> u64 {
        self.raw.fetchq_size as u64
    }

    pub fn fetch_state(&self) -> FetchState {
        FetchState::from_raw(self.raw.fetch_state)
    }

    pub fn app_offset(&self) -> i64 {
        self.raw.app_offset
    }
    pub fn stored_offset(&self) -> i64 {
        self.raw.stored_offset
    }
    pub fn committed_offset(&self) -> i64 {
        self.raw.committed_offset
    }
    pub fn lo_offset(&self) -> i64 {
        self.raw.lo_offset
    }
    pub fn hi_offset(&self) -> i64 {
        self.raw.hi_offset
    }
    pub fn ls_offset(&self) -> i64 {
        self.raw.ls_offset
    }

    pub fn consumer_lag(&self) -> i64 {
        self.raw.consumer_lag
    }
    pub fn consumer_lag_stored(&self) -> i64 {
        self.raw.consumer_lag_stored
    }

    pub fn txmsgs(&self) -> u64 {
        self.raw.txmsgs as u64
    }
    pub fn txbytes(&self) -> u64 {
        self.raw.txbytes as u64
    }
    pub fn rxmsgs(&self) -> u64 {
        self.raw.rxmsgs as u64
    }
    pub fn rxbytes(&self) -> u64 {
        self.raw.rxbytes as u64
    }
    pub fn msgs(&self) -> u64 {
        self.raw.msgs as u64
    }
}

#[doc(hidden)]
impl<'a> ConsumerGroupView<'a> {
    pub fn state(&self) -> ConsumerGroupState {
        ConsumerGroupState::from_raw(self.raw.state)
    }

    pub fn stateage(&self) -> i64 {
        self.raw.stateage_us / 1000
    }

    pub fn join_state(&self) -> ConsumerGroupJoinState {
        ConsumerGroupJoinState::from_raw(self.raw.join_state)
    }

    pub fn rebalance_age(&self) -> i64 {
        self.raw.rebalance_age_us / 1000
    }
    pub fn rebalance_cnt(&self) -> i64 {
        self.raw.rebalance_cnt as i64
    }
    pub fn rebalance_reason(&self) -> Cow<'a, str> {
        c_char_array_to_cow(&self.raw.rebalance_reason)
    }
    pub fn assignment_size(&self) -> i32 {
        self.raw.assignment_size
    }
}

#[doc(hidden)]
impl<'a> ExactlyOnceSemanticsView<'a> {
    pub fn idemp_state(&self) -> IdempotentState {
        IdempotentState::from_raw(self.raw.idemp_state)
    }

    pub fn idemp_stateage(&self) -> i64 {
        self.raw.idemp_stateage_us / 1000
    }

    pub fn txn_state(&self) -> TransactionState {
        TransactionState::from_raw(self.raw.txn_state)
    }

    pub fn txn_stateage(&self) -> i64 {
        self.raw.txn_stateage_us / 1000
    }
    pub fn txn_may_enq(&self) -> bool {
        self.raw.txn_may_enq != 0
    }
    pub fn producer_id(&self) -> i64 {
        self.raw.producer_id
    }
    pub fn producer_epoch(&self) -> i64 {
        self.raw.producer_epoch as i64
    }
    pub fn epoch_cnt(&self) -> i64 {
        self.raw.epoch_cnt as i64
    }
}

#[doc(hidden)]
impl<'a> WindowView<'a> {
    pub fn min(&self) -> i64 {
        self.raw.min
    }
    pub fn max(&self) -> i64 {
        self.raw.max
    }
    pub fn avg(&self) -> i64 {
        self.raw.avg
    }
    pub fn sum(&self) -> i64 {
        self.raw.sum
    }
    pub fn cnt(&self) -> i64 {
        self.raw.cnt
    }
    pub fn stddev(&self) -> i64 {
        self.raw.stddev
    }
    pub fn hdrsize(&self) -> i64 {
        self.raw.hdrsize as i64
    }
    pub fn p50(&self) -> i64 {
        self.raw.p50
    }
    pub fn p75(&self) -> i64 {
        self.raw.p75
    }
    pub fn p90(&self) -> i64 {
        self.raw.p90
    }
    pub fn p95(&self) -> i64 {
        self.raw.p95
    }
    pub fn p99(&self) -> i64 {
        self.raw.p99
    }
    pub fn p99_99(&self) -> i64 {
        self.raw.p99_99
    }
    pub fn outofrange(&self) -> i64 {
        self.raw.oor
    }
}

#[doc(hidden)]
impl<'a> BrokerRequestView<'a> {
    pub fn name(&self) -> Cow<'a, str> {
        c_char_array_to_cow(&self.raw.name)
    }
    pub fn count(&self) -> i64 {
        self.raw.count
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    struct RawStatsFixture {
        stats: rdsys::rd_kafka_stats_t,
        brokers: Vec<rdsys::rd_kafka_broker_stats_t>,
        topics: Vec<rdsys::rd_kafka_topic_stats_t>,
        partitions: Vec<rdsys::rd_kafka_partition_stats_t>,
        reqs: Vec<rdsys::rd_kafka_req_count_t>,
    }

    impl RawStatsFixture {
        fn populated() -> Self {
            let mut reqs = vec![
                unsafe { std::mem::zeroed::<rdsys::rd_kafka_req_count_t>() },
                unsafe { std::mem::zeroed::<rdsys::rd_kafka_req_count_t>() },
            ];
            write_cstr(&mut reqs[0].name, "Produce");
            reqs[0].count = 12;
            write_cstr(&mut reqs[1].name, "Metadata");
            reqs[1].count = 0;

            let mut partitions =
                vec![unsafe { std::mem::zeroed::<rdsys::rd_kafka_partition_stats_t>() }];
            let partition = &mut partitions[0];
            partition.partition = 7;
            partition.broker_id = 9;
            partition.leader = 11;
            partition.msgq_cnt = 1;
            partition.msgq_bytes = 2;
            partition.xmit_msgq_cnt = 3;
            partition.xmit_msgq_bytes = 4;
            partition.fetchq_cnt = 5;
            partition.fetchq_size = 6;
            partition.fetch_state = 5;
            partition.app_offset = 17;
            partition.stored_offset = 18;
            partition.committed_offset = 19;
            partition.lo_offset = 20;
            partition.hi_offset = 21;
            partition.ls_offset = 22;
            partition.consumer_lag = 23;
            partition.consumer_lag_stored = 24;
            partition.txmsgs = 25;
            partition.txbytes = 26;
            partition.rxmsgs = 27;
            partition.rxbytes = 28;
            partition.msgs = 29;

            let mut brokers = vec![unsafe { std::mem::zeroed::<rdsys::rd_kafka_broker_stats_t>() }];
            let broker = &mut brokers[0];
            write_cstr(&mut broker.name, "broker-1");
            broker.nodeid = 9;
            write_cstr(&mut broker.nodename, "localhost:9092");
            write_cstr(&mut broker.source, "configured");
            broker.state = 6;
            broker.stateage_us = 1_234;
            broker.waitresp_cnt = 3;
            broker.req_timeouts = 4;
            broker.tx_retries = 5;
            broker.tx_errs = 6;
            broker.connects = 7;
            broker.disconnects = 8;
            broker.rx = 9;
            broker.tx = 10;
            broker.tx_idle_us = 11;
            broker.rx_idle_us = 12;
            broker.wakeups = 13;
            broker.outbuf_cnt = 14;
            broker.int_latency = make_window(100);
            broker.outbuf_latency = make_window(200);
            broker.rtt = make_window(300);
            broker.produce_reqsize = make_window(400);
            broker.produce_partitions = make_window(500);
            broker.produce_messages = make_window(600);
            broker.produce_fill = make_window(700);
            broker.batch_wait = make_window(750);
            broker.req_cnt = reqs.len() as u32;
            broker.reqs = reqs.as_mut_ptr();

            let mut topics = vec![unsafe { std::mem::zeroed::<rdsys::rd_kafka_topic_stats_t>() }];
            let topic = &mut topics[0];
            write_cstr(&mut topic.name, "topic-1");
            topic.metadata_age_us = 42_000;
            topic.batchsize = make_window(800);
            topic.batchcnt = make_window(900);
            topic.partition_cnt = partitions.len() as u32;
            topic.partitions = partitions.as_mut_ptr();

            let mut stats = unsafe { std::mem::zeroed::<rdsys::rd_kafka_stats_t>() };
            write_cstr(&mut stats.name, "handle-1");
            write_cstr(&mut stats.client_id, "client-1");
            stats.type_ = 0;
            stats.msg_cnt = 21;
            stats.msg_size = 22;
            stats.msg_max = 23;
            stats.msg_size_max = 24;
            stats.txmsgs = 25;
            stats.broker_cnt = brokers.len() as u32;
            stats.brokers = brokers.as_mut_ptr();
            stats.topic_cnt = topics.len() as u32;
            stats.topics = topics.as_mut_ptr();
            stats.has_cgrp = 1;
            stats.cgrp.state = 6;
            stats.cgrp.stateage_us = 31_000;
            stats.cgrp.join_state = 9;
            stats.cgrp.rebalance_age_us = 32_000;
            stats.cgrp.rebalance_cnt = 33;
            write_cstr(&mut stats.cgrp.rebalance_reason, "assign");
            stats.cgrp.assignment_size = 34;
            stats.has_eos = 1;
            stats.eos.idemp_state = 3;
            stats.eos.idemp_stateage_us = 41_000;
            stats.eos.txn_state = 5;
            stats.eos.txn_stateage_us = 42_000;
            stats.eos.txn_may_enq = 1;
            stats.eos.producer_id = 43;
            stats.eos.producer_epoch = 44;
            stats.eos.epoch_cnt = 45;

            Self {
                stats,
                brokers,
                topics,
                partitions,
                reqs,
            }
        }

        fn backing_len(&self) -> usize {
            self.brokers.len() + self.topics.len() + self.partitions.len() + self.reqs.len()
        }
    }

    fn write_cstr<const N: usize>(dst: &mut [c_char; N], value: &str) {
        assert!(value.len() < N);
        for (slot, byte) in dst.iter_mut().zip(value.bytes()) {
            *slot = byte as c_char;
        }
    }

    fn make_window(base: i64) -> rdsys::rd_kafka_avg_stats_t {
        rdsys::rd_kafka_avg_stats_t {
            min: base,
            max: base + 1,
            avg: base + 2,
            sum: base + 3,
            cnt: base + 4,
            stddev: base + 5,
            p50: base + 6,
            p75: base + 7,
            p90: base + 8,
            p95: base + 9,
            p99: base + 10,
            p99_99: base + 11,
            oor: base + 12,
            hdrsize: (base + 13) as i32,
            _pad: 0,
        }
    }

    fn assert_window_matches_raw(view: WindowView<'_>, expected: &rdsys::rd_kafka_avg_stats_t) {
        assert_eq!(view.min(), expected.min);
        assert_eq!(view.max(), expected.max);
        assert_eq!(view.avg(), expected.avg);
        assert_eq!(view.sum(), expected.sum);
        assert_eq!(view.cnt(), expected.cnt);
        assert_eq!(view.stddev(), expected.stddev);
        assert_eq!(view.hdrsize(), expected.hdrsize as i64);
        assert_eq!(view.p50(), expected.p50);
        assert_eq!(view.p75(), expected.p75);
        assert_eq!(view.p90(), expected.p90);
        assert_eq!(view.p95(), expected.p95);
        assert_eq!(view.p99(), expected.p99);
        assert_eq!(view.p99_99(), expected.p99_99);
        assert_eq!(view.outofrange(), expected.oor);
    }

    #[test]
    fn test_stats_view_handles_empty_native_stats() {
        let mut stats = unsafe { std::mem::zeroed::<rdsys::rd_kafka_stats_t>() };
        stats.type_ = 99;

        let view = unsafe { StatsView::new(&stats) };

        assert_eq!(view.name(), "");
        assert_eq!(view.client_id(), "");
        assert_eq!(view.client_type(), ClientType::Unknown(99));
        assert_eq!(view.msg_cnt(), 0);
        assert_eq!(view.msg_size(), 0);
        assert_eq!(view.msg_max(), 0);
        assert_eq!(view.msg_size_max(), 0);
        assert_eq!(view.txmsgs(), 0);

        let mut brokers = view.brokers();
        assert_eq!(brokers.len(), 0);
        assert!(brokers.next().is_none());

        let mut topics = view.topics();
        assert_eq!(topics.len(), 0);
        assert!(topics.next().is_none());

        assert!(view.cgrp().is_none());
        assert!(view.eos().is_none());
    }

    #[test]
    fn test_stats_view_exposes_typed_fields() {
        let fixture = RawStatsFixture::populated();
        assert_eq!(fixture.backing_len(), 5);

        let view = unsafe { StatsView::new(&fixture.stats) };

        assert_eq!(view.name(), "handle-1");
        assert_eq!(view.client_id(), "client-1");
        assert_eq!(view.client_type(), ClientType::Producer);
        assert_eq!(view.msg_cnt(), 21);
        assert_eq!(view.msg_size(), 22);
        assert_eq!(view.msg_max(), 23);
        assert_eq!(view.msg_size_max(), 24);
        assert_eq!(view.txmsgs(), 25);

        let broker = &fixture.brokers[0];
        let mut brokers = view.brokers();
        assert_eq!(brokers.len(), 1);
        let broker_view = brokers.next().unwrap();
        assert_eq!(broker_view.name(), "broker-1");
        assert_eq!(broker_view.nodeid(), broker.nodeid);
        assert_eq!(broker_view.nodename(), "localhost:9092");
        assert_eq!(broker_view.source(), "configured");
        assert_eq!(broker_view.state(), BrokerState::Up);
        assert_eq!(broker_view.stateage(), broker.stateage_us);
        assert_eq!(broker_view.waitresp_cnt(), broker.waitresp_cnt as i64);
        assert_eq!(broker_view.req_timeouts(), broker.req_timeouts as u64);
        assert_eq!(broker_view.txretries(), broker.tx_retries as u64);
        assert_eq!(broker_view.txerrs(), broker.tx_errs as u64);
        assert_eq!(broker_view.connects(), Some(broker.connects));
        assert_eq!(broker_view.disconnects(), Some(broker.disconnects));
        assert_eq!(broker_view.rx(), broker.rx as u64);
        assert_eq!(broker_view.tx(), broker.tx as u64);
        assert_eq!(broker_view.txidle(), broker.tx_idle_us);
        assert_eq!(broker_view.rxidle(), broker.rx_idle_us);
        assert_eq!(broker_view.wakeups(), Some(broker.wakeups as u64));
        assert_eq!(broker_view.outbuf_cnt(), broker.outbuf_cnt as i64);
        assert_window_matches_raw(broker_view.int_latency().unwrap(), &broker.int_latency);
        assert_window_matches_raw(
            broker_view.outbuf_latency().unwrap(),
            &broker.outbuf_latency,
        );
        assert_window_matches_raw(broker_view.rtt().unwrap(), &broker.rtt);
        assert_window_matches_raw(
            broker_view.produce_reqsize().unwrap(),
            &broker.produce_reqsize,
        );
        assert_window_matches_raw(
            broker_view.produce_partitions().unwrap(),
            &broker.produce_partitions,
        );
        assert_window_matches_raw(
            broker_view.produce_messages().unwrap(),
            &broker.produce_messages,
        );
        assert_window_matches_raw(broker_view.produce_fill().unwrap(), &broker.produce_fill);
        assert_window_matches_raw(broker_view.batch_wait().unwrap(), &broker.batch_wait);

        let request_counts: HashMap<_, _> = broker_view
            .requests()
            .map(|request| (request.name().into_owned(), request.count()))
            .collect();
        assert_eq!(request_counts.len(), 1);
        assert_eq!(request_counts.get("Produce"), Some(&12));
        assert!(!request_counts.contains_key("Metadata"));

        let topic = &fixture.topics[0];
        let mut topics = view.topics();
        assert_eq!(topics.len(), 1);
        let topic_view = topics.next().unwrap();
        assert_eq!(topic_view.name(), "topic-1");
        assert_eq!(topic_view.metadata_age(), topic.metadata_age_us / 1000);
        assert_window_matches_raw(topic_view.batchsize(), &topic.batchsize);
        assert_window_matches_raw(topic_view.batchcnt(), &topic.batchcnt);

        let partition = &fixture.partitions[0];
        let mut partitions = topic_view.partitions();
        assert_eq!(partitions.len(), 1);
        let partition_view = partitions.next().unwrap();
        assert_eq!(partition_view.partition(), partition.partition);
        assert_eq!(partition_view.broker(), partition.broker_id);
        assert_eq!(partition_view.leader(), partition.leader);
        assert_eq!(partition_view.msgq_cnt(), partition.msgq_cnt as i64);
        assert_eq!(partition_view.msgq_bytes(), partition.msgq_bytes as u64);
        assert_eq!(
            partition_view.xmit_msgq_cnt(),
            partition.xmit_msgq_cnt as i64
        );
        assert_eq!(
            partition_view.xmit_msgq_bytes(),
            partition.xmit_msgq_bytes as u64
        );
        assert_eq!(partition_view.fetchq_cnt(), partition.fetchq_cnt as i64);
        assert_eq!(partition_view.fetchq_size(), partition.fetchq_size as u64);
        assert_eq!(partition_view.fetch_state(), FetchState::ValidateEpochWait);
        assert_eq!(partition_view.app_offset(), partition.app_offset);
        assert_eq!(partition_view.stored_offset(), partition.stored_offset);
        assert_eq!(
            partition_view.committed_offset(),
            partition.committed_offset
        );
        assert_eq!(partition_view.lo_offset(), partition.lo_offset);
        assert_eq!(partition_view.hi_offset(), partition.hi_offset);
        assert_eq!(partition_view.ls_offset(), partition.ls_offset);
        assert_eq!(partition_view.consumer_lag(), partition.consumer_lag);
        assert_eq!(
            partition_view.consumer_lag_stored(),
            partition.consumer_lag_stored
        );
        assert_eq!(partition_view.txmsgs(), partition.txmsgs as u64);
        assert_eq!(partition_view.txbytes(), partition.txbytes as u64);
        assert_eq!(partition_view.rxmsgs(), partition.rxmsgs as u64);
        assert_eq!(partition_view.rxbytes(), partition.rxbytes as u64);
        assert_eq!(partition_view.msgs(), partition.msgs as u64);

        let cgrp_view = view.cgrp().unwrap();
        assert_eq!(cgrp_view.state(), ConsumerGroupState::Up);
        assert_eq!(cgrp_view.stateage(), 31);
        assert_eq!(cgrp_view.join_state(), ConsumerGroupJoinState::Steady);
        assert_eq!(cgrp_view.rebalance_age(), 32);
        assert_eq!(cgrp_view.rebalance_cnt(), 33);
        assert_eq!(cgrp_view.rebalance_reason(), "assign");
        assert_eq!(cgrp_view.assignment_size(), 34);

        let eos_view = view.eos().unwrap();
        assert_eq!(eos_view.idemp_state(), IdempotentState::Assigned);
        assert_eq!(eos_view.idemp_stateage(), 41);
        assert_eq!(
            eos_view.txn_state(),
            TransactionState::CommittingTransaction
        );
        assert_eq!(eos_view.txn_stateage(), 42);
        assert!(eos_view.txn_may_enq());
        assert_eq!(eos_view.producer_id(), 43);
        assert_eq!(eos_view.producer_epoch(), 44);
        assert_eq!(eos_view.epoch_cnt(), 45);
    }

    #[test]
    fn test_stats_view_unknown_enum_values() {
        assert_eq!(FetchState::from_raw(5), FetchState::ValidateEpochWait);
        assert_eq!(FetchState::from_raw(5).as_str(), "validate-epoch-wait");
        assert_eq!(FetchState::from_raw(6), FetchState::Active);
        assert_eq!(FetchState::from_raw(6).as_str(), "active");

        assert_eq!(BrokerState::from_raw(-1), BrokerState::Unknown(-1));
        assert_eq!(BrokerState::from_raw(-1).as_str(), "UNKNOWN");

        assert_eq!(ClientType::from_raw(99), ClientType::Unknown(99));
        assert_eq!(ClientType::from_raw(99).as_str(), "unknown");

        assert_eq!(FetchState::from_raw(-1), FetchState::Unknown(-1));
        assert_eq!(FetchState::from_raw(-1).as_str(), "unknown");

        assert_eq!(
            ConsumerGroupState::from_raw(99),
            ConsumerGroupState::Unknown(99)
        );
        assert_eq!(ConsumerGroupState::from_raw(99).as_str(), "unknown");

        assert_eq!(
            ConsumerGroupJoinState::from_raw(99),
            ConsumerGroupJoinState::Unknown(99)
        );
        assert_eq!(ConsumerGroupJoinState::from_raw(99).as_str(), "unknown");

        assert_eq!(IdempotentState::from_raw(99), IdempotentState::Unknown(99));
        assert_eq!(IdempotentState::from_raw(99).as_str(), "Unknown");

        assert_eq!(
            TransactionState::from_raw(99),
            TransactionState::Unknown(99)
        );
        assert_eq!(TransactionState::from_raw(99).as_str(), "Unknown");
    }
}
