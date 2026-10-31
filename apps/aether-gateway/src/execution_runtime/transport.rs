use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::error::Error as _;
use std::future::Future;
use std::io::Read;
use std::io::Write;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex as StdMutex, OnceLock, RwLock as StdRwLock};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use aether_contracts::{
    ExecutionPlan, ExecutionResponseBodyMode, ExecutionResponseObservation, ExecutionResult,
    ExecutionTelemetry, ProxySnapshot, ResolvedTransportProfile, ResponseBody,
    EXECUTION_REQUEST_FOLLOW_REDIRECTS_HEADER, EXECUTION_REQUEST_HTTP1_ONLY_HEADER,
    EXECUTION_RESPONSE_BODY_MODE_HEADER, PROXY_NODE_GENERATION_EXTRA_KEY,
    TRANSPORT_BACKEND_BROWSER_WREQ, TRANSPORT_BACKEND_REQWEST_RUSTLS,
    TRANSPORT_HTTP_MODE_H2C_PRIOR_KNOWLEDGE, TRANSPORT_HTTP_MODE_HTTP1_ONLY,
};
use aether_data::repository::proxy_nodes::ProxyNodeTrafficMutation;
use aether_http::{apply_http_client_config, is_private_or_reserved_ip, HttpClientConfig};
use aether_runtime::{MetricKind, MetricSample};
use axum::body::Bytes;
use base64::Engine as _;
use brotli::Decompressor as BrotliDecoder;
use flate2::read::{DeflateDecoder, GzDecoder};
use flate2::write::GzEncoder;
use flate2::Compression;
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming as HyperIncomingBody;
use hyper::client::conn::http2::SendRequest as HyperH2cSendRequest;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client as HyperLegacyClient;
#[cfg(test)]
use hyper_util::rt::TokioTimer;
use hyper_util::rt::{TokioExecutor, TokioIo};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::redirect::Policy;
use serde::Serialize;
use serde_json::json;
use serde_json::Value;
use sha2::Digest as _;
use thiserror::Error;
use tokio::net::TcpStream;
use tokio::sync::OnceCell as TokioOnceCell;

use crate::ai_serving::api::extract_provider_private_stream_error_body;
#[cfg(test)]
use crate::execution_runtime::remote_compat::execute_sync_plan_via_remote_execution_runtime;
use crate::execution_runtime::stream_read_timeout::resolve_stream_idle_timeout;
use crate::frontdoor_loop_guard::gateway_frontdoor_self_loop_guard_error;
use crate::stage_metrics::observe_gateway_stage_ms;
use crate::upstream_admission::UpstreamTargetAdmissionPermit;
use crate::{AppState, GatewayError};

const MAX_SAFE_REDIRECTS: usize = 10;
const MAX_UPSTREAM_ERROR_DETAIL_BYTES: usize = 2_048;
const DEFAULT_STREAM_FIRST_BYTE_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_NON_STREAM_TOTAL_TIMEOUT_MS: u64 = 300_000;
const DEFAULT_CODEX_COMPACT_TOTAL_TIMEOUT_MS: u64 = 1_200_000;
const EXECUTION_RESPONSE_BODY_LIMIT_HEADER: &str = "x-aether-execution-response-body-limit-bytes";
const DEFAULT_SCOPED_RESPONSE_BODY_LIMIT_BYTES: usize = 8 * 1024 * 1024;
const MIN_SCOPED_RESPONSE_BODY_LIMIT_BYTES: usize = 64 * 1024;
const MAX_SCOPED_RESPONSE_BODY_LIMIT_BYTES: usize = 64 * 1024 * 1024;
// A remote execution result is JSON and may carry both the parsed JSON body
// and the original bytes.  Keep room for base64 expansion, the second body
// representation, and bounded response metadata while retaining a hard cap.
#[cfg(test)]
const MAX_EXECUTION_RESULT_ENVELOPE_BYTES: usize = 256 * 1024 * 1024;
#[cfg(test)]
const EXECUTION_RESULT_ENVELOPE_METADATA_BYTES: usize = 8 * 1024 * 1024;
const DIRECT_REQWEST_H2_CLIENT_SHARDS_ENV: &str = "AETHER_GATEWAY_DIRECT_REQWEST_H2_CLIENT_SHARDS";
const DIRECT_REQWEST_CLIENT_SHARDS_ENV: &str = "AETHER_GATEWAY_DIRECT_REQWEST_CLIENT_SHARDS";
const DIRECT_REQWEST_H2_TARGET_STREAMS_PER_CLIENT_ENV: &str =
    "AETHER_GATEWAY_DIRECT_REQWEST_H2_TARGET_STREAMS_PER_CLIENT";
const DIRECT_REQWEST_HTTP1_TARGET_STREAMS_PER_CLIENT_ENV: &str =
    "AETHER_GATEWAY_DIRECT_REQWEST_HTTP1_TARGET_STREAMS_PER_CLIENT";
const DIRECT_REQWEST_STREAM_HTTP_MODE_ENV: &str = "AETHER_GATEWAY_DIRECT_REQWEST_STREAM_HTTP_MODE";
const DIRECT_REQWEST_CACHE_PER_ORIGIN_ENV: &str = "AETHER_GATEWAY_DIRECT_REQWEST_CACHE_PER_ORIGIN";
const DIRECT_H2C_FAST_PATH_ENV: &str = "AETHER_GATEWAY_DIRECT_H2C_FAST_PATH";
const DIRECT_H2C_CLIENT_SHARDS_ENV: &str = "AETHER_GATEWAY_DIRECT_H2C_CLIENT_SHARDS";
const DIRECT_H2C_POOL_MAX_IDLE_PER_HOST_ENV: &str =
    "AETHER_GATEWAY_DIRECT_H2C_POOL_MAX_IDLE_PER_HOST";
const DIRECT_H2C_TARGET_STREAMS_PER_CLIENT_ENV: &str =
    "AETHER_GATEWAY_DIRECT_H2C_TARGET_STREAMS_PER_CLIENT";
const DIRECT_H2C_SENDER_SELECT_WINDOW_ENV: &str = "AETHER_GATEWAY_DIRECT_H2C_SENDER_SELECT_WINDOW";
const DIRECT_H2C_ADAPTIVE_WINDOW_ENV: &str = "AETHER_GATEWAY_DIRECT_H2C_ADAPTIVE_WINDOW";
const DIRECT_H2C_DRIVER_RUNTIME_THREADS_ENV: &str =
    "AETHER_GATEWAY_DIRECT_H2C_DRIVER_RUNTIME_THREADS";
const DIRECT_H2C_PREWARM_URLS_ENV: &str = "AETHER_GATEWAY_DIRECT_H2C_PREWARM_URLS";
const DIRECT_H2C_PREWARM_READY_ENV: &str = "AETHER_GATEWAY_DIRECT_H2C_PREWARM_READY";
const DIRECT_H2C_PREWARM_CONNECT_TIMEOUT_MS_ENV: &str =
    "AETHER_GATEWAY_DIRECT_H2C_PREWARM_CONNECT_TIMEOUT_MS";
const DIRECT_REQWEST_SYNC_WARM_CLIENTS_ENV: &str =
    "AETHER_GATEWAY_DIRECT_REQWEST_SYNC_WARM_CLIENTS";
const DIRECT_REQWEST_PREWARM_SYNC_CLIENTS_ENV: &str =
    "AETHER_GATEWAY_DIRECT_REQWEST_PREWARM_SYNC_CLIENTS";
const DEFAULT_H2_TARGET_STREAMS_PER_CLIENT: usize = 8;
const DEFAULT_HTTP1_TARGET_STREAMS_PER_CLIENT: usize = 512;
const DEFAULT_DIRECT_H2C_POOL_MAX_IDLE_PER_HOST: usize = 32;
const DEFAULT_DIRECT_H2C_TARGET_STREAMS_PER_CLIENT: usize = 128;
const DEFAULT_DIRECT_H2C_SENDER_SELECT_WINDOW: usize = 4;
const MAX_DIRECT_H2C_DRIVER_RUNTIME_THREADS: usize = 16;
const DIRECT_H2C_DRIVER_RUNTIME_MAX_BLOCKING_THREADS: usize = 16;
const DIRECT_H2C_DRIVER_RUNTIME_STACK_BYTES: usize = 2 * 1024 * 1024;
const DIRECT_H2C_DRIVER_RUNTIME_THREAD_NAME: &str = "aether-h2c-driver";
const DEFAULT_DIRECT_REQWEST_SYNC_WARM_CLIENTS: usize = 4;
const MAX_DIRECT_REQWEST_SYNC_WARM_CLIENTS: usize = 16;
const MAX_DIRECT_H2C_CLIENT_SHARDS: usize = 512;
const MAX_DIRECT_REQWEST_H2_CLIENT_SHARDS: usize = 2048;
// One entry owns a few `reqwest::Client` pools, so its footprint is dominated
// by connection state rather than by the key. A budget over that estimate keeps
// this cache in the same unit as the rest of the gateway's caches instead of
// carrying a second, differently-shaped cap.
const DIRECT_REQWEST_CLIENT_ENTRY_ESTIMATED_BYTES: usize = 64 * 1024;
const DIRECT_REQWEST_CLIENT_CACHE_CAPACITY_BYTES: usize = 64 * 1024 * 1024;

type DirectHyperH2cRequestBody = Full<Bytes>;
type DirectHyperH2cClient =
    HyperLegacyClient<HttpConnector<ExecutionSafeHyperDnsResolver>, DirectHyperH2cRequestBody>;
type DirectHyperH2cSender = HyperH2cSendRequest<DirectHyperH2cRequestBody>;
type DirectHyperH2cSenderCacheCell = TokioOnceCell<Arc<DirectHyperH2cSenderCacheEntry>>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct DirectReqwestClientCacheKey {
    upstream_origin: Option<String>,
    pool_partition: Option<String>,
    connect_timeout_ms: Option<u64>,
    proxy_digest: Option<String>,
    follow_redirects: bool,
    http1_only: bool,
    transport_profile: Option<DirectReqwestTransportProfileCacheKey>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct DirectReqwestTransportProfileCacheKey {
    profile_id: String,
    backend: String,
    http_mode: String,
    pool_scope: String,
    header_fingerprint: Option<String>,
    extra: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct DirectHyperH2cClientCacheKey {
    upstream_origin: String,
    connect_timeout_ms: Option<u64>,
    pool_max_idle_per_host: usize,
}

struct DirectReqwestClientCacheEntry {
    clients: Vec<reqwest::Client>,
    next: AtomicU64,
    target_len: usize,
    warming: bool,
    last_used: u64,
}

impl DirectReqwestClientCacheEntry {
    fn new(clients: Vec<reqwest::Client>, target_len: usize, warming: bool) -> Self {
        Self {
            clients,
            next: AtomicU64::new(0),
            target_len: target_len.max(1),
            warming,
            last_used: next_direct_reqwest_client_cache_clock(),
        }
    }

    fn select(&self) -> reqwest::Client {
        if self.clients.len() <= 1 {
            return self
                .clients
                .first()
                .expect("direct reqwest client cache entry should contain a client")
                .clone();
        }
        let index = self.next.fetch_add(1, Ordering::Relaxed) as usize % self.clients.len();
        self.clients[index].clone()
    }

    fn len(&self) -> usize {
        self.clients.len()
    }

    fn should_warm(&self) -> bool {
        self.clients.len() < self.target_len && !self.warming
    }

    fn touch(&mut self) {
        self.last_used = next_direct_reqwest_client_cache_clock();
    }
}

struct DirectHyperH2cClientCacheEntry {
    clients: Vec<DirectHyperH2cClient>,
    #[cfg_attr(not(test), allow(dead_code))]
    next: AtomicU64,
    target_len: usize,
}

struct DirectHyperH2cSenderCacheEntry {
    senders: Vec<Arc<DirectHyperH2cSenderSlot>>,
    next: AtomicU64,
    target_len: usize,
}

impl DirectHyperH2cSenderCacheEntry {
    fn new(senders: Vec<DirectHyperH2cSender>, target_len: usize) -> Self {
        Self {
            senders: senders
                .into_iter()
                .map(DirectHyperH2cSenderSlot::new)
                .collect(),
            next: AtomicU64::new(0),
            target_len: target_len.max(1),
        }
    }

    fn select(&self) -> DirectHyperH2cSenderLease {
        if self.senders.len() <= 1 {
            let slot = self
                .senders
                .first()
                .expect("direct h2c sender cache entry should contain a sender")
                .clone();
            return DirectHyperH2cSenderLease::new(slot);
        }
        let start = self.next.fetch_add(1, Ordering::Relaxed) as usize;
        let window = direct_h2c_sender_select_window()
            .min(self.senders.len())
            .max(1);
        let mut selected_index = start % self.senders.len();
        let mut selected_load = self.senders[selected_index].in_flight();
        for offset in 1..window {
            let index = start.wrapping_add(offset) % self.senders.len();
            let load = self.senders[index].in_flight();
            if load < selected_load {
                selected_index = index;
                selected_load = load;
                if load == 0 {
                    break;
                }
            }
        }
        DirectHyperH2cSenderLease::new(Arc::clone(&self.senders[selected_index]))
    }

    fn len(&self) -> usize {
        self.senders.len()
    }

    fn in_flight(&self) -> u64 {
        self.senders.iter().map(|sender| sender.in_flight()).sum()
    }

    fn max_in_flight(&self) -> u64 {
        self.senders
            .iter()
            .map(|sender| sender.max_in_flight())
            .max()
            .unwrap_or(0)
    }
}

struct DirectHyperH2cSenderSlot {
    sender: DirectHyperH2cSender,
    in_flight: AtomicU64,
    max_in_flight: AtomicU64,
}

impl DirectHyperH2cSenderSlot {
    fn new(sender: DirectHyperH2cSender) -> Arc<Self> {
        Arc::new(Self {
            sender,
            in_flight: AtomicU64::new(0),
            max_in_flight: AtomicU64::new(0),
        })
    }

    fn acquire(self: &Arc<Self>) -> DirectHyperH2cSenderLease {
        let in_flight = self.in_flight.fetch_add(1, Ordering::AcqRel) + 1;
        self.max_in_flight.fetch_max(in_flight, Ordering::AcqRel);
        DirectHyperH2cSenderLease {
            sender: self.sender.clone(),
            slot: Some(Arc::clone(self)),
        }
    }

    fn in_flight(&self) -> u64 {
        self.in_flight.load(Ordering::Acquire)
    }

    fn max_in_flight(&self) -> u64 {
        self.max_in_flight.load(Ordering::Acquire)
    }
}

struct DirectHyperH2cSenderLease {
    sender: DirectHyperH2cSender,
    slot: Option<Arc<DirectHyperH2cSenderSlot>>,
}

impl DirectHyperH2cSenderLease {
    fn new(slot: Arc<DirectHyperH2cSenderSlot>) -> Self {
        slot.acquire()
    }

    fn sender(&mut self) -> &mut DirectHyperH2cSender {
        &mut self.sender
    }

    fn release(&mut self) {
        if let Some(slot) = self.slot.take() {
            slot.in_flight.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

impl Drop for DirectHyperH2cSenderLease {
    fn drop(&mut self) {
        self.release();
    }
}

impl DirectHyperH2cClientCacheEntry {
    #[cfg(test)]
    fn new(clients: Vec<DirectHyperH2cClient>, target_len: usize) -> Self {
        Self {
            clients,
            next: AtomicU64::new(0),
            target_len: target_len.max(1),
        }
    }

    #[cfg(test)]
    fn select(&self) -> DirectHyperH2cClient {
        if self.clients.len() <= 1 {
            return self
                .clients
                .first()
                .expect("direct h2c client cache entry should contain a client")
                .clone();
        }
        let index = self.next.fetch_add(1, Ordering::Relaxed) as usize % self.clients.len();
        self.clients[index].clone()
    }

    fn len(&self) -> usize {
        self.clients.len()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DirectReqwestStreamHttpMode {
    Http1,
    Auto,
}

static DIRECT_REQWEST_CLIENT_CACHE: LazyLock<
    StdMutex<HashMap<DirectReqwestClientCacheKey, DirectReqwestClientCacheEntry>>,
> = LazyLock::new(|| StdMutex::new(HashMap::new()));

static DIRECT_REQWEST_CLIENT_CACHE_CLOCK: AtomicU64 = AtomicU64::new(0);

static DIRECT_H2C_CLIENT_CACHE: LazyLock<
    StdMutex<HashMap<DirectHyperH2cClientCacheKey, DirectHyperH2cClientCacheEntry>>,
> = LazyLock::new(|| StdMutex::new(HashMap::new()));

static DIRECT_H2C_SENDER_CACHE: LazyLock<
    StdRwLock<HashMap<DirectHyperH2cClientCacheKey, Arc<DirectHyperH2cSenderCacheCell>>>,
> = LazyLock::new(|| StdRwLock::new(HashMap::new()));

static DIRECT_H2C_POOL_MAX_IDLE_PER_HOST: LazyLock<usize> = LazyLock::new(|| {
    env_positive_usize(DIRECT_H2C_POOL_MAX_IDLE_PER_HOST_ENV)
        .unwrap_or(DEFAULT_DIRECT_H2C_POOL_MAX_IDLE_PER_HOST)
        .min(1024)
});

static DIRECT_H2C_SENDER_SELECT_WINDOW: LazyLock<usize> = LazyLock::new(|| {
    env_positive_usize(DIRECT_H2C_SENDER_SELECT_WINDOW_ENV)
        .unwrap_or(DEFAULT_DIRECT_H2C_SENDER_SELECT_WINDOW)
        .clamp(1, MAX_DIRECT_H2C_CLIENT_SHARDS)
});

static DIRECT_REQWEST_STREAM_HTTP_MODE: LazyLock<DirectReqwestStreamHttpMode> =
    LazyLock::new(|| {
        std::env::var(DIRECT_REQWEST_STREAM_HTTP_MODE_ENV)
            .ok()
            .map(|value| parse_direct_reqwest_stream_http_mode(&value))
            .unwrap_or(DirectReqwestStreamHttpMode::Http1)
    });

#[derive(Debug, Default)]
struct DirectReqwestClientCacheMetrics {
    hits: AtomicU64,
    misses: AtomicU64,
    builds: AtomicU64,
    warm_enqueues: AtomicU64,
    warm_skipped_total: AtomicU64,
    http1_selections: AtomicU64,
    h2c_selections: AtomicU64,
    auto_selections: AtomicU64,
    evictions: AtomicU64,
}

static DIRECT_REQWEST_CLIENT_CACHE_METRICS: LazyLock<DirectReqwestClientCacheMetrics> =
    LazyLock::new(DirectReqwestClientCacheMetrics::default);

#[derive(Debug, Default)]
struct DirectHyperH2cClientCacheMetrics {
    hits: AtomicU64,
    misses: AtomicU64,
    builds: AtomicU64,
}

static DIRECT_H2C_CLIENT_CACHE_METRICS: LazyLock<DirectHyperH2cClientCacheMetrics> =
    LazyLock::new(DirectHyperH2cClientCacheMetrics::default);

#[derive(Debug, Default)]
struct DirectHyperH2cSenderCacheMetrics {
    hits: AtomicU64,
    misses: AtomicU64,
    builds: AtomicU64,
    prewarm_requested: AtomicU64,
    prewarm_success: AtomicU64,
    prewarm_failed: AtomicU64,
}

static DIRECT_H2C_SENDER_CACHE_METRICS: LazyLock<DirectHyperH2cSenderCacheMetrics> =
    LazyLock::new(DirectHyperH2cSenderCacheMetrics::default);

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct ExecutionSafeDnsResolver;

#[derive(Debug, Clone, Copy, Default)]
struct ExecutionSafeHyperDnsResolver;

fn validate_resolved_execution_addresses(
    addresses: Vec<SocketAddr>,
) -> Result<Vec<SocketAddr>, std::io::Error> {
    if addresses.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "upstream DNS resolution returned no addresses",
        ));
    }
    Ok(addresses)
}

async fn resolve_execution_dns_addresses(host: &str) -> Result<Vec<SocketAddr>, std::io::Error> {
    resolve_execution_target_addresses(host, 0).await
}

async fn resolve_execution_target_addresses(
    host: &str,
    port: u16,
) -> Result<Vec<SocketAddr>, std::io::Error> {
    let addresses =
        aether_http::lookup_host_with_limits(host, port, aether_http::DEFAULT_DNS_LOOKUP_TIMEOUT)
            .await?;
    validate_resolved_execution_addresses(addresses)
}

impl reqwest::dns::Resolve for ExecutionSafeDnsResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let host = name.as_str().to_owned();
        Box::pin(async move {
            let addresses = resolve_execution_dns_addresses(&host)
                .await
                .map_err(|error| Box::new(error) as Box<dyn std::error::Error + Send + Sync>)?;
            Ok(Box::new(addresses.into_iter()) as reqwest::dns::Addrs)
        })
    }
}

impl wreq::dns::Resolve for ExecutionSafeDnsResolver {
    fn resolve(&self, name: wreq::dns::Name) -> wreq::dns::Resolving {
        let host = name.as_str().to_owned();
        Box::pin(async move {
            let addresses = resolve_execution_dns_addresses(&host)
                .await
                .map_err(|error| Box::new(error) as Box<dyn std::error::Error + Send + Sync>)?;
            Ok(Box::new(addresses.into_iter()) as wreq::dns::Addrs)
        })
    }
}

impl tower::Service<hyper_util::client::legacy::connect::dns::Name>
    for ExecutionSafeHyperDnsResolver
{
    type Response = std::vec::IntoIter<SocketAddr>;
    type Error = std::io::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, name: hyper_util::client::legacy::connect::dns::Name) -> Self::Future {
        let host = name.as_str().to_owned();
        Box::pin(async move {
            resolve_execution_dns_addresses(&host)
                .await
                .map(|addrs| addrs.into_iter())
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct DirectH2cSenderPrewarmReport {
    pub requested_urls: u64,
    pub unique_targets: u64,
    pub warmed_targets: u64,
    pub failed_targets: u64,
    pub ready_required: bool,
    pub first_error: Option<String>,
}

pub(crate) fn format_upstream_request_error(err: &reqwest::Error) -> String {
    let mut kinds = Vec::new();
    if err.is_connect() {
        kinds.push("connect");
    }
    if err.is_timeout() {
        kinds.push("timeout");
    }
    if err.is_redirect() {
        kinds.push("redirect");
    }
    if err.is_body() {
        kinds.push("body");
    }
    if err.is_decode() {
        kinds.push("decode");
    }
    if err.is_request() {
        kinds.push("request");
    }

    let mut detail = err.to_string();
    let mut source = err.source();
    while let Some(cause) = source {
        let cause_text = cause.to_string();
        if !cause_text.is_empty() && !detail.contains(&cause_text) {
            detail.push_str(": ");
            detail.push_str(&cause_text);
        }
        source = cause.source();
    }

    if let Some(url) = err.url() {
        let (sanitized_detail, sanitized_url) =
            sanitize_upstream_request_error_detail(&detail, url.as_str());
        detail = sanitized_detail;
        detail.push_str(" [url=");
        detail.push_str(&sanitized_url);
        detail.push(']');
    }
    if !kinds.is_empty() {
        detail.push_str(" [kind=");
        detail.push_str(&kinds.join(","));
        detail.push(']');
    }

    sanitize_error_detail(&detail)
}

fn sanitize_upstream_request_error_detail(detail: &str, upstream_url: &str) -> (String, String) {
    let sanitized_url = sanitize_upstream_url_text(upstream_url);
    (detail.replace(upstream_url, &sanitized_url), sanitized_url)
}

fn sanitize_upstream_url_text(upstream_url: &str) -> String {
    if let Ok(mut parsed_url) = reqwest::Url::parse(upstream_url) {
        // URL userinfo can contain proxy or upstream credentials. reqwest's
        // error chain may include the original URL, so remove it alongside
        // query and fragment data before the error crosses a trust boundary.
        let _ = parsed_url.set_username("");
        let _ = parsed_url.set_password(None);
        parsed_url.set_query(None);
        parsed_url.set_fragment(None);
        let private_literal = match parsed_url.host() {
            Some(url::Host::Ipv4(address)) => is_private_or_reserved_ip(IpAddr::V4(address)),
            Some(url::Host::Ipv6(address)) => is_private_or_reserved_ip(IpAddr::V6(address)),
            _ => false,
        };
        if private_literal {
            let _ = parsed_url.set_host(Some("redacted.invalid"));
        }
        return parsed_url.to_string();
    }

    let suffix_offset = upstream_url
        .char_indices()
        .find_map(|(offset, character)| matches!(character, '?' | '#').then_some(offset))
        .unwrap_or(upstream_url.len());
    let mut sanitized = upstream_url[..suffix_offset].to_string();
    // Keep malformed URL diagnostics useful without carrying userinfo across
    // the boundary.  All indices here are ASCII delimiters discovered in a
    // UTF-8 string, so the range boundaries remain valid.
    if let Some(scheme_end) = sanitized.find("://") {
        let authority_end = sanitized[scheme_end + 3..]
            .find('/')
            .map(|offset| scheme_end + 3 + offset)
            .unwrap_or(sanitized.len());
        if let Some(at) = sanitized[scheme_end + 3..authority_end].rfind('@') {
            let at = scheme_end + 3 + at;
            sanitized.replace_range(scheme_end + 3..=at, "");
        }
    }
    sanitized
}

fn sanitize_error_detail(detail: &str) -> String {
    let mut sanitized = String::with_capacity(detail.len().min(MAX_UPSTREAM_ERROR_DETAIL_BYTES));
    for (index, token) in detail.split_whitespace().enumerate() {
        if index > 0 {
            sanitized.push(' ');
        }
        sanitized.push_str(&sanitize_error_token(token));
    }
    if sanitized.len() > MAX_UPSTREAM_ERROR_DETAIL_BYTES {
        let mut end = MAX_UPSTREAM_ERROR_DETAIL_BYTES;
        while !sanitized.is_char_boundary(end) {
            end = end.saturating_sub(1);
        }
        sanitized.truncate(end);
        sanitized.push_str("...");
    }
    sanitized
}

fn sanitize_error_token(token: &str) -> String {
    let Some(scheme_offset) = token.find("://") else {
        return token.to_string();
    };
    let mut start = scheme_offset;
    while start > 0 {
        let previous = token[..start]
            .chars()
            .next_back()
            .expect("non-empty URL prefix should contain a character");
        if matches!(
            previous,
            '(' | '[' | '{' | '"' | '\'' | '=' | ';' | ',' | ':'
        ) {
            break;
        }
        start -= previous.len_utf8();
    }
    let mut end = token.len();
    while end > start {
        let last = token.as_bytes()[end - 1] as char;
        if matches!(last, ')' | ']' | '}' | '"' | '\'' | ',' | ';') {
            end -= 1;
        } else {
            break;
        }
    }
    let candidate = &token[start..end];
    let Ok(parsed) = reqwest::Url::parse(candidate) else {
        return token.to_string();
    };
    let sanitized = sanitize_upstream_url_text(parsed.as_str());
    let mut result = String::with_capacity(token.len());
    result.push_str(&token[..start]);
    result.push_str(&sanitized);
    result.push_str(&token[end..]);
    result
}

/// Return a bounded diagnostic suitable for scheduler/usage records and
/// structured logs.  `ExecutionRuntimeTransportError` keeps rich dynamic
/// details for local control flow, but its `Display` implementation is also
/// used by older call sites that persist the message.  Route those boundaries
/// through the same URL/query/credential sanitizer as the custom `Debug`
/// implementation so a future error constructor cannot leak request secrets.
pub(crate) fn safe_transport_error_message(error: &ExecutionRuntimeTransportError) -> String {
    sanitize_error_detail(&error.to_string())
}

pub(crate) fn format_wreq_upstream_request_error(err: &wreq::Error) -> String {
    let mut kinds = Vec::new();
    if err.is_connect() {
        kinds.push("connect");
    }
    if err.is_timeout() {
        kinds.push("timeout");
    }
    if err.is_redirect() {
        kinds.push("redirect");
    }
    if err.is_body() {
        kinds.push("body");
    }
    if err.is_decode() {
        kinds.push("decode");
    }
    if err.is_request() {
        kinds.push("request");
    }

    let mut detail = err.to_string();
    let mut source = err.source();
    while let Some(cause) = source {
        let cause_text = cause.to_string();
        if !cause_text.is_empty() && !detail.contains(&cause_text) {
            detail.push_str(": ");
            detail.push_str(&cause_text);
        }
        source = cause.source();
    }

    if let Some(uri) = err.uri() {
        let uri = uri.to_string();
        let (sanitized_detail, sanitized_uri) =
            sanitize_upstream_request_error_detail(&detail, &uri);
        detail = sanitized_detail;
        detail.push_str(" [uri=");
        detail.push_str(&sanitized_uri);
        detail.push(']');
    }
    if !kinds.is_empty() {
        detail.push_str(" [kind=");
        detail.push_str(&kinds.join(","));
        detail.push(']');
    }

    sanitize_error_detail(&detail)
}

pub(crate) fn format_hyper_error_chain(err: &dyn std::error::Error) -> String {
    let mut detail = err.to_string();
    let mut source = err.source();
    while let Some(cause) = source {
        let cause_text = cause.to_string();
        if !cause_text.is_empty() && !detail.contains(&cause_text) {
            detail.push_str(": ");
            detail.push_str(&cause_text);
        }
        source = cause.source();
    }
    sanitize_error_detail(&detail)
}

#[derive(Error)]
pub(crate) enum ExecutionRuntimeTransportError {
    #[allow(dead_code)]
    #[error("request body must contain json_body or body_bytes_b64")]
    RequestBodyRequired,
    #[error("request body must not contain both json_body and body_bytes_b64")]
    RequestBodyAmbiguous,
    #[error("request body base64 is invalid: {0}")]
    BodyDecode(base64::DecodeError),
    #[error("request body exceeds {limit_bytes} decoded bytes")]
    BodyTooLarge { limit_bytes: usize },
    #[error("request content-encoding is not supported: {}", sanitize_error_detail(.0))]
    UnsupportedContentEncoding(String),
    #[error("proxy execution is not supported")]
    ProxyUnsupported,
    #[error("invalid method: {}", sanitize_error_detail(&.0.to_string()))]
    InvalidMethod(#[from] http::method::InvalidMethod),
    #[error("invalid upstream header name: {}", sanitize_error_detail(.0))]
    InvalidHeaderName(String),
    #[error("invalid upstream header value for {}", sanitize_error_detail(.0))]
    InvalidHeaderValue(String),
    #[error("invalid proxy configuration")]
    InvalidProxy(#[source] reqwest::Error),
    #[error("unsupported transport profile backend: {}", sanitize_error_detail(.0))]
    UnsupportedTransportProfile(String),
    #[error("failed to encode request body")]
    BodyEncode(#[source] serde_json::Error),
    #[error("failed to build HTTP client")]
    ClientBuild(#[source] reqwest::Error),
    #[error("failed to build browser impersonation HTTP client")]
    BrowserClientBuild(#[source] wreq::Error),
    #[error("browser impersonation response body failed: {}", sanitize_error_detail(.0))]
    BrowserBody(String),
    #[error("{}", sanitize_error_detail(message))]
    #[allow(dead_code)]
    UpstreamHttpStatus { status_code: u16, message: String },
    #[error("failed to execute upstream request: {}", sanitize_error_detail(.0))]
    UpstreamRequest(String),
    #[error("upstream response {phase} body exceeds {limit_bytes} bytes")]
    UpstreamResponseTooLarge {
        phase: UpstreamResponseBodyPhase,
        limit_bytes: usize,
    },
    #[error(
        "failed to decode upstream response body with content-encoding {}: {}",
        sanitize_error_detail(encoding),
        sanitize_error_detail(message)
    )]
    UpstreamResponseDecode { encoding: String, message: String },
    #[error("hub relay request failed: {}", sanitize_error_detail(.0))]
    RelayError(String),
    #[error("upstream response is not valid JSON: {0}")]
    InvalidJson(serde_json::Error),
}

// `reqwest::Error` and `wreq::Error` retain the URL associated with a failed
// request.  Their derived `Debug` implementations therefore may include
// proxy credentials or query-string tokens.  This error is logged with
// structured `?error` fields in a few execution paths, so both `Debug` and
// `Display` must be safe if a caller accidentally crosses that boundary.
// Dynamic details are passed through the same URL-aware, bounded sanitizer
// used by the upstream request formatters.
impl std::fmt::Debug for ExecutionRuntimeTransportError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RequestBodyRequired => formatter.write_str("RequestBodyRequired"),
            Self::RequestBodyAmbiguous => formatter.write_str("RequestBodyAmbiguous"),
            Self::BodyDecode(error) => formatter
                .debug_tuple("BodyDecode")
                .field(&sanitize_error_detail(&error.to_string()))
                .finish(),
            Self::BodyTooLarge { limit_bytes } => formatter
                .debug_struct("BodyTooLarge")
                .field("limit_bytes", limit_bytes)
                .finish(),
            Self::UnsupportedContentEncoding(encoding) => formatter
                .debug_tuple("UnsupportedContentEncoding")
                .field(&sanitize_error_detail(encoding))
                .finish(),
            Self::ProxyUnsupported => formatter.write_str("ProxyUnsupported"),
            Self::InvalidMethod(error) => formatter
                .debug_tuple("InvalidMethod")
                .field(&sanitize_error_detail(&error.to_string()))
                .finish(),
            Self::InvalidHeaderName(name) => formatter
                .debug_tuple("InvalidHeaderName")
                .field(&sanitize_error_detail(name))
                .finish(),
            Self::InvalidHeaderValue(name) => formatter
                .debug_tuple("InvalidHeaderValue")
                .field(&sanitize_error_detail(name))
                .finish(),
            Self::InvalidProxy(error) => formatter
                .debug_tuple("InvalidProxy")
                .field(&format_upstream_request_error(error))
                .finish(),
            Self::UnsupportedTransportProfile(profile) => formatter
                .debug_tuple("UnsupportedTransportProfile")
                .field(&sanitize_error_detail(profile))
                .finish(),
            Self::BodyEncode(error) => formatter
                .debug_tuple("BodyEncode")
                .field(&sanitize_error_detail(&error.to_string()))
                .finish(),
            Self::ClientBuild(error) => formatter
                .debug_tuple("ClientBuild")
                .field(&format_upstream_request_error(error))
                .finish(),
            Self::BrowserClientBuild(error) => formatter
                .debug_tuple("BrowserClientBuild")
                .field(&format_wreq_upstream_request_error(error))
                .finish(),
            Self::BrowserBody(detail) => formatter
                .debug_tuple("BrowserBody")
                .field(&sanitize_error_detail(detail))
                .finish(),
            Self::UpstreamHttpStatus {
                status_code,
                message,
            } => formatter
                .debug_struct("UpstreamHttpStatus")
                .field("status_code", status_code)
                .field("message", &sanitize_error_detail(message))
                .finish(),
            Self::UpstreamRequest(detail) => formatter
                .debug_tuple("UpstreamRequest")
                .field(&sanitize_error_detail(detail))
                .finish(),
            Self::UpstreamResponseTooLarge { phase, limit_bytes } => formatter
                .debug_struct("UpstreamResponseTooLarge")
                .field("phase", phase)
                .field("limit_bytes", limit_bytes)
                .finish(),
            Self::UpstreamResponseDecode { encoding, message } => formatter
                .debug_struct("UpstreamResponseDecode")
                .field("encoding", &sanitize_error_detail(encoding))
                .field("message", &sanitize_error_detail(message))
                .finish(),
            Self::RelayError(detail) => formatter
                .debug_tuple("RelayError")
                .field(&sanitize_error_detail(detail))
                .finish(),
            Self::InvalidJson(error) => formatter
                .debug_tuple("InvalidJson")
                .field(&sanitize_error_detail(&error.to_string()))
                .finish(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UpstreamResponseBodyPhase {
    Wire,
    Decoded,
}

impl std::fmt::Display for UpstreamResponseBodyPhase {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Wire => "wire",
            Self::Decoded => "decoded",
        })
    }
}

pub(crate) fn with_upstream_response_body_limit(
    plan: &ExecutionPlan,
    limit_bytes: usize,
) -> ExecutionPlan {
    let mut bounded_plan = plan.clone();
    apply_upstream_response_body_limit(&mut bounded_plan, limit_bytes);
    bounded_plan
}

pub(crate) fn apply_upstream_response_body_limit(plan: &mut ExecutionPlan, limit_bytes: usize) {
    plan.headers
        .retain(|name, _| !name.eq_ignore_ascii_case(EXECUTION_RESPONSE_BODY_LIMIT_HEADER));
    plan.headers.insert(
        EXECUTION_RESPONSE_BODY_LIMIT_HEADER.to_string(),
        normalize_scoped_response_body_limit(limit_bytes)
            .unwrap_or(DEFAULT_SCOPED_RESPONSE_BODY_LIMIT_BYTES)
            .to_string(),
    );
}

pub(crate) fn execution_plan_response_body_limit_bytes(plan: &ExecutionPlan) -> usize {
    effective_response_body_limit_bytes(
        execution_transport_header_value(&plan.headers, EXECUTION_RESPONSE_BODY_LIMIT_HEADER),
        crate::headers::max_internal_buffered_body_bytes(),
    )
}

fn effective_response_body_limit_bytes(
    raw_scoped_limit: Option<&str>,
    global_limit: usize,
) -> usize {
    let Some(raw_scoped_limit) = raw_scoped_limit else {
        return global_limit;
    };
    parse_scoped_response_body_limit(raw_scoped_limit)
        .unwrap_or(DEFAULT_SCOPED_RESPONSE_BODY_LIMIT_BYTES)
        .min(global_limit)
}

fn parse_scoped_response_body_limit(value: &str) -> Option<usize> {
    let raw_limit = value.trim().parse::<u64>().ok()?;
    usize::try_from(raw_limit)
        .ok()
        .and_then(normalize_scoped_response_body_limit)
}

fn normalize_scoped_response_body_limit(limit_bytes: usize) -> Option<usize> {
    (limit_bytes > 0).then_some(limit_bytes.clamp(
        MIN_SCOPED_RESPONSE_BODY_LIMIT_BYTES,
        MAX_SCOPED_RESPONSE_BODY_LIMIT_BYTES,
    ))
}

pub(crate) fn append_upstream_response_body_chunk(
    body: &mut Vec<u8>,
    chunk: &[u8],
) -> Result<(), ExecutionRuntimeTransportError> {
    append_upstream_response_body_chunk_with_limit(
        body,
        chunk,
        crate::headers::max_internal_buffered_body_bytes(),
    )
}

pub(crate) fn append_upstream_response_body_chunk_with_limit(
    body: &mut Vec<u8>,
    chunk: &[u8],
    limit_bytes: usize,
) -> Result<(), ExecutionRuntimeTransportError> {
    if body.len() > limit_bytes || chunk.len() > limit_bytes.saturating_sub(body.len()) {
        return Err(ExecutionRuntimeTransportError::UpstreamResponseTooLarge {
            phase: UpstreamResponseBodyPhase::Wire,
            limit_bytes,
        });
    }
    body.extend_from_slice(chunk);
    Ok(())
}

/// Return the maximum base64 text length that can decode to at most
/// `decoded_limit` bytes.  This is intentionally checked before invoking the
/// base64 decoder, whose allocation is based on the input text length.
pub(crate) fn maximum_base64_len_for_decoded_limit(decoded_limit: usize) -> usize {
    decoded_limit
        .checked_add(2)
        .and_then(|value| value.checked_div(3))
        .and_then(|value| value.checked_mul(4))
        .unwrap_or(usize::MAX)
}

/// Decode a body carried in an execution plan/result only after enforcing a
/// decoded-size bound.  Both representations are checked: the encoded check
/// prevents an attacker-controlled allocation, while the decoded check covers
/// padding and decoder edge cases.
pub(crate) fn decode_base64_body_with_limit(
    body_base64: &str,
    decoded_limit: usize,
) -> Result<Vec<u8>, ExecutionRuntimeTransportError> {
    if body_base64.len() > maximum_base64_len_for_decoded_limit(decoded_limit) {
        return Err(ExecutionRuntimeTransportError::BodyTooLarge {
            limit_bytes: decoded_limit,
        });
    }

    let bytes = base64::engine::general_purpose::STANDARD
        .decode(body_base64)
        .map_err(ExecutionRuntimeTransportError::BodyDecode)?;
    if bytes.len() > decoded_limit {
        return Err(ExecutionRuntimeTransportError::BodyTooLarge {
            limit_bytes: decoded_limit,
        });
    }
    Ok(bytes)
}

struct JsonSerializedSizeLimiter {
    remaining: usize,
}

impl Write for JsonSerializedSizeLimiter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.remaining {
            return Err(std::io::Error::other("serialized JSON exceeds limit"));
        }
        self.remaining -= bytes.len();
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub(crate) fn json_value_fits_serialized_limit(value: &Value, limit_bytes: usize) -> bool {
    serde_json::to_writer(
        JsonSerializedSizeLimiter {
            remaining: limit_bytes,
        },
        value,
    )
    .is_ok()
}

/// Bound the JSON envelope used by the test/compatibility remote execution
/// runtime.  A result can contain a raw JSON representation and a base64 wire
/// representation at the same time, so the limit is larger than either body
/// limit.  It remains capped even when the raw-body cap is explicitly
/// disabled (`usize::MAX`).
#[cfg(test)]
pub(crate) fn execution_result_envelope_limit_bytes(decoded_body_limit: usize) -> usize {
    maximum_base64_len_for_decoded_limit(decoded_body_limit)
        .saturating_add(decoded_body_limit)
        .saturating_add(EXECUTION_RESULT_ENVELOPE_METADATA_BYTES)
        .min(MAX_EXECUTION_RESULT_ENVELOPE_BYTES)
}

/// Serialize a JSON body without allowing serde_json to grow an unbounded
/// temporary `Vec`.  The value itself is already owned by the execution plan;
/// this bound covers the wire representation that will be sent upstream.
pub(crate) fn serialize_json_body_with_limit(
    body: &Value,
    limit_bytes: usize,
) -> Result<Vec<u8>, ExecutionRuntimeTransportError> {
    serialize_serializable_with_limit(body, limit_bytes)
}

pub(crate) fn serialize_serializable_with_limit<T: Serialize>(
    value: &T,
    limit_bytes: usize,
) -> Result<Vec<u8>, ExecutionRuntimeTransportError> {
    let mut writer = LimitedJsonWriter::new(limit_bytes);
    match serde_json::to_writer(&mut writer, value) {
        Ok(()) => Ok(writer.bytes),
        Err(_error) if writer.exceeded => {
            Err(ExecutionRuntimeTransportError::BodyTooLarge { limit_bytes })
        }
        Err(error) => Err(ExecutionRuntimeTransportError::BodyEncode(error)),
    }
}

struct LimitedJsonWriter {
    bytes: Vec<u8>,
    limit: usize,
    exceeded: bool,
}

impl LimitedJsonWriter {
    fn new(limit: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(limit.min(16 * 1024)),
            limit,
            exceeded: false,
        }
    }
}

impl Write for LimitedJsonWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            self.exceeded = true;
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "json body exceeds configured limit",
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct DirectSyncExecutionRuntime;

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct ExecutionTransportControls {
    follow_redirects: Option<bool>,
    http1_only: bool,
}

pub(crate) enum DirectUpstreamResponse {
    Reqwest(reqwest::Response),
    HyperH2c(hyper::Response<HyperIncomingBody>),
    BrowserWreq(wreq::Response),
}

pub(crate) fn direct_upstream_response_byte_stream(
    prefetched_body: VecDeque<Result<Bytes, String>>,
    response: DirectUpstreamResponse,
) -> futures_util::stream::BoxStream<'static, Result<Bytes, String>> {
    let response_stream = match response {
        DirectUpstreamResponse::Reqwest(response) => response
            .bytes_stream()
            .map(|item| item.map_err(|err| format_upstream_request_error(&err)))
            .boxed(),
        DirectUpstreamResponse::HyperH2c(response) => response
            .into_body()
            .into_data_stream()
            .map(|item| item.map_err(|err| format_hyper_error_chain(&err)))
            .boxed(),
        DirectUpstreamResponse::BrowserWreq(response) => response
            .bytes_stream()
            .map(|item| item.map_err(|err| format_wreq_upstream_request_error(&err)))
            .boxed(),
    };
    let upstream = futures_util::stream::iter(prefetched_body).chain(response_stream);
    crate::execution_runtime::stream_read_timeout::skip_empty_upstream_chunks(upstream).boxed()
}

pub(crate) struct DirectUpstreamStreamExecution {
    pub(crate) status_code: u16,
    pub(crate) headers: BTreeMap<String, String>,
    /// The upstream length is retained for stream classification only.  The
    /// hop-by-hop header itself remains filtered from the client-facing map.
    pub(crate) upstream_content_length: Option<u64>,
    pub(crate) provider_api_format: String,
    pub(crate) stream_summary_report_context: Value,
    pub(crate) prefetched_body: VecDeque<Result<Bytes, String>>,
    pub(crate) stream_precommit_committed: bool,
    pub(crate) response: DirectUpstreamResponse,
    pub(crate) started_at: Instant,
    pub(crate) response_observation: ExecutionResponseObservation,
    pub(crate) stream_first_byte_timeout: Option<Duration>,
    pub(crate) stream_idle_timeout: Option<Duration>,
    pub(crate) upstream_target_permit: Option<UpstreamTargetAdmissionPermit>,
}

#[derive(Debug, Clone)]
pub(crate) struct DirectSyncResponseStarted {
    pub(crate) status_code: u16,
    pub(crate) ttfb_ms: u64,
    pub(crate) response_observation: ExecutionResponseObservation,
}

impl DirectSyncExecutionRuntime {
    pub(crate) const fn new() -> Self {
        Self
    }

    pub(crate) async fn execute_sync(
        &self,
        plan: &ExecutionPlan,
    ) -> Result<ExecutionResult, ExecutionRuntimeTransportError> {
        self.execute_sync_with_response_started(plan, |_| {}).await
    }

    pub(crate) async fn execute_sync_with_response_started<F>(
        &self,
        plan: &ExecutionPlan,
        on_response_started: F,
    ) -> Result<ExecutionResult, ExecutionRuntimeTransportError>
    where
        F: FnOnce(DirectSyncResponseStarted),
    {
        let body_bytes = build_request_body(plan)?;
        let response_body_limit_bytes = execution_plan_response_body_limit_bytes(plan);

        let started_at = Instant::now();
        let request_started_at_unix_ms = crate::clock::current_unix_ms();
        let request_order_id = uuid::Uuid::now_v7().to_string();
        with_non_stream_total_timeout(plan, async move {
            let response = send_request_inner(plan, body_bytes, false).await?;
            let ttfb_ms = started_at.elapsed().as_millis() as u64;
            let response_headers_observed_at_unix_ms = crate::clock::current_unix_ms();
            let status_code = response.status_code();
            let headers = response.headers();
            let response_observation = ExecutionResponseObservation {
                request_started_at_unix_ms,
                response_headers_observed_at_unix_ms,
                request_order_id,
            };
            on_response_started(DirectSyncResponseStarted {
                status_code,
                ttfb_ms,
                response_observation: response_observation.clone(),
            });
            let (body_bytes, stream_ttfb_ms) = response
                .bytes_with_stream_timeout(plan, started_at, response_body_limit_bytes)
                .await?;
            let decoded_body_bytes = decode_response_body_bytes_with_limit(
                &headers,
                &body_bytes,
                response_body_limit_bytes,
            )?;
            let elapsed_ms = started_at.elapsed().as_millis() as u64;
            let upstream_bytes = body_bytes.len() as u64;

            let body = build_execution_response_body(
                &headers,
                &body_bytes,
                decoded_body_bytes.as_ref(),
                plan.stream,
                execution_response_body_mode(plan),
            )?;

            Ok(ExecutionResult {
                request_id: plan.request_id.clone(),
                candidate_id: plan.candidate_id.clone(),
                status_code,
                headers,
                response_observation: Some(response_observation),
                body,
                telemetry: Some(ExecutionTelemetry {
                    ttfb_ms: stream_ttfb_ms.or(Some(ttfb_ms)),
                    elapsed_ms: Some(elapsed_ms),
                    upstream_bytes: Some(upstream_bytes),
                }),
                error: None,
            })
        })
        .await
    }

    pub(crate) async fn execute_stream(
        &self,
        plan: &ExecutionPlan,
    ) -> Result<DirectUpstreamStreamExecution, ExecutionRuntimeTransportError> {
        let build_body_started_at = Instant::now();
        let body_bytes = build_request_body(plan)?;
        observe_gateway_stage_ms(
            "direct_build_body",
            build_body_started_at.elapsed().as_millis() as u64,
        );

        let started_at = Instant::now();
        let request_started_at_unix_ms = crate::clock::current_unix_ms();
        let request_order_id = uuid::Uuid::now_v7().to_string();
        let response = send_request(plan, body_bytes).await?;
        observe_gateway_stage_ms(
            "direct_send_headers",
            started_at.elapsed().as_millis() as u64,
        );
        let status_code = response.status_code();
        let upstream_content_length = response.content_length();
        let headers = response.headers();
        let response_headers_observed_at_unix_ms = crate::clock::current_unix_ms();

        let stream_summary_report_context = build_stream_summary_report_context(plan);

        Ok(DirectUpstreamStreamExecution {
            status_code,
            headers,
            upstream_content_length,
            provider_api_format: plan.provider_api_format.clone(),
            stream_summary_report_context,
            prefetched_body: VecDeque::new(),
            stream_precommit_committed: false,
            response: response.into_direct_upstream_response(),
            started_at,
            response_observation: ExecutionResponseObservation {
                request_started_at_unix_ms,
                response_headers_observed_at_unix_ms,
                request_order_id,
            },
            stream_first_byte_timeout: resolve_stream_first_byte_timeout(plan),
            stream_idle_timeout: resolve_stream_idle_timeout(plan),
            upstream_target_permit: None,
        })
    }
}

pub(crate) async fn execute_sync_plan(
    state: &AppState,
    trace_id: Option<&str>,
    plan: &ExecutionPlan,
) -> Result<ExecutionResult, GatewayError> {
    execute_sync_plan_with_report_context(state, trace_id, plan, None).await
}

pub(crate) async fn execute_sync_plan_with_report_context(
    state: &AppState,
    trace_id: Option<&str>,
    plan: &ExecutionPlan,
    report_context: Option<&serde_json::Value>,
) -> Result<ExecutionResult, GatewayError> {
    #[cfg(test)]
    {
        let remote_execution_runtime_base_url = state
            .execution_runtime_override_base_url()
            .unwrap_or_default();
        if !remote_execution_runtime_base_url.trim().is_empty() {
            return execute_sync_plan_via_remote_execution_runtime(
                state,
                remote_execution_runtime_base_url,
                trace_id,
                plan,
            )
            .await;
        }
    }

    let _ = trace_id;
    let state_for_response_started = state.clone();
    match DirectSyncExecutionRuntime::new()
        .execute_sync_with_response_started(plan, move |event| {
            crate::orchestration::spawn_local_oauth_success_effect(
                state_for_response_started,
                plan,
                report_context,
                crate::orchestration::LocalOAuthSuccessEffect {
                    status_code: event.status_code,
                    request_started_at_unix_ms: Some(
                        event.response_observation.request_started_at_unix_ms,
                    ),
                    request_order_id: Some(&event.response_observation.request_order_id),
                },
            );
        })
        .await
    {
        Ok(result) => {
            record_manual_proxy_request_outcome(state, plan, result.status_code).await;
            Ok(result)
        }
        Err(err) => {
            record_manual_proxy_request_failure(state, plan).await;
            Err(GatewayError::Internal(safe_transport_error_message(&err)))
        }
    }
}

fn build_stream_summary_report_context(plan: &ExecutionPlan) -> Value {
    json!({
        "provider_api_format": plan.provider_api_format,
        "client_api_format": plan.client_api_format,
        "model": plan.model_name,
        "upstream_is_stream": plan.stream,
    })
}

pub(crate) async fn record_manual_proxy_request_success(state: &AppState, plan: &ExecutionPlan) {
    record_manual_proxy_traffic(state, plan, 1, 0, 0, 0).await;
}

pub(crate) async fn record_manual_proxy_request_outcome(
    state: &AppState,
    plan: &ExecutionPlan,
    status_code: u16,
) {
    let failed_requests_delta = i64::from(status_code >= 400);
    record_manual_proxy_traffic(state, plan, 1, failed_requests_delta, 0, 0).await;
}

pub(crate) async fn record_manual_proxy_request_failure(state: &AppState, plan: &ExecutionPlan) {
    record_manual_proxy_traffic(state, plan, 1, 1, 0, 0).await;
}

pub(crate) async fn record_manual_proxy_stream_error(state: &AppState, plan: &ExecutionPlan) {
    record_manual_proxy_traffic(state, plan, 0, 0, 0, 1).await;
}

async fn record_manual_proxy_traffic(
    state: &AppState,
    plan: &ExecutionPlan,
    total_requests_delta: i64,
    failed_requests_delta: i64,
    dns_failures_delta: i64,
    stream_errors_delta: i64,
) {
    let Some((node_id, expected_node_generation)) = manual_proxy_node_binding(plan.proxy.as_ref())
    else {
        return;
    };
    let mutation = ProxyNodeTrafficMutation {
        node_id: node_id.clone(),
        expected_node_generation: Some(expected_node_generation),
        total_requests_delta,
        failed_requests_delta,
        dns_failures_delta,
        stream_errors_delta,
    };

    if let Err(error) = state.record_proxy_node_traffic(&mutation).await {
        tracing::warn!(
            node_id = %node_id,
            error = ?error,
            "failed to record manual proxy node traffic"
        );
    }
}

fn manual_proxy_node_binding(proxy: Option<&ProxySnapshot>) -> Option<(String, String)> {
    let proxy = proxy?;
    if proxy.enabled == Some(false) {
        return None;
    }
    let node_id = proxy
        .node_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)?;
    let expected_node_generation = proxy
        .extra
        .as_ref()
        .and_then(|extra| extra.get(PROXY_NODE_GENERATION_EXTRA_KEY))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)?;
    Some((node_id, expected_node_generation))
}

pub(crate) async fn send_request(
    plan: &ExecutionPlan,
    body_bytes: Vec<u8>,
) -> Result<DirectHttpResponse, ExecutionRuntimeTransportError> {
    send_request_inner(plan, body_bytes, true).await
}

async fn send_request_inner(
    plan: &ExecutionPlan,
    body_bytes: Vec<u8>,
    apply_request_total_timeout: bool,
) -> Result<DirectHttpResponse, ExecutionRuntimeTransportError> {
    validate_execution_upstream_url(plan.url.as_str())?;
    if let Some(detail) = gateway_frontdoor_self_loop_guard_error(plan.url.as_str()) {
        return Err(ExecutionRuntimeTransportError::UpstreamRequest(detail));
    }

    let prepare_started_at = Instant::now();
    let method = plan.method.parse::<reqwest::Method>()?;
    let transport_controls = resolve_execution_transport_controls(&plan.headers);
    let headers = build_request_headers(
        &plan.headers,
        plan.content_encoding.as_deref(),
        plan.body.body_bytes_b64.is_some(),
    )?;
    let total_timeout = if apply_request_total_timeout {
        resolve_non_stream_total_timeout(plan)
    } else {
        None
    };
    let stream_first_byte_timeout = resolve_stream_first_byte_timeout(plan);
    observe_gateway_stage_ms(
        "direct_request_prepare",
        prepare_started_at.elapsed().as_millis() as u64,
    );

    if transport_profile_uses_browser_wreq(plan.transport_profile.as_ref()) {
        return send_via_browser_wreq_transport(
            plan,
            method,
            headers,
            body_bytes,
            total_timeout,
            stream_first_byte_timeout,
            transport_controls,
            apply_request_total_timeout,
        )
        .await;
    }

    let direct_transport_controls =
        direct_reqwest_effective_transport_controls(plan, transport_controls);
    if direct_h2c_fast_path_applies(plan, direct_transport_controls) {
        return send_via_direct_h2c_fast_path(
            plan,
            method,
            headers,
            body_bytes,
            stream_first_byte_timeout,
        )
        .await
        .map(DirectHttpResponse::HyperH2c);
    }

    let client_select_started_at = Instant::now();
    let client = build_client(
        &plan.url,
        &plan.key_id,
        plan.timeouts.as_ref(),
        plan.proxy.as_ref(),
        plan.transport_profile.as_ref(),
        direct_transport_controls,
    )?;
    observe_gateway_stage_ms(
        "direct_reqwest_client_select",
        client_select_started_at.elapsed().as_millis() as u64,
    );
    let request_build_started_at = Instant::now();
    let mut request = client.request(method, &plan.url);
    request = request.headers(headers).body(body_bytes);
    if let Some(timeout) = total_timeout {
        request = request.timeout(timeout);
    }
    observe_gateway_stage_ms(
        "direct_reqwest_request_build",
        request_build_started_at.elapsed().as_millis() as u64,
    );
    send_reqwest_request(request, stream_first_byte_timeout)
        .await
        .map(DirectHttpResponse::Reqwest)
}

pub(crate) enum DirectHttpResponse {
    Reqwest(reqwest::Response),
    HyperH2c(hyper::Response<HyperIncomingBody>),
    BrowserWreq(wreq::Response),
}

impl DirectHttpResponse {
    pub(crate) fn status_code(&self) -> u16 {
        match self {
            DirectHttpResponse::Reqwest(response) => response.status().as_u16(),
            DirectHttpResponse::HyperH2c(response) => response.status().as_u16(),
            DirectHttpResponse::BrowserWreq(response) => response.status().as_u16(),
        }
    }

    pub(crate) fn headers(&self) -> BTreeMap<String, String> {
        match self {
            DirectHttpResponse::Reqwest(response) => collect_response_headers(response.headers()),
            DirectHttpResponse::HyperH2c(response) => collect_response_headers(response.headers()),
            DirectHttpResponse::BrowserWreq(response) => {
                collect_response_headers(response.headers())
            }
        }
    }

    pub(crate) fn content_length(&self) -> Option<u64> {
        let value = match self {
            DirectHttpResponse::Reqwest(response) => response.headers().get("content-length"),
            DirectHttpResponse::HyperH2c(response) => response.headers().get("content-length"),
            DirectHttpResponse::BrowserWreq(response) => response.headers().get("content-length"),
        }?;
        value.to_str().ok()?.trim().parse::<u64>().ok()
    }

    pub(crate) async fn bytes_with_limit(
        self,
        response_body_limit_bytes: usize,
    ) -> Result<Bytes, ExecutionRuntimeTransportError> {
        let started_at = Instant::now();
        match self {
            DirectHttpResponse::Reqwest(response) => {
                collect_reqwest_stream_body(response, started_at, None, response_body_limit_bytes)
                    .await
                    .map(|(body, _)| body)
            }
            DirectHttpResponse::HyperH2c(response) => {
                collect_hyper_stream_body(response, started_at, None, response_body_limit_bytes)
                    .await
                    .map(|(body, _)| body)
            }
            DirectHttpResponse::BrowserWreq(response) => {
                collect_wreq_stream_body(response, started_at, None, response_body_limit_bytes)
                    .await
                    .map(|(body, _)| body)
            }
        }
    }

    async fn bytes_with_stream_timeout(
        self,
        plan: &ExecutionPlan,
        started_at: Instant,
        response_body_limit_bytes: usize,
    ) -> Result<(Bytes, Option<u64>), ExecutionRuntimeTransportError> {
        if !plan.stream {
            return self
                .bytes_with_limit(response_body_limit_bytes)
                .await
                .map(|bytes| (bytes, None));
        }

        let first_byte_timeout = resolve_stream_first_byte_timeout(plan);
        match self {
            DirectHttpResponse::Reqwest(response) => {
                collect_reqwest_stream_body(
                    response,
                    started_at,
                    first_byte_timeout,
                    response_body_limit_bytes,
                )
                .await
            }
            DirectHttpResponse::HyperH2c(response) => {
                collect_hyper_stream_body(
                    response,
                    started_at,
                    first_byte_timeout,
                    response_body_limit_bytes,
                )
                .await
            }
            DirectHttpResponse::BrowserWreq(response) => {
                collect_wreq_stream_body(
                    response,
                    started_at,
                    first_byte_timeout,
                    response_body_limit_bytes,
                )
                .await
            }
        }
    }

    fn into_direct_upstream_response(self) -> DirectUpstreamResponse {
        match self {
            DirectHttpResponse::Reqwest(response) => DirectUpstreamResponse::Reqwest(response),
            DirectHttpResponse::HyperH2c(response) => DirectUpstreamResponse::HyperH2c(response),
            DirectHttpResponse::BrowserWreq(response) => {
                DirectUpstreamResponse::BrowserWreq(response)
            }
        }
    }
}

async fn await_stream_body_first_item<T, F>(
    future: F,
    started_at: Instant,
    timeout: Option<Duration>,
) -> Result<T, ExecutionRuntimeTransportError>
where
    F: Future<Output = T>,
{
    let Some(timeout) = timeout else {
        return Ok(future.await);
    };
    let Some(remaining) = timeout.checked_sub(started_at.elapsed()) else {
        return Err(ExecutionRuntimeTransportError::UpstreamRequest(
            stream_first_byte_timeout_message(timeout),
        ));
    };
    if remaining.is_zero() {
        return Err(ExecutionRuntimeTransportError::UpstreamRequest(
            stream_first_byte_timeout_message(timeout),
        ));
    }
    tokio::time::timeout(remaining, future).await.map_err(|_| {
        ExecutionRuntimeTransportError::UpstreamRequest(stream_first_byte_timeout_message(timeout))
    })
}

async fn collect_reqwest_stream_body(
    response: reqwest::Response,
    started_at: Instant,
    first_byte_timeout: Option<Duration>,
    response_body_limit_bytes: usize,
) -> Result<(Bytes, Option<u64>), ExecutionRuntimeTransportError> {
    let mut stream = response.bytes_stream();
    let mut body_bytes = Vec::new();
    let mut first_byte_ms = None;

    loop {
        let item = if first_byte_ms.is_none() {
            await_stream_body_first_item(stream.next(), started_at, first_byte_timeout).await?
        } else {
            stream.next().await
        };
        let Some(item) = item else {
            break;
        };
        let chunk = item.map_err(|err| {
            ExecutionRuntimeTransportError::UpstreamRequest(format_upstream_request_error(&err))
        })?;
        if first_byte_ms.is_none() && !chunk.is_empty() {
            first_byte_ms = Some(started_at.elapsed().as_millis() as u64);
        }
        append_upstream_response_body_chunk_with_limit(
            &mut body_bytes,
            &chunk,
            response_body_limit_bytes,
        )?;
    }

    Ok((Bytes::from(body_bytes), first_byte_ms))
}

async fn collect_hyper_stream_body(
    response: hyper::Response<HyperIncomingBody>,
    started_at: Instant,
    first_byte_timeout: Option<Duration>,
    response_body_limit_bytes: usize,
) -> Result<(Bytes, Option<u64>), ExecutionRuntimeTransportError> {
    let mut stream = response.into_body().into_data_stream();
    let mut body_bytes = Vec::new();
    let mut first_byte_ms = None;

    loop {
        let item = if first_byte_ms.is_none() {
            await_stream_body_first_item(stream.next(), started_at, first_byte_timeout).await?
        } else {
            stream.next().await
        };
        let Some(item) = item else {
            break;
        };
        let chunk = item.map_err(|err| {
            ExecutionRuntimeTransportError::UpstreamRequest(format_hyper_error_chain(&err))
        })?;
        if first_byte_ms.is_none() && !chunk.is_empty() {
            first_byte_ms = Some(started_at.elapsed().as_millis() as u64);
        }
        append_upstream_response_body_chunk_with_limit(
            &mut body_bytes,
            &chunk,
            response_body_limit_bytes,
        )?;
    }

    Ok((Bytes::from(body_bytes), first_byte_ms))
}

async fn collect_wreq_stream_body(
    response: wreq::Response,
    started_at: Instant,
    first_byte_timeout: Option<Duration>,
    response_body_limit_bytes: usize,
) -> Result<(Bytes, Option<u64>), ExecutionRuntimeTransportError> {
    let mut stream = response.bytes_stream();
    let mut body_bytes = Vec::new();
    let mut first_byte_ms = None;

    loop {
        let item = if first_byte_ms.is_none() {
            await_stream_body_first_item(stream.next(), started_at, first_byte_timeout).await?
        } else {
            stream.next().await
        };
        let Some(item) = item else {
            break;
        };
        let chunk = item.map_err(|err| {
            ExecutionRuntimeTransportError::BrowserBody(format_wreq_upstream_request_error(&err))
        })?;
        if first_byte_ms.is_none() && !chunk.is_empty() {
            first_byte_ms = Some(started_at.elapsed().as_millis() as u64);
        }
        append_upstream_response_body_chunk_with_limit(
            &mut body_bytes,
            &chunk,
            response_body_limit_bytes,
        )?;
    }

    Ok((Bytes::from(body_bytes), first_byte_ms))
}

fn direct_h2c_fast_path_applies(
    plan: &ExecutionPlan,
    transport_controls: ExecutionTransportControls,
) -> bool {
    if !direct_h2c_fast_path_enabled()
        || !plan.stream
        || transport_controls.http1_only
        || plan.proxy.is_some()
        || !transport_profile_h2c_prior_knowledge(plan.transport_profile.as_ref())
    {
        return false;
    }

    reqwest::Url::parse(plan.url.as_str())
        .ok()
        .is_some_and(|url| url.scheme() == "http")
}

fn direct_h2c_fast_path_enabled() -> bool {
    std::env::var(DIRECT_H2C_FAST_PATH_ENV)
        .ok()
        .is_some_and(|value| matches_truthy_env_value(value.trim()))
}

pub(crate) async fn prewarm_direct_h2c_sender_cache_from_env(
) -> Result<Option<DirectH2cSenderPrewarmReport>, ExecutionRuntimeTransportError> {
    let urls = direct_h2c_prewarm_urls_from_env();
    if urls.is_empty() {
        return Ok(None);
    }

    let ready_required = direct_h2c_prewarm_ready_required();
    let report = prewarm_direct_h2c_sender_cache_urls(urls, ready_required).await;
    if ready_required && report.failed_targets > 0 {
        return Err(ExecutionRuntimeTransportError::UpstreamRequest(format!(
            "direct h2c sender prewarm failed for {}/{} targets{}",
            report.failed_targets,
            report.unique_targets,
            report
                .first_error
                .as_deref()
                .map(|err| format!(": {err}"))
                .unwrap_or_default()
        )));
    }
    Ok(Some(report))
}

async fn prewarm_direct_h2c_sender_cache_urls(
    urls: Vec<String>,
    ready_required: bool,
) -> DirectH2cSenderPrewarmReport {
    let started_at = Instant::now();
    let requested_urls = urls.len() as u64;
    DIRECT_H2C_SENDER_CACHE_METRICS
        .prewarm_requested
        .fetch_add(requested_urls, Ordering::Relaxed);

    let connect_timeout_ms =
        env_positive_usize(DIRECT_H2C_PREWARM_CONNECT_TIMEOUT_MS_ENV).map(|value| value as u64);
    let timeouts = connect_timeout_ms.map(|connect_ms| aether_contracts::ExecutionTimeouts {
        connect_ms: Some(connect_ms),
        ..Default::default()
    });
    let (keys, parse_failures, mut first_error) =
        direct_h2c_sender_prewarm_cache_keys(&urls, timeouts.as_ref());
    let unique_targets = keys.len() as u64;
    if parse_failures > 0 {
        DIRECT_H2C_SENDER_CACHE_METRICS
            .prewarm_failed
            .fetch_add(parse_failures, Ordering::Relaxed);
    }

    let mut warmed_targets = 0;
    let mut failed_targets = parse_failures;
    let mut pending = FuturesUnordered::new();
    for key in keys {
        pending.push(prewarm_direct_h2c_sender_cache_key(key));
    }

    while let Some(result) = pending.next().await {
        match result {
            Ok(()) => {
                warmed_targets += 1;
                DIRECT_H2C_SENDER_CACHE_METRICS
                    .prewarm_success
                    .fetch_add(1, Ordering::Relaxed);
            }
            Err(err) => {
                failed_targets += 1;
                DIRECT_H2C_SENDER_CACHE_METRICS
                    .prewarm_failed
                    .fetch_add(1, Ordering::Relaxed);
                if first_error.is_none() {
                    first_error = Some(safe_transport_error_message(&err));
                }
            }
        }
    }

    observe_gateway_stage_ms(
        "direct_h2c_sender_cache_prewarm",
        started_at.elapsed().as_millis() as u64,
    );
    DirectH2cSenderPrewarmReport {
        requested_urls,
        unique_targets,
        warmed_targets,
        failed_targets,
        ready_required,
        first_error,
    }
}

async fn prewarm_direct_h2c_sender_cache_key(
    cache_key: DirectHyperH2cClientCacheKey,
) -> Result<(), ExecutionRuntimeTransportError> {
    let cell = direct_h2c_sender_cache_cell(&cache_key);
    cell.get_or_try_init(|| async {
        let target_len = direct_h2c_client_shard_count();
        build_direct_h2c_sender_cache_entry_from_cache_key(&cache_key, target_len)
            .await
            .map(Arc::new)
    })
    .await?;
    Ok(())
}

fn direct_h2c_sender_prewarm_cache_keys(
    urls: &[String],
    timeouts: Option<&aether_contracts::ExecutionTimeouts>,
) -> (Vec<DirectHyperH2cClientCacheKey>, u64, Option<String>) {
    let mut seen = HashSet::new();
    let mut keys = Vec::new();
    let mut failed = 0;
    let mut first_error = None;
    for url in urls {
        match direct_h2c_client_cache_key(url, timeouts) {
            Ok(key) => {
                if seen.insert(key.clone()) {
                    keys.push(key);
                }
            }
            Err(err) => {
                failed += 1;
                if first_error.is_none() {
                    first_error = Some(err.to_string());
                }
            }
        }
    }
    (keys, failed, first_error)
}

fn direct_h2c_prewarm_urls_from_env() -> Vec<String> {
    std::env::var(DIRECT_H2C_PREWARM_URLS_ENV)
        .ok()
        .map(|value| {
            value
                .split([',', ';', '\n', '\t', ' '])
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

fn direct_h2c_prewarm_ready_required() -> bool {
    std::env::var(DIRECT_H2C_PREWARM_READY_ENV)
        .ok()
        .is_some_and(|value| matches_truthy_env_value(value.trim()))
}

async fn cached_direct_h2c_sender(
    request_url: &str,
    timeouts: Option<&aether_contracts::ExecutionTimeouts>,
) -> Result<DirectHyperH2cSenderLease, ExecutionRuntimeTransportError> {
    let cache_key = direct_h2c_client_cache_key(request_url, timeouts)?;
    let cell = direct_h2c_sender_cache_cell(&cache_key);
    let entry = cell
        .get_or_try_init(|| async {
            let target_len = direct_h2c_client_shard_count();
            build_direct_h2c_sender_cache_entry_from_cache_key(&cache_key, target_len)
                .await
                .map(Arc::new)
        })
        .await?;
    Ok(entry.select())
}

fn direct_h2c_sender_cache_cell(
    cache_key: &DirectHyperH2cClientCacheKey,
) -> Arc<DirectHyperH2cSenderCacheCell> {
    let cache_lock_started_at = Instant::now();
    if let Ok(cache) = DIRECT_H2C_SENDER_CACHE.read() {
        if let Some(cell) = cache.get(cache_key) {
            let cell = Arc::clone(cell);
            drop(cache);
            observe_gateway_stage_ms(
                "direct_reqwest_client_cache_lock",
                cache_lock_started_at.elapsed().as_millis() as u64,
            );
            DIRECT_H2C_SENDER_CACHE_METRICS
                .hits
                .fetch_add(1, Ordering::Relaxed);
            return cell;
        }
    }

    // Recheck after acquiring the write lock so simultaneous first requests
    // still share one OnceCell and one connection warmup.
    if let Ok(mut cache) = DIRECT_H2C_SENDER_CACHE.write() {
        let (cell, hit) = match cache.get(cache_key) {
            Some(cell) => (Arc::clone(cell), true),
            None => {
                let cell = Arc::new(TokioOnceCell::new());
                cache.insert(cache_key.clone(), Arc::clone(&cell));
                (cell, false)
            }
        };
        drop(cache);
        observe_gateway_stage_ms(
            "direct_reqwest_client_cache_lock",
            cache_lock_started_at.elapsed().as_millis() as u64,
        );
        if hit {
            DIRECT_H2C_SENDER_CACHE_METRICS
                .hits
                .fetch_add(1, Ordering::Relaxed);
        } else {
            DIRECT_H2C_SENDER_CACHE_METRICS
                .misses
                .fetch_add(1, Ordering::Relaxed);
        }
        return cell;
    } else {
        observe_gateway_stage_ms(
            "direct_reqwest_client_cache_lock",
            cache_lock_started_at.elapsed().as_millis() as u64,
        );
        DIRECT_H2C_SENDER_CACHE_METRICS
            .misses
            .fetch_add(1, Ordering::Relaxed);
    }
    Arc::new(TokioOnceCell::new())
}

fn direct_h2c_client_cache_key(
    request_url: &str,
    timeouts: Option<&aether_contracts::ExecutionTimeouts>,
) -> Result<DirectHyperH2cClientCacheKey, ExecutionRuntimeTransportError> {
    if reqwest::Url::parse(request_url).is_err() {
        return Err(ExecutionRuntimeTransportError::UpstreamRequest(
            "invalid h2c upstream origin".to_string(),
        ));
    }
    validate_execution_upstream_url(request_url)?;
    let upstream_origin = direct_reqwest_upstream_origin(request_url).ok_or_else(|| {
        ExecutionRuntimeTransportError::UpstreamRequest("invalid h2c upstream origin".to_string())
    })?;
    Ok(DirectHyperH2cClientCacheKey {
        upstream_origin,
        connect_timeout_ms: timeouts.and_then(|timeouts| timeouts.connect_ms),
        pool_max_idle_per_host: direct_h2c_pool_max_idle_per_host(),
    })
}

async fn build_direct_h2c_sender_cache_entry_from_cache_key(
    cache_key: &DirectHyperH2cClientCacheKey,
    target_len: usize,
) -> Result<DirectHyperH2cSenderCacheEntry, ExecutionRuntimeTransportError> {
    let mut pending = FuturesUnordered::new();
    for _ in 0..target_len {
        pending.push(connect_direct_h2c_sender(cache_key));
    }

    let mut senders = Vec::with_capacity(target_len);
    while let Some(sender) = pending.next().await {
        senders.push(sender?);
        DIRECT_H2C_SENDER_CACHE_METRICS
            .builds
            .fetch_add(1, Ordering::Relaxed);
    }
    Ok(DirectHyperH2cSenderCacheEntry::new(senders, target_len))
}

async fn connect_direct_h2c_sender(
    cache_key: &DirectHyperH2cClientCacheKey,
) -> Result<DirectHyperH2cSender, ExecutionRuntimeTransportError> {
    let driver_runtime = configured_direct_h2c_driver_runtime()?;
    connect_direct_h2c_sender_on_runtime(cache_key, driver_runtime).await
}

async fn connect_direct_h2c_sender_on_runtime(
    cache_key: &DirectHyperH2cClientCacheKey,
    driver_runtime: Option<&'static tokio::runtime::Runtime>,
) -> Result<DirectHyperH2cSender, ExecutionRuntimeTransportError> {
    let Some(driver_runtime) = driver_runtime else {
        return connect_direct_h2c_sender_on_current_runtime(cache_key).await;
    };

    let cache_key = cache_key.clone();
    driver_runtime
        .handle()
        .spawn(async move { connect_direct_h2c_sender_on_current_runtime(&cache_key).await })
        .await
        .map_err(|err| {
            ExecutionRuntimeTransportError::UpstreamRequest(format!(
                "direct H2C connect task failed: {err}"
            ))
        })?
}

async fn connect_direct_h2c_sender_on_current_runtime(
    cache_key: &DirectHyperH2cClientCacheKey,
) -> Result<DirectHyperH2cSender, ExecutionRuntimeTransportError> {
    let upstream = reqwest::Url::parse(&cache_key.upstream_origin).map_err(|err| {
        tracing::debug!(error = %err, "invalid direct h2c upstream origin");
        ExecutionRuntimeTransportError::UpstreamRequest("invalid h2c upstream origin".to_string())
    })?;
    let host = upstream.host_str().ok_or_else(|| {
        ExecutionRuntimeTransportError::UpstreamRequest("missing h2c upstream host".to_string())
    })?;
    let port = upstream.port_or_known_default().ok_or_else(|| {
        ExecutionRuntimeTransportError::UpstreamRequest("missing h2c upstream port".to_string())
    })?;
    let addresses = resolve_execution_target_addresses(host, port)
        .await
        .map_err(|_| {
            ExecutionRuntimeTransportError::UpstreamRequest(
                "h2c upstream DNS resolution failed".to_string(),
            )
        })?;
    // Passing concrete socket addresses prevents TcpStream from performing a
    // second hostname lookup after the validated DNS answer.
    let connect = TcpStream::connect(addresses.as_slice());
    let stream = if let Some(timeout_ms) = cache_key.connect_timeout_ms {
        let timeout = Duration::from_millis(timeout_ms);
        tokio::time::timeout(timeout, connect)
            .await
            .map_err(|_| {
                ExecutionRuntimeTransportError::UpstreamRequest(direct_h2c_connect_timeout_message(
                    timeout,
                ))
            })?
            .map_err(|err| {
                tracing::debug!(error = %err, "failed to connect direct h2c upstream");
                ExecutionRuntimeTransportError::UpstreamRequest(
                    "failed to connect h2c upstream".to_string(),
                )
            })?
    } else {
        connect.await.map_err(|err| {
            tracing::debug!(error = %err, "failed to connect direct h2c upstream");
            ExecutionRuntimeTransportError::UpstreamRequest(
                "failed to connect h2c upstream".to_string(),
            )
        })?
    };
    stream.set_nodelay(true).map_err(|err| {
        tracing::debug!(error = %err, "failed to configure direct h2c upstream socket");
        ExecutionRuntimeTransportError::UpstreamRequest(
            "failed to configure h2c upstream socket".to_string(),
        )
    })?;
    let io = TokioIo::new(stream);
    let mut builder = hyper::client::conn::http2::Builder::new(TokioExecutor::new());
    builder.adaptive_window(direct_h2c_adaptive_window_enabled());
    let (sender, connection) = builder.handshake(io).await.map_err(|err| {
        ExecutionRuntimeTransportError::UpstreamRequest(format_hyper_error_chain(&err))
    })?;
    // Connect, handshake, and drive the connection on the same runtime so the
    // socket remains registered with the reactor polling the H2 connection.
    spawn_direct_h2c_driver_task(None, async move {
        if let Err(err) = connection.await {
            tracing::debug!(
                error = %format_hyper_error_chain(&err),
                "direct h2c sender connection closed"
            );
        }
    });
    Ok(sender)
}

#[cfg(test)]
fn cached_direct_h2c_client(
    request_url: &str,
    timeouts: Option<&aether_contracts::ExecutionTimeouts>,
) -> Result<DirectHyperH2cClient, ExecutionRuntimeTransportError> {
    let cache_key = direct_h2c_client_cache_key(request_url, timeouts)?;

    let cache_lock_started_at = Instant::now();
    if let Ok(mut cache) = DIRECT_H2C_CLIENT_CACHE.lock() {
        observe_gateway_stage_ms(
            "direct_reqwest_client_cache_lock",
            cache_lock_started_at.elapsed().as_millis() as u64,
        );
        if let Some(entry) = cache.get(&cache_key) {
            DIRECT_H2C_CLIENT_CACHE_METRICS
                .hits
                .fetch_add(1, Ordering::Relaxed);
            return Ok(entry.select());
        }

        DIRECT_H2C_CLIENT_CACHE_METRICS
            .misses
            .fetch_add(1, Ordering::Relaxed);
        let target_len = direct_h2c_client_shard_count();
        let mut clients = Vec::with_capacity(target_len);
        for _ in 0..target_len {
            clients.push(build_direct_h2c_client_from_cache_key(&cache_key));
            DIRECT_H2C_CLIENT_CACHE_METRICS
                .builds
                .fetch_add(1, Ordering::Relaxed);
        }
        let entry = DirectHyperH2cClientCacheEntry::new(clients, target_len);
        let client = entry.select();
        cache.insert(cache_key, entry);
        return Ok(client);
    }

    observe_gateway_stage_ms(
        "direct_reqwest_client_cache_lock",
        cache_lock_started_at.elapsed().as_millis() as u64,
    );
    DIRECT_H2C_CLIENT_CACHE_METRICS
        .misses
        .fetch_add(1, Ordering::Relaxed);
    DIRECT_H2C_CLIENT_CACHE_METRICS
        .builds
        .fetch_add(1, Ordering::Relaxed);
    Ok(build_direct_h2c_client_from_cache_key(&cache_key))
}

#[cfg(test)]
fn build_direct_h2c_client_from_cache_key(
    cache_key: &DirectHyperH2cClientCacheKey,
) -> DirectHyperH2cClient {
    let mut connector = HttpConnector::new_with_resolver(ExecutionSafeHyperDnsResolver);
    connector.enforce_http(true);
    connector.set_nodelay(true);
    connector.set_connect_timeout(cache_key.connect_timeout_ms.map(Duration::from_millis));

    let mut builder = HyperLegacyClient::builder(TokioExecutor::new());
    builder.http2_only(true);
    builder.http2_adaptive_window(true);
    builder.pool_max_idle_per_host(cache_key.pool_max_idle_per_host);
    builder.pool_timer(TokioTimer::new());
    builder.pool_idle_timeout(Duration::from_millis(upstream_pool_idle_timeout_ms()));
    builder.build(connector)
}

fn direct_h2c_pool_max_idle_per_host() -> usize {
    *DIRECT_H2C_POOL_MAX_IDLE_PER_HOST
}

fn direct_h2c_client_shard_count() -> usize {
    if let Some(shards) = env_positive_usize(DIRECT_H2C_CLIENT_SHARDS_ENV) {
        return shards.clamp(1, MAX_DIRECT_H2C_CLIENT_SHARDS);
    }
    let target_gate_limit = crate::state::upstream_target_gate_limit_from_env()
        .unwrap_or_else(crate::state::upstream_target_gate_auto_limit);
    let streams_per_client = env_positive_usize(DIRECT_H2C_TARGET_STREAMS_PER_CLIENT_ENV)
        .unwrap_or(DEFAULT_DIRECT_H2C_TARGET_STREAMS_PER_CLIENT)
        .max(1);
    target_gate_limit
        .max(1)
        .div_ceil(streams_per_client)
        .clamp(1, MAX_DIRECT_H2C_CLIENT_SHARDS)
}

fn direct_h2c_sender_select_window() -> usize {
    *DIRECT_H2C_SENDER_SELECT_WINDOW
}

fn direct_h2c_adaptive_window_enabled() -> bool {
    std::env::var(DIRECT_H2C_ADAPTIVE_WINDOW_ENV)
        .ok()
        .map(|value| matches_truthy_env_value(value.trim()))
        .unwrap_or(true)
}

fn direct_h2c_driver_runtime_threads() -> Option<usize> {
    parse_direct_h2c_driver_runtime_threads(
        std::env::var(DIRECT_H2C_DRIVER_RUNTIME_THREADS_ENV)
            .ok()
            .as_deref(),
    )
}

fn parse_direct_h2c_driver_runtime_threads(value: Option<&str>) -> Option<usize> {
    value
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|threads| *threads > 0)
        .map(|threads| threads.clamp(1, MAX_DIRECT_H2C_DRIVER_RUNTIME_THREADS))
}

fn configured_direct_h2c_driver_runtime(
) -> Result<Option<&'static tokio::runtime::Runtime>, ExecutionRuntimeTransportError> {
    direct_h2c_driver_runtime_threads()
        .map(direct_h2c_driver_runtime)
        .transpose()
}

fn direct_h2c_driver_runtime(
    worker_threads: usize,
) -> Result<&'static tokio::runtime::Runtime, ExecutionRuntimeTransportError> {
    struct RuntimeEntry {
        runtime: &'static tokio::runtime::Runtime,
        worker_threads: usize,
    }

    static RUNTIME: OnceLock<Result<RuntimeEntry, String>> = OnceLock::new();
    let entry = RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(worker_threads)
            .max_blocking_threads(DIRECT_H2C_DRIVER_RUNTIME_MAX_BLOCKING_THREADS)
            .thread_name(DIRECT_H2C_DRIVER_RUNTIME_THREAD_NAME)
            .thread_stack_size(DIRECT_H2C_DRIVER_RUNTIME_STACK_BYTES)
            .build()
            .map(|runtime| RuntimeEntry {
                runtime: Box::leak(Box::new(runtime)),
                worker_threads,
            })
            .map_err(|err| format!("failed to build direct H2C driver runtime: {err}"))
    });
    match entry {
        Ok(entry) if entry.worker_threads == worker_threads => Ok(entry.runtime),
        Ok(entry) => Err(ExecutionRuntimeTransportError::UpstreamRequest(format!(
            "direct H2C driver runtime was initialized with {} worker threads, not {worker_threads}",
            entry.worker_threads
        ))),
        Err(err) => Err(ExecutionRuntimeTransportError::UpstreamRequest(err.clone())),
    }
}

fn spawn_direct_h2c_driver_task<F>(
    driver_runtime: Option<&'static tokio::runtime::Runtime>,
    task: F,
) -> tokio::task::JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    match driver_runtime {
        Some(runtime) => runtime.handle().spawn(task),
        None => tokio::spawn(task),
    }
}

async fn send_via_direct_h2c_fast_path(
    plan: &ExecutionPlan,
    method: reqwest::Method,
    headers: HeaderMap,
    body_bytes: Vec<u8>,
    stream_first_byte_timeout: Option<Duration>,
) -> Result<hyper::Response<HyperIncomingBody>, ExecutionRuntimeTransportError> {
    let client_select_started_at = Instant::now();
    let sender = cached_direct_h2c_sender(&plan.url, plan.timeouts.as_ref()).await?;
    observe_gateway_stage_ms(
        "direct_h2c_client_select",
        client_select_started_at.elapsed().as_millis() as u64,
    );

    let request_build_started_at = Instant::now();
    let uri = plan.url.parse::<hyper::Uri>().map_err(|_| {
        ExecutionRuntimeTransportError::UpstreamRequest("invalid h2c upstream uri".to_string())
    })?;
    let authority = uri
        .authority()
        .map(|authority| authority.as_str().to_string());
    let mut builder = hyper::Request::builder().method(method.as_str()).uri(uri);
    {
        let target_headers = builder.headers_mut().ok_or_else(|| {
            ExecutionRuntimeTransportError::UpstreamRequest(
                "failed to prepare h2c request headers".to_string(),
            )
        })?;
        *target_headers = headers;
        if !target_headers.contains_key(reqwest::header::HOST) {
            if let Some(authority) = authority.as_deref() {
                let value = HeaderValue::from_str(authority).map_err(|_| {
                    ExecutionRuntimeTransportError::InvalidHeaderValue("host".to_string())
                })?;
                target_headers.insert(reqwest::header::HOST, value);
            }
        }
    }
    let request = builder
        .body(Full::new(Bytes::from(body_bytes)))
        .map_err(|err| {
            ExecutionRuntimeTransportError::UpstreamRequest(format!(
                "failed to build h2c request: {err}"
            ))
        })?;
    observe_gateway_stage_ms(
        "direct_h2c_request_build",
        request_build_started_at.elapsed().as_millis() as u64,
    );

    send_hyper_h2c_request(sender, request, stream_first_byte_timeout).await
}

async fn send_hyper_h2c_request(
    mut sender: DirectHyperH2cSenderLease,
    request: hyper::Request<DirectHyperH2cRequestBody>,
    stream_first_byte_timeout: Option<Duration>,
) -> Result<hyper::Response<HyperIncomingBody>, ExecutionRuntimeTransportError> {
    let started_at = Instant::now();
    let deadline = stream_first_byte_timeout.map(|timeout| (timeout, Instant::now() + timeout));

    let ready_started_at = Instant::now();
    let ready_result = if let Some((timeout, deadline)) = deadline {
        match direct_h2c_remaining_timeout(deadline) {
            Some(remaining) => match tokio::time::timeout(remaining, sender.sender().ready()).await
            {
                Ok(Ok(())) => Ok(()),
                Ok(Err(err)) => Err(ExecutionRuntimeTransportError::UpstreamRequest(
                    format_hyper_error_chain(&err),
                )),
                Err(_) => Err(ExecutionRuntimeTransportError::UpstreamRequest(
                    stream_first_byte_timeout_message(timeout),
                )),
            },
            None => Err(ExecutionRuntimeTransportError::UpstreamRequest(
                stream_first_byte_timeout_message(timeout),
            )),
        }
    } else {
        sender.sender().ready().await.map_err(|err| {
            ExecutionRuntimeTransportError::UpstreamRequest(format_hyper_error_chain(&err))
        })
    };
    observe_gateway_stage_ms(
        "direct_h2c_sender_ready_wait",
        ready_started_at.elapsed().as_millis() as u64,
    );
    ready_result?;

    let headers_started_at = Instant::now();
    let dispatch_started_at = Instant::now();
    let response_future = sender.sender().send_request(request);
    observe_gateway_stage_ms(
        "direct_h2c_request_dispatch",
        dispatch_started_at.elapsed().as_millis() as u64,
    );

    let response_headers_started_at = Instant::now();
    let response_result = if let Some((timeout, deadline)) = deadline {
        match direct_h2c_remaining_timeout(deadline) {
            Some(remaining) => match tokio::time::timeout(remaining, response_future).await {
                Ok(Ok(response)) => Ok(response),
                Ok(Err(err)) => Err(ExecutionRuntimeTransportError::UpstreamRequest(
                    format_hyper_error_chain(&err),
                )),
                Err(_) => Err(ExecutionRuntimeTransportError::UpstreamRequest(
                    stream_first_byte_timeout_message(timeout),
                )),
            },
            None => Err(ExecutionRuntimeTransportError::UpstreamRequest(
                stream_first_byte_timeout_message(timeout),
            )),
        }
    } else {
        response_future.await.map_err(|err| {
            ExecutionRuntimeTransportError::UpstreamRequest(format_hyper_error_chain(&err))
        })
    };
    observe_gateway_stage_ms(
        "direct_h2c_response_headers_wait",
        response_headers_started_at.elapsed().as_millis() as u64,
    );
    observe_gateway_stage_ms(
        "direct_h2c_request_headers_wait",
        headers_started_at.elapsed().as_millis() as u64,
    );
    let response = response_result?;
    sender.release();
    observe_gateway_stage_ms(
        "direct_h2c_request_send",
        started_at.elapsed().as_millis() as u64,
    );
    Ok(response)
}

fn direct_h2c_remaining_timeout(deadline: Instant) -> Option<Duration> {
    deadline.checked_duration_since(Instant::now())
}

fn direct_h2c_connect_timeout_message(timeout: Duration) -> String {
    format!(
        "direct h2c upstream connect timeout after {} ms",
        timeout.as_millis()
    )
}

async fn send_via_browser_wreq_transport(
    plan: &ExecutionPlan,
    method: reqwest::Method,
    headers: HeaderMap,
    body_bytes: Vec<u8>,
    total_timeout: Option<Duration>,
    stream_first_byte_timeout: Option<Duration>,
    transport_controls: ExecutionTransportControls,
    apply_request_total_timeout: bool,
) -> Result<DirectHttpResponse, ExecutionRuntimeTransportError> {
    let profile = plan.transport_profile.as_ref().ok_or_else(|| {
        ExecutionRuntimeTransportError::UnsupportedTransportProfile(String::new())
    })?;
    let mut client_timeouts = plan.timeouts.clone();
    if plan.stream {
        if let Some(timeouts) = client_timeouts.as_mut() {
            // Streamed responses use the shared idle reader; sync collectors retain
            // their existing client read timeout. Zero explicitly disables either.
            if apply_request_total_timeout || timeouts.read_ms == Some(0) {
                timeouts.read_ms = None;
            }
        }
    }
    let client = build_browser_wreq_client(
        client_timeouts.as_ref(),
        plan.proxy.as_ref(),
        profile,
        transport_controls,
        apply_request_total_timeout && !plan.stream,
    )?;
    let method = wreq::Method::from_bytes(method.as_str().as_bytes())
        .map_err(ExecutionRuntimeTransportError::InvalidMethod)?;
    let mut request = client
        .request(method, plan.url.as_str())
        .headers(headers)
        .body(body_bytes);
    if let Some(timeout) = total_timeout {
        request = request.timeout(timeout);
    }
    send_wreq_request(request, stream_first_byte_timeout)
        .await
        .map(DirectHttpResponse::BrowserWreq)
}

pub(crate) fn build_request_body(
    plan: &ExecutionPlan,
) -> Result<Vec<u8>, ExecutionRuntimeTransportError> {
    if plan.body.json_body.is_some() && plan.body.body_bytes_b64.is_some() {
        return Err(ExecutionRuntimeTransportError::RequestBodyAmbiguous);
    }
    let body_limit = crate::headers::max_internal_buffered_body_bytes();
    let mut body_bytes = if let Some(json_body) = plan.body.json_body.as_ref() {
        serialize_json_body_with_limit(json_body, body_limit)?
    } else if let Some(body_b64) = plan.body.body_bytes_b64.as_deref() {
        decode_base64_body_with_limit(body_b64, body_limit)?
    } else {
        Vec::new()
    };

    if plan.body.json_body.is_some() {
        body_bytes = match normalize_content_encoding(plan.content_encoding.as_deref()).as_deref() {
            Some("gzip") => gzip_bytes(&body_bytes)?,
            Some("zstd") => zstd_bytes(&body_bytes)?,
            _ => body_bytes,
        };
    }

    Ok(body_bytes)
}

fn normalize_content_encoding(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.to_ascii_lowercase())
}

fn gzip_bytes(body_bytes: &[u8]) -> Result<Vec<u8>, ExecutionRuntimeTransportError> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(body_bytes)
        .map_err(|err| ExecutionRuntimeTransportError::RelayError(err.to_string()))?;
    encoder
        .finish()
        .map_err(|err| ExecutionRuntimeTransportError::RelayError(err.to_string()))
}

fn zstd_bytes(body_bytes: &[u8]) -> Result<Vec<u8>, ExecutionRuntimeTransportError> {
    zstd::stream::encode_all(std::io::Cursor::new(body_bytes), 3)
        .map_err(|err| ExecutionRuntimeTransportError::RelayError(err.to_string()))
}

pub(crate) fn resolve_non_stream_total_timeout_for_request(
    is_stream: bool,
    provider_api_format: &str,
    timeouts: Option<&aether_contracts::ExecutionTimeouts>,
) -> Option<Duration> {
    if is_stream {
        return None;
    }
    let default_timeout_ms =
        if crate::ai_serving::is_openai_responses_compact_format(provider_api_format) {
            DEFAULT_CODEX_COMPACT_TOTAL_TIMEOUT_MS
        } else {
            DEFAULT_NON_STREAM_TOTAL_TIMEOUT_MS
        };
    let timeout_ms = timeouts
        .and_then(|timeouts| timeouts.total_ms)
        .unwrap_or(default_timeout_ms);
    Some(Duration::from_millis(timeout_ms.max(1)))
}

fn resolve_non_stream_total_timeout(plan: &ExecutionPlan) -> Option<Duration> {
    resolve_non_stream_total_timeout_for_request(
        plan.stream,
        &plan.provider_api_format,
        plan.timeouts.as_ref(),
    )
}

pub(crate) fn resolve_stream_first_byte_timeout_for_request(
    is_stream: bool,
    timeouts: Option<&aether_contracts::ExecutionTimeouts>,
) -> Option<Duration> {
    if !is_stream {
        return None;
    }
    let timeout_ms = timeouts
        .and_then(|timeouts| timeouts.first_byte_ms)
        .unwrap_or(DEFAULT_STREAM_FIRST_BYTE_TIMEOUT_MS);
    Some(Duration::from_millis(timeout_ms.max(1)))
}

pub(crate) fn resolve_stream_first_byte_timeout(plan: &ExecutionPlan) -> Option<Duration> {
    resolve_stream_first_byte_timeout_for_request(plan.stream, plan.timeouts.as_ref())
}

pub(crate) async fn with_non_stream_total_timeout<T, F>(
    plan: &ExecutionPlan,
    future: F,
) -> Result<T, ExecutionRuntimeTransportError>
where
    F: Future<Output = Result<T, ExecutionRuntimeTransportError>>,
{
    let Some(timeout) = resolve_non_stream_total_timeout(plan) else {
        return future.await;
    };

    match tokio::time::timeout(timeout, future).await {
        Ok(result) => result,
        Err(_) => Err(ExecutionRuntimeTransportError::UpstreamRequest(
            non_stream_total_timeout_message(timeout),
        )),
    }
}

async fn send_reqwest_request(
    request: reqwest::RequestBuilder,
    stream_first_byte_timeout: Option<Duration>,
) -> Result<reqwest::Response, ExecutionRuntimeTransportError> {
    let started_at = Instant::now();
    if let Some(timeout) = stream_first_byte_timeout {
        return match tokio::time::timeout(timeout, request.send()).await {
            Ok(Ok(response)) => {
                observe_gateway_stage_ms(
                    "direct_reqwest_request_send",
                    started_at.elapsed().as_millis() as u64,
                );
                Ok(response)
            }
            Ok(Err(error)) => Err(ExecutionRuntimeTransportError::UpstreamRequest(
                format_upstream_request_error(&error),
            )),
            Err(_) => Err(ExecutionRuntimeTransportError::UpstreamRequest(
                stream_first_byte_timeout_message(timeout),
            )),
        };
    }

    let response = request.send().await.map_err(|err| {
        ExecutionRuntimeTransportError::UpstreamRequest(format_upstream_request_error(&err))
    })?;
    observe_gateway_stage_ms(
        "direct_reqwest_request_send",
        started_at.elapsed().as_millis() as u64,
    );
    Ok(response)
}

async fn send_wreq_request(
    request: wreq::RequestBuilder,
    stream_first_byte_timeout: Option<Duration>,
) -> Result<wreq::Response, ExecutionRuntimeTransportError> {
    if let Some(timeout) = stream_first_byte_timeout {
        return match tokio::time::timeout(timeout, request.send()).await {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(error)) => Err(ExecutionRuntimeTransportError::UpstreamRequest(
                format_wreq_upstream_request_error(&error),
            )),
            Err(_) => Err(ExecutionRuntimeTransportError::UpstreamRequest(
                stream_first_byte_timeout_message(timeout),
            )),
        };
    }

    request.send().await.map_err(|err| {
        ExecutionRuntimeTransportError::UpstreamRequest(format_wreq_upstream_request_error(&err))
    })
}

fn non_stream_total_timeout_message(timeout: Duration) -> String {
    format!(
        "provider non-stream request total timeout after {} ms",
        timeout.as_millis()
    )
}

pub(crate) fn stream_first_byte_timeout_message(timeout: Duration) -> String {
    format!(
        "provider stream first byte timeout after {} ms",
        timeout.as_millis()
    )
}

fn build_client(
    request_url: &str,
    key_id: &str,
    timeouts: Option<&aether_contracts::ExecutionTimeouts>,
    proxy: Option<&ProxySnapshot>,
    transport_profile: Option<&ResolvedTransportProfile>,
    transport_controls: ExecutionTransportControls,
) -> Result<reqwest::Client, ExecutionRuntimeTransportError> {
    validate_reqwest_transport_profile(transport_profile)?;
    let resolved_proxy_url = resolve_proxy_url(proxy)?;
    let cache_key = direct_reqwest_client_cache_key(
        request_url,
        key_id,
        timeouts,
        resolved_proxy_url.clone(),
        transport_profile,
        transport_controls,
    );
    cached_direct_reqwest_client(cache_key, resolved_proxy_url)
}

fn direct_reqwest_effective_transport_controls(
    plan: &ExecutionPlan,
    mut transport_controls: ExecutionTransportControls,
) -> ExecutionTransportControls {
    if transport_controls.http1_only || !plan.stream {
        return transport_controls;
    }
    if transport_profile_h2c_prior_knowledge(plan.transport_profile.as_ref()) {
        return transport_controls;
    }
    if direct_reqwest_stream_http_mode() == DirectReqwestStreamHttpMode::Http1 {
        transport_controls.http1_only = true;
    }
    transport_controls
}

fn direct_reqwest_stream_http_mode() -> DirectReqwestStreamHttpMode {
    *DIRECT_REQWEST_STREAM_HTTP_MODE
}

fn parse_direct_reqwest_stream_http_mode(value: &str) -> DirectReqwestStreamHttpMode {
    match value.trim().to_ascii_lowercase().as_str() {
        "auto" | "profile" | "provider" => DirectReqwestStreamHttpMode::Auto,
        _ => DirectReqwestStreamHttpMode::Http1,
    }
}

pub(crate) fn prewarm_direct_reqwest_client_cache_for_plan(plan: &ExecutionPlan) {
    match try_prewarm_direct_reqwest_client_cache_for_plan(plan) {
        Ok(true) => {}
        Ok(false) => {}
        Err(err) => {
            tracing::debug!(
                error = %sanitize_error_detail(&err.to_string()),
                request_id = %plan.request_id,
                candidate_id = ?plan.candidate_id,
                provider_id = %plan.provider_id,
                endpoint_id = %plan.endpoint_id,
                key_partition = ?direct_reqwest_pool_partition(
                    plan.transport_profile.as_ref(),
                    &plan.key_id,
                ),
                "gateway direct reqwest client prewarm skipped"
            );
        }
    }
}

fn try_prewarm_direct_reqwest_client_cache_for_plan(
    plan: &ExecutionPlan,
) -> Result<bool, ExecutionRuntimeTransportError> {
    if transport_profile_uses_browser_wreq(plan.transport_profile.as_ref()) {
        return Ok(false);
    }
    let transport_controls = direct_reqwest_effective_transport_controls(
        plan,
        resolve_execution_transport_controls(&plan.headers),
    );
    if direct_h2c_fast_path_applies(plan, transport_controls) {
        return Ok(false);
    }
    validate_reqwest_transport_profile(plan.transport_profile.as_ref())?;
    let resolved_proxy_url = resolve_proxy_url(plan.proxy.as_ref())?;
    let cache_key = direct_reqwest_client_cache_key(
        &plan.url,
        &plan.key_id,
        plan.timeouts.as_ref(),
        resolved_proxy_url.clone(),
        plan.transport_profile.as_ref(),
        transport_controls,
    );
    prewarm_direct_reqwest_client_cache(cache_key, resolved_proxy_url)?;
    Ok(true)
}

fn prewarm_direct_reqwest_client_cache(
    cache_key: DirectReqwestClientCacheKey,
    proxy_url: Option<String>,
) -> Result<(), ExecutionRuntimeTransportError> {
    validate_direct_reqwest_proxy_material(&cache_key, proxy_url.as_deref())?;
    let mut warm_after_unlock = None;
    let cache_lock_started_at = Instant::now();
    if let Ok(mut cache) = DIRECT_REQWEST_CLIENT_CACHE.lock() {
        observe_gateway_stage_ms(
            "direct_reqwest_client_cache_lock",
            cache_lock_started_at.elapsed().as_millis() as u64,
        );
        if let Some(entry) = cache.get_mut(&cache_key) {
            entry.touch();
            if entry.should_warm() {
                entry.warming = true;
                warm_after_unlock = Some((
                    cache_key.clone(),
                    proxy_url.clone(),
                    entry.len(),
                    entry.target_len,
                ));
            }
            drop(cache);
            if let Some((cache_key, proxy_url, existing_len, target_len)) = warm_after_unlock {
                let spawned = spawn_direct_reqwest_client_cache_warm(
                    cache_key.clone(),
                    proxy_url,
                    existing_len,
                    target_len,
                );
                if !spawned {
                    mark_direct_reqwest_client_cache_not_warming(&cache_key);
                }
            }
            return Ok(());
        }

        let target_len = direct_reqwest_client_shard_count(&cache_key);
        let initial_len = direct_reqwest_prewarm_client_shard_count(target_len);
        let mut clients = Vec::with_capacity(initial_len);
        for _ in 0..initial_len {
            clients.push(build_direct_reqwest_client_from_cache_key(
                &cache_key,
                proxy_url.as_deref(),
            )?);
            DIRECT_REQWEST_CLIENT_CACHE_METRICS
                .builds
                .fetch_add(1, Ordering::Relaxed);
        }
        let entry =
            DirectReqwestClientCacheEntry::new(clients, target_len, target_len > initial_len);
        let warm_key = (target_len > initial_len).then(|| cache_key.clone());
        evict_direct_reqwest_client_cache_for_insert(
            &mut cache,
            &cache_key,
            direct_reqwest_client_cache_max_entries(),
        );
        cache.insert(cache_key, entry);
        if let Some(warm_key) = warm_key {
            warm_after_unlock = Some((warm_key, proxy_url, initial_len, target_len));
        }
        drop(cache);
        if let Some((cache_key, proxy_url, existing_len, target_len)) = warm_after_unlock {
            let spawned = spawn_direct_reqwest_client_cache_warm(
                cache_key.clone(),
                proxy_url,
                existing_len,
                target_len,
            );
            if !spawned {
                mark_direct_reqwest_client_cache_not_warming(&cache_key);
            }
        }
    } else {
        observe_gateway_stage_ms(
            "direct_reqwest_client_cache_lock",
            cache_lock_started_at.elapsed().as_millis() as u64,
        );
    }
    Ok(())
}

fn cached_direct_reqwest_client(
    cache_key: DirectReqwestClientCacheKey,
    proxy_url: Option<String>,
) -> Result<reqwest::Client, ExecutionRuntimeTransportError> {
    validate_direct_reqwest_proxy_material(&cache_key, proxy_url.as_deref())?;
    let mut warm_after_unlock = None;
    let cache_lock_started_at = Instant::now();
    if let Ok(mut cache) = DIRECT_REQWEST_CLIENT_CACHE.lock() {
        observe_gateway_stage_ms(
            "direct_reqwest_client_cache_lock",
            cache_lock_started_at.elapsed().as_millis() as u64,
        );
        if let Some(entry) = cache.get_mut(&cache_key) {
            entry.touch();
            DIRECT_REQWEST_CLIENT_CACHE_METRICS
                .hits
                .fetch_add(1, Ordering::Relaxed);
            record_direct_reqwest_client_protocol_selection(&cache_key);
            let client = entry.select();
            if entry.should_warm() {
                entry.warming = true;
                warm_after_unlock = Some((
                    cache_key.clone(),
                    proxy_url.clone(),
                    entry.len(),
                    entry.target_len,
                ));
            }
            drop(cache);
            if let Some((cache_key, proxy_url, existing_len, target_len)) = warm_after_unlock {
                let spawned = spawn_direct_reqwest_client_cache_warm(
                    cache_key.clone(),
                    proxy_url,
                    existing_len,
                    target_len,
                );
                if !spawned {
                    mark_direct_reqwest_client_cache_not_warming(&cache_key);
                }
            }
            return Ok(client);
        }
        DIRECT_REQWEST_CLIENT_CACHE_METRICS
            .misses
            .fetch_add(1, Ordering::Relaxed);
        let target_len = direct_reqwest_client_shard_count(&cache_key);
        let initial_len = direct_reqwest_initial_client_shard_count(target_len);
        let mut clients = Vec::with_capacity(initial_len);
        for _ in 0..initial_len {
            clients.push(build_direct_reqwest_client_from_cache_key(
                &cache_key,
                proxy_url.as_deref(),
            )?);
            DIRECT_REQWEST_CLIENT_CACHE_METRICS
                .builds
                .fetch_add(1, Ordering::Relaxed);
        }
        let entry =
            DirectReqwestClientCacheEntry::new(clients, target_len, target_len > initial_len);
        record_direct_reqwest_client_protocol_selection(&cache_key);
        let client = entry.select();
        let warm_key = (target_len > initial_len).then(|| cache_key.clone());
        evict_direct_reqwest_client_cache_for_insert(
            &mut cache,
            &cache_key,
            direct_reqwest_client_cache_max_entries(),
        );
        cache.insert(cache_key, entry);
        if let Some(warm_key) = warm_key {
            warm_after_unlock = Some((warm_key, proxy_url, initial_len, target_len));
        }
        drop(cache);
        if let Some((cache_key, proxy_url, existing_len, target_len)) = warm_after_unlock {
            let spawned = spawn_direct_reqwest_client_cache_warm(
                cache_key.clone(),
                proxy_url,
                existing_len,
                target_len,
            );
            if !spawned {
                mark_direct_reqwest_client_cache_not_warming(&cache_key);
            }
        }
        return Ok(client);
    }

    observe_gateway_stage_ms(
        "direct_reqwest_client_cache_lock",
        cache_lock_started_at.elapsed().as_millis() as u64,
    );
    DIRECT_REQWEST_CLIENT_CACHE_METRICS
        .misses
        .fetch_add(1, Ordering::Relaxed);
    record_direct_reqwest_client_protocol_selection(&cache_key);
    let client = build_direct_reqwest_client_from_cache_key(&cache_key, proxy_url.as_deref())?;
    DIRECT_REQWEST_CLIENT_CACHE_METRICS
        .builds
        .fetch_add(1, Ordering::Relaxed);
    Ok(client)
}

fn spawn_direct_reqwest_client_cache_warm(
    cache_key: DirectReqwestClientCacheKey,
    proxy_url: Option<String>,
    existing_len: usize,
    target_len: usize,
) -> bool {
    if target_len <= existing_len {
        DIRECT_REQWEST_CLIENT_CACHE_METRICS
            .warm_skipped_total
            .fetch_add(1, Ordering::Relaxed);
        return false;
    }
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        DIRECT_REQWEST_CLIENT_CACHE_METRICS
            .warm_skipped_total
            .fetch_add(1, Ordering::Relaxed);
        return false;
    };
    DIRECT_REQWEST_CLIENT_CACHE_METRICS
        .warm_enqueues
        .fetch_add(1, Ordering::Relaxed);
    let enqueue_started_at = Instant::now();
    handle.spawn_blocking(move || {
        for _ in existing_len..target_len {
            match build_direct_reqwest_client_from_cache_key(&cache_key, proxy_url.as_deref()) {
                Ok(client) => {
                    DIRECT_REQWEST_CLIENT_CACHE_METRICS
                        .builds
                        .fetch_add(1, Ordering::Relaxed);
                    let Ok(mut cache) = DIRECT_REQWEST_CLIENT_CACHE.lock() else {
                        return;
                    };
                    let Some(entry) = cache.get_mut(&cache_key) else {
                        return;
                    };
                    if entry.clients.len() >= entry.target_len {
                        entry.warming = false;
                        return;
                    }
                    entry.clients.push(client);
                    if entry.clients.len() >= entry.target_len {
                        entry.warming = false;
                        return;
                    }
                }
                Err(err) => {
                    tracing::debug!(
                        error = %sanitize_error_detail(&err.to_string()),
                        "gateway direct reqwest client cache warm failed"
                    );
                    mark_direct_reqwest_client_cache_not_warming(&cache_key);
                    break;
                }
            }
        }

        let Ok(mut cache) = DIRECT_REQWEST_CLIENT_CACHE.lock() else {
            return;
        };
        let Some(entry) = cache.get_mut(&cache_key) else {
            return;
        };
        entry.warming = false;
    });
    observe_gateway_stage_ms(
        "direct_reqwest_client_cache_warm_enqueue",
        enqueue_started_at.elapsed().as_millis() as u64,
    );
    true
}

fn mark_direct_reqwest_client_cache_not_warming(cache_key: &DirectReqwestClientCacheKey) {
    if let Ok(mut cache) = DIRECT_REQWEST_CLIENT_CACHE.lock() {
        if let Some(entry) = cache.get_mut(cache_key) {
            entry.warming = false;
        }
    }
}

fn next_direct_reqwest_client_cache_clock() -> u64 {
    DIRECT_REQWEST_CLIENT_CACHE_CLOCK
        .fetch_add(1, Ordering::Relaxed)
        .wrapping_add(1)
}

fn direct_reqwest_client_cache_max_entries() -> usize {
    (DIRECT_REQWEST_CLIENT_CACHE_CAPACITY_BYTES / DIRECT_REQWEST_CLIENT_ENTRY_ESTIMATED_BYTES)
        .max(1)
}

fn evict_direct_reqwest_client_cache_for_insert(
    cache: &mut HashMap<DirectReqwestClientCacheKey, DirectReqwestClientCacheEntry>,
    incoming: &DirectReqwestClientCacheKey,
    max_entries: usize,
) {
    if cache.contains_key(incoming) {
        return;
    }
    while cache.len() >= max_entries {
        let Some(oldest_key) = cache
            .iter()
            .min_by_key(|(_, entry)| entry.last_used)
            .map(|(key, _)| key.clone())
        else {
            break;
        };
        cache.remove(&oldest_key);
        DIRECT_REQWEST_CLIENT_CACHE_METRICS
            .evictions
            .fetch_add(1, Ordering::Relaxed);
    }
}

fn direct_reqwest_client_cache_key(
    request_url: &str,
    key_id: &str,
    timeouts: Option<&aether_contracts::ExecutionTimeouts>,
    proxy_url: Option<String>,
    transport_profile: Option<&ResolvedTransportProfile>,
    transport_controls: ExecutionTransportControls,
) -> DirectReqwestClientCacheKey {
    DirectReqwestClientCacheKey {
        upstream_origin: direct_reqwest_cache_per_origin()
            .then(|| direct_reqwest_upstream_origin(request_url))
            .flatten(),
        pool_partition: direct_reqwest_pool_partition(transport_profile, key_id),
        connect_timeout_ms: timeouts.and_then(|timeouts| timeouts.connect_ms),
        proxy_digest: proxy_url.map(|proxy_url| direct_reqwest_proxy_digest(&proxy_url)),
        follow_redirects: transport_controls.follow_redirects == Some(true),
        http1_only: transport_controls.http1_only,
        transport_profile: transport_profile.map(direct_reqwest_transport_profile_cache_key),
    }
}

fn direct_reqwest_proxy_digest(proxy_url: &str) -> String {
    hex::encode(sha2::Sha256::digest(proxy_url.as_bytes()))
}

fn validate_direct_reqwest_proxy_material(
    cache_key: &DirectReqwestClientCacheKey,
    proxy_url: Option<&str>,
) -> Result<(), ExecutionRuntimeTransportError> {
    if cache_key.proxy_digest.as_deref() == proxy_url.map(direct_reqwest_proxy_digest).as_deref() {
        Ok(())
    } else {
        Err(ExecutionRuntimeTransportError::ProxyUnsupported)
    }
}

fn direct_reqwest_pool_partition(
    transport_profile: Option<&ResolvedTransportProfile>,
    key_id: &str,
) -> Option<String> {
    let key_id = key_id.trim();
    transport_profile
        .filter(|profile| profile.pool_scope.trim().eq_ignore_ascii_case("key"))
        .filter(|_| !key_id.is_empty())
        .map(|_| hex::encode(sha2::Sha256::digest(key_id.as_bytes())))
}

fn direct_reqwest_cache_per_origin() -> bool {
    std::env::var(DIRECT_REQWEST_CACHE_PER_ORIGIN_ENV)
        .ok()
        .is_some_and(|value| matches_truthy_env_value(value.trim()))
}

fn matches_truthy_env_value(value: &str) -> bool {
    matches!(
        value.to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

fn direct_reqwest_upstream_origin(request_url: &str) -> Option<String> {
    let url = reqwest::Url::parse(request_url).ok()?;
    let scheme = url.scheme();
    if scheme != "http" && scheme != "https" {
        return None;
    }
    let host = url.host_str()?;
    let port = url.port_or_known_default()?;
    let authority_host = match url.host() {
        Some(url::Host::Ipv6(_)) if !host.starts_with('[') => format!("[{host}]"),
        _ => host.to_string(),
    };
    Some(format!("{scheme}://{authority_host}:{port}"))
}

fn direct_reqwest_transport_profile_cache_key(
    profile: &ResolvedTransportProfile,
) -> DirectReqwestTransportProfileCacheKey {
    DirectReqwestTransportProfileCacheKey {
        profile_id: profile.profile_id.trim().to_string(),
        backend: profile.backend.trim().to_ascii_lowercase(),
        http_mode: profile.http_mode.trim().to_ascii_lowercase(),
        pool_scope: profile.pool_scope.trim().to_ascii_lowercase(),
        header_fingerprint: stable_json_cache_key(profile.header_fingerprint.as_ref()),
        extra: stable_json_cache_key(profile.extra.as_ref()),
    }
}

fn stable_json_cache_key(value: Option<&Value>) -> Option<String> {
    value.and_then(|value| serde_json::to_string(value).ok())
}

fn direct_reqwest_client_shard_count(cache_key: &DirectReqwestClientCacheKey) -> usize {
    if let Some(shards) = env_positive_usize(DIRECT_REQWEST_CLIENT_SHARDS_ENV) {
        return shards.clamp(1, MAX_DIRECT_REQWEST_H2_CLIENT_SHARDS);
    }
    let target_gate_limit = crate::state::upstream_target_gate_limit_from_env()
        .unwrap_or_else(crate::state::upstream_target_gate_auto_limit);
    if !direct_reqwest_client_cache_key_uses_http2(cache_key) {
        return direct_reqwest_client_shards_from_config(
            None,
            target_gate_limit,
            env_positive_usize(DIRECT_REQWEST_HTTP1_TARGET_STREAMS_PER_CLIENT_ENV)
                .unwrap_or(DEFAULT_HTTP1_TARGET_STREAMS_PER_CLIENT),
        );
    }
    direct_reqwest_h2_client_shards_from_config(
        env_positive_usize(DIRECT_REQWEST_H2_CLIENT_SHARDS_ENV),
        target_gate_limit,
        env_positive_usize(DIRECT_REQWEST_H2_TARGET_STREAMS_PER_CLIENT_ENV)
            .unwrap_or(DEFAULT_H2_TARGET_STREAMS_PER_CLIENT),
    )
}

fn direct_reqwest_client_cache_key_uses_http2(cache_key: &DirectReqwestClientCacheKey) -> bool {
    if cache_key.http1_only {
        return false;
    }
    direct_reqwest_client_cache_key_uses_h2c_prior_knowledge(cache_key)
}

fn direct_reqwest_client_cache_key_uses_h2c_prior_knowledge(
    cache_key: &DirectReqwestClientCacheKey,
) -> bool {
    cache_key
        .transport_profile
        .as_ref()
        .is_some_and(|profile| profile.http_mode == TRANSPORT_HTTP_MODE_H2C_PRIOR_KNOWLEDGE)
}

fn record_direct_reqwest_client_protocol_selection(cache_key: &DirectReqwestClientCacheKey) {
    if cache_key.http1_only {
        DIRECT_REQWEST_CLIENT_CACHE_METRICS
            .http1_selections
            .fetch_add(1, Ordering::Relaxed);
        return;
    }
    if direct_reqwest_client_cache_key_uses_h2c_prior_knowledge(cache_key) {
        DIRECT_REQWEST_CLIENT_CACHE_METRICS
            .h2c_selections
            .fetch_add(1, Ordering::Relaxed);
        return;
    }
    DIRECT_REQWEST_CLIENT_CACHE_METRICS
        .auto_selections
        .fetch_add(1, Ordering::Relaxed);
}

fn direct_reqwest_h2_client_shards_from_config(
    explicit_shards: Option<usize>,
    target_gate_limit: usize,
    target_streams_per_client: usize,
) -> usize {
    direct_reqwest_client_shards_from_config(
        explicit_shards,
        target_gate_limit,
        target_streams_per_client,
    )
}

fn direct_reqwest_client_shards_from_config(
    explicit_shards: Option<usize>,
    target_gate_limit: usize,
    target_streams_per_client: usize,
) -> usize {
    if let Some(shards) = explicit_shards {
        return shards.clamp(1, MAX_DIRECT_REQWEST_H2_CLIENT_SHARDS);
    }
    let streams_per_client = target_streams_per_client.max(1);
    target_gate_limit
        .max(1)
        .div_ceil(streams_per_client)
        .clamp(1, MAX_DIRECT_REQWEST_H2_CLIENT_SHARDS)
}

fn direct_reqwest_initial_client_shard_count(target_len: usize) -> usize {
    env_positive_usize(DIRECT_REQWEST_SYNC_WARM_CLIENTS_ENV)
        .unwrap_or(DEFAULT_DIRECT_REQWEST_SYNC_WARM_CLIENTS)
        .clamp(1, target_len.clamp(1, MAX_DIRECT_REQWEST_SYNC_WARM_CLIENTS))
}

fn direct_reqwest_prewarm_client_shard_count(target_len: usize) -> usize {
    let request_path_cap = direct_reqwest_initial_client_shard_count(target_len);
    env_positive_usize(DIRECT_REQWEST_PREWARM_SYNC_CLIENTS_ENV)
        .unwrap_or(request_path_cap)
        .clamp(1, target_len.max(1).min(request_path_cap))
}

fn env_positive_usize(name: &str) -> Option<usize> {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
}

fn build_direct_reqwest_client_from_cache_key(
    cache_key: &DirectReqwestClientCacheKey,
    proxy_url: Option<&str>,
) -> Result<reqwest::Client, ExecutionRuntimeTransportError> {
    validate_direct_reqwest_proxy_material(cache_key, proxy_url)?;
    if let Some(proxy_url) = proxy_url {
        validate_execution_proxy_url(proxy_url)?;
    }
    let mut builder = reqwest::Client::builder().no_proxy();
    if proxy_url.is_none() {
        builder = builder.dns_resolver(Arc::new(ExecutionSafeDnsResolver));
    }
    builder = builder.redirect(if cache_key.follow_redirects {
        same_origin_reqwest_redirect_policy()
    } else {
        Policy::none()
    });
    if cache_key.http1_only
        || cache_key
            .transport_profile
            .as_ref()
            .is_some_and(|profile| profile.http_mode == TRANSPORT_HTTP_MODE_HTTP1_ONLY)
    {
        builder = builder.http1_only();
    } else if cache_key
        .transport_profile
        .as_ref()
        .is_some_and(|profile| profile.http_mode == TRANSPORT_HTTP_MODE_H2C_PRIOR_KNOWLEDGE)
    {
        builder = builder.http2_prior_knowledge();
    }
    let mut builder = apply_http_client_config(
        builder,
        &HttpClientConfig {
            connect_timeout_ms: cache_key.connect_timeout_ms,
            pool_max_idle_per_host: Some(direct_reqwest_pool_max_idle_per_host()),
            pool_idle_timeout_ms: Some(upstream_pool_idle_timeout_ms()),
            ..HttpClientConfig::default()
        },
    );
    builder = apply_transport_profile_cache_key(
        builder,
        cache_key.transport_profile.as_ref(),
        cache_key.http1_only,
    );
    if let Some(proxy_url) = proxy_url {
        let proxy =
            reqwest::Proxy::all(proxy_url).map_err(ExecutionRuntimeTransportError::InvalidProxy)?;
        builder = builder.proxy(proxy);
    }
    builder
        .build()
        .map_err(ExecutionRuntimeTransportError::ClientBuild)
}

fn direct_reqwest_pool_max_idle_per_host() -> usize {
    const DEFAULT_MAX_IDLE_PER_HOST: usize = 32;
    std::env::var("AETHER_GATEWAY_UPSTREAM_POOL_MAX_IDLE_PER_HOST")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_MAX_IDLE_PER_HOST)
        .min(1024)
}

fn upstream_pool_idle_timeout_ms() -> u64 {
    std::env::var("AETHER_GATEWAY_UPSTREAM_POOL_IDLE_TIMEOUT_MS")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(15_000)
        .min(300_000)
}

pub(crate) fn direct_reqwest_client_cache_metric_samples() -> Vec<MetricSample> {
    let (entries, clients, target_clients, ready_entries, warming_entries, pending_clients) =
        DIRECT_REQWEST_CLIENT_CACHE
            .lock()
            .map(|cache| {
                let entries = cache.len() as u64;
                let clients = cache.values().map(|entry| entry.len() as u64).sum();
                let target_clients = cache.values().map(|entry| entry.target_len as u64).sum();
                let ready_entries = cache
                    .values()
                    .filter(|entry| entry.len() >= entry.target_len)
                    .count() as u64;
                let warming_entries = cache.values().filter(|entry| entry.warming).count() as u64;
                let pending_clients = cache
                    .values()
                    .map(|entry| entry.target_len.saturating_sub(entry.len()) as u64)
                    .sum();
                (
                    entries,
                    clients,
                    target_clients,
                    ready_entries,
                    warming_entries,
                    pending_clients,
                )
            })
            .unwrap_or((0, 0, 0, 0, 0, 0));
    let (h2c_entries, h2c_clients, h2c_target_clients) = DIRECT_H2C_CLIENT_CACHE
        .lock()
        .map(|cache| {
            let entries = cache.len() as u64;
            let clients = cache.values().map(|entry| entry.len() as u64).sum();
            let target_clients = cache.values().map(|entry| entry.target_len as u64).sum();
            (entries, clients, target_clients)
        })
        .unwrap_or((0, 0, 0));
    let (
        h2c_sender_entries,
        h2c_sender_ready_entries,
        h2c_senders,
        h2c_target_senders,
        h2c_pending_senders,
        h2c_sender_in_flight,
        h2c_sender_max_in_flight,
    ) = DIRECT_H2C_SENDER_CACHE
        .read()
        .map_or((0, 0, 0, 0, 0, 0, 0), |cache| {
            let entries = cache.len() as u64;
            let ready_entries = cache
                .values()
                .filter_map(|cell| cell.get())
                .filter(|entry| entry.len() >= entry.target_len)
                .count() as u64;
            let senders = cache
                .values()
                .filter_map(|cell| cell.get())
                .map(|entry| entry.len() as u64)
                .sum();
            let target_senders = cache
                .values()
                .filter_map(|cell| cell.get())
                .map(|entry| entry.target_len as u64)
                .sum();
            let pending_senders = cache
                .values()
                .map(|cell| {
                    cell.get()
                        .map(|entry| entry.target_len.saturating_sub(entry.len()) as u64)
                        .unwrap_or_else(|| direct_h2c_client_shard_count() as u64)
                })
                .sum();
            let in_flight = cache
                .values()
                .filter_map(|cell| cell.get())
                .map(|entry| entry.in_flight())
                .sum();
            let max_in_flight = cache
                .values()
                .filter_map(|cell| cell.get())
                .map(|entry| entry.max_in_flight())
                .max()
                .unwrap_or(0);
            (
                entries,
                ready_entries,
                senders,
                target_senders,
                pending_senders,
                in_flight,
                max_in_flight,
            )
        });
    let mut samples = vec![
        MetricSample::new(
            "direct_reqwest_client_cache_entries",
            "Number of cached direct reqwest clients.",
            MetricKind::Gauge,
            entries,
        ),
        MetricSample::new(
            "direct_reqwest_client_cache_clients",
            "Number of direct reqwest clients across all cache entries.",
            MetricKind::Gauge,
            clients,
        ),
        MetricSample::new(
            "direct_reqwest_client_cache_target_clients",
            "Target number of direct reqwest clients across all cache entries.",
            MetricKind::Gauge,
            target_clients,
        ),
        MetricSample::new(
            "direct_reqwest_client_cache_ready_entries",
            "Number of direct reqwest client cache entries at target shard count.",
            MetricKind::Gauge,
            ready_entries,
        ),
        MetricSample::new(
            "direct_reqwest_client_cache_warming_entries",
            "Number of direct reqwest client cache entries currently warming in the background.",
            MetricKind::Gauge,
            warming_entries,
        ),
        MetricSample::new(
            "direct_reqwest_client_cache_pending_clients",
            "Number of direct reqwest client shards still missing from target cache size.",
            MetricKind::Gauge,
            pending_clients,
        ),
        MetricSample::new(
            "direct_reqwest_client_cache_hits_total",
            "Number of direct reqwest client cache hits.",
            MetricKind::Counter,
            DIRECT_REQWEST_CLIENT_CACHE_METRICS
                .hits
                .load(Ordering::Relaxed),
        ),
        MetricSample::new(
            "direct_reqwest_client_cache_misses_total",
            "Number of direct reqwest client cache misses.",
            MetricKind::Counter,
            DIRECT_REQWEST_CLIENT_CACHE_METRICS
                .misses
                .load(Ordering::Relaxed),
        ),
        MetricSample::new(
            "direct_reqwest_client_cache_builds_total",
            "Number of direct reqwest clients built after cache misses.",
            MetricKind::Counter,
            DIRECT_REQWEST_CLIENT_CACHE_METRICS
                .builds
                .load(Ordering::Relaxed),
        ),
        MetricSample::new(
            "direct_reqwest_client_cache_warm_enqueue_total",
            "Number of background direct reqwest client cache warm jobs enqueued.",
            MetricKind::Counter,
            DIRECT_REQWEST_CLIENT_CACHE_METRICS
                .warm_enqueues
                .load(Ordering::Relaxed),
        ),
        MetricSample::new(
            "direct_reqwest_client_cache_warm_skipped_total",
            "Number of direct reqwest client cache warm attempts skipped before enqueue.",
            MetricKind::Counter,
            DIRECT_REQWEST_CLIENT_CACHE_METRICS
                .warm_skipped_total
                .load(Ordering::Relaxed),
        ),
        MetricSample::new(
            "direct_reqwest_client_cache_evictions_total",
            "Number of least-recently-used direct reqwest client cache entries evicted at the configured capacity.",
            MetricKind::Counter,
            DIRECT_REQWEST_CLIENT_CACHE_METRICS
                .evictions
                .load(Ordering::Relaxed),
        ),
        MetricSample::new(
            "direct_reqwest_client_http1_select_total",
            "Number of direct reqwest client selections using forced HTTP/1.",
            MetricKind::Counter,
            DIRECT_REQWEST_CLIENT_CACHE_METRICS
                .http1_selections
                .load(Ordering::Relaxed),
        ),
        MetricSample::new(
            "direct_reqwest_client_h2c_select_total",
            "Number of direct reqwest client selections using h2c prior knowledge.",
            MetricKind::Counter,
            DIRECT_REQWEST_CLIENT_CACHE_METRICS
                .h2c_selections
                .load(Ordering::Relaxed),
        ),
        MetricSample::new(
            "direct_reqwest_client_auto_select_total",
            "Number of direct reqwest client selections using reqwest automatic protocol negotiation.",
            MetricKind::Counter,
            DIRECT_REQWEST_CLIENT_CACHE_METRICS
                .auto_selections
                .load(Ordering::Relaxed),
        ),
    ];
    samples.extend([
        MetricSample::new(
            "direct_h2c_client_cache_entries",
            "Number of cached direct H2C client entries.",
            MetricKind::Gauge,
            h2c_entries,
        ),
        MetricSample::new(
            "direct_h2c_client_cache_clients",
            "Number of direct H2C clients across all cache entries.",
            MetricKind::Gauge,
            h2c_clients,
        ),
        MetricSample::new(
            "direct_h2c_client_cache_target_clients",
            "Target number of direct H2C clients across all cache entries.",
            MetricKind::Gauge,
            h2c_target_clients,
        ),
        MetricSample::new(
            "direct_h2c_client_cache_hits_total",
            "Number of direct H2C client cache hits.",
            MetricKind::Counter,
            DIRECT_H2C_CLIENT_CACHE_METRICS.hits.load(Ordering::Relaxed),
        ),
        MetricSample::new(
            "direct_h2c_client_cache_misses_total",
            "Number of direct H2C client cache misses.",
            MetricKind::Counter,
            DIRECT_H2C_CLIENT_CACHE_METRICS
                .misses
                .load(Ordering::Relaxed),
        ),
        MetricSample::new(
            "direct_h2c_client_cache_builds_total",
            "Number of direct H2C clients built after cache misses.",
            MetricKind::Counter,
            DIRECT_H2C_CLIENT_CACHE_METRICS
                .builds
                .load(Ordering::Relaxed),
        ),
        MetricSample::new(
            "direct_h2c_sender_cache_entries",
            "Number of cached direct H2C sender entries.",
            MetricKind::Gauge,
            h2c_sender_entries,
        ),
        MetricSample::new(
            "direct_h2c_sender_cache_senders",
            "Number of direct H2C senders across all cache entries.",
            MetricKind::Gauge,
            h2c_senders,
        ),
        MetricSample::new(
            "direct_h2c_sender_cache_ready_entries",
            "Number of direct H2C sender cache entries at target sender count.",
            MetricKind::Gauge,
            h2c_sender_ready_entries,
        ),
        MetricSample::new(
            "direct_h2c_sender_cache_target_senders",
            "Target number of direct H2C senders across all cache entries.",
            MetricKind::Gauge,
            h2c_target_senders,
        ),
        MetricSample::new(
            "direct_h2c_sender_cache_pending_senders",
            "Number of direct H2C sender connections still missing from target cache size.",
            MetricKind::Gauge,
            h2c_pending_senders,
        ),
        MetricSample::new(
            "direct_h2c_sender_cache_in_flight",
            "Current number of direct H2C requests waiting for upstream headers across sender slots.",
            MetricKind::Gauge,
            h2c_sender_in_flight,
        ),
        MetricSample::new(
            "direct_h2c_sender_cache_max_slot_in_flight",
            "Highest observed in-flight request count on a single direct H2C sender slot.",
            MetricKind::Gauge,
            h2c_sender_max_in_flight,
        ),
        MetricSample::new(
            "direct_h2c_sender_cache_hits_total",
            "Number of direct H2C sender cache hits.",
            MetricKind::Counter,
            DIRECT_H2C_SENDER_CACHE_METRICS.hits.load(Ordering::Relaxed),
        ),
        MetricSample::new(
            "direct_h2c_sender_cache_misses_total",
            "Number of direct H2C sender cache misses.",
            MetricKind::Counter,
            DIRECT_H2C_SENDER_CACHE_METRICS
                .misses
                .load(Ordering::Relaxed),
        ),
        MetricSample::new(
            "direct_h2c_sender_cache_builds_total",
            "Number of direct H2C senders built after cache misses.",
            MetricKind::Counter,
            DIRECT_H2C_SENDER_CACHE_METRICS
                .builds
                .load(Ordering::Relaxed),
        ),
        MetricSample::new(
            "direct_h2c_sender_cache_prewarm_requested_total",
            "Number of direct H2C sender prewarm URLs requested.",
            MetricKind::Counter,
            DIRECT_H2C_SENDER_CACHE_METRICS
                .prewarm_requested
                .load(Ordering::Relaxed),
        ),
        MetricSample::new(
            "direct_h2c_sender_cache_prewarm_success_total",
            "Number of direct H2C sender cache targets successfully prewarmed.",
            MetricKind::Counter,
            DIRECT_H2C_SENDER_CACHE_METRICS
                .prewarm_success
                .load(Ordering::Relaxed),
        ),
        MetricSample::new(
            "direct_h2c_sender_cache_prewarm_failed_total",
            "Number of direct H2C sender cache prewarm targets or URLs that failed.",
            MetricKind::Counter,
            DIRECT_H2C_SENDER_CACHE_METRICS
                .prewarm_failed
                .load(Ordering::Relaxed),
        ),
    ]);
    samples
}

pub(crate) fn build_browser_wreq_client(
    timeouts: Option<&aether_contracts::ExecutionTimeouts>,
    proxy: Option<&ProxySnapshot>,
    transport_profile: &ResolvedTransportProfile,
    transport_controls: ExecutionTransportControls,
    apply_total_timeout: bool,
) -> Result<wreq::Client, ExecutionRuntimeTransportError> {
    let emulation = browser_wreq_emulation_from_profile(transport_profile)?;
    let proxy_url = resolve_proxy_url(proxy)?;
    let mut builder = wreq::Client::builder()
        .no_proxy()
        .emulation(emulation)
        .pool_max_idle_per_host(direct_reqwest_pool_max_idle_per_host())
        .pool_idle_timeout(Duration::from_millis(upstream_pool_idle_timeout_ms()));
    if proxy_url.is_none() {
        builder = builder.dns_resolver(ExecutionSafeDnsResolver);
    }
    builder = builder.redirect(if transport_controls.follow_redirects == Some(true) {
        same_origin_wreq_redirect_policy()
    } else {
        wreq::redirect::Policy::none()
    });
    if transport_controls.http1_only || transport_profile_http1_only(Some(transport_profile)) {
        builder = builder.http1_only();
    }
    if let Some(connect_ms) = timeouts.and_then(|timeouts| timeouts.connect_ms) {
        builder = builder.connect_timeout(Duration::from_millis(connect_ms));
    }
    if apply_total_timeout {
        if let Some(total_ms) = timeouts.and_then(|timeouts| timeouts.total_ms) {
            builder = builder.timeout(Duration::from_millis(total_ms));
        }
    }
    if let Some(read_ms) = timeouts.and_then(|timeouts| timeouts.read_ms) {
        builder = builder.read_timeout(Duration::from_millis(read_ms));
    }
    if let Some(proxy_url) = proxy_url {
        let proxy = wreq::Proxy::all(proxy_url.as_str())
            .map_err(ExecutionRuntimeTransportError::BrowserClientBuild)?;
        builder = builder.proxy(proxy);
    }
    builder
        .build()
        .map_err(ExecutionRuntimeTransportError::BrowserClientBuild)
}

fn same_origin_reqwest_redirect_policy() -> Policy {
    Policy::custom(|attempt| {
        let same_origin = attempt
            .previous()
            .last()
            .is_some_and(|previous| reqwest_urls_have_same_origin(previous, attempt.url()));
        match safe_redirect_decision(attempt.previous().len(), same_origin) {
            SafeRedirectDecision::Follow => attempt.follow(),
            SafeRedirectDecision::Stop => attempt.stop(),
            SafeRedirectDecision::TooMany => attempt.error("too many redirects"),
        }
    })
}

fn same_origin_wreq_redirect_policy() -> wreq::redirect::Policy {
    wreq::redirect::Policy::custom(|attempt| {
        let same_origin = attempt
            .previous
            .last()
            .is_some_and(|previous| http_uris_have_same_origin(previous, &attempt.uri));
        match safe_redirect_decision(attempt.previous.len(), same_origin) {
            SafeRedirectDecision::Follow => attempt.follow(),
            SafeRedirectDecision::Stop => attempt.stop(),
            SafeRedirectDecision::TooMany => attempt.error("too many redirects"),
        }
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SafeRedirectDecision {
    Follow,
    Stop,
    TooMany,
}

fn safe_redirect_decision(previous_len: usize, same_origin: bool) -> SafeRedirectDecision {
    if previous_len > MAX_SAFE_REDIRECTS {
        SafeRedirectDecision::TooMany
    } else if same_origin {
        SafeRedirectDecision::Follow
    } else {
        SafeRedirectDecision::Stop
    }
}

fn reqwest_urls_have_same_origin(previous: &reqwest::Url, next: &reqwest::Url) -> bool {
    previous.scheme().eq_ignore_ascii_case(next.scheme())
        && previous
            .host_str()
            .zip(next.host_str())
            .is_some_and(|(previous, next)| previous.eq_ignore_ascii_case(next))
        && previous.port_or_known_default() == next.port_or_known_default()
}

fn http_uris_have_same_origin(previous: &http::Uri, next: &http::Uri) -> bool {
    previous
        .scheme_str()
        .zip(next.scheme_str())
        .is_some_and(|(previous, next)| previous.eq_ignore_ascii_case(next))
        && previous
            .host()
            .zip(next.host())
            .is_some_and(|(previous, next)| previous.eq_ignore_ascii_case(next))
        && http_uri_effective_port(previous) == http_uri_effective_port(next)
}

fn http_uri_effective_port(uri: &http::Uri) -> Option<u16> {
    uri.port_u16().or_else(|| {
        let scheme = uri.scheme_str()?;
        if scheme.eq_ignore_ascii_case("http") {
            Some(80)
        } else if scheme.eq_ignore_ascii_case("https") {
            Some(443)
        } else {
            None
        }
    })
}

fn browser_wreq_emulation_from_profile(
    profile: &ResolvedTransportProfile,
) -> Result<wreq_util::Profile, ExecutionRuntimeTransportError> {
    match normalize_browser_profile_name(browser_transport_profile_name(profile)).as_str() {
        "chrome100" => Ok(wreq_util::Profile::Chrome100),
        "chrome101" => Ok(wreq_util::Profile::Chrome101),
        "chrome104" => Ok(wreq_util::Profile::Chrome104),
        "chrome105" => Ok(wreq_util::Profile::Chrome105),
        "chrome106" => Ok(wreq_util::Profile::Chrome106),
        "chrome107" => Ok(wreq_util::Profile::Chrome107),
        "chrome108" => Ok(wreq_util::Profile::Chrome108),
        "chrome109" => Ok(wreq_util::Profile::Chrome109),
        "chrome110" => Ok(wreq_util::Profile::Chrome110),
        "chrome114" => Ok(wreq_util::Profile::Chrome114),
        "chrome116" => Ok(wreq_util::Profile::Chrome116),
        "chrome117" => Ok(wreq_util::Profile::Chrome117),
        "chrome118" => Ok(wreq_util::Profile::Chrome118),
        "chrome119" => Ok(wreq_util::Profile::Chrome119),
        "chrome120" => Ok(wreq_util::Profile::Chrome120),
        "chrome123" => Ok(wreq_util::Profile::Chrome123),
        "chrome124" => Ok(wreq_util::Profile::Chrome124),
        "chrome126" => Ok(wreq_util::Profile::Chrome126),
        "chrome127" => Ok(wreq_util::Profile::Chrome127),
        "chrome128" => Ok(wreq_util::Profile::Chrome128),
        "chrome129" => Ok(wreq_util::Profile::Chrome129),
        "chrome130" => Ok(wreq_util::Profile::Chrome130),
        "chrome131" => Ok(wreq_util::Profile::Chrome131),
        "chrome132" => Ok(wreq_util::Profile::Chrome132),
        "chrome133" => Ok(wreq_util::Profile::Chrome133),
        "chrome134" => Ok(wreq_util::Profile::Chrome134),
        "chrome135" => Ok(wreq_util::Profile::Chrome135),
        "chrome136" => Ok(wreq_util::Profile::Chrome136),
        "chrome137" => Ok(wreq_util::Profile::Chrome137),
        "chrome138" => Ok(wreq_util::Profile::Chrome138),
        "chrome139" => Ok(wreq_util::Profile::Chrome139),
        "chrome140" => Ok(wreq_util::Profile::Chrome140),
        "chrome141" => Ok(wreq_util::Profile::Chrome141),
        "chrome142" => Ok(wreq_util::Profile::Chrome142),
        "chrome143" => Ok(wreq_util::Profile::Chrome143),
        "chrome144" => Ok(wreq_util::Profile::Chrome144),
        "chrome145" => Ok(wreq_util::Profile::Chrome145),
        other => Err(ExecutionRuntimeTransportError::UnsupportedTransportProfile(
            format!("browser_wreq:{other}"),
        )),
    }
}

fn normalize_browser_profile_name(value: String) -> String {
    value
        .trim()
        .to_ascii_lowercase()
        .replace(['_', '-', ' '], "")
}

fn validate_reqwest_transport_profile(
    transport_profile: Option<&ResolvedTransportProfile>,
) -> Result<(), ExecutionRuntimeTransportError> {
    let Some(profile) = transport_profile else {
        return Ok(());
    };
    if profile
        .backend
        .trim()
        .eq_ignore_ascii_case(TRANSPORT_BACKEND_REQWEST_RUSTLS)
    {
        return Ok(());
    }
    Err(ExecutionRuntimeTransportError::UnsupportedTransportProfile(
        profile.backend.clone(),
    ))
}

fn transport_profile_uses_browser_wreq(
    transport_profile: Option<&ResolvedTransportProfile>,
) -> bool {
    transport_profile
        .map(|profile| {
            profile
                .backend
                .trim()
                .eq_ignore_ascii_case(TRANSPORT_BACKEND_BROWSER_WREQ)
        })
        .unwrap_or(false)
}

fn browser_transport_profile_name(profile: &ResolvedTransportProfile) -> String {
    profile
        .extra
        .as_ref()
        .and_then(|value| {
            value
                .get("browser_profile")
                .or_else(|| value.get("impersonate"))
                .and_then(Value::as_str)
        })
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .or_else(|| {
            profile
                .profile_id
                .trim()
                .is_empty()
                .then_some("chrome136".to_string())
                .or_else(|| Some(profile.profile_id.trim().to_string()))
        })
        .unwrap_or_else(|| "chrome136".to_string())
}

fn transport_profile_http1_only(transport_profile: Option<&ResolvedTransportProfile>) -> bool {
    transport_profile
        .map(|profile| {
            profile
                .http_mode
                .trim()
                .eq_ignore_ascii_case(TRANSPORT_HTTP_MODE_HTTP1_ONLY)
        })
        .unwrap_or(false)
}

fn transport_profile_h2c_prior_knowledge(
    transport_profile: Option<&ResolvedTransportProfile>,
) -> bool {
    transport_profile
        .map(|profile| {
            profile
                .http_mode
                .trim()
                .eq_ignore_ascii_case(TRANSPORT_HTTP_MODE_H2C_PRIOR_KNOWLEDGE)
        })
        .unwrap_or(false)
}

fn apply_transport_profile_cache_key(
    builder: reqwest::ClientBuilder,
    transport_profile: Option<&DirectReqwestTransportProfileCacheKey>,
    http1_only: bool,
) -> reqwest::ClientBuilder {
    let Some(profile) = transport_profile else {
        return builder;
    };
    if profile.profile_id.is_empty() || profile.http_mode == TRANSPORT_HTTP_MODE_H2C_PRIOR_KNOWLEDGE
    {
        return builder;
    }

    crate::ensure_rustls_crypto_provider();

    builder.tls_backend_preconfigured(build_best_effort_transport_tls_config(http1_only))
}

fn build_best_effort_transport_tls_config(http1_only: bool) -> rustls::ClientConfig {
    let root_store =
        rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let mut config = rustls::ClientConfig::builder_with_protocol_versions(&[
        &rustls::version::TLS13,
        &rustls::version::TLS12,
    ])
    .with_root_certificates(root_store)
    .with_no_client_auth();
    config.alpn_protocols = if http1_only {
        vec![b"http/1.1".to_vec()]
    } else {
        vec![b"h2".to_vec(), b"http/1.1".to_vec()]
    };
    config
}

fn resolve_proxy_url(
    proxy: Option<&ProxySnapshot>,
) -> Result<Option<String>, ExecutionRuntimeTransportError> {
    let Some(proxy) = proxy else {
        return Ok(None);
    };

    if proxy.enabled == Some(false) {
        return Ok(None);
    }

    if let Some(proxy_url) = proxy
        .url
        .as_ref()
        .map(|url| url.trim())
        .filter(|url| !url.is_empty())
    {
        return normalize_execution_proxy_url(proxy_url).map(Some);
    }

    Err(ExecutionRuntimeTransportError::ProxyUnsupported)
}

fn validate_execution_proxy_url(raw_url: &str) -> Result<(), ExecutionRuntimeTransportError> {
    parse_execution_proxy_url(raw_url).map(|_| ())
}

/// Normalize a configured proxy URL before handing it to reqwest/wreq.
///
/// `socks5://` has a particularly dangerous ambiguity in a gateway: reqwest
/// and wreq interpret it as *local* target-name resolution, while
/// `socks5h://` delegates target resolution to the proxy.  Local resolution
/// would bypass the execution DNS guard (and could turn a rebinding hostname
/// into a private address).  Keep accepting the established `socks5` config
/// syntax for compatibility, but make its runtime semantics the safe remote
/// DNS variant.  HTTP/HTTPS and already-remote `socks5h` URLs are unchanged.
pub(crate) fn normalize_execution_proxy_url(
    raw_url: &str,
) -> Result<String, ExecutionRuntimeTransportError> {
    let mut parsed = parse_execution_proxy_url(raw_url)?;
    if parsed.scheme().eq_ignore_ascii_case("socks5") {
        parsed
            .set_scheme("socks5h")
            .map_err(|_| ExecutionRuntimeTransportError::ProxyUnsupported)?;
    }
    Ok(parsed.to_string())
}

fn parse_execution_proxy_url(raw_url: &str) -> Result<url::Url, ExecutionRuntimeTransportError> {
    let parsed =
        url::Url::parse(raw_url).map_err(|_| ExecutionRuntimeTransportError::ProxyUnsupported)?;
    if !matches!(parsed.scheme(), "http" | "https" | "socks5" | "socks5h")
        || parsed.host_str().is_none()
        || !matches!(parsed.path(), "" | "/")
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return Err(ExecutionRuntimeTransportError::ProxyUnsupported);
    }
    Ok(parsed)
}

pub(crate) fn build_request_headers(
    headers: &BTreeMap<String, String>,
    content_encoding: Option<&str>,
    allow_passthrough_content_encoding: bool,
) -> Result<HeaderMap, ExecutionRuntimeTransportError> {
    let mut out = HeaderMap::new();
    let connection_declared = aether_http::connection_declared_header_names(
        headers
            .iter()
            .filter(|(name, _)| name.eq_ignore_ascii_case(http::header::CONNECTION.as_str()))
            .map(|(_, value)| value.as_str()),
    );
    let normalized_content_encoding = normalize_content_encoding(content_encoding);
    if let Some(encoding) = normalized_content_encoding.as_deref() {
        if !matches!(encoding, "gzip" | "zstd") && !allow_passthrough_content_encoding {
            return Err(ExecutionRuntimeTransportError::UnsupportedContentEncoding(
                encoding.to_string(),
            ));
        }
    }
    for (key, value) in headers {
        let normalized_key = key.trim().to_ascii_lowercase();
        if crate::headers::should_skip_request_header(&normalized_key)
            || is_hop_by_hop_header(&normalized_key)
            || connection_declared.contains(&normalized_key)
            || normalized_key == "content-encoding"
            || normalized_key == EXECUTION_REQUEST_FOLLOW_REDIRECTS_HEADER
            || normalized_key == EXECUTION_REQUEST_HTTP1_ONLY_HEADER
            || normalized_key == EXECUTION_RESPONSE_BODY_MODE_HEADER
            || normalized_key == EXECUTION_RESPONSE_BODY_LIMIT_HEADER
        {
            continue;
        }

        let header_name = HeaderName::from_bytes(key.as_bytes())
            .map_err(|_| ExecutionRuntimeTransportError::InvalidHeaderName(key.clone()))?;
        let header_value = HeaderValue::from_str(value)
            .map_err(|_| ExecutionRuntimeTransportError::InvalidHeaderValue(key.clone()))?;
        out.insert(header_name, header_value);
    }
    if let Some(encoding) = normalized_content_encoding {
        out.insert(
            reqwest::header::CONTENT_ENCODING,
            HeaderValue::from_str(&encoding).map_err(|_| {
                ExecutionRuntimeTransportError::InvalidHeaderValue("content-encoding".into())
            })?,
        );
    }
    Ok(out)
}

fn resolve_execution_transport_controls(
    headers: &BTreeMap<String, String>,
) -> ExecutionTransportControls {
    ExecutionTransportControls {
        follow_redirects: execution_transport_header_value(
            headers,
            EXECUTION_REQUEST_FOLLOW_REDIRECTS_HEADER,
        )
        .and_then(parse_execution_transport_bool),
        http1_only: execution_transport_header_value(headers, EXECUTION_REQUEST_HTTP1_ONLY_HEADER)
            .and_then(parse_execution_transport_bool)
            .unwrap_or(false),
    }
}

pub(crate) fn execution_response_body_mode(plan: &ExecutionPlan) -> ExecutionResponseBodyMode {
    if plan.stream
        || plan.body.body_bytes_b64.is_none()
        || !plan
            .client_api_format
            .trim()
            .eq_ignore_ascii_case(plan.provider_api_format.trim())
    {
        return ExecutionResponseBodyMode::StructuredJson;
    }

    ExecutionResponseBodyMode::from_header_value(execution_transport_header_value(
        &plan.headers,
        EXECUTION_RESPONSE_BODY_MODE_HEADER,
    ))
}

fn execution_transport_header_value<'a>(
    headers: &'a BTreeMap<String, String>,
    target: &str,
) -> Option<&'a str> {
    headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(target))
        .map(|(_, value)| value.as_str())
}

fn parse_execution_transport_bool(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

fn header_map_to_string_map(headers: &HeaderMap) -> BTreeMap<String, String> {
    headers
        .iter()
        .filter_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|value| (name.as_str().to_string(), value.to_string()))
        })
        .collect()
}

fn is_hop_by_hop_header(name: &str) -> bool {
    matches!(
        name,
        "host"
            | "content-length"
            | "connection"
            | "upgrade"
            | "keep-alive"
            | "proxy-authorization"
            | "proxy-connection"
            | "te"
            | "trailer"
            | "transfer-encoding"
    )
}

pub(crate) fn collect_response_headers(headers: &HeaderMap) -> BTreeMap<String, String> {
    let connection_declared = aether_http::connection_declared_header_names(
        headers
            .get_all(http::header::CONNECTION)
            .iter()
            .filter_map(|value| value.to_str().ok()),
    );
    header_map_to_string_map(headers)
        .into_iter()
        .filter(|(name, _)| {
            !crate::headers::should_skip_response_header(name)
                && !connection_declared.contains(&name.to_ascii_lowercase())
        })
        .collect()
}

pub(crate) fn validate_execution_upstream_url(
    raw_url: &str,
) -> Result<url::Url, ExecutionRuntimeTransportError> {
    let url = url::Url::parse(raw_url).map_err(|_| {
        ExecutionRuntimeTransportError::UpstreamRequest("invalid upstream URL".to_string())
    })?;
    if url.host().is_none() || !matches!(url.scheme(), "http" | "https") {
        return Err(ExecutionRuntimeTransportError::UpstreamRequest(
            "upstream URL must use HTTP or HTTPS and include a host".to_string(),
        ));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(ExecutionRuntimeTransportError::UpstreamRequest(
            "upstream URL must not include credentials".to_string(),
        ));
    }
    if url.fragment().is_some() {
        return Err(ExecutionRuntimeTransportError::UpstreamRequest(
            "upstream URL must not include a fragment".to_string(),
        ));
    }
    let literal_ip = match url.host() {
        Some(url::Host::Ipv4(address)) => Some(IpAddr::V4(address)),
        Some(url::Host::Ipv6(address)) => Some(IpAddr::V6(address)),
        _ => None,
    };
    if literal_ip.is_some_and(|ip| {
        is_private_or_reserved_ip(ip) && !(url.scheme() == "http" && ip.is_loopback())
    }) {
        return Err(ExecutionRuntimeTransportError::UpstreamRequest(
            "upstream URL must not target a private or reserved address".to_string(),
        ));
    }
    Ok(url)
}

pub(crate) fn decode_response_body_bytes<'a>(
    headers: &BTreeMap<String, String>,
    body_bytes: &'a [u8],
) -> Result<Cow<'a, [u8]>, ExecutionRuntimeTransportError> {
    decode_response_body_bytes_with_limit(
        headers,
        body_bytes,
        crate::headers::max_internal_buffered_body_bytes(),
    )
}

pub(crate) fn decode_response_body_bytes_with_limit<'a>(
    headers: &BTreeMap<String, String>,
    body_bytes: &'a [u8],
    limit_bytes: usize,
) -> Result<Cow<'a, [u8]>, ExecutionRuntimeTransportError> {
    let encoding = headers
        .get("content-encoding")
        .map(String::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.to_ascii_lowercase());
    match encoding.as_deref() {
        Some("gzip") => {
            let mut decoder = GzDecoder::new(body_bytes);
            read_upstream_response_decoder_with_limit("gzip", &mut decoder, limit_bytes)
                .map(Cow::Owned)
        }
        Some("deflate") => {
            let mut decoder = DeflateDecoder::new(body_bytes);
            read_upstream_response_decoder_with_limit("deflate", &mut decoder, limit_bytes)
                .map(Cow::Owned)
        }
        Some("br") => {
            let mut decoder = BrotliDecoder::new(body_bytes, 4_096);
            read_upstream_response_decoder_with_limit("br", &mut decoder, limit_bytes)
                .map(Cow::Owned)
        }
        _ => Ok(Cow::Borrowed(body_bytes)),
    }
}

fn read_upstream_response_decoder_with_limit(
    encoding: &str,
    decoder: &mut impl Read,
    limit_bytes: usize,
) -> Result<Vec<u8>, ExecutionRuntimeTransportError> {
    let read_limit = u64::try_from(limit_bytes)
        .unwrap_or(u64::MAX)
        .saturating_add(1);
    let mut limited = decoder.take(read_limit);
    let mut out = Vec::new();
    limited.read_to_end(&mut out).map_err(|error| {
        ExecutionRuntimeTransportError::UpstreamResponseDecode {
            encoding: encoding.to_string(),
            message: error.to_string(),
        }
    })?;
    if out.len() > limit_bytes {
        return Err(ExecutionRuntimeTransportError::UpstreamResponseTooLarge {
            phase: UpstreamResponseBodyPhase::Decoded,
            limit_bytes,
        });
    }
    Ok(out)
}

pub(crate) fn response_body_is_json(headers: &BTreeMap<String, String>, body_bytes: &[u8]) -> bool {
    let content_type = headers
        .get("content-type")
        .map(|value| value.to_ascii_lowercase())
        .unwrap_or_default();
    if content_type.contains("json") {
        return true;
    }

    serde_json::from_slice::<Value>(body_bytes).is_ok()
}

pub(crate) fn build_execution_response_body(
    headers: &BTreeMap<String, String>,
    body_bytes: &[u8],
    decoded_body_bytes: &[u8],
    stream: bool,
    response_body_mode: ExecutionResponseBodyMode,
) -> Result<Option<ResponseBody>, ExecutionRuntimeTransportError> {
    if body_bytes.is_empty() {
        return Ok(None);
    }

    if !stream && response_body_is_json(headers, decoded_body_bytes) {
        let body_json: Value = serde_json::from_slice(decoded_body_bytes)
            .map_err(ExecutionRuntimeTransportError::InvalidJson)?;
        return Ok(Some(ResponseBody {
            json_body: Some(body_json),
            body_bytes_b64: (response_body_mode == ExecutionResponseBodyMode::PreserveBytes)
                .then(|| base64::engine::general_purpose::STANDARD.encode(body_bytes)),
        }));
    }

    if let Some(body_json) = extract_provider_private_stream_error_body(None, decoded_body_bytes)
        .or_else(|| extract_provider_private_stream_error_body(None, body_bytes))
    {
        return Ok(Some(ResponseBody {
            json_body: Some(body_json),
            body_bytes_b64: None,
        }));
    }

    if stream {
        return Ok(Some(ResponseBody {
            json_body: None,
            body_bytes_b64: Some(base64::engine::general_purpose::STANDARD.encode(body_bytes)),
        }));
    }

    Ok(Some(ResponseBody {
        json_body: None,
        body_bytes_b64: Some(base64::engine::general_purpose::STANDARD.encode(body_bytes)),
    }))
}

#[cfg(test)]
#[path = "transport/tests.rs"]
mod tests;
