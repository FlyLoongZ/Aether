//! Paired usage body frames.
//!
//! A captured request produces up to four bodies (`request`, `provider_request`,
//! `response`, `client_response`), and the two halves of each direction are near
//! copies of one another: a request body differs from its provider-bound twin by
//! a model name or a rewritten message list, and a response body differs from
//! what the client received by stream framing and envelope fields. Storing the
//! two halves as independent payloads throws that duplication away, and the
//! database cannot recover it: TOAST compresses ~2 KB chunks on their own, so
//! its window never spans two bodies that sit hundreds of kilobytes apart. On a
//! 700 KB + 700 KB pair, TOAST `pglz` lands at 274 934 bytes while one zstd
//! frame over both bodies lands at 132 599.
//!
//! So a stored row holds one *pair*: the two capture fields of one direction,
//! serialized together as a JSON container and compressed as a single frame.
//! The row is keyed by the pair's base field (`request_body` / `response_body`),
//! and a body that was not captured is written as `null` rather than omitted, so
//! a pair always occupies one row per request and direction.
//!
//! # Stored payloads
//!
//! `usage_body_blobs.encoding` says how to read `payload`:
//!
//! | `encoding` | `payload` | written by |
//! |---|---|---|
//! | [`ENCODING_RAW_SINGLE`] (0) | a raw JSON body | rows written before paired frames existed |
//! | [`ENCODING_ZSTD_CONTAINER`] (1) | a zstd frame over the container | the capture write path |
//! | [`ENCODING_RAW_CONTAINER`] (2) | the container itself | the write path, when zstd failed |
//!
//! The container is `{"<capture key>": <body>|null, ...}` over the two keys from
//! [`UsageBodyField::as_capture_key`], the base field first. Writing `null` for a
//! missing half keeps every field of a pair reachable from the same row, and it
//! also means a body that is literally JSON `null` reads back as absent rather
//! than as a body of its own.
//!
//! # Compression parameters
//!
//! The level is [`ZSTD_LEVEL`], and it has to stay positive: zstd's negative
//! levels select its fast mode, which gives up the long-range match search that
//! makes pairing pay off at all. Measured on the 1.4 MB container above,
//! `--fast=3` produced 289 651 bytes where level 3 produced 132 552.
//!
//! The window is `clamp(ceil(log2(input_len)), 23, 27)`, and it is set
//! explicitly because an explicit value overrides whatever the level would pick:
//! a level whose default window is small would stop seeing the duplicated body,
//! which is the only reason the two halves share a frame.
//!
//! # Browser decoding
//!
//! Frames whose window is at most [`BROWSER_DECODABLE_MAX_BYTES`] (8 MiB) can be
//! handed to a browser as `Content-Encoding: zstd` and decompressed there. That
//! 8 MiB is a hard limit: RFC 9659 has decoders support no more than 8 MB of
//! window, Chromium refuses larger frames with `ERR_ZSTD_WINDOW_SIZE_TOO_BIG`,
//! and `DecompressionStream('zstd')` is not available in any browser, so the
//! HTTP content coding is the only browser-side option. Inputs up to 8 MiB need
//! a window log of at most 23, which is the floor below, so those frames always
//! declare a window no larger than the input itself and fit that limit. Larger
//! inputs declare a larger window and are decoded on the server instead.

use std::io::Write;

use aether_data_contracts::repository::usage::{UsageBodyField, MAX_USAGE_BODY_BYTES};
use aether_data_contracts::DataLayerError;
use serde::ser::SerializeMap;
use serde::Serializer;
use serde_json::{Map, Value};

// The encoding values reach clients as `x-aether-body-encoding`, so they are
// defined with the rest of the usage body contract and re-exported here for the
// codec's own vocabulary.
pub use aether_data_contracts::repository::usage::{
    ENCODING_RAW_CONTAINER, ENCODING_RAW_SINGLE, ENCODING_ZSTD_CONTAINER,
};

/// zstd compression level for stored containers.
///
/// Positive on purpose: negative levels select zstd's fast mode, which loses the
/// long-range matches that pairing depends on (see the module docs).
pub const ZSTD_LEVEL: i32 = 12;

// The sign is part of the format's correctness, not a tuning knob, so it is
// checked at compile time rather than left to a test to notice.
const _: () = assert!(ZSTD_LEVEL > 0);

/// Smallest window log, i.e. 2^23 bytes: the largest window a browser decodes.
pub const ZSTD_WINDOW_LOG_MIN: u32 = 23;
/// Largest window log, i.e. 2^27 bytes: bounds how much memory compression uses.
pub const ZSTD_WINDOW_LOG_MAX: u32 = 27;

/// Largest window that can be handed to a browser to decompress.
pub const BROWSER_DECODABLE_MAX_BYTES: u64 = 8 * 1024 * 1024;

/// Largest container the read path will decode.
///
/// A container holds both bodies of a pair, each already capped at
/// [`MAX_USAGE_BODY_BYTES`] by the capture path, plus container framing.
pub const MAX_DECODABLE_CONTAINER_BYTES: u64 =
    2 * MAX_USAGE_BODY_BYTES as u64 + CONTAINER_FRAMING_BYTES;

/// Room for the container's keys and punctuation, well above the real ~40 bytes.
const CONTAINER_FRAMING_BYTES: u64 = 64 * 1024;

/// The two capture fields a stored row pairs up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BodyPair {
    base: UsageBodyField,
    sibling: UsageBodyField,
}

impl BodyPair {
    /// The pair a capture field belongs to, whichever half of it that field is.
    pub fn for_field(field: UsageBodyField) -> Self {
        match field {
            UsageBodyField::RequestBody | UsageBodyField::ProviderRequestBody => Self {
                base: UsageBodyField::RequestBody,
                sibling: UsageBodyField::ProviderRequestBody,
            },
            UsageBodyField::ResponseBody | UsageBodyField::ClientResponseBody => Self {
                base: UsageBodyField::ResponseBody,
                sibling: UsageBodyField::ClientResponseBody,
            },
        }
    }

    /// The field the row is keyed by.
    pub fn base(self) -> UsageBodyField {
        self.base
    }

    /// The other half of the pair.
    pub fn sibling(self) -> UsageBodyField {
        self.sibling
    }

    /// Both fields, base first.
    pub fn fields(self) -> [UsageBodyField; 2] {
        [self.base, self.sibling]
    }
}

/// A payload ready to store, with the metadata that describes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedBody {
    pub payload: Vec<u8>,
    pub encoding: i16,
    pub logical_bytes: i64,
}

/// Builds the container for one stored row.
///
/// A body that was not captured becomes `null` rather than a missing key, so a
/// container always describes both halves of its pair.
pub fn build_container(
    pair: BodyPair,
    base_body: Option<&Value>,
    sibling_body: Option<&Value>,
) -> Value {
    let mut container = Map::with_capacity(2);
    container.insert(
        pair.base().as_capture_key().to_string(),
        base_body.cloned().unwrap_or(Value::Null),
    );
    container.insert(
        pair.sibling().as_capture_key().to_string(),
        sibling_body.cloned().unwrap_or(Value::Null),
    );
    Value::Object(container)
}

/// Compresses a serialized container into the payload to store.
///
/// The write path serializes a container while preparing a record and compresses
/// it afterwards, so the two steps are separate here as well.
pub fn encode_container_bytes(logical: &[u8]) -> Result<EncodedBody, DataLayerError> {
    Ok(EncodedBody {
        payload: compress(logical)?,
        encoding: ENCODING_ZSTD_CONTAINER,
        logical_bytes: logical.len() as i64,
    })
}

/// A serialized container, stored without compressing it.
///
/// This is what the write path keeps when compression fails: it keeps both bodies
/// instead of dropping one, at the cost of not compressing them.
pub fn raw_container_bytes(logical: Vec<u8>) -> EncodedBody {
    EncodedBody {
        logical_bytes: logical.len() as i64,
        payload: logical,
        encoding: ENCODING_RAW_CONTAINER,
    }
}

/// Whether payloads with this `encoding` are pair containers.
pub fn is_container(encoding: i16) -> bool {
    matches!(encoding, ENCODING_ZSTD_CONTAINER | ENCODING_RAW_CONTAINER)
}

/// Decodes a container payload.
pub fn decode_container(encoding: i16, payload: &[u8]) -> Result<Value, DataLayerError> {
    match encoding {
        ENCODING_ZSTD_CONTAINER => parse(&decompress(payload)?),
        ENCODING_RAW_CONTAINER => parse(payload),
        ENCODING_RAW_SINGLE => Err(DataLayerError::UnexpectedValue(
            "a raw single usage body is not a pair container".to_string(),
        )),
        other => Err(DataLayerError::UnexpectedValue(format!(
            "usage body encoding {other} is not a pair container"
        ))),
    }
}

/// The body `field` carries inside a decoded container.
pub fn container_body(container: &Value, field: UsageBodyField) -> Option<Value> {
    container
        .as_object()?
        .get(field.as_capture_key())
        .filter(|body| !body.is_null())
        .cloned()
}

/// The window log to compress `len` bytes with.
pub fn window_log_for_len(len: u64) -> u32 {
    // ceil(log2(len)), so the window spans the whole input whenever one can.
    let needed = 64 - len.saturating_sub(1).leading_zeros();
    needed.clamp(ZSTD_WINDOW_LOG_MIN, ZSTD_WINDOW_LOG_MAX)
}

/// Serializes the container for a pair straight from references.
///
/// Preparing a record must not copy its bodies, so the container is written
/// without ever materializing it as a [`Value`]. Keys are written base field
/// first, the order the format documents.
pub fn serialize_pair_container(
    pair: BodyPair,
    base_body: Option<&Value>,
    sibling_body: Option<&Value>,
) -> Result<Vec<u8>, DataLayerError> {
    const ABSENT: Value = Value::Null;
    let mut serializer = serde_json::Serializer::new(Vec::new());
    {
        let mut map = serializer
            .serialize_map(Some(2))
            .map_err(|error| container_error("serialize", error))?;
        map.serialize_entry(pair.base().as_capture_key(), base_body.unwrap_or(&ABSENT))
            .map_err(|error| container_error("serialize", error))?;
        map.serialize_entry(
            pair.sibling().as_capture_key(),
            sibling_body.unwrap_or(&ABSENT),
        )
        .map_err(|error| container_error("serialize", error))?;
        map.end()
            .map_err(|error| container_error("serialize", error))?;
    }
    Ok(serializer.into_inner())
}

/// Serializes a container into the bytes that are stored, compressed or not.
pub fn serialize_container(container: &Value) -> Result<Vec<u8>, DataLayerError> {
    serde_json::to_vec(container).map_err(|error| {
        DataLayerError::UnexpectedValue(format!(
            "failed to serialize usage body container: {error}"
        ))
    })
}

fn parse(bytes: &[u8]) -> Result<Value, DataLayerError> {
    serde_json::from_slice(bytes).map_err(|error| {
        DataLayerError::UnexpectedValue(format!("failed to parse usage body container: {error}"))
    })
}

fn container_error(action: &str, error: serde_json::Error) -> DataLayerError {
    DataLayerError::UnexpectedValue(format!("failed to {action} usage body container: {error}"))
}

fn compress(input: &[u8]) -> Result<Vec<u8>, DataLayerError> {
    let mut encoder = zstd::stream::write::Encoder::new(Vec::new(), ZSTD_LEVEL)
        .map_err(|error| frame_error("initialize", error))?;
    encoder
        .set_parameter(zstd::zstd_safe::CParameter::WindowLog(window_log_for_len(
            input.len() as u64,
        )))
        .map_err(|error| frame_error("set the window of", error))?;
    encoder
        .include_checksum(true)
        .map_err(|error| frame_error("enable the checksum of", error))?;
    encoder
        .set_pledged_src_size(Some(input.len() as u64))
        .map_err(|error| frame_error("pledge the size of", error))?;
    encoder
        .write_all(input)
        .map_err(|error| frame_error("write", error))?;
    encoder
        .finish()
        .map_err(|error| frame_error("finish", error))
}

fn decompress(payload: &[u8]) -> Result<Vec<u8>, DataLayerError> {
    zstd::stream::decode_all(payload).map_err(|error| frame_error("decompress", error))
}

fn frame_error(action: &str, error: std::io::Error) -> DataLayerError {
    DataLayerError::UnexpectedValue(format!("failed to {action} usage body frame: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

    /// The frame header fields the read path's browser decision depends on.
    struct FrameHeader {
        single_segment: bool,
        checksum: bool,
        window_size: u64,
    }

    fn frame_header(payload: &[u8]) -> FrameHeader {
        assert_eq!(&payload[..4], ZSTD_MAGIC, "expected a zstd frame");
        let descriptor = payload[4];
        let single_segment = descriptor & 0b0010_0000 != 0;
        assert_eq!(
            descriptor & 0b0000_0011,
            0,
            "frames must not reference a dictionary"
        );
        let cursor = 5;
        let window_size = if single_segment {
            // Window_Size is the frame content size when there is one segment.
            frame_content_size(descriptor >> 6, &payload[cursor..]).0
        } else {
            let window_descriptor = payload[cursor];
            let base = 1u64 << (10 + u32::from(window_descriptor >> 3));
            base + (base / 8) * u64::from(window_descriptor & 0b0000_0111)
        };
        FrameHeader {
            single_segment,
            checksum: descriptor & 0b0000_0100 != 0,
            window_size,
        }
    }

    fn frame_content_size(flag: u8, bytes: &[u8]) -> (u64, usize) {
        match flag {
            0 => (u64::from(bytes[0]), 1),
            1 => (u64::from(u16::from_le_bytes([bytes[0], bytes[1]])) + 256, 2),
            2 => (
                u64::from(u32::from_le_bytes(bytes[..4].try_into().unwrap())),
                4,
            ),
            _ => (u64::from_le_bytes(bytes[..8].try_into().unwrap()), 8),
        }
    }

    fn request_pair() -> BodyPair {
        BodyPair::for_field(UsageBodyField::RequestBody)
    }

    /// Compresses a container the way the write path does.
    fn encode(container: &Value) -> EncodedBody {
        encode_container_bytes(&serialize_container(container).expect("container should serialize"))
            .expect("container should encode")
    }

    /// Stores a container without compressing it.
    fn encode_raw(container: &Value) -> EncodedBody {
        raw_container_bytes(serialize_container(container).expect("container should serialize"))
    }

    fn round_trip(encoded: &EncodedBody) -> Value {
        decode_container(encoded.encoding, &encoded.payload).expect("container should decode")
    }

    #[test]
    fn encoding_values_are_a_stable_contract() {
        // The value reaches clients as `x-aether-body-encoding`, so it is part of
        // the admin API surface rather than an internal detail.
        assert_eq!(ENCODING_RAW_SINGLE, 0);
        assert_eq!(ENCODING_ZSTD_CONTAINER, 1);
        assert_eq!(ENCODING_RAW_CONTAINER, 2);
    }

    #[test]
    fn pairs_group_one_direction_at_a_time() {
        for (field, base, sibling) in [
            (
                UsageBodyField::RequestBody,
                UsageBodyField::RequestBody,
                UsageBodyField::ProviderRequestBody,
            ),
            (
                UsageBodyField::ProviderRequestBody,
                UsageBodyField::RequestBody,
                UsageBodyField::ProviderRequestBody,
            ),
            (
                UsageBodyField::ResponseBody,
                UsageBodyField::ResponseBody,
                UsageBodyField::ClientResponseBody,
            ),
            (
                UsageBodyField::ClientResponseBody,
                UsageBodyField::ResponseBody,
                UsageBodyField::ClientResponseBody,
            ),
        ] {
            let pair = BodyPair::for_field(field);
            assert_eq!(pair.base(), base, "{field:?}");
            assert_eq!(pair.sibling(), sibling, "{field:?}");
            assert_eq!(pair.fields(), [base, sibling], "{field:?}");
        }
    }

    #[test]
    fn containers_round_trip_both_bodies() {
        let pair = request_pair();
        let base = json!({"model": "gpt-5", "messages": [{"role": "user", "content": "hi"}]});
        let sibling = json!({"model": "gpt-5.4", "messages": [{"role": "user", "content": "hi"}]});
        let container = build_container(pair, Some(&base), Some(&sibling));

        let encoded = encode(&container);
        assert_eq!(encoded.encoding, ENCODING_ZSTD_CONTAINER);
        assert!(encoded.logical_bytes > 0);

        let decoded = round_trip(&encoded);
        assert_eq!(decoded, container);
        assert_eq!(
            container_body(&decoded, UsageBodyField::RequestBody),
            Some(base)
        );
        assert_eq!(
            container_body(&decoded, UsageBodyField::ProviderRequestBody),
            Some(sibling)
        );
    }

    #[test]
    fn containers_mark_a_missing_half_null() {
        let pair = request_pair();
        let base = json!({"model": "gpt-5"});
        let container = build_container(pair, Some(&base), None);
        assert_eq!(container["provider_request"], Value::Null);

        let decoded = round_trip(&encode(&container));
        assert_eq!(
            container_body(&decoded, UsageBodyField::RequestBody),
            Some(base)
        );
        assert_eq!(
            container_body(&decoded, UsageBodyField::ProviderRequestBody),
            None
        );
    }

    #[test]
    fn raw_containers_are_stored_uncompressed() {
        let container = build_container(request_pair(), Some(&json!({"model": "gpt-5"})), None);
        let encoded = encode_raw(&container);

        assert_eq!(encoded.encoding, ENCODING_RAW_CONTAINER);
        assert_ne!(
            &encoded.payload[..4],
            ZSTD_MAGIC,
            "the fallback must not claim to be a frame"
        );
        assert_eq!(encoded.logical_bytes as usize, encoded.payload.len());
        assert_eq!(round_trip(&encoded), container);
    }

    #[test]
    fn only_containers_are_decoded_as_containers() {
        assert!(!is_container(ENCODING_RAW_SINGLE));
        assert!(is_container(ENCODING_ZSTD_CONTAINER));
        assert!(is_container(ENCODING_RAW_CONTAINER));
        assert!(decode_container(ENCODING_RAW_SINGLE, b"{}").is_err());
        assert!(decode_container(9, b"{}").is_err());
    }

    #[test]
    fn window_logs_stay_inside_the_configured_range() {
        assert_eq!(window_log_for_len(0), ZSTD_WINDOW_LOG_MIN);
        assert_eq!(window_log_for_len(1), ZSTD_WINDOW_LOG_MIN);
        assert_eq!(window_log_for_len(7 * 1024 * 1024), ZSTD_WINDOW_LOG_MIN);
        assert_eq!(
            window_log_for_len(BROWSER_DECODABLE_MAX_BYTES),
            ZSTD_WINDOW_LOG_MIN
        );
        assert_eq!(
            window_log_for_len(BROWSER_DECODABLE_MAX_BYTES + 1),
            24,
            "one byte past 8 MiB needs a window a browser will not accept"
        );
        assert_eq!(window_log_for_len(u64::MAX), ZSTD_WINDOW_LOG_MAX);
    }

    #[test]
    fn small_frames_declare_a_browser_sized_window() {
        let body = json!({"model": "gpt-5", "messages": [{"role": "user", "content": "hello"}]});
        let container = build_container(request_pair(), Some(&body), Some(&body));
        let encoded = encode(&container);

        let header = frame_header(&encoded.payload);
        assert!(header.checksum, "frames must carry a checksum");
        assert!(
            header.single_segment,
            "a small container fits into one segment"
        );
        assert_eq!(header.window_size, encoded.logical_bytes as u64);
        assert!(
            header.window_size <= BROWSER_DECODABLE_MAX_BYTES,
            "a browser must be able to decode this frame"
        );
    }

    #[test]
    fn large_frames_exceed_what_a_browser_decodes() {
        // 6 MiB per body, so the container is past the 8 MiB window limit.
        let body = json!({"content": "abcdefgh".repeat(768 * 1024)});
        let container = build_container(request_pair(), Some(&body), Some(&body));
        let encoded = encode(&container);
        assert!(encoded.logical_bytes as u64 > BROWSER_DECODABLE_MAX_BYTES);

        let header = frame_header(&encoded.payload);
        assert!(
            header.window_size > BROWSER_DECODABLE_MAX_BYTES,
            "this frame has to be decoded on the server: window {}",
            header.window_size
        );
        assert!(header.window_size <= 1u64 << ZSTD_WINDOW_LOG_MAX);
        assert_eq!(round_trip(&encoded), container);
    }

    #[test]
    fn corrupt_frames_are_rejected() {
        let container = build_container(request_pair(), Some(&json!({"model": "gpt-5"})), None);
        let mut encoded = encode(&container);
        let last = encoded.payload.len() - 1;
        encoded.payload[last] ^= 0xFF;

        let error = decode_container(encoded.encoding, &encoded.payload)
            .expect_err("a damaged frame must not decode");
        assert!(format!("{error}").contains("usage body frame"), "{error}");
    }

    #[test]
    fn the_decode_limit_covers_two_capped_bodies() {
        assert!(MAX_DECODABLE_CONTAINER_BYTES >= 2 * MAX_USAGE_BODY_BYTES as u64);
    }

    #[test]
    fn one_frame_beats_two_independent_frames() {
        // The reason the format exists: one frame sees the duplication between the
        // two halves, two frames cannot. The prompt has to be hard to compress on
        // its own, otherwise each frame collapses to its own overhead and the
        // duplication stops mattering. Real captured bodies look like this, and
        // this case measures 168 614 bytes against 337 157 for two frames, i.e.
        // the second copy of a pair costs almost nothing.
        let prompt: String = (0..20_000u64)
            .map(|index| format!("{:x} ", index.wrapping_mul(0x9E37_79B9_7F4A_7C15)))
            .collect();
        let base = json!({"model": "gpt-5", "messages": [{"role": "user", "content": prompt}]});
        let sibling =
            json!({"model": "gpt-5.4", "messages": [{"role": "user", "content": prompt}]});
        let pair = request_pair();

        let paired = encode(&build_container(pair, Some(&base), Some(&sibling)));
        let base_only = encode(&build_container(pair, Some(&base), None));
        let sibling_only = encode(&build_container(pair, Some(&sibling), None));
        let independent = base_only.payload.len() + sibling_only.payload.len();

        assert!(
            paired.payload.len() * 10 < independent * 6,
            "paired frame ({}) should beat two independent frames ({independent})",
            paired.payload.len(),
        );
    }
}
