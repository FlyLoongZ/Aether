/// RFC 2046 limits a multipart boundary to at most 70 characters.  Using
/// HTTP token syntax here also keeps the value safe to embed in the byte
/// delimiter used by the lightweight parsers.
pub const MAX_MULTIPART_BOUNDARY_BYTES: usize = 70;
/// Keep multipart metadata bounded independently of the file payload size.
pub const MAX_MULTIPART_PARTS: usize = 128;
pub const MAX_MULTIPART_PART_HEADER_BYTES: usize = 64 * 1024;

/// Find a multipart delimiter at the beginning of a buffer or after CRLF.
/// The returned index points at the delimiter itself (not the preceding CRLF).
pub fn find_multipart_boundary(haystack: &[u8], delimiter: &[u8]) -> Option<usize> {
    find_multipart_boundary_inner(haystack, delimiter, true)
}

/// Find a multipart delimiter that is preceded by CRLF.  This variant is used
/// while scanning a part payload, where an apparent delimiter at byte zero is
/// payload data rather than a valid framing boundary.
pub fn find_multipart_boundary_after_crlf(haystack: &[u8], delimiter: &[u8]) -> Option<usize> {
    find_multipart_boundary_inner(haystack, delimiter, false)
}

fn find_multipart_boundary_inner(
    haystack: &[u8],
    delimiter: &[u8],
    allow_start: bool,
) -> Option<usize> {
    if delimiter.is_empty() {
        return None;
    }
    if allow_start && multipart_boundary_is_valid_at(haystack, 0, delimiter) {
        return Some(0);
    }

    let window_len = delimiter.len().checked_add(2)?;
    haystack
        .windows(window_len)
        .enumerate()
        .find_map(|(index, window)| {
            if &window[..2] != b"\r\n" || &window[2..] != delimiter {
                return None;
            }
            let delimiter_index = index + 2;
            multipart_boundary_is_valid_at(haystack, delimiter_index, delimiter)
                .then_some(delimiter_index)
        })
}

fn multipart_boundary_is_valid_at(haystack: &[u8], index: usize, delimiter: &[u8]) -> bool {
    let suffix_start = index.checked_add(delimiter.len());
    let Some(suffix_start) = suffix_start else {
        return false;
    };
    if !haystack
        .get(index..)
        .is_some_and(|remaining| remaining.starts_with(delimiter))
    {
        return false;
    }
    let Some(suffix) = haystack.get(suffix_start..) else {
        return false;
    };
    if suffix.starts_with(b"\r\n") {
        return true;
    }
    suffix
        .strip_prefix(b"--")
        .is_some_and(|remaining| remaining.is_empty() || remaining.starts_with(b"\r\n"))
}

/// Extract and validate a multipart boundary parameter.
///
/// The parameter may use the usual optional surrounding quotes, but the
/// boundary value itself must be an ASCII HTTP token.  Rejecting malformed
/// values at the content-type boundary prevents parser ambiguity and bounds
/// the work performed by downstream delimiter scans.
pub fn parse_multipart_boundary(content_type: &str) -> Option<String> {
    let segments = split_multipart_header_parameters(content_type)?;
    let media_type = segments.first()?.trim();
    if !media_type.eq_ignore_ascii_case("multipart/form-data") {
        return None;
    }

    let mut boundary = None;
    let mut seen_keys = Vec::new();
    for segment in segments.into_iter().skip(1) {
        let segment = segment.trim();
        if segment.is_empty() {
            return None;
        }
        let (raw_key, raw_value) = segment.split_once('=')?;
        let key = raw_key.trim();
        if key.is_empty() || !key.as_bytes().iter().copied().all(is_http_token_byte) {
            return None;
        }
        if seen_keys
            .iter()
            .any(|seen: &String| seen.eq_ignore_ascii_case(key))
        {
            return None;
        }
        seen_keys.push(key.to_ascii_lowercase());

        let (value, had_escape) = parse_multipart_parameter_value(raw_value.trim())?;
        if !key.eq_ignore_ascii_case("boundary") {
            continue;
        }
        // Keep boundary parsing deliberately narrower than generic quoted
        // parameter parsing: escaped boundary values are ambiguous across
        // HTTP stacks and are rejected here.
        if had_escape {
            return None;
        }
        if !is_valid_multipart_boundary(&value) {
            return None;
        }
        boundary = Some(value);
    }

    boundary
}

/// Split a semicolon-delimited HTTP header while honoring quoted strings.
/// Returning `None` for an unterminated quote or escape prevents a malformed
/// parameter from being reinterpreted by a downstream parser.
fn split_multipart_header_parameters(value: &str) -> Option<Vec<&str>> {
    let mut segments = Vec::new();
    let mut start = 0usize;
    let mut in_quotes = false;
    let mut escaped = false;

    for (index, character) in value.char_indices() {
        if character.is_ascii_control() {
            return None;
        }
        if in_quotes {
            if escaped {
                escaped = false;
            } else if character == '\\' {
                escaped = true;
            } else if character == '"' {
                in_quotes = false;
            }
        } else if character == '"' {
            in_quotes = true;
        } else if character == ';' {
            segments.push(&value[start..index]);
            start = index + character.len_utf8();
        }
    }

    if in_quotes || escaped {
        return None;
    }
    segments.push(&value[start..]);
    Some(segments)
}

fn parse_multipart_parameter_value(value: &str) -> Option<(String, bool)> {
    if value.is_empty() {
        return None;
    }
    if value.starts_with('"') {
        if value.len() < 2 || !value.ends_with('"') {
            return None;
        }
        let inner = &value[1..value.len() - 1];
        let mut parsed = String::with_capacity(inner.len());
        let mut escaped = false;
        let mut had_escape = false;
        for character in inner.chars() {
            if escaped {
                if character.is_ascii_control() {
                    return None;
                }
                parsed.push(character);
                escaped = false;
                had_escape = true;
            } else if character == '\\' {
                escaped = true;
            } else {
                if character == '"' || character.is_ascii_control() {
                    return None;
                }
                parsed.push(character);
            }
        }
        if escaped {
            return None;
        }
        return Some((parsed, had_escape));
    }

    value
        .as_bytes()
        .iter()
        .copied()
        .all(is_http_token_byte)
        .then(|| (value.to_string(), false))
}

fn is_valid_multipart_boundary(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_MULTIPART_BOUNDARY_BYTES
        && value.as_bytes().iter().copied().all(is_http_token_byte)
}

fn is_http_token_byte(byte: u8) -> bool {
    matches!(
        byte,
        b'0'..=b'9'
            | b'A'..=b'Z'
            | b'a'..=b'z'
            | b'!'
            | b'#'
            | b'$'
            | b'%'
            | b'&'
            | b'\''
            | b'*'
            | b'+'
            | b'-'
            | b'.'
            | b'^'
            | b'_'
            | b'`'
            | b'|'
            | b'~'
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultipartField {
    pub name: String,
    pub content_type: Option<String>,
    pub data: Vec<u8>,
}

/// Parse a bounded `multipart/form-data` body into fields.
///
/// The parser returns an empty vector on malformed, truncated, or oversized
/// input so callers can fail closed without observing a partially parsed body.
pub fn parse_multipart_fields(body: &[u8], boundary: &str) -> Vec<MultipartField> {
    let delimiter = format!("--{boundary}").into_bytes();
    let mut parts = Vec::new();
    let mut cursor = 0usize;
    let mut part_count = 0usize;

    while let Some(index) = find_multipart_boundary(&body[cursor..], &delimiter) {
        let start = cursor + index + delimiter.len();
        if body.get(start..start + 2) == Some(b"--") {
            let closing_suffix = body.get(start + 2..).unwrap_or_default();
            if !(closing_suffix.is_empty() || closing_suffix.starts_with(b"\r\n")) {
                return Vec::new();
            }
            break;
        }
        part_count = part_count.saturating_add(1);
        if part_count > MAX_MULTIPART_PARTS {
            return Vec::new();
        }
        let mut part = &body[start..];
        if part.starts_with(b"\r\n") {
            part = &part[2..];
        }
        let Some(next) = find_multipart_boundary_after_crlf(part, &delimiter) else {
            return Vec::new();
        };
        let raw = &part[..next];
        let raw = raw.strip_suffix(b"\r\n").unwrap_or(raw);
        if find_subslice(raw, b"\r\n\r\n")
            .is_some_and(|header_end| header_end > MAX_MULTIPART_PART_HEADER_BYTES)
        {
            return Vec::new();
        }
        let Some(field) = parse_multipart_field(raw) else {
            return Vec::new();
        };
        parts.push(field);
        cursor = start + next;
    }

    parts
}

fn parse_multipart_field(raw: &[u8]) -> Option<MultipartField> {
    let header_end = find_subslice(raw, b"\r\n\r\n")?;
    let headers = &raw[..header_end];
    let data = raw.get(header_end + 4..)?.to_vec();
    let header_text = std::str::from_utf8(headers).ok()?;

    let mut name = None;
    let mut content_type = None;
    let mut disposition_seen = false;
    let mut content_type_seen = false;
    for line in header_text.split("\r\n") {
        let (header_name, header_value) = line.split_once(':')?;
        let header_name = header_name.trim();
        let header_value = header_value.trim();
        if header_name.eq_ignore_ascii_case("content-disposition") {
            if disposition_seen {
                return None;
            }
            disposition_seen = true;
            name = parse_multipart_content_disposition_name(header_value);
        } else if header_name.eq_ignore_ascii_case("content-type") {
            if content_type_seen || header_value.is_empty() {
                return None;
            }
            content_type_seen = true;
            content_type = Some(header_value.to_string());
        }
    }

    Some(MultipartField {
        name: name?,
        content_type,
        data,
    })
}

fn parse_multipart_content_disposition_name(value: &str) -> Option<String> {
    let segments = split_multipart_header_parameters(value)?;
    let disposition = segments.first()?.trim();
    if !disposition.eq_ignore_ascii_case("form-data") {
        return None;
    }

    let mut seen_keys = Vec::new();
    let mut name = None;
    for segment in segments.into_iter().skip(1) {
        let segment = segment.trim();
        if segment.is_empty() {
            return None;
        }
        let (raw_key, raw_value) = segment.split_once('=')?;
        let key = raw_key.trim();
        if key.is_empty() || !key.as_bytes().iter().copied().all(is_http_token_byte) {
            return None;
        }
        if seen_keys
            .iter()
            .any(|seen: &String| seen.eq_ignore_ascii_case(key))
        {
            return None;
        }
        seen_keys.push(key.to_ascii_lowercase());

        let parsed_value = parse_multipart_parameter_value(raw_value.trim())?.0;
        if key.eq_ignore_ascii_case("name") {
            if parsed_value.is_empty() {
                return None;
            }
            name = Some(parsed_value);
        }
    }

    name
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || haystack.len() < needle.len() {
        return None;
    }
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

#[cfg(test)]
mod tests {
    use super::{
        find_multipart_boundary, find_multipart_boundary_after_crlf, parse_multipart_boundary,
        parse_multipart_fields, MAX_MULTIPART_BOUNDARY_BYTES, MAX_MULTIPART_PARTS,
        MAX_MULTIPART_PART_HEADER_BYTES,
    };

    #[test]
    fn accepts_token_boundary_and_optional_quotes() {
        assert_eq!(
            parse_multipart_boundary("Multipart/Form-Data; boundary=----WebKitFormBoundaryabc123")
                .as_deref(),
            Some("----WebKitFormBoundaryabc123")
        );
        assert_eq!(
            parse_multipart_boundary("multipart/form-data; boundary=\"quoted-boundary\"")
                .as_deref(),
            Some("quoted-boundary")
        );
    }

    #[test]
    fn rejects_non_token_control_quote_and_oversized_boundaries() {
        for content_type in [
            "multipart/form-data; boundary=",
            "multipart/form-data; boundary=bad boundary",
            "multipart/form-data; boundary=\"bad;boundary\"",
            "multipart/form-data; boundary=bad\r\nvalue",
            "multipart/form-data; boundary=bad\"quote",
            "multipart/form-data; boundary=\"unterminated",
            "multipart/form-data; foo",
            "multipart/form-data; foo=\"unterminated; boundary=valid",
            "multipart/form-data; boundary=valid trailing",
        ] {
            assert!(
                parse_multipart_boundary(content_type).is_none(),
                "{content_type:?}"
            );
        }

        let oversized = "a".repeat(MAX_MULTIPART_BOUNDARY_BYTES + 1);
        assert!(
            parse_multipart_boundary(&format!("multipart/form-data; boundary={oversized}"))
                .is_none()
        );
    }

    #[test]
    fn rejects_duplicate_boundary_parameters() {
        for content_type in [
            "multipart/form-data; boundary=first; boundary=second",
            "multipart/form-data; boundary=first; BOUNDARY=second",
        ] {
            assert!(
                parse_multipart_boundary(content_type).is_none(),
                "duplicate boundary parameters must be rejected: {content_type}"
            );
        }
    }

    #[test]
    fn accepts_quoted_unknown_parameters_with_semicolons() {
        assert_eq!(
            parse_multipart_boundary(
                "multipart/form-data; note=\"semi;colon\"; boundary=quoted-token"
            )
            .as_deref(),
            Some("quoted-token")
        );
    }

    #[test]
    fn rejects_escaped_boundary_and_duplicate_unknown_parameters() {
        for content_type in [
            "multipart/form-data; boundary=\"escaped\\\"token\"",
            "multipart/form-data; note=one; NOTE=two; boundary=token",
            "multipart/form-data; note=\"unterminated; boundary=token",
            "multipart/form-data; note=\"closed\"trailing; boundary=token",
        ] {
            assert!(
                parse_multipart_boundary(content_type).is_none(),
                "malformed content type must be rejected: {content_type:?}"
            );
        }
    }

    #[test]
    fn rejects_non_multipart_media_types() {
        assert!(parse_multipart_boundary("application/json; boundary=abc").is_none());
        assert!(parse_multipart_boundary("x-multipart/form-data; boundary=abc").is_none());
    }

    #[test]
    fn multipart_metadata_limits_remain_bounded() {
        assert_eq!(MAX_MULTIPART_PARTS, 128);
        assert_eq!(MAX_MULTIPART_PART_HEADER_BYTES, 64 * 1024);
    }

    #[test]
    fn boundary_scanner_ignores_embedded_markers_and_invalid_suffixes() {
        let delimiter = b"--boundary";
        let payload = b"prefix\r\n--boundaryX\r\nmore\r\n--boundary\r\n";
        assert_eq!(
            find_multipart_boundary_after_crlf(payload, delimiter),
            Some(payload.len() - delimiter.len() - 2)
        );
        assert_eq!(
            find_multipart_boundary(b"payload--boundary\r\n", delimiter),
            None
        );
        assert_eq!(find_multipart_boundary(b"--boundaryX\r\n", delimiter), None);
        assert_eq!(
            find_multipart_boundary_after_crlf(b"payload\r\n--boundaryX\r\n", delimiter),
            None
        );
    }

    #[test]
    fn multipart_parser_caps_part_count_and_header_size() {
        let boundary = "bounded-parts";
        let mut accepted_body = Vec::new();
        for index in 0..MAX_MULTIPART_PARTS {
            accepted_body.extend_from_slice(
                format!(
                    "--{boundary}\r\nContent-Disposition: form-data; name=\"field-{index}\"\r\n\r\nvalue\r\n"
                )
                .as_bytes(),
            );
        }
        accepted_body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
        assert_eq!(
            parse_multipart_fields(&accepted_body, boundary).len(),
            MAX_MULTIPART_PARTS
        );

        let mut rejected_body = Vec::new();
        for index in 0..=MAX_MULTIPART_PARTS {
            rejected_body.extend_from_slice(
                format!(
                    "--{boundary}\r\nContent-Disposition: form-data; name=\"field-{index}\"\r\n\r\nvalue\r\n"
                )
                .as_bytes(),
            );
        }
        rejected_body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
        assert!(parse_multipart_fields(&rejected_body, boundary).is_empty());

        let mut oversized_header =
            format!("--{boundary}\r\nContent-Disposition: form-data; name=\"prompt\"\r\nX-Fill: ")
                .into_bytes();
        oversized_header.extend(std::iter::repeat_n(b'x', MAX_MULTIPART_PART_HEADER_BYTES));
        oversized_header.extend_from_slice(b"\r\n\r\nvalue\r\n");
        oversized_header.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
        assert!(parse_multipart_fields(&oversized_header, boundary).is_empty());
    }

    #[test]
    fn multipart_parser_does_not_extract_name_from_filename_and_rejects_duplicates() {
        let boundary = "header-parameters";
        let filename_only = format!(
            concat!(
                "--{boundary}\r\n",
                "Content-Disposition: form-data; filename=\"name=\\\"prompt\\\"\"\r\n\r\n",
                "attacker-value\r\n",
                "--{boundary}--\r\n"
            ),
            boundary = boundary,
        );
        assert!(parse_multipart_fields(filename_only.as_bytes(), boundary).is_empty());

        let duplicate_name = format!(
            concat!(
                "--{boundary}\r\n",
                "Content-Disposition: form-data; name=\"prompt\"; name=\"other\"\r\n\r\n",
                "value\r\n",
                "--{boundary}--\r\n"
            ),
            boundary = boundary,
        );
        assert!(parse_multipart_fields(duplicate_name.as_bytes(), boundary).is_empty());
    }

    #[test]
    fn multipart_parser_preserves_boundary_like_payload_and_fails_closed() {
        let boundary = "payload-boundary";
        let body = format!(
            concat!(
                "--{boundary}\r\n",
                "Content-Disposition: form-data; name=\"prompt\"\r\n\r\n",
                "prefix\r\n--{boundary}X\r\nsuffix--{boundary}\r\n",
                "--{boundary}--\r\n"
            ),
            boundary = boundary,
        );
        let fields = parse_multipart_fields(body.as_bytes(), boundary);
        assert_eq!(fields.len(), 1);
        assert_eq!(
            String::from_utf8_lossy(&fields[0].data),
            format!("prefix\r\n--{boundary}X\r\nsuffix--{boundary}")
        );

        let truncated =
            format!("--{boundary}\r\nContent-Disposition: form-data; name=\"prompt\"\r\n\r\nvalue");
        assert!(parse_multipart_fields(truncated.as_bytes(), boundary).is_empty());
    }

    #[test]
    fn multipart_parser_rejects_garbage_after_closing_boundary() {
        let boundary = "closing-suffix";
        let body = format!(
            concat!(
                "--{boundary}\r\n",
                "Content-Disposition: form-data; name=\"prompt\"\r\n\r\n",
                "value\r\n",
                "--{boundary}--junk"
            ),
            boundary = boundary,
        );
        assert!(parse_multipart_fields(body.as_bytes(), boundary).is_empty());
    }

    #[test]
    fn multipart_scanner_skips_short_prefix_before_later_valid_boundary() {
        let delimiter = b"--scanner-boundary";
        let haystack = b"x--scanner-boundary\r\ncontent\r\n--scanner-boundary\r\n";
        assert_eq!(
            find_multipart_boundary_after_crlf(haystack, delimiter),
            Some(haystack.len() - delimiter.len() - 2)
        );
    }
}
