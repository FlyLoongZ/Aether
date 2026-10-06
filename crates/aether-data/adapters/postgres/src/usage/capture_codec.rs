use std::time::{Duration, Instant};

use sha2::{Digest, Sha256};
use similar::{capture_diff_deadline, Algorithm, DiffOp};

use crate::DataLayerError;

use aether_data_contracts::repository::usage::MAX_USAGE_BODY_BYTES;

const DELTA_MAGIC: &[u8; 4] = b"ADL1";
const DELTA_HEADER: usize = 76;
const MAX_OPERATIONS: usize = 262_144;
const MAX_DIFF_INPUT_BYTES: usize = 1024 * 1024;
const DIFF_TIMEOUT: Duration = Duration::from_millis(50);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct UsageBodyStorageBundle {
    pub encoding: i32,
    pub payload: Vec<u8>,
    pub base: Option<Vec<u8>>,
}

fn invalid() -> DataLayerError {
    DataLayerError::UnexpectedValue("invalid usage capture encoding".to_string())
}

fn too_large() -> DataLayerError {
    DataLayerError::InvalidInput("usage capture exceeds the body size limit".to_string())
}

fn digest(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

fn word(bytes: &[u8], offset: usize) -> Result<usize, DataLayerError> {
    let value = bytes.get(offset..offset + 4).ok_or_else(invalid)?;
    Ok(u32::from_le_bytes(value.try_into().map_err(|_| invalid())?) as usize)
}

fn push_word(bytes: &mut Vec<u8>, value: usize) {
    bytes.extend_from_slice(&(value as u32).to_le_bytes());
}

pub fn encode_usage_body_delta(base: &[u8], target: &[u8]) -> Option<Vec<u8>> {
    encode_usage_body_delta_until(base, target, Instant::now() + DIFF_TIMEOUT)
}

fn encode_usage_body_delta_until(base: &[u8], target: &[u8], deadline: Instant) -> Option<Vec<u8>> {
    if target.len() < 256
        || base.len() > MAX_USAGE_BODY_BYTES
        || target.len() > MAX_USAGE_BODY_BYTES
    {
        return None;
    }
    let prefix = base.iter().zip(target).take_while(|(a, b)| a == b).count();
    let suffix = base[prefix..]
        .iter()
        .rev()
        .zip(target[prefix..].iter().rev())
        .take_while(|(a, b)| a == b)
        .count();
    let base_end = base.len() - suffix;
    let target_end = target.len() - suffix;
    if base_end - prefix + target_end - prefix > MAX_DIFF_INPUT_BYTES || Instant::now() >= deadline
    {
        return None;
    }
    let changes = capture_diff_deadline(
        Algorithm::Myers,
        base,
        prefix..base_end,
        target,
        prefix..target_end,
        Some(deadline),
    );
    if Instant::now() >= deadline {
        return None;
    }

    let mut delta = Vec::with_capacity(target.len().min(4096));
    delta.extend_from_slice(DELTA_MAGIC);
    push_word(&mut delta, base.len());
    push_word(&mut delta, target.len());
    delta.extend_from_slice(&digest(base));
    delta.extend_from_slice(&digest(target));

    let mut operations = 0;
    for change in std::iter::once(DiffOp::Equal {
        old_index: 0,
        new_index: 0,
        len: prefix,
    })
    .chain(changes)
    .chain(std::iter::once(DiffOp::Equal {
        old_index: base_end,
        new_index: target_end,
        len: suffix,
    })) {
        match change {
            DiffOp::Equal { old_index, len, .. } if len > 0 => {
                delta.push(0);
                push_word(&mut delta, old_index);
                push_word(&mut delta, len);
            }
            DiffOp::Insert {
                new_index, new_len, ..
            }
            | DiffOp::Replace {
                new_index, new_len, ..
            } if new_len > 0 => {
                if delta.len() + 5 + new_len + 64 >= target.len() {
                    return None;
                }
                delta.push(1);
                push_word(&mut delta, new_len);
                delta.extend_from_slice(&target[new_index..new_index + new_len]);
            }
            _ => continue,
        }
        operations += 1;
        if operations > MAX_OPERATIONS || delta.len() + 64 >= target.len() {
            return None;
        }
    }
    Some(delta)
}

fn restore_usage_body_delta(base: &[u8], delta: &[u8]) -> Result<Vec<u8>, DataLayerError> {
    if delta.len() > MAX_USAGE_BODY_BYTES || base.len() > MAX_USAGE_BODY_BYTES {
        return Err(too_large());
    }
    if delta.len() < DELTA_HEADER || delta.get(..4) != Some(DELTA_MAGIC) {
        return Err(invalid());
    }
    let base_length = word(delta, 4)?;
    let target_length = word(delta, 8)?;
    if target_length > MAX_USAGE_BODY_BYTES {
        return Err(too_large());
    }
    if base_length != base.len() || delta[12..44] != digest(base) {
        return Err(invalid());
    }
    let mut output = Vec::with_capacity(target_length);
    let mut cursor = DELTA_HEADER;
    let mut operations = 0;
    while cursor < delta.len() {
        operations += 1;
        if operations > MAX_OPERATIONS {
            return Err(invalid());
        }
        let opcode = delta[cursor];
        cursor += 1;
        let bytes = match opcode {
            0 => {
                let offset = word(delta, cursor)?;
                let length = word(delta, cursor + 4)?;
                cursor += 8;
                if length == 0 {
                    return Err(invalid());
                }
                base.get(offset..offset.checked_add(length).ok_or_else(invalid)?)
                    .ok_or_else(invalid)?
            }
            1 => {
                let length = word(delta, cursor)?;
                cursor += 4;
                if length == 0 {
                    return Err(invalid());
                }
                let end = cursor.checked_add(length).ok_or_else(invalid)?;
                let bytes = delta.get(cursor..end).ok_or_else(invalid)?;
                cursor = end;
                bytes
            }
            _ => return Err(invalid()),
        };
        if bytes.len() > target_length.saturating_sub(output.len()) {
            return Err(invalid());
        }
        output.extend_from_slice(bytes);
    }
    if output.len() != target_length || delta[44..76] != digest(&output) {
        return Err(invalid());
    }
    Ok(output)
}

impl UsageBodyStorageBundle {
    pub fn raw(payload: Vec<u8>) -> Self {
        Self {
            encoding: 0,
            payload,
            base: None,
        }
    }

    pub fn restore(self) -> Result<Vec<u8>, DataLayerError> {
        match (self.encoding, self.base) {
            (0, None) if self.payload.len() <= MAX_USAGE_BODY_BYTES => Ok(self.payload),
            (0, None) => Err(too_large()),
            (1, Some(base)) => restore_usage_body_delta(&base, &self.payload),
            _ => Err(invalid()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capture_delta_roundtrips_exact_bytes() {
        for seed in 0..32 {
            let base = (0..2000)
                .map(|i| format!("{{\"index\":{i},\"value\":\"{}\"}},", "汉字🌍abc".repeat(8)))
                .collect::<String>();
            let mut target = base.clone().into_bytes();
            let offset = seed * 137;
            target.splice(
                offset..offset + seed,
                b"changed model and header".iter().copied(),
            );
            let delta = encode_usage_body_delta(base.as_bytes(), &target).unwrap();
            assert!(delta.len() < target.len());
            assert_eq!(
                restore_usage_body_delta(base.as_bytes(), &delta).unwrap(),
                target
            );
        }
    }

    #[test]
    fn capture_delta_rejects_corruption_and_wrong_base() {
        let base = b"original".repeat(1024);
        let delta = encode_usage_body_delta(&base, &base).unwrap();
        for length in 0..delta.len() {
            assert!(restore_usage_body_delta(&base, &delta[..length]).is_err());
        }
        for offset in [0, 4, 8, 12, 44, 76, 80, 84] {
            let mut damaged = delta.clone();
            damaged[offset] ^= 255;
            assert!(restore_usage_body_delta(&base, &damaged).is_err());
        }
        assert!(restore_usage_body_delta(&b"different".repeat(1024), &delta).is_err());
    }

    #[test]
    fn capture_delta_falls_back_when_not_profitable() {
        assert!(encode_usage_body_delta(b"{}", b"{}").is_none());
        assert!(encode_usage_body_delta(&b"a".repeat(4096), &b"b".repeat(4096)).is_none());
    }

    #[test]
    fn capture_delta_handles_insert_delete_replace_and_binary_bytes() {
        let base = (0..8192).map(|i| (i % 251) as u8).collect::<Vec<_>>();
        let mut targets = Vec::new();
        let mut target = base.clone();
        target.splice(0..0, [0xff, 0, 0x80]);
        targets.push(target);
        let mut target = base.clone();
        target.extend_from_slice(&[0xff, 0, 0x80]);
        targets.push(target);
        let mut target = base.clone();
        target.drain(100..140);
        targets.push(target);
        let mut target = base.clone();
        target.splice(100..140, [0xff, 0, 0x80]);
        target.splice(4000..4000, [0, 0xff]);
        target.drain(6000..6100);
        targets.push(target);

        for target in targets {
            let delta = encode_usage_body_delta_until(
                &base,
                &target,
                Instant::now() + Duration::from_secs(5),
            )
            .unwrap();
            assert!(delta.len() + 64 < target.len());
            assert_eq!(restore_usage_body_delta(&base, &delta).unwrap(), target);
        }
    }

    #[test]
    fn capture_delta_limits_search_input_without_truncating_large_equal_edges() {
        let base = b"a".repeat(MAX_DIFF_INPUT_BYTES);
        let mut target = base.clone();
        target[MAX_DIFF_INPUT_BYTES / 2] = b'b';
        let delta =
            encode_usage_body_delta_until(&base, &target, Instant::now() + Duration::from_secs(5))
                .unwrap();
        assert_eq!(restore_usage_body_delta(&base, &delta).unwrap(), target);

        target[0] = b'b';
        target[MAX_DIFF_INPUT_BYTES - 1] = b'b';
        assert!(encode_usage_body_delta_until(
            &base,
            &target,
            Instant::now() + Duration::from_secs(5),
        )
        .is_none());
    }

    #[test]
    fn capture_delta_falls_back_when_deadline_has_expired() {
        let base = b"shared content".repeat(1024);
        let mut target = base.clone();
        target[100] = b'X';
        assert!(encode_usage_body_delta_until(&base, &target, Instant::now()).is_none());
    }

    #[test]
    fn capture_delta_shared_fixture() {
        let value: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../../../tests/fixtures/usage-capture-codec.json"
        ))
        .unwrap();
        for fixture in value.as_array().unwrap() {
            let bytes = |key: &str| {
                fixture[key]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|v| v.as_u64().unwrap() as u8)
                    .collect::<Vec<_>>()
            };
            assert_eq!(
                restore_usage_body_delta(&bytes("base"), &bytes("delta")).unwrap(),
                bytes("target")
            );
        }
    }
}
