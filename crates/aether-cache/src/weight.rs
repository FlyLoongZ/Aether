//! Heap accounting for cache values.

/// Approximate heap footprint of a value, including buffers it owns.
///
/// This is the only input to a cache's byte budget. Implementations should
/// over-report rather than under-report: over-reporting evicts early, while
/// under-reporting makes `capacity_bytes` a fiction.
pub trait CacheWeight {
    fn cache_weight(&self) -> usize;
}

/// `size_of::<T>()` plus whatever the value owns beyond its inline fields.
#[inline]
pub fn value_weight<T>(owned_bytes: usize) -> usize {
    std::mem::size_of::<T>().saturating_add(owned_bytes)
}

/// Weight of a buffer plus whatever each element owns.
///
/// Use this when the element type lives in another crate and so cannot carry a
/// [`CacheWeight`] impl of its own.
pub fn vec_weight<T>(items: &Vec<T>, weigh: impl Fn(&T) -> usize) -> usize {
    value_weight::<Vec<T>>(items.capacity() * std::mem::size_of::<T>())
        .saturating_add(items.iter().map(weigh).sum::<usize>())
}

/// Combined footprint of a string buffer and the heap behind each element.
pub fn optional_strings_weight(values: Option<&Vec<String>>) -> usize {
    let Some(values) = values else {
        return 0;
    };
    value_weight::<Vec<String>>(values.capacity() * std::mem::size_of::<String>())
        .saturating_add(values.iter().map(|value| value.capacity()).sum::<usize>())
}

impl CacheWeight for () {
    fn cache_weight(&self) -> usize {
        0
    }
}

impl CacheWeight for u32 {
    fn cache_weight(&self) -> usize {
        0
    }
}

impl CacheWeight for u64 {
    fn cache_weight(&self) -> usize {
        0
    }
}

impl CacheWeight for usize {
    fn cache_weight(&self) -> usize {
        0
    }
}

impl CacheWeight for bool {
    fn cache_weight(&self) -> usize {
        0
    }
}

impl CacheWeight for f64 {
    fn cache_weight(&self) -> usize {
        0
    }
}

impl CacheWeight for String {
    fn cache_weight(&self) -> usize {
        value_weight::<Self>(self.capacity())
    }
}

impl CacheWeight for Vec<u8> {
    fn cache_weight(&self) -> usize {
        value_weight::<Self>(self.capacity())
    }
}

impl CacheWeight for serde_json::Value {
    fn cache_weight(&self) -> usize {
        match self {
            Self::Null | Self::Bool(_) | Self::Number(_) => 0,
            Self::String(text) => text.capacity(),
            Self::Array(items) => {
                value_weight::<Self>(items.capacity() * std::mem::size_of::<Self>())
                    .saturating_add(items.iter().map(CacheWeight::cache_weight).sum::<usize>())
            }
            Self::Object(entries) => value_weight::<Self>(
                entries.len() * (std::mem::size_of::<String>() + std::mem::size_of::<Self>()),
            )
            .saturating_add(
                entries
                    .iter()
                    .map(|(key, value)| key.capacity() + value.cache_weight())
                    .sum::<usize>(),
            ),
        }
    }
}

impl CacheWeight for bytes::Bytes {
    fn cache_weight(&self) -> usize {
        // A `Bytes` can be a view into a larger allocation, but the ones cached
        // here own their buffer, so their length is the honest share.
        value_weight::<Self>(self.len())
    }
}

impl<T: CacheWeight> CacheWeight for Option<T> {
    fn cache_weight(&self) -> usize {
        self.as_ref().map(CacheWeight::cache_weight).unwrap_or(0)
    }
}

impl<T: CacheWeight> CacheWeight for Vec<T> {
    fn cache_weight(&self) -> usize {
        value_weight::<Self>(self.capacity() * std::mem::size_of::<T>())
            .saturating_add(self.iter().map(CacheWeight::cache_weight).sum::<usize>())
    }
}

impl<T: CacheWeight> CacheWeight for std::sync::Arc<T> {
    fn cache_weight(&self) -> usize {
        (**self).cache_weight()
    }
}

#[cfg(test)]
mod tests {
    use super::{optional_strings_weight, CacheWeight};

    #[test]
    fn reports_owned_string_capacity() {
        let values = vec!["a".to_string(), "bb".to_string()];
        // Inline buffer plus the capacity of both strings.
        assert_eq!(
            optional_strings_weight(Some(&values)),
            std::mem::size_of::<Vec<String>>()
                + values.capacity() * std::mem::size_of::<String>()
                + 1
                + 2
        );
    }

    #[test]
    fn nested_values_accumulate() {
        let nested = vec![vec![1_u8; 8], vec![2_u8; 16]];
        assert!(nested.cache_weight() >= 24);
    }

    #[test]
    fn primitives_are_free() {
        assert_eq!(0, 7_u32.cache_weight());
        assert_eq!(0, ().cache_weight());
        assert_eq!(0, None::<Vec<u8>>.cache_weight());
    }
}
