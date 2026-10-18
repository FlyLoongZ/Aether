/// A colon-separated prefix for cache keys.
///
/// The separator is structural and is **not** escaped: `child("a:b")` composes
/// the same key as `child("a").child("b")`. Callers that build a suffix from
/// user input must therefore treat `:` as reserved; prefixes themselves may
/// contain it, which is what lets an operator name a namespace `app:prod`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CacheKeyNamespace {
    prefix: String,
}

impl CacheKeyNamespace {
    pub fn new(prefix: impl Into<String>) -> Self {
        Self {
            prefix: prefix.into(),
        }
    }

    pub fn child(&self, suffix: &str) -> Self {
        if self.prefix.is_empty() {
            return Self::new(suffix);
        }
        if suffix.is_empty() {
            return self.clone();
        }
        Self::new(format!("{}:{}", self.prefix, suffix))
    }

    pub fn key(&self, raw_key: &str) -> String {
        if self.prefix.is_empty() {
            return raw_key.to_string();
        }
        if raw_key.is_empty() {
            return self.prefix.clone();
        }
        format!("{}:{}", self.prefix, raw_key)
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
    }
}

#[cfg(test)]
mod tests {
    use super::CacheKeyNamespace;

    #[test]
    fn the_separator_is_structural_not_escaped() {
        let root = CacheKeyNamespace::new("aether");

        // Documented property, not an accident: a suffix may itself be a path,
        // and it composes to the same key as the equivalent child chain.
        assert_eq!(
            root.child("a:b").key("k"),
            root.child("a").child("b").key("k")
        );
        // A prefix may carry its own colon, so "app:prod" stays one segment.
        assert_eq!(
            CacheKeyNamespace::new("app:prod").child("lock").key("k"),
            "app:prod:lock:k"
        );
    }

    #[test]
    fn composes_scoped_keys() {
        let root = CacheKeyNamespace::new("aether");
        let child = root.child("auth");

        assert_eq!(root.key("user-1"), "aether:user-1");
        assert_eq!(child.key("user-1"), "aether:auth:user-1");
        assert_eq!(child.prefix(), "aether:auth");
    }
}
