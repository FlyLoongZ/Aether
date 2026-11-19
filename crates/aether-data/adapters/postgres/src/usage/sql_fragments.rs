//! Shared SQL fragments for dashboard projections.
//!
//! Kept in a dedicated module so the canonical token projection has a single
//! definition while the surrounding queries stay readable.

macro_rules! dashboard_total_tokens_expr {
    () => {
        r#"  GREATEST(
    COALESCE(
      CASE
        WHEN s.billing_effective_input_tokens IS NOT NULL
        THEN GREATEST(s.billing_effective_input_tokens, 0)
          + GREATEST(COALESCE(s.billing_output_tokens, 0), 0)
          + GREATEST(
              COALESCE(
                s.billing_cache_creation_tokens,
                COALESCE(s.billing_cache_creation_5m_tokens, 0)
                  + COALESCE(s.billing_cache_creation_1h_tokens, 0),
                0
              ),
              0
            )
          + GREATEST(COALESCE(s.billing_cache_read_tokens, 0), 0)
        WHEN s.billing_total_input_context IS NOT NULL
        THEN GREATEST(s.billing_total_input_context, 0)
          + GREATEST(COALESCE(s.billing_output_tokens, 0), 0)
      END,
      0
    ),
    0
  )::bigint END AS total_tokens,"#
    };
}
