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
          + GREATEST(COALESCE(s.billing_output_tokens, u.output_tokens, 0), 0)
          + GREATEST(
              COALESCE(
                s.billing_cache_creation_tokens,
                CASE
                  WHEN s.billing_cache_creation_5m_tokens IS NOT NULL
                    OR s.billing_cache_creation_1h_tokens IS NOT NULL
                  THEN COALESCE(s.billing_cache_creation_5m_tokens, 0)
                    + COALESCE(s.billing_cache_creation_1h_tokens, 0)
                END,
                CASE
                  WHEN COALESCE(u.cache_creation_input_tokens, 0) = 0
                       AND (
                         COALESCE(u.cache_creation_input_tokens_5m, 0)
                         + COALESCE(u.cache_creation_input_tokens_1h, 0)
                       ) > 0
                  THEN COALESCE(u.cache_creation_input_tokens_5m, 0)
                    + COALESCE(u.cache_creation_input_tokens_1h, 0)
                  ELSE COALESCE(u.cache_creation_input_tokens, 0)
                END,
                0
              ),
              0
            )
          + GREATEST(
              COALESCE(
                s.billing_cache_read_tokens,
                u.cache_read_input_tokens,
                0
              ),
              0
            )
        WHEN s.billing_total_input_context IS NOT NULL
        THEN GREATEST(s.billing_total_input_context, 0)
          + GREATEST(COALESCE(s.billing_output_tokens, u.output_tokens, 0), 0)
      END,
      NULLIF(GREATEST(COALESCE(u.total_tokens, 0), 0), 0),
      CASE
        WHEN split_part(lower(COALESCE(COALESCE(u.endpoint_api_format, u.api_format), '')), ':', 1)
             IN ('openai', 'gemini', 'google')
        THEN GREATEST(COALESCE(u.input_tokens, 0), 0)
          + GREATEST(COALESCE(u.output_tokens, 0), 0)
        ELSE GREATEST(COALESCE(u.input_tokens, 0), 0)
          + GREATEST(COALESCE(u.output_tokens, 0), 0)
          + GREATEST(
              CASE
                WHEN COALESCE(u.cache_creation_input_tokens, 0) = 0
                     AND (
                       COALESCE(u.cache_creation_input_tokens_5m, 0)
                       + COALESCE(u.cache_creation_input_tokens_1h, 0)
                     ) > 0
                THEN COALESCE(u.cache_creation_input_tokens_5m, 0)
                  + COALESCE(u.cache_creation_input_tokens_1h, 0)
                ELSE COALESCE(u.cache_creation_input_tokens, 0)
              END,
              0
            )
          + GREATEST(COALESCE(u.cache_read_input_tokens, 0), 0)
      END,
      0
    ),
    0
  )::bigint END AS total_tokens,"#
    };
}
