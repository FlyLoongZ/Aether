use super::*;

const REMOVAL_VERSION: i64 = 20261119030000;

async fn apply_removal_sql(pool: &PgPool) {
    let removal = POSTGRES_MIGRATOR
        .iter()
        .find(|migration| migration.version == REMOVAL_VERSION)
        .expect("video removal migration must remain embedded");
    sqlx::raw_sql(removal.sql.clone())
        .execute(pool)
        .await
        .unwrap();
}

#[tokio::test]
async fn video_removal_disables_video_only_tokens_and_strips_format_maps() {
    let Some(server) = ManagedPostgresServer::try_start().await.unwrap() else {
        return;
    };
    let pool = PgPool::connect(server.database_url()).await.unwrap();
    prepare_and_apply_clean_postgres_database(&pool).await;

    assert!(!table_exists(&pool, "video_tasks").await.unwrap());

    // Seed rows that a pre-removal database could still contain, then replay the
    // migration: the baseline fragments no longer create these shapes.
    sqlx::raw_sql(
        r#"
INSERT INTO public.users (id, email, username, email_verified)
VALUES ('video-removal-user', 'video-removal@example.com', 'video-removal', true);
INSERT INTO public.providers (id, name)
VALUES ('video-removal-provider', 'video-removal-provider');
INSERT INTO public.management_tokens (id, user_id, token_hash, name, permissions, is_active)
VALUES
    ('token-video-only', 'video-removal-user', repeat('a', 64), 'video-only',
     '["admin:video_tasks:read"]'::jsonb, true),
    ('token-mixed', 'video-removal-user', repeat('b', 64), 'mixed',
     '["admin:video_tasks:read","admin:system:read"]'::jsonb, true),
    ('token-unrelated', 'video-removal-user', repeat('c', 64), 'unrelated',
     '["admin:system:read"]'::jsonb, true);
INSERT INTO public.provider_api_keys (
    id, name, provider_id, total_tokens, total_cost_usd, api_formats, auth_type_by_format, rate_multipliers,
    health_by_format, circuit_breaker_by_format, allow_auth_channel_mismatch_formats,
    global_priority_by_format
) VALUES (
    'video-removal-key', 'video-removal-key', 'video-removal-provider', 0, 0,
    '["openai:chat","openai:video","gemini:video"]'::jsonb,
    '{"openai:video":"bearer","openai:chat":"bearer"}'::jsonb,
    '{"gemini:video":2}'::jsonb,
    '{"openai:video":{"status":"healthy"}}'::jsonb,
    '{"gemini:video":true}'::jsonb,
    '{"openai:video":true,"openai:chat":false}'::jsonb,
    '{"openai:video":10}'::jsonb
);
"#,
    )
    .execute(&pool)
    .await
    .unwrap();

    apply_removal_sql(&pool).await;

    let tokens: Vec<(String, bool, serde_json::Value)> = sqlx::query_as(
        "SELECT id, is_active, permissions FROM public.management_tokens ORDER BY id",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        tokens,
        vec![
            (
                "token-mixed".to_string(),
                true,
                serde_json::json!(["admin:system:read"])
            ),
            (
                "token-unrelated".to_string(),
                true,
                serde_json::json!(["admin:system:read"])
            ),
            ("token-video-only".to_string(), false, serde_json::json!([])),
        ]
    );

    let key: (
        serde_json::Value,
        serde_json::Value,
        serde_json::Value,
        serde_json::Value,
        serde_json::Value,
        serde_json::Value,
    ) = sqlx::query_as(
        "SELECT api_formats, auth_type_by_format, rate_multipliers, health_by_format,
                allow_auth_channel_mismatch_formats, global_priority_by_format
           FROM public.provider_api_keys WHERE id = 'video-removal-key'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        key.0,
        serde_json::json!(["openai:chat"]),
        "api_formats keeps order and drops video entries"
    );
    for (label, value) in [
        ("auth_type_by_format", &key.1),
        ("rate_multipliers", &key.2),
        ("health_by_format", &key.3),
        ("allow_auth_channel_mismatch_formats", &key.4),
        ("global_priority_by_format", &key.5),
    ] {
        assert!(
            !value.to_string().contains("video"),
            "{label} must not keep video entries: {value}"
        );
    }
    assert_eq!(key.5, serde_json::json!({}));
}

#[tokio::test]
async fn settlement_wallet_finalized_index_exists_after_bootstrap() {
    let Some(server) = ManagedPostgresServer::try_start().await.unwrap() else {
        return;
    };
    let pool = PgPool::connect(server.database_url()).await.unwrap();
    prepare_and_apply_clean_postgres_database(&pool).await;

    let index_exists: bool = query_scalar(
        "SELECT to_regclass('public.ix_usage_settlement_wallet_finalized') IS NOT NULL",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(index_exists);
}
