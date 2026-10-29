use super::*;

const REMOVAL_VERSION: i64 = 20261019010000;

async fn assert_proxy_tunnel_schema_removed(pool: &PgPool) {
    for table in [
        "proxy_node_events",
        "proxy_node_metrics_1m",
        "proxy_node_metrics_1h",
    ] {
        assert!(
            !table_exists(pool, table).await.unwrap(),
            "retired table {table} must be absent"
        );
    }
    for column in [
        "is_manual",
        "tunnel_generation",
        "last_heartbeat_at",
        "tunnel_mode",
        "tunnel_connected",
        "tunnel_connected_at",
        "hardware_info",
        "estimated_max_concurrency",
        "heartbeat_interval",
        "active_connections",
        "proxy_metadata",
        "remote_config",
        "config_version",
    ] {
        assert!(
            !column_exists(pool, "proxy_nodes", column).await.unwrap(),
            "retired proxy_nodes.{column} must be absent"
        );
    }
    assert!(column_exists(pool, "proxy_nodes", "node_generation")
        .await
        .unwrap());
    assert!(column_exists(pool, "proxy_nodes", "last_used_at")
        .await
        .unwrap());
    assert!(
        !column_exists(pool, "usage_counter_deltas", "target_tunnel_generation")
            .await
            .unwrap()
    );
    assert!(
        column_exists(pool, "usage_counter_deltas", "target_node_generation")
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn legacy_proxy_tunnel_removal_preserves_manual_nodes_and_is_idempotent() {
    let Some(server) = ManagedPostgresServer::try_start().await.unwrap() else {
        return;
    };
    let mut connection = PgConnection::connect(server.database_url()).await.unwrap();
    connection.ensure_migrations_table().await.unwrap();
    for migration in POSTGRES_MIGRATOR
        .iter()
        .filter(|migration| migration.version < REMOVAL_VERSION)
    {
        connection.apply(migration).await.unwrap();
    }
    let removal = POSTGRES_MIGRATOR
        .iter()
        .find(|migration| migration.version == REMOVAL_VERSION)
        .expect("proxy tunnel removal migration must remain embedded");

    sqlx::raw_sql(
        r#"
INSERT INTO public.proxy_nodes (
    id, name, ip, port, tunnel_generation, is_manual, proxy_url, proxy_password,
    last_heartbeat_at, total_requests, failed_requests
) VALUES
    ('manual-node', 'manual', 'proxy.example', 8080, 'manual-generation', TRUE,
     'http://proxy.example:8080', 'sealed-password', '2026-08-31 00:00:00+00', 42, 3),
    ('automatic-node', 'automatic', 'proxy.example', 8081, 'automatic-generation', FALSE,
     NULL, NULL, NULL, 0, 0);
INSERT INTO public.usage_counter_deltas (
    id, request_id, kind, target_id, target_tunnel_generation, total_requests_delta
) VALUES ('manual-delta', 'request-1', 'proxy_node', 'manual-node', 'manual-generation', 7);
INSERT INTO public.system_configs (id, key, value) VALUES
    ('retired-1m', 'proxy_node_metrics_1m_retention_days', '1'::jsonb),
    ('retired-1h', 'proxy_node_metrics_1h_retention_days', '30'::jsonb),
    ('unrelated-setting', 'tunnel-removal-test-unrelated', 'true'::jsonb)
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;
"#,
    )
    .execute(&mut connection)
    .await
    .unwrap();

    connection.apply(removal).await.unwrap();
    // A regenerated snapshot may already have the trimmed shape. Replaying the
    // cleanup against that shape must not delete manual nodes or fail on old columns.
    sqlx::raw_sql(removal.sql.as_ref())
        .execute(&mut connection)
        .await
        .unwrap();
    connection.close().await.unwrap();
    let pool = PgPool::connect(server.database_url()).await.unwrap();
    assert_proxy_tunnel_schema_removed(&pool).await;

    let nodes: Vec<(String, String, String, String, i64, i64, bool)> = sqlx::query_as(
        r#"
SELECT id, node_generation, proxy_url, proxy_password, total_requests, failed_requests,
       last_used_at = TIMESTAMPTZ '2026-08-31 00:00:00+00'
FROM public.proxy_nodes ORDER BY id
"#,
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        nodes,
        vec![(
            "manual-node".to_string(),
            "manual-generation".to_string(),
            "http://proxy.example:8080".to_string(),
            "sealed-password".to_string(),
            42,
            3,
            true,
        )]
    );
    let delta: (String, i64) = sqlx::query_as(
        "SELECT target_node_generation, total_requests_delta FROM public.usage_counter_deltas WHERE id = 'manual-delta'"
    ).fetch_one(&pool).await.unwrap();
    assert_eq!(delta, ("manual-generation".to_string(), 7));
    let retired_configs: i64 = query_scalar(
        "SELECT COUNT(*) FROM public.system_configs WHERE key IN ('proxy_node_metrics_1m_retention_days', 'proxy_node_metrics_1h_retention_days')"
    ).fetch_one(&pool).await.unwrap();
    assert_eq!(retired_configs, 0);
    let unrelated_exists: bool = query_scalar(
        "SELECT EXISTS (SELECT 1 FROM public.system_configs WHERE key = 'tunnel-removal-test-unrelated' AND value = 'true'::jsonb)"
    ).fetch_one(&pool).await.unwrap();
    assert!(unrelated_exists);
}

#[tokio::test]
async fn fresh_bootstrap_removes_proxy_tunnel_schema_after_migrations() {
    let Some(server) = ManagedPostgresServer::try_start().await.unwrap() else {
        return;
    };
    let pool = PgPool::connect(server.database_url()).await.unwrap();
    prepare_and_apply_clean_postgres_database(&pool).await;
    assert_proxy_tunnel_schema_removed(&pool).await;
}
