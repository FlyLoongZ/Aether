use serde_json::Value;

#[tokio::test]
#[ignore = "requires AETHER_TEST_DATABASE_URL and PostgreSQL migrations"]
async fn live_jsonb_migration_preserves_values_and_leaves_no_json_columns() {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&std::env::var("AETHER_TEST_DATABASE_URL").unwrap())
        .await
        .unwrap();
    crate::run_migrations(&pool).await.unwrap();
    let remaining_json: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM information_schema.columns WHERE table_schema = 'public' AND data_type = 'json'",
    ).fetch_one(&pool).await.unwrap();
    assert_eq!(
        remaining_json, 0,
        "every public JSON column should be jsonb after migrations"
    );
    for (table, column) in [
        ("api_keys", "allowed_providers"),
        ("audit_logs", "event_metadata"),
        ("provider_api_keys", "api_formats"),
        ("provider_api_keys", "status_snapshot"),
        ("provider_endpoints", "body_rules"),
        ("providers", "config"),
        ("proxy_nodes", "proxy_metadata"),
        ("users", "allowed_models"),
        ("user_groups", "allowed_api_formats"),
        ("video_tasks", "original_request_body"),
    ] {
        let data_type: String = sqlx::query_scalar(
            "SELECT data_type FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1 AND column_name = $2",
        )
        .bind(table)
        .bind(column)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(data_type, "jsonb", "{table}.{column} should be jsonb");
    }
    let tables = [
        (
            "usage",
            vec![
                "request_headers",
                "provider_request_headers",
                "response_headers",
                "client_response_headers",
                "request_body",
                "provider_request_body",
                "response_body",
                "client_response_body",
                "request_metadata",
            ],
        ),
        (
            "usage_http_audits",
            vec![
                "request_headers",
                "provider_request_headers",
                "response_headers",
                "client_response_headers",
            ],
        ),
        (
            "request_candidates",
            vec!["extra_data", "required_capabilities"],
        ),
    ];
    let mut tx = pool.begin().await.unwrap();
    let mut expected = Vec::new();
    for (table, columns) in &tables {
        let declarations = columns
            .iter()
            .map(|column| format!("{column} json"))
            .collect::<Vec<_>>()
            .join(", ");
        sqlx::query(&format!(
            "CREATE TEMP TABLE legacy_{table} ({declarations})"
        ))
        .execute(&mut *tx)
        .await
        .unwrap();
        for value in [
            r#"{"unicode":"汉字🌍","literal":"\\u0000","nullable":null}"#,
            "{}",
            "[1,2,3]",
            "null",
        ] {
            let values = columns
                .iter()
                .map(|_| "$1::json")
                .collect::<Vec<_>>()
                .join(", ");
            sqlx::query(&format!("INSERT INTO legacy_{table} VALUES ({values})"))
                .bind(value)
                .execute(&mut *tx)
                .await
                .unwrap();
        }
        sqlx::query(&format!("INSERT INTO legacy_{table} DEFAULT VALUES"))
            .execute(&mut *tx)
            .await
            .unwrap();
        let rows: Vec<Value> =
            sqlx::query_scalar(&format!("SELECT to_jsonb(t) FROM legacy_{table} t"))
                .fetch_all(&mut *tx)
                .await
                .unwrap();
        expected.push(rows);
    }
    let migration = include_str!("../../migrations/20260923120000_normalize_usage_jsonb.sql")
        .replace(
            "ALTER TABLE public.usage_http_audits",
            "ALTER TABLE pg_temp.legacy_usage_http_audits",
        )
        .replace(
            "ALTER TABLE public.usage\n",
            "ALTER TABLE pg_temp.legacy_usage\n",
        )
        .replace(
            "ALTER TABLE public.request_candidates",
            "ALTER TABLE pg_temp.legacy_request_candidates",
        );
    for _ in 0..2 {
        sqlx::raw_sql(&migration).execute(&mut *tx).await.unwrap();
        for ((table, columns), expected) in tables.iter().zip(&expected) {
            let rows: Vec<Value> =
                sqlx::query_scalar(&format!("SELECT to_jsonb(t) FROM legacy_{table} t"))
                    .fetch_all(&mut *tx)
                    .await
                    .unwrap();
            assert_eq!(&rows, expected);
            for column in columns {
                let data_type: String = sqlx::query_scalar(&format!(
                    "SELECT pg_typeof({column})::text FROM legacy_{table} LIMIT 1"
                ))
                .fetch_one(&mut *tx)
                .await
                .unwrap();
                assert_eq!(data_type, "jsonb");
            }
        }
    }
    tx.rollback().await.unwrap();
}
