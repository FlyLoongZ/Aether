use std::sync::Arc;

use crate::DataLayerError;
use aether_data_contracts::repository::announcements::{
    AnnouncementReadRepository, AnnouncementWriteRepository,
};
use aether_data_contracts::repository::audit::AuditLogReadRepository;
use aether_data_contracts::repository::auth::{
    AuthApiKeyReadRepository, AuthApiKeyWriteRepository,
};
use aether_data_contracts::repository::auth_modules::{
    AuthModuleReadRepository, AuthModuleWriteRepository,
};
use aether_data_contracts::repository::background_tasks::{
    BackgroundTaskReadRepository, BackgroundTaskWriteRepository,
};
use aether_data_contracts::repository::billing::BillingReadRepository;
use aether_data_contracts::repository::candidate_selection::MinimalCandidateSelectionReadRepository;
use aether_data_contracts::repository::candidates::{
    RequestCandidateReadRepository, RequestCandidateWriteRepository,
};
use aether_data_contracts::repository::gemini_file_mappings::{
    GeminiFileMappingReadRepository, GeminiFileMappingWriteRepository,
};
use aether_data_contracts::repository::global_models::{
    GlobalModelReadRepository, GlobalModelWriteRepository,
};
use aether_data_contracts::repository::management_tokens::{
    ManagementTokenReadRepository, ManagementTokenWriteRepository,
};
use aether_data_contracts::repository::oauth_providers::{
    OAuthProviderReadRepository, OAuthProviderWriteRepository,
};
use aether_data_contracts::repository::pool_scores::{
    PoolMemberScoreWriteRepository, PoolScoreReadRepository,
};
use aether_data_contracts::repository::provider_catalog::{
    ProviderCatalogReadRepository, ProviderCatalogWriteRepository,
};
use aether_data_contracts::repository::proxy_nodes::{
    ProxyNodeReadRepository, ProxyNodeWriteRepository,
};
use aether_data_contracts::repository::quota::{
    ProviderQuotaReadRepository, ProviderQuotaWriteRepository,
};
use aether_data_contracts::repository::routing_profiles::{
    RoutingGroupReadRepository, RoutingGroupWriteRepository,
};
use aether_data_contracts::repository::settlement::SettlementWriteRepository;
use aether_data_contracts::repository::usage::{UsageReadRepository, UsageWriteRepository};
use aether_data_contracts::repository::users::UserReadRepository;
use aether_data_contracts::repository::video_tasks::{
    VideoTaskReadRepository, VideoTaskWriteRepository,
};
use aether_data_contracts::repository::wallet::{WalletReadRepository, WalletWriteRepository};
use aether_data_postgres::PostgresAuditLogReadRepository;
use aether_data_postgres::PostgresPoolMemberScoreRepository;
use aether_data_postgres::PostgresRoutingGroupRepository;
use aether_data_postgres::SqlxAnnouncementReadRepository;
use aether_data_postgres::SqlxAuthApiKeySnapshotReadRepository;
use aether_data_postgres::SqlxBackgroundTaskRepository;
use aether_data_postgres::SqlxBillingReadRepository;
use aether_data_postgres::SqlxGeminiFileMappingRepository;
use aether_data_postgres::SqlxGlobalModelReadRepository;
use aether_data_postgres::SqlxManagementTokenRepository;
use aether_data_postgres::SqlxMinimalCandidateSelectionReadRepository;
use aether_data_postgres::SqlxOAuthProviderRepository;
use aether_data_postgres::SqlxProviderCatalogReadRepository;
use aether_data_postgres::SqlxProviderQuotaRepository;
use aether_data_postgres::SqlxProxyNodeRepository;
use aether_data_postgres::SqlxRequestCandidateReadRepository;
use aether_data_postgres::SqlxSettlementRepository;
use aether_data_postgres::SqlxUsageReadRepository;
use aether_data_postgres::SqlxUserReadRepository;
use aether_data_postgres::SqlxWalletRepository;
use aether_data_postgres::{
    PostgresLeaseRunner, PostgresLeaseRunnerConfig, PostgresPool, PostgresPoolConfig,
    PostgresPoolFactory, PostgresTransactionRunner,
};
use aether_data_postgres::{SqlxAuthModuleReadRepository, SqlxAuthModuleRepository};
use aether_data_postgres::{SqlxVideoTaskReadRepository, SqlxVideoTaskRepository};

#[derive(Debug, Clone)]
pub struct PostgresBackend {
    config: PostgresPoolConfig,
    pool: PostgresPool,
}

impl PostgresBackend {
    pub fn from_config(config: PostgresPoolConfig) -> Result<Self, DataLayerError> {
        let factory = PostgresPoolFactory::new(config.clone())?;
        let pool = factory.connect_lazy()?;

        Ok(Self { config, pool })
    }

    pub fn config(&self) -> &PostgresPoolConfig {
        &self.config
    }

    pub fn pool(&self) -> &PostgresPool {
        &self.pool
    }

    pub fn pool_clone(&self) -> PostgresPool {
        self.pool.clone()
    }

    pub fn auth_api_key_read_repository(&self) -> Arc<dyn AuthApiKeyReadRepository> {
        Arc::new(SqlxAuthApiKeySnapshotReadRepository::new(self.pool_clone()))
    }

    pub fn announcement_read_repository(&self) -> Arc<dyn AnnouncementReadRepository> {
        Arc::new(SqlxAnnouncementReadRepository::new(self.pool_clone()))
    }

    pub fn audit_log_read_repository(&self) -> Arc<dyn AuditLogReadRepository> {
        Arc::new(PostgresAuditLogReadRepository::new(self.pool_clone()))
    }

    pub fn announcement_write_repository(&self) -> Arc<dyn AnnouncementWriteRepository> {
        Arc::new(SqlxAnnouncementReadRepository::new(self.pool_clone()))
    }

    pub fn auth_api_key_write_repository(&self) -> Arc<dyn AuthApiKeyWriteRepository> {
        Arc::new(SqlxAuthApiKeySnapshotReadRepository::new(self.pool_clone()))
    }

    pub fn auth_module_read_repository(&self) -> Arc<dyn AuthModuleReadRepository> {
        Arc::new(SqlxAuthModuleReadRepository::new(self.pool_clone()))
    }

    pub fn auth_module_write_repository(&self) -> Arc<dyn AuthModuleWriteRepository> {
        Arc::new(SqlxAuthModuleRepository::new(self.pool_clone()))
    }

    pub fn billing_read_repository(&self) -> Arc<dyn BillingReadRepository> {
        Arc::new(SqlxBillingReadRepository::new(self.pool_clone()))
    }

    pub fn background_task_read_repository(&self) -> Arc<dyn BackgroundTaskReadRepository> {
        Arc::new(SqlxBackgroundTaskRepository::new(self.pool_clone()))
    }

    pub fn background_task_write_repository(&self) -> Arc<dyn BackgroundTaskWriteRepository> {
        Arc::new(SqlxBackgroundTaskRepository::new(self.pool_clone()))
    }

    pub fn minimal_candidate_selection_read_repository(
        &self,
    ) -> Arc<dyn MinimalCandidateSelectionReadRepository> {
        Arc::new(SqlxMinimalCandidateSelectionReadRepository::new(
            self.pool_clone(),
        ))
    }

    pub fn request_candidate_read_repository(&self) -> Arc<dyn RequestCandidateReadRepository> {
        Arc::new(SqlxRequestCandidateReadRepository::new(self.pool_clone()))
    }

    pub fn request_candidate_write_repository(&self) -> Arc<dyn RequestCandidateWriteRepository> {
        Arc::new(SqlxRequestCandidateReadRepository::new(self.pool_clone()))
    }

    pub fn gemini_file_mapping_read_repository(&self) -> Arc<dyn GeminiFileMappingReadRepository> {
        Arc::new(SqlxGeminiFileMappingRepository::new(self.pool_clone()))
    }

    pub fn gemini_file_mapping_write_repository(
        &self,
    ) -> Arc<dyn GeminiFileMappingWriteRepository> {
        Arc::new(SqlxGeminiFileMappingRepository::new(self.pool_clone()))
    }

    pub fn global_model_read_repository(&self) -> Arc<dyn GlobalModelReadRepository> {
        Arc::new(SqlxGlobalModelReadRepository::new(self.pool_clone()))
    }

    pub fn global_model_write_repository(&self) -> Arc<dyn GlobalModelWriteRepository> {
        Arc::new(SqlxGlobalModelReadRepository::new(self.pool_clone()))
    }

    pub fn management_token_read_repository(&self) -> Arc<dyn ManagementTokenReadRepository> {
        Arc::new(SqlxManagementTokenRepository::new(self.pool_clone()))
    }

    pub fn management_token_write_repository(&self) -> Arc<dyn ManagementTokenWriteRepository> {
        Arc::new(SqlxManagementTokenRepository::new(self.pool_clone()))
    }

    pub fn oauth_provider_read_repository(&self) -> Arc<dyn OAuthProviderReadRepository> {
        Arc::new(SqlxOAuthProviderRepository::new(self.pool_clone()))
    }

    pub fn oauth_provider_write_repository(&self) -> Arc<dyn OAuthProviderWriteRepository> {
        Arc::new(SqlxOAuthProviderRepository::new(self.pool_clone()))
    }

    pub fn proxy_node_read_repository(&self) -> Arc<dyn ProxyNodeReadRepository> {
        Arc::new(SqlxProxyNodeRepository::new(self.pool_clone()))
    }

    pub fn proxy_node_write_repository(&self) -> Arc<dyn ProxyNodeWriteRepository> {
        Arc::new(SqlxProxyNodeRepository::new(self.pool_clone()))
    }

    pub fn provider_catalog_read_repository(&self) -> Arc<dyn ProviderCatalogReadRepository> {
        Arc::new(SqlxProviderCatalogReadRepository::new(self.pool_clone()))
    }

    pub fn provider_catalog_write_repository(&self) -> Arc<dyn ProviderCatalogWriteRepository> {
        Arc::new(SqlxProviderCatalogReadRepository::new(self.pool_clone()))
    }

    pub fn pool_score_read_repository(&self) -> Arc<dyn PoolScoreReadRepository> {
        Arc::new(PostgresPoolMemberScoreRepository::new(self.pool_clone()))
    }

    pub fn pool_score_write_repository(&self) -> Arc<dyn PoolMemberScoreWriteRepository> {
        Arc::new(PostgresPoolMemberScoreRepository::new(self.pool_clone()))
    }

    pub fn routing_group_read_repository(&self) -> Arc<dyn RoutingGroupReadRepository> {
        Arc::new(PostgresRoutingGroupRepository::new(self.pool_clone()))
    }

    pub fn routing_group_write_repository(&self) -> Arc<dyn RoutingGroupWriteRepository> {
        Arc::new(PostgresRoutingGroupRepository::new(self.pool_clone()))
    }

    pub fn provider_quota_read_repository(&self) -> Arc<dyn ProviderQuotaReadRepository> {
        Arc::new(SqlxProviderQuotaRepository::new(self.pool_clone()))
    }

    pub fn usage_read_repository(&self) -> Arc<dyn UsageReadRepository> {
        Arc::new(SqlxUsageReadRepository::new(self.pool_clone()))
    }

    pub fn user_read_repository(&self) -> Arc<dyn UserReadRepository> {
        Arc::new(SqlxUserReadRepository::new(self.pool_clone()))
    }

    pub fn usage_write_repository(&self) -> Arc<dyn UsageWriteRepository> {
        Arc::new(SqlxUsageReadRepository::new(self.pool_clone()))
    }

    pub fn wallet_read_repository(&self) -> Arc<dyn WalletReadRepository> {
        Arc::new(SqlxWalletRepository::new(self.pool_clone()))
    }

    pub fn wallet_write_repository(&self) -> Arc<dyn WalletWriteRepository> {
        Arc::new(SqlxWalletRepository::new(self.pool_clone()))
    }

    pub fn settlement_write_repository(&self) -> Arc<dyn SettlementWriteRepository> {
        Arc::new(SqlxSettlementRepository::new(self.pool_clone()))
    }

    pub fn video_task_read_repository(&self) -> Arc<dyn VideoTaskReadRepository> {
        Arc::new(SqlxVideoTaskReadRepository::new(self.pool_clone()))
    }

    pub fn video_task_write_repository(&self) -> Arc<dyn VideoTaskWriteRepository> {
        Arc::new(SqlxVideoTaskRepository::new(self.pool_clone()))
    }

    pub fn transaction_runner(&self) -> PostgresTransactionRunner {
        PostgresTransactionRunner::new(self.pool_clone())
    }

    pub fn lease_runner(
        &self,
        config: PostgresLeaseRunnerConfig,
    ) -> Result<PostgresLeaseRunner, DataLayerError> {
        PostgresLeaseRunner::new(self.transaction_runner(), config)
    }

    pub fn provider_quota_write_repository(&self) -> Arc<dyn ProviderQuotaWriteRepository> {
        Arc::new(SqlxProviderQuotaRepository::new(self.pool_clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::PostgresBackend;
    use aether_data_postgres::{PostgresLeaseRunnerConfig, PostgresPoolConfig};

    #[tokio::test]
    async fn maintenance_and_aggregation_futures_are_send() {
        fn assert_send(_: impl Send) {}

        let backend = PostgresBackend::from_config(PostgresPoolConfig {
            database_url: "postgres://localhost/aether".to_string(),
            min_connections: 0,
            ..PostgresPoolConfig::default()
        })
        .unwrap();
        let now = chrono::Utc::now();
        let daily = crate::StatsDailyAggregationInput {
            target_day_utc: now,
            aggregated_at: now,
        };
        let hourly = crate::StatsHourlyAggregationInput {
            target_hour_utc: now,
            aggregated_at: now,
        };
        let wallet = crate::WalletDailyUsageAggregationInput {
            billing_date: "2026-09-09".to_string(),
            billing_timezone: "UTC".to_string(),
            window_start_unix_secs: 0,
            window_end_unix_secs: 86_400,
            aggregated_at_unix_secs: 86_400,
        };

        // Drop without polling: these are compile-time checks for spawned workers.
        assert_send(backend.run_table_maintenance(&["usage"]));
        assert_send(backend.aggregate_stats_daily(&daily));
        assert_send(backend.aggregate_stats_hourly(&hourly));
        assert_send(backend.aggregate_wallet_daily_usage(&wallet));
    }

    #[tokio::test]
    async fn backend_retains_config_and_pool() {
        let config = PostgresPoolConfig {
            database_url: "postgres://localhost/aether".to_string(),
            min_connections: 1,
            max_connections: 4,
            acquire_timeout_ms: 1_000,
            idle_timeout_ms: 5_000,
            max_lifetime_ms: 30_000,
            statement_cache_capacity: 64,
            require_ssl: false,
        };

        let backend =
            PostgresBackend::from_config(config.clone()).expect("backend should build lazily");

        assert_eq!(backend.config(), &config);
        let _pool = backend.pool();
        let _pool_clone = backend.pool_clone();
        let _auth_api_key_reader = backend.auth_api_key_read_repository();
        let _auth_api_key_writer = backend.auth_api_key_write_repository();
        let _auth_module_reader = backend.auth_module_read_repository();
        let _billing_reader = backend.billing_read_repository();
        let _gemini_file_mapping_reader = backend.gemini_file_mapping_read_repository();
        let _global_model_reader = backend.global_model_read_repository();
        let _global_model_writer = backend.global_model_write_repository();
        let _management_token_reader = backend.management_token_read_repository();
        let _management_token_writer = backend.management_token_write_repository();
        let _oauth_provider_reader = backend.oauth_provider_read_repository();
        let _oauth_provider_writer = backend.oauth_provider_write_repository();
        let _proxy_node_reader = backend.proxy_node_read_repository();
        let _proxy_node_writer = backend.proxy_node_write_repository();
        let _minimal_candidate_selection_reader =
            backend.minimal_candidate_selection_read_repository();
        let _request_candidate_reader = backend.request_candidate_read_repository();
        let _request_candidate_writer = backend.request_candidate_write_repository();
        let _gemini_file_mapping_writer = backend.gemini_file_mapping_write_repository();
        let _provider_catalog_reader = backend.provider_catalog_read_repository();
        let _provider_catalog_writer = backend.provider_catalog_write_repository();
        let _provider_quota_reader = backend.provider_quota_read_repository();
        let _usage_reader = backend.usage_read_repository();
        let _usage_writer = backend.usage_write_repository();
        let _wallet_reader = backend.wallet_read_repository();
        let _wallet_writer = backend.wallet_write_repository();
        let _settlement_writer = backend.settlement_write_repository();
        let _video_task_reader = backend.video_task_read_repository();
        let _video_task_writer = backend.video_task_write_repository();
        let _transaction_runner = backend.transaction_runner();
        let _lease_runner = backend
            .lease_runner(PostgresLeaseRunnerConfig::default())
            .expect("lease runner should build");
        let _provider_quota_writer = backend.provider_quota_write_repository();
    }
}
