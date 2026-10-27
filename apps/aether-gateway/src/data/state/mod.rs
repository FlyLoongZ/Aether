use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;

use super::auth::GatewayAuthApiKeySnapshot;
use super::candidates::{read_request_candidate_trace, RequestCandidateTrace};
use super::config::GatewayDataConfig;
use super::decision_trace::{read_decision_trace, DecisionTrace};
use crate::provider_transport::{
    read_provider_transport_snapshot, GatewayProviderTransportSnapshot,
};
use crate::video_tasks::LocalVideoTaskReadResponse;
use aether_cache::{value_weight, CacheWeight, OnceMap, OnceMapError};
use aether_data::repository::announcements::{
    AnnouncementListQuery, AnnouncementReadRepository, AnnouncementWriteRepository,
    CreateAnnouncementRecord, StoredAnnouncement, StoredAnnouncementPage, UpdateAnnouncementRecord,
};
use aether_data::repository::audit::{
    AuditLogListQuery, RequestAuditBundle, StoredAdminAuditLogPage, StoredSuspiciousActivity,
    StoredUserAuditLogPage,
};
use aether_data::repository::auth::{
    AuthApiKeyLookupKey, AuthApiKeyReadRepository, AuthApiKeyWriteRepository,
    StoredAuthApiKeyExportRecord, StoredAuthApiKeySnapshot,
};
use aether_data::repository::auth_modules::{
    AuthModuleReadRepository, AuthModuleWriteRepository, CompareAndSwapLdapConfigResult,
    LdapBindPasswordUpdate, StoredLdapModuleConfig, StoredOAuthProviderModuleConfig,
};
use aether_data::repository::gemini_file_mappings::{
    GeminiFileMappingListQuery, GeminiFileMappingReadRepository, GeminiFileMappingStats,
    GeminiFileMappingWriteRepository, StoredGeminiFileMapping, StoredGeminiFileMappingListPage,
    UpsertGeminiFileMappingRecord,
};
use aether_data::repository::management_tokens::{
    CreateManagementTokenRecord, ManagementTokenListQuery, ManagementTokenReadRepository,
    ManagementTokenWriteRepository, RegenerateManagementTokenSecret, StoredManagementToken,
    StoredManagementTokenListPage, StoredManagementTokenWithUser, UpdateManagementTokenRecord,
};
use aether_data::repository::oauth_providers::{
    OAuthProviderReadRepository, OAuthProviderWriteRepository, StoredOAuthProviderConfig,
    UpsertOAuthProviderConfigRecord,
};
use aether_data::repository::proxy_nodes::{
    ProxyNodeManualCreateMutation, ProxyNodeManualUpdateMutation, ProxyNodeReadRepository,
    ProxyNodeTrafficMutation, ProxyNodeWriteRepository, StoredProxyNode,
};
pub(crate) use aether_data::repository::system::{AdminSystemStats, StoredSystemConfigEntry};
use aether_data::repository::users::{
    StoredUserAuthRecord, StoredUserExportRow, StoredUserOAuthLinkSummary, StoredUserSummary,
    UserReadRepository,
};
pub(crate) use aether_data::repository::users::{
    StoredUserPreferenceRecord, StoredUserSessionRecord,
};
use aether_data::repository::wallet::{
    AdjustWalletBalanceInBatchInput, AdjustWalletBalanceInput, AdminPaymentOrderListQuery,
    AdminRedeemCodeBatchListQuery, AdminRedeemCodeListQuery,
    AdminUserWalletBalanceBatchUserOutcome, AdminWalletLedgerQuery, AdminWalletListQuery,
    AdminWalletRefundRequestListQuery, CompareAndSwapPaymentOrderStripeClientSecretInput,
    CompleteAdminWalletRefundInput, CreateAdminRedeemCodeBatchInput,
    CreateAdminRedeemCodeBatchResult, CreateManualWalletRechargeInput,
    CreatePlanPurchaseOrderInput, CreatePlanPurchaseOrderOutcome, CreateWalletRechargeOrderInput,
    CreateWalletRechargeOrderOutcome, CreateWalletRefundRequestInput,
    CreateWalletRefundRequestOutcome, CreditAdminPaymentOrderInput,
    DeleteAdminRedeemCodeBatchInput, DisableAdminRedeemCodeBatchInput, DisableAdminRedeemCodeInput,
    FailAdminWalletRefundInput, FailWalletRechargeCheckoutInput, InitializeAuthWalletOutcome,
    PrepareAdminUserWalletBalanceBatchInput, PrepareAdminUserWalletBalanceBatchOutcome,
    ProcessAdminWalletRefundInput, ProcessPaymentCallbackInput, ProcessPaymentCallbackOutcome,
    ReclaimWalletRechargeCheckoutInput, RedeemWalletCodeInput, RedeemWalletCodeOutcome,
    StoredAdminPaymentCallbackPage, StoredAdminPaymentOrder, StoredAdminPaymentOrderPage,
    StoredAdminRedeemCodeBatch, StoredAdminRedeemCodeBatchPage, StoredAdminRedeemCodePage,
    StoredAdminWalletLedgerPage, StoredAdminWalletListPage, StoredAdminWalletRefund,
    StoredAdminWalletRefundPage, StoredAdminWalletRefundRequestPage, StoredAdminWalletTransaction,
    StoredAdminWalletTransactionPage, StoredWalletDailyUsageLedger,
    StoredWalletDailyUsageLedgerPage, StoredWalletSnapshot, UpdateAdminWalletRefundGatewayInput,
    UpdateWalletRechargeCheckoutInput, WalletLookupKey, WalletMutationOutcome,
    WalletReadRepository, WalletWriteRepository,
};

use aether_data::{
    DataBackends, DataLayerError, DatabaseMaintenanceSummary, WalletDailyUsageAggregationInput,
    WalletDailyUsageAggregationResult,
};
use aether_data_contracts::repository::background_tasks::{
    BackgroundTaskListQuery, BackgroundTaskReadRepository, BackgroundTaskSummary,
    BackgroundTaskWriteRepository, StoredBackgroundTaskEvent, StoredBackgroundTaskRun,
    StoredBackgroundTaskRunPage, UpsertBackgroundTaskEvent, UpsertBackgroundTaskRun,
};
use aether_data_contracts::repository::billing::{
    AdminBillingCollectorRecord, AdminBillingCollectorWriteInput, AdminBillingMutationOutcome,
    AdminBillingPresetApplyResult, AdminBillingRuleRecord, AdminBillingRuleWriteInput,
    BillingPlanRecord, BillingPlanWriteInput, BillingReadRepository,
    PaymentGatewayConfigCasWriteInput, PaymentGatewayConfigRecord, PaymentGatewaySecretCasUpdate,
    StoredBillingModelContext, UserDailyQuotaAvailabilityRecord, UserPlanEntitlementRecord,
};
use aether_data_contracts::repository::candidate_selection::{
    MinimalCandidateSelectionReadRepository, StoredMinimalCandidateSelectionRow,
    StoredPoolKeyCandidateRowsByKeyIdsQuery, StoredPoolKeyCandidateRowsQuery,
};
use aether_data_contracts::repository::candidates::{
    PublicHealthStatusCount, PublicHealthTimelineBucket, RequestCandidateReadRepository,
    RequestCandidateWriteRepository, StoredRequestCandidate, UpsertRequestCandidateRecord,
};
use aether_data_contracts::repository::global_models::{
    AdminGlobalModelListQuery, AdminProviderModelListQuery, CreateAdminGlobalModelRecord,
    GlobalModelReadRepository, GlobalModelWriteRepository, PublicCatalogModelListQuery,
    PublicCatalogModelSearchQuery, PublicGlobalModelQuery, StoredAdminGlobalModel,
    StoredAdminGlobalModelPage, StoredAdminProviderModel, StoredProviderActiveGlobalModel,
    StoredProviderModelStats, StoredPublicCatalogModel, StoredPublicGlobalModel,
    StoredPublicGlobalModelPage, UpdateAdminGlobalModelRecord, UpsertAdminProviderModelRecord,
};
use aether_data_contracts::repository::pool_scores::{
    GetPoolMemberScoresByIdsQuery, ListPoolMemberScoresQuery, ListRankedPoolMembersQuery,
    PoolMemberIdentity, PoolMemberProbeAttempt, PoolMemberProbeResult, PoolMemberScheduleFeedback,
    PoolMemberScoreWriteRepository, PoolScoreReadRepository, StoredPoolMemberScore,
    UpsertPoolMemberScore,
};
use aether_data_contracts::repository::provider_catalog::{
    ProviderCatalogKeyAdaptiveStateUpdate, ProviderCatalogKeyAdminCasUpdate,
    ProviderCatalogKeyCredentialsCasUpdate, ProviderCatalogKeyHealthStateUpdate,
    ProviderCatalogKeyListQuery, ProviderCatalogKeyOAuthCredentialCasDelete,
    ProviderCatalogKeyOAuthRuntimeStateCasUpdate, ProviderCatalogKeyRuntimeMetadataUpdate,
    ProviderCatalogKeyStatusSnapshotUpdate, ProviderCatalogProviderConfigCasUpdate,
    ProviderCatalogProxyCasUpdate, ProviderCatalogReadRepository, ProviderCatalogWriteRepository,
    StoredProviderCatalogEndpoint, StoredProviderCatalogKey,
    StoredProviderCatalogKeyMaintenanceSummary, StoredProviderCatalogKeyPage,
    StoredProviderCatalogKeyStats, StoredProviderCatalogProvider,
};
use aether_data_contracts::repository::quota::{
    ProviderQuotaReadRepository, ProviderQuotaWriteRepository, StoredProviderQuotaSnapshot,
};
use aether_data_contracts::repository::routing_profiles::{
    RoutingGroupReadRepository, RoutingGroupWriteRepository,
};
use aether_data_contracts::repository::settlement::{
    ReconcileUsagePolicyCostInput, ReleaseUsagePolicyRequestAdmissionInput,
    ReserveUsagePolicyCostInput, ReserveUsagePolicyCostOutcome, ReserveUsagePolicyRequestInput,
    ReserveUsagePolicyRequestOutcome, SettlementWriteRepository, StoredUsagePolicyCostReservation,
    StoredUsagePolicyRequestAdmission, StoredUsageSettlement, UsageSettlementInput,
};
use aether_data_contracts::repository::usage::{
    ApiKeyLastUsedDelta, ManagementTokenCounterDelta, ProxyNodeCounterDelta,
    StoredProviderUsageSummary, StoredRequestUsageAudit, UpsertUsageRecord, UsageReadRepository,
    UsageWriteRepository,
};
use aether_data_contracts::repository::video_tasks::{
    StoredVideoTask, UpsertVideoTask, VideoTaskLookupKey, VideoTaskModelCount,
    VideoTaskQueryFilter, VideoTaskReadRepository, VideoTaskStatusCount, VideoTaskWriteRepository,
};
use aether_runtime_state::RuntimeQueueStore;

pub(crate) use self::referrals::{
    LocalAuthRegistration, ReferralAdminStats, ReferralReconciliationSummary,
    ReferralRelationshipListQuery, ReferralRelationshipRecord, ReferralRewardConfig,
    ReferralRewardDetail, ReferralRewardListQuery, ReferralRewardRecord, ReferralUserDashboard,
};

#[derive(Clone, Default)]
pub(crate) struct GatewayDataState {
    config: GatewayDataConfig,
    backends: Option<DataBackends>,
    auth_api_key_reader: Option<Arc<dyn AuthApiKeyReadRepository>>,
    auth_api_key_writer: Option<Arc<dyn AuthApiKeyWriteRepository>>,
    auth_module_reader: Option<Arc<dyn AuthModuleReadRepository>>,
    auth_module_writer: Option<Arc<dyn AuthModuleWriteRepository>>,
    announcement_reader: Option<Arc<dyn AnnouncementReadRepository>>,
    announcement_writer: Option<Arc<dyn AnnouncementWriteRepository>>,
    management_token_reader: Option<Arc<dyn ManagementTokenReadRepository>>,
    management_token_writer: Option<Arc<dyn ManagementTokenWriteRepository>>,
    oauth_provider_reader: Option<Arc<dyn OAuthProviderReadRepository>>,
    oauth_provider_writer: Option<Arc<dyn OAuthProviderWriteRepository>>,
    proxy_node_reader: Option<Arc<dyn ProxyNodeReadRepository>>,
    proxy_node_writer: Option<Arc<dyn ProxyNodeWriteRepository>>,
    billing_reader: Option<Arc<dyn BillingReadRepository>>,
    background_task_reader: Option<Arc<dyn BackgroundTaskReadRepository>>,
    background_task_writer: Option<Arc<dyn BackgroundTaskWriteRepository>>,
    gemini_file_mapping_reader: Option<Arc<dyn GeminiFileMappingReadRepository>>,
    gemini_file_mapping_writer: Option<Arc<dyn GeminiFileMappingWriteRepository>>,
    global_model_reader: Option<Arc<dyn GlobalModelReadRepository>>,
    global_model_writer: Option<Arc<dyn GlobalModelWriteRepository>>,
    minimal_candidate_selection_reader: Option<Arc<dyn MinimalCandidateSelectionReadRepository>>,
    request_candidate_reader: Option<Arc<dyn RequestCandidateReadRepository>>,
    request_candidate_writer: Option<Arc<dyn RequestCandidateWriteRepository>>,
    provider_catalog_reader: Option<Arc<dyn ProviderCatalogReadRepository>>,
    provider_catalog_writer: Option<Arc<dyn ProviderCatalogWriteRepository>>,
    pool_score_reader: Option<Arc<dyn PoolScoreReadRepository>>,
    pool_score_writer: Option<Arc<dyn PoolMemberScoreWriteRepository>>,
    provider_quota_reader: Option<Arc<dyn ProviderQuotaReadRepository>>,
    provider_quota_writer: Option<Arc<dyn ProviderQuotaWriteRepository>>,
    routing_group_reader: Option<Arc<dyn RoutingGroupReadRepository>>,
    routing_group_writer: Option<Arc<dyn RoutingGroupWriteRepository>>,
    usage_reader: Option<Arc<dyn UsageReadRepository>>,
    usage_writer: Option<Arc<dyn UsageWriteRepository>>,
    user_reader: Option<Arc<dyn UserReadRepository>>,
    user_preferences: Option<Arc<RwLock<BTreeMap<String, StoredUserPreferenceRecord>>>>,
    usage_worker_queue: Option<Arc<dyn RuntimeQueueStore>>,
    video_task_reader: Option<Arc<dyn VideoTaskReadRepository>>,
    video_task_writer: Option<Arc<dyn VideoTaskWriteRepository>>,
    wallet_reader: Option<Arc<dyn WalletReadRepository>>,
    wallet_writer: Option<Arc<dyn WalletWriteRepository>>,
    settlement_writer: Option<Arc<dyn SettlementWriteRepository>>,
    system_config_values: Option<Arc<RwLock<BTreeMap<String, StoredSystemConfigEntry>>>>,
    billing_model_context_cache: Arc<BillingModelContextCacheState>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) enum BillingModelContextCacheKey {
    ByModelId {
        provider_id: String,
        provider_api_key_id: Option<String>,
        model_id: String,
    },
    ByGlobalModelName {
        provider_id: String,
        provider_api_key_id: Option<String>,
        global_model_name: String,
    },
}

impl CacheWeight for BillingModelContextCacheKey {
    fn cache_weight(&self) -> usize {
        match self {
            Self::ByModelId {
                provider_id,
                provider_api_key_id,
                model_id,
            } => value_weight::<Self>(
                provider_id.capacity()
                    + provider_api_key_id.as_ref().map_or(0, String::capacity)
                    + model_id.capacity(),
            ),
            Self::ByGlobalModelName {
                provider_id,
                provider_api_key_id,
                global_model_name,
            } => value_weight::<Self>(
                provider_id.capacity()
                    + provider_api_key_id.as_ref().map_or(0, String::capacity)
                    + global_model_name.capacity(),
            ),
        }
    }
}

/// Billing context lookups, cached with request coalescing.
///
/// The cache owns its TTL and byte budget, so both are decided at one boundary
/// instead of being threaded through every caller.
pub(super) struct BillingModelContextCacheState {
    entries: OnceMap<BillingModelContextCacheKey, BillingModelContextCacheValue, DataLayerError>,
}

/// Wrapper so the weight can live next to the cache that stores the value.
#[derive(Debug, Clone)]
pub(super) struct BillingModelContextCacheValue(pub(super) Option<Arc<StoredBillingModelContext>>);

impl CacheWeight for BillingModelContextCacheValue {
    fn cache_weight(&self) -> usize {
        let Some(context) = self.0.as_ref() else {
            return 0;
        };
        value_weight::<StoredBillingModelContext>(
            context.provider_id.capacity()
                + context
                    .provider_billing_type
                    .as_ref()
                    .map_or(0, String::capacity)
                + context
                    .provider_api_key_id
                    .as_ref()
                    .map_or(0, String::capacity)
                + context
                    .provider_api_key_rate_multipliers
                    .as_ref()
                    .map_or(0, CacheWeight::cache_weight)
                + context.global_model_id.capacity()
                + context.global_model_name.capacity()
                + context
                    .global_model_config
                    .as_ref()
                    .map_or(0, CacheWeight::cache_weight)
                + context
                    .default_tiered_pricing
                    .as_ref()
                    .map_or(0, CacheWeight::cache_weight)
                + context.model_id.as_ref().map_or(0, String::capacity)
                + context
                    .model_provider_model_name
                    .as_ref()
                    .map_or(0, String::capacity)
                + context
                    .model_config
                    .as_ref()
                    .map_or(0, CacheWeight::cache_weight)
                + context
                    .model_tiered_pricing
                    .as_ref()
                    .map_or(0, CacheWeight::cache_weight),
        )
    }
}

/// Answers go stale quickly because pricing edits must take effect, so the
/// window is short and the byte budget is the real bound.
const BILLING_MODEL_CONTEXT_CACHE_TTL: Duration = Duration::from_secs(30);
const BILLING_MODEL_CONTEXT_CACHE_CAPACITY_BYTES: usize = 8 * 1024 * 1024;
const BILLING_MODEL_CONTEXT_CACHE_MAX_INFLIGHT: usize = 4096;

impl Default for BillingModelContextCacheState {
    fn default() -> Self {
        Self {
            entries: OnceMap::builder()
                .capacity_bytes(BILLING_MODEL_CONTEXT_CACHE_CAPACITY_BYTES)
                .max_inflight(BILLING_MODEL_CONTEXT_CACHE_MAX_INFLIGHT)
                .build(),
        }
    }
}

impl BillingModelContextCacheState {
    pub(super) async fn get_or_load<F, Fut>(
        &self,
        key: BillingModelContextCacheKey,
        load: F,
    ) -> Result<Arc<BillingModelContextCacheValue>, OnceMapError<DataLayerError>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<BillingModelContextCacheValue, DataLayerError>>,
    {
        self.entries
            .get_or_load(key, BILLING_MODEL_CONTEXT_CACHE_TTL, load)
            .await
    }

    pub(super) fn invalidate_all(&self) {
        self.entries.invalidate_all();
    }
}

impl fmt::Debug for GatewayDataState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GatewayDataState")
            .field("config", &self.config)
            .field("has_backends", &self.backends.is_some())
            .field(
                "has_auth_api_key_reader",
                &self.auth_api_key_reader.is_some(),
            )
            .field(
                "has_auth_api_key_writer",
                &self.auth_api_key_writer.is_some(),
            )
            .field("has_auth_module_reader", &self.auth_module_reader.is_some())
            .field("has_auth_module_writer", &self.auth_module_writer.is_some())
            .field(
                "has_announcement_reader",
                &self.announcement_reader.is_some(),
            )
            .field(
                "has_announcement_writer",
                &self.announcement_writer.is_some(),
            )
            .field(
                "has_management_token_reader",
                &self.management_token_reader.is_some(),
            )
            .field(
                "has_management_token_writer",
                &self.management_token_writer.is_some(),
            )
            .field(
                "has_oauth_provider_reader",
                &self.oauth_provider_reader.is_some(),
            )
            .field(
                "has_oauth_provider_writer",
                &self.oauth_provider_writer.is_some(),
            )
            .field("has_proxy_node_reader", &self.proxy_node_reader.is_some())
            .field("has_proxy_node_writer", &self.proxy_node_writer.is_some())
            .field("has_billing_reader", &self.billing_reader.is_some())
            .field(
                "has_background_task_reader",
                &self.background_task_reader.is_some(),
            )
            .field(
                "has_background_task_writer",
                &self.background_task_writer.is_some(),
            )
            .field(
                "has_gemini_file_mapping_reader",
                &self.gemini_file_mapping_reader.is_some(),
            )
            .field(
                "has_gemini_file_mapping_writer",
                &self.gemini_file_mapping_writer.is_some(),
            )
            .field(
                "has_global_model_reader",
                &self.global_model_reader.is_some(),
            )
            .field(
                "has_global_model_writer",
                &self.global_model_writer.is_some(),
            )
            .field(
                "has_minimal_candidate_selection_reader",
                &self.minimal_candidate_selection_reader.is_some(),
            )
            .field(
                "has_request_candidate_reader",
                &self.request_candidate_reader.is_some(),
            )
            .field(
                "has_request_candidate_writer",
                &self.request_candidate_writer.is_some(),
            )
            .field(
                "has_provider_catalog_reader",
                &self.provider_catalog_reader.is_some(),
            )
            .field(
                "has_provider_catalog_writer",
                &self.provider_catalog_writer.is_some(),
            )
            .field("has_pool_score_reader", &self.pool_score_reader.is_some())
            .field("has_pool_score_writer", &self.pool_score_writer.is_some())
            .field(
                "has_provider_quota_reader",
                &self.provider_quota_reader.is_some(),
            )
            .field(
                "has_provider_quota_writer",
                &self.provider_quota_writer.is_some(),
            )
            .field(
                "has_routing_group_reader",
                &self.routing_group_reader.is_some(),
            )
            .field(
                "has_routing_group_writer",
                &self.routing_group_writer.is_some(),
            )
            .field("has_usage_reader", &self.usage_reader.is_some())
            .field("has_usage_writer", &self.usage_writer.is_some())
            .field("has_user_preferences", &self.user_preferences.is_some())
            .field("has_usage_worker_queue", &self.usage_worker_queue.is_some())
            .field("has_video_task_reader", &self.video_task_reader.is_some())
            .field("has_video_task_writer", &self.video_task_writer.is_some())
            .field("has_wallet_reader", &self.wallet_reader.is_some())
            .field("has_wallet_writer", &self.wallet_writer.is_some())
            .field("has_settlement_writer", &self.settlement_writer.is_some())
            .field(
                "has_system_config_values",
                &self.system_config_values.is_some(),
            )
            .finish()
    }
}

mod auth;
mod auth_api_key_cache;
mod candidate_cache;
mod catalog;
mod core;
mod integrations;
mod models;
mod pool_scores;
mod provider_catalog_cache;
mod referrals;
mod request_candidate_cache;
mod routing_group_cache;
mod routing_profiles;
mod runtime;
#[cfg(test)]
mod testing;
#[cfg(feature = "testkit")]
pub(crate) mod testkit;
