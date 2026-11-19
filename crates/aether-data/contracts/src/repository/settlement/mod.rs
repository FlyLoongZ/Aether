mod types;

pub use types::{
    finite_wallet_available_usd, plan_finite_wallet_debit, settlement_billable_cost_usd,
    settlement_billing_status_for_usage_status, validate_wallet_settlement_values,
    ReconcileUsagePolicyCostInput, ReleaseUsagePolicyRequestAdmissionInput,
    ReserveUsagePolicyCostInput, ReserveUsagePolicyCostOutcome, ReserveUsagePolicyRequestInput,
    ReserveUsagePolicyRequestOutcome, SettlementRepository, SettlementWriteRepository,
    StoredUsagePolicyCostReservation, StoredUsagePolicyRequestAdmission, StoredUsageSettlement,
    UsagePolicyCostReservationState, UsagePolicyCostWindow, UsagePolicyRequestAdmissionState,
    UsagePolicyRequestWindow, UsageSettlementInput, WalletDebitPlan, SETTLEMENT_EPSILON_USD,
};

#[cfg(test)]
mod tests {
    use super::settlement_billing_status_for_usage_status;

    #[test]
    fn cancelled_usage_status_is_billable() {
        assert_eq!(
            settlement_billing_status_for_usage_status("completed"),
            "settled"
        );
        assert_eq!(
            settlement_billing_status_for_usage_status("cancelled"),
            "settled"
        );
        assert_eq!(settlement_billing_status_for_usage_status("failed"), "void");
    }
}
