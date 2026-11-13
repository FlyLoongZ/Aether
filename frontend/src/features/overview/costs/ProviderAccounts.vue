<template>
  <section
    class="space-y-4"
    data-provider-accounts
  >
    <div class="flex flex-wrap items-center justify-between gap-2">
      <h2 class="text-sm font-semibold">
        {{ t('overview.balancesAndSubscriptions') }}
      </h2>
      <span class="text-xs text-muted-foreground">{{ t('overview.currentAccountStatus') }}</span>
    </div>
    <div
      v-if="accounts.length"
      class="grid grid-cols-1 gap-4 xl:grid-cols-2"
    >
      <Card
        v-for="account in orderedAccounts"
        :key="account.provider_id"
        class="min-w-0 space-y-4 p-4 sm:p-5"
      >
        <div class="flex items-start justify-between gap-3">
          <div class="min-w-0">
            <h3 class="break-words text-sm font-semibold">
              {{ account.provider_name }}
            </h3>
            <p class="mt-1 text-xs text-muted-foreground">
              {{ billingLabel(account.billing_type) }}<span v-if="!account.is_active"> · {{ t('ui.disableda8c3698') }}</span>
            </p>
          </div>
          <span
            v-if="attention(account)"
            class="shrink-0 rounded-full bg-amber-500/10 px-2 py-1 text-xs text-amber-700 dark:text-amber-400"
          >{{ attention(account) }}</span>
        </div>
        <div class="grid grid-cols-2 gap-4">
          <div>
            <p class="text-xs text-muted-foreground">
              {{ t('overview.upstreamBalance') }}
            </p>
            <p class="mt-2 text-xl font-semibold tabular-nums">
              {{ amount(account.balance?.available, account.balance?.currency) }}
            </p>
            <p class="mt-1 text-xs text-muted-foreground">
              {{ account.balance?.observed_at ? `${t('overview.updatedAt')} ${timestamp(account.balance.observed_at, timezone)}` : t('overview.noBalanceSnapshot') }}
            </p>
            <p
              v-if="account.balance && account.balance.status !== 'success'"
              class="mt-1 text-xs text-amber-700 dark:text-amber-400"
            >
              {{ t('overview.lastQueryUnsuccessful') }}
            </p>
          </div>
          <div>
            <p class="text-xs text-muted-foreground">
              {{ t('overview.configuredPlanRemaining') }}
            </p>
            <p class="mt-2 text-xl font-semibold tabular-nums">
              {{ amount(account.quota?.remaining, account.quota?.currency) }}
            </p>
            <p class="mt-1 text-xs text-muted-foreground">
              {{ account.quota ? `${t('overview.periodQuota')} ${amount(account.quota.limit, account.quota.currency)}` : t('overview.noConfiguredPlanQuota') }}
            </p>
          </div>
        </div>
        <div
          v-if="account.quota"
          class="space-y-2"
        >
          <div class="flex flex-wrap justify-between gap-2 text-xs text-muted-foreground">
            <span>{{ t('ui.used9845c16') }} {{ amount(account.quota.used, account.quota.currency) }}</span>
            <span>{{ account.quota.expires_at ? `${t('overview.expires')} ${timestamp(account.quota.expires_at, timezone)}` : t('overview.noExpiryConfigured') }}</span>
          </div>
          <div class="h-1.5 overflow-hidden rounded-full bg-muted">
            <div
              class="h-full rounded-full"
              :class="quotaRatio(account) >= 90 ? 'bg-amber-500' : 'bg-primary/65'"
              :style="{ width: `${quotaRatio(account)}%` }"
            />
          </div>
        </div>
        <div
          v-if="account.balance?.plan_name || account.balance?.subscriptions.length"
          class="space-y-3 border-t pt-3"
        >
          <p
            v-if="account.balance.plan_name"
            class="text-xs font-medium"
          >
            {{ t('overview.upstreamPlan') }} · {{ account.balance.plan_name }}
          </p>
          <div
            v-for="(plan, index) in account.balance.subscriptions"
            :key="index"
            class="space-y-1 text-xs"
          >
            <div class="flex flex-wrap justify-between gap-2">
              <span class="font-medium">{{ plan.group_name || t('overview.subscription') }}</span>
              <span class="text-muted-foreground">{{ plan.expires_at ? timestamp(plan.expires_at, timezone) : t('overview.noExpiryReported') }}</span>
            </div>
            <p class="text-muted-foreground">
              {{ planUsage(plan) }}
            </p>
          </div>
        </div>
      </Card>
    </div>
    <Card
      v-else
      class="p-10 text-center text-sm text-muted-foreground"
    >
      {{ t('overview.noProviderAccounts') }}
    </Card>
  </section>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { Card } from '@/components/ui'
import type { ProviderAccount, ProviderAccountSubscription } from '@/api/providerFinance'
import { money, timestamp } from '../format'
import { useI18n } from '@/i18n'
const props = defineProps<{ accounts: ProviderAccount[]; timezone: string; observedAt?: string }>()
const { t } = useI18n()
function amount(value: string | number | null | undefined, currency: string | null = 'USD') { return value == null || !currency || !/^[a-z]{3}$/i.test(currency) ? '-' : money({ value: String(value), currency, basis: 'provider_account', status: 'known' }) }
function billingLabel(value: string | null) {
  return value === 'monthly_quota' ? t('overview.periodPlan') : value === 'free_tier' ? t('overview.freeTier') : value === 'pay_as_you_go' ? t('overview.payAsYouGo') : t('overview.billingTypeNotConfigured')
}
function quotaRatio(account: ProviderAccount) {
  return account.quota && Number(account.quota.limit) > 0 ? Math.min(100, Math.max(0, Number(account.quota.used) / Number(account.quota.limit) * 100)) : 0
}
function attention(account: ProviderAccount) {
  if (!account.is_active) return ''
  const now = props.observedAt ? Date.parse(props.observedAt) : Date.now()
  const end = account.quota?.expires_at ? Date.parse(account.quota.expires_at) : NaN
  if (Number.isFinite(end) && end <= now) return t('overview.planExpired')
  if (Number.isFinite(end) && end - now <= 7 * 86_400_000) return t('overview.expiresWithin7Days')
  if (account.quota?.remaining != null && Number(account.quota.remaining) <= 0) return t('overview.planQuotaExhausted')
  if (quotaRatio(account) >= 90) return t('overview.under10QuotaRemaining')
  return ''
}
const orderedAccounts = computed(() => [...props.accounts].sort((a, b) => Number(Boolean(attention(b))) - Number(Boolean(attention(a))) || a.provider_name.localeCompare(b.provider_name)))
function planUsage(plan: ProviderAccountSubscription) {
  return [
    [t('overview.daily'), plan.daily_used_usd, plan.daily_limit_usd],
    [t('overview.weekly'), plan.weekly_used_usd, plan.weekly_limit_usd],
    [t('overview.monthly'), plan.monthly_used_usd, plan.monthly_limit_usd],
  ].filter(([, , limit]) => limit != null).map(([label, used, limit]) => `${label} ${amount(used)} / ${amount(limit)}`).join(' · ') || t('overview.quotaDetailsUnavailable')
}
</script>
