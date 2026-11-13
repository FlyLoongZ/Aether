<template>
  <div class="grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-4">
    <section
      class="rounded-2xl border bg-card p-5 shadow-sm"
      data-user-summary="consumption"
    >
      <p class="text-xs font-medium text-muted-foreground">
        {{ t('overview.consumption') }}
      </p>
      <p class="mt-3 break-all text-2xl font-semibold tabular-nums">
        {{ money(summary?.billable_amount) }}
      </p>
      <p class="mt-3 flex flex-wrap gap-x-3 gap-y-1 text-xs text-muted-foreground tabular-nums">
        <span class="whitespace-nowrap">{{ t('poolQuota.claudeCode.unknownWindow') }} {{ money(summary?.quota_covered_amount) }}</span>
        <span class="whitespace-nowrap">{{ t('ui.balance') }} {{ money(summary?.wallet_debit_amount) }}</span>
      </p>
    </section>
    <section
      class="rounded-2xl border bg-card p-5 shadow-sm"
      data-user-summary="recharge"
    >
      <p class="text-xs font-medium text-muted-foreground">
        {{ t('overview.recharges') }}
      </p>
      <p class="mt-3 break-all text-2xl font-semibold tabular-nums">
        {{ money(finance?.recharge_amount) }}
      </p>
      <p class="mt-3 flex flex-wrap gap-x-3 gap-y-1 text-xs text-muted-foreground tabular-nums">
        <span class="whitespace-nowrap">{{ t('overview.plans') }} {{ money(finance?.plan_purchase_amount) }}</span>
        <span class="whitespace-nowrap">{{ t('overview.gifts') }} {{ money(finance?.gift_credit_amount) }}</span>
      </p>
    </section>
    <section
      class="rounded-2xl border bg-card p-5 shadow-sm"
      data-user-summary="balance"
    >
      <p class="text-xs font-medium text-muted-foreground">
        {{ t('ui.balance') }}
      </p>
      <p class="mt-3 break-all text-2xl font-semibold tabular-nums">
        {{ money(finance?.wallet_balance) }}
      </p>
      <p class="mt-3 flex flex-wrap gap-x-3 gap-y-1 text-xs text-muted-foreground tabular-nums">
        <span class="whitespace-nowrap">{{ t('overview.recharges') }} {{ money(finance?.recharge_balance) }}</span>
        <span class="whitespace-nowrap">{{ t('overview.gifts') }} {{ money(finance?.gift_balance) }}</span>
      </p>
    </section>
    <section
      class="rounded-2xl border bg-card p-5 shadow-sm"
      data-user-summary="activity"
    >
      <p class="text-xs font-medium text-muted-foreground">
        {{ detail ? t('ui.nodeRequests') : t('overview.users') }}
      </p>
      <p
        class="mt-3 text-2xl font-semibold tabular-nums"
        :title="detail ? undefined : t('overview.activeUsersTotalUsers')"
      >
        {{ count(detail ? summary?.request_count : activeUserCount) }}<span
          v-if="!detail"
          class="ml-2 text-sm font-normal text-muted-foreground"
        >/ {{ count(userCount) }}</span>
      </p>
      <p class="mt-3 flex flex-wrap gap-x-3 gap-y-1 text-xs text-muted-foreground tabular-nums">
        <span class="whitespace-nowrap">{{ detail ? t('overview.successful') : t('ui.nodeRequests') }} {{ count(detail ? summary?.successful_request_count : summary?.request_count) }}</span>
        <span class="whitespace-nowrap">Tokens {{ count(summary?.total_tokens) }}</span>
      </p>
    </section>
  </div>
</template>

<script setup lang="ts">
import type { OverviewMetrics, OverviewUserFinance } from '@/api/overview'
import { count, money } from '../format'
import { useI18n } from '@/i18n'
defineProps<{ summary?: OverviewMetrics | null; finance?: OverviewUserFinance | null; userCount?: number; activeUserCount?: number; detail?: boolean }>()
const { t } = useI18n()
</script>
