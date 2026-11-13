<template>
  <section
    class="min-w-0 rounded-2xl border bg-card p-4 shadow-sm sm:p-5"
    data-user-payments
  >
    <div class="mb-4">
      <h2 class="text-sm font-semibold">
        {{ t('overview.rechargeAndPlanRecords') }}
      </h2>
      <p class="mt-1 text-xs text-muted-foreground">
        {{ t('overview.reportedByCreditTimeGiftCreditsAreListedSeparately') }}
      </p>
    </div>
    <div
      class="overflow-x-auto"
      :aria-busy="loading"
    >
      <table class="w-full min-w-[650px] text-sm">
        <thead class="border-y text-xs text-muted-foreground">
          <tr>
            <th class="py-3 text-left font-medium">
              {{ t('overview.order') }}
            </th><th class="px-3 py-3 text-left font-medium">
              {{ t('ui.typeba40014') }}
            </th><th class="px-3 py-3 text-left font-medium">
              {{ t('ui.paymentMethod') }}
            </th><th class="px-3 py-3 text-right font-medium">
              {{ t('overview.creditedAmount') }}
            </th><th class="py-3 text-right font-medium">
              {{ t('overview.creditedAt') }}
            </th>
          </tr>
        </thead>
        <tbody>
          <tr
            v-for="payment in payments?.items || []"
            :key="payment.id"
            class="border-b last:border-0"
          >
            <td class="max-w-56 break-all py-3 font-mono text-xs">
              {{ payment.order_no || payment.id }}
            </td><td class="whitespace-nowrap px-3 py-3">
              {{ kindLabel(payment.kind) }}
            </td><td class="px-3 py-3 text-muted-foreground">
              {{ methodLabel(payment.payment_method) }}
            </td><td class="whitespace-nowrap px-3 py-3 text-right font-medium tabular-nums">
              {{ money(payment.amount) }}
            </td><td class="whitespace-nowrap py-3 text-right text-xs text-muted-foreground">
              {{ timestamp(payment.credited_at, timezone) }}
            </td>
          </tr>
          <tr v-if="!payments?.items.length">
            <td
              colspan="5"
              class="py-12 text-center text-muted-foreground"
            >
              {{ loading ? t('overview.loading') : !payments ? t('overview.creditRecordsUnavailable') : t('overview.noCreditedPaymentsInThisPeriod') }}
            </td>
          </tr>
        </tbody>
      </table>
    </div>
    <OverviewPagination
      v-if="payments"
      :total="payments.total"
      :offset="offset"
      :limit="limit"
      :loading="loading"
      @page="value => $emit('page', value)"
      @size="value => $emit('size', value)"
    />
  </section>
</template>

<script setup lang="ts">
import type { OverviewPage, OverviewUserPayment } from '@/api/overview'
import OverviewPagination from '../components/OverviewPagination.vue'
import { money, timestamp } from '../format'
import { useI18n } from '@/i18n'
defineProps<{ payments?: OverviewPage<OverviewUserPayment> | null; timezone: string; loading: boolean; limit: number; offset: number }>()
defineEmits<{ page: [value: number]; size: [value: number] }>()
const { t } = useI18n()
function kindLabel(kind: string) {
  return ({ wallet_recharge: t('overview.walletRecharge'), plan_purchase: t('overview.planPurchase'), gift_credit: t('overview.giftCredit') })[kind] || kind
}
function methodLabel(method: string) {
  return ({ alipay: t('ui.alipay'), wechat: t('ui.wechatPay'), stripe: 'Stripe', manual: t('overview.manualCredit'), gift_code: t('overview.giftCode') })[method] || method || '-'
}
</script>
