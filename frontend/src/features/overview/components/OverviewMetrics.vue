<template>
  <div
    v-if="isOperations"
    class="grid grid-cols-2 gap-3 sm:grid-cols-3 sm:gap-4 xl:grid-cols-6"
  >
    <OperationsMetricCard
      v-for="(item, index) in items"
      :key="item.label"
      :label="item.label"
      :value="item.value"
      :note="item.note"
      :icon="metricIcons[index]"
    />
  </div>
  <dl
    v-else
    class="grid grid-cols-2 gap-x-5 gap-y-5 border-y py-5 sm:grid-cols-3 xl:grid-cols-6"
  >
    <div
      v-for="item in items"
      :key="item.label"
      class="min-w-0"
    >
      <dt class="text-xs text-muted-foreground">
        {{ item.label }}
      </dt>
      <dd class="mt-2 font-semibold tabular-nums">
        <MetricValue :value="item.value" />
      </dd>
      <p
        v-if="item.note"
        class="mt-1 text-xs text-muted-foreground"
      >
        {{ item.note }}
      </p>
    </div>
  </dl>
  <p
    v-if="metrics.usage_source_counts && mode !== 'cost'"
    class="flex flex-wrap gap-x-4 gap-y-1 text-xs text-muted-foreground"
  >
    <span>{{ t('overview.tokenSourceRequests') }}</span>
    <span
      v-for="source in tokenSources"
      :key="source.key"
    >{{ source.label }} {{ count(metrics.usage_source_counts[source.key]) }}</span>
  </p>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { Activity, CheckCircle2, Clock, Gauge, Hash, Timer, TriangleAlert, Zap } from '@lucide/vue'
import type { OverviewMetrics } from '@/api/overview'
import { count, money, percent } from '../format'
import MetricValue from './MetricValue.vue'
import OperationsMetricCard from '../operations/OperationsMetricCard.vue'
import { useI18n } from '@/i18n'
const props = defineProps<{ metrics: OverviewMetrics; mode?: 'global' | 'employee' | 'cost' | 'performance' | 'runtime' }>()
const { t } = useI18n()
const isOperations = computed(() => props.mode === 'runtime' || props.mode === 'performance')
const metricIcons = computed(() => props.mode === 'performance'
  ? [Activity, CheckCircle2, Clock, Timer, Gauge, Zap]
  : [Activity, CheckCircle2, Hash, TriangleAlert, Clock, Gauge])
const tokenSources = computed(() => [
  { key: 'reported' as const, label: t('overview.reported') },
  { key: 'estimated' as const, label: t('overview.estimated') },
  { key: 'mixed' as const, label: t('providers.migration.mixeda4a06e') },
  { key: 'unknown' as const, label: t('chart.unknown') },
])
const items = computed<{ label: string; value: string; note?: string }[]>(() => {
  const m = props.metrics
  if (props.mode === 'cost') return [
    { label: t('overview.ratedAmount'), value: money(m.rated_amount) },
    { label: t('overview.billableConsumption'), value: money(m.billable_amount) },
    { label: t('overview.quotaCovered'), value: money(m.quota_covered_amount) },
    { label: t('overview.walletConsumed'), value: money(m.wallet_consumed_amount) },
    { label: t('overview.walletDebited'), value: money(m.wallet_debit_amount) },
    { label: t('stats.metric.requests'), value: count(m.request_count) },
  ]
  if (props.mode === 'performance') return [
    { label: t('stats.metric.requests'), value: count(m.request_count), note: `${count(m.total_tokens)} Tokens` },
    { label: t('overview.requestSuccessRate'), value: percent(m.success_rate?.value), note: `${count(m.success_rate?.numerator)}/${count(m.success_rate?.denominator)}` },
    { label: t('overview.avgResponseMs'), value: count(m.latency_ms?.avg) },
    ...(['p50', 'p95', 'p99'] as const).map(key => ({
      label: `${key.toUpperCase()} (ms)`, value: count(m.latency_ms?.[key]),
      note: key === 'p99' ? `${count(m.latency_ms?.sample_count)} ${t('overview.samples')}` : undefined,
    })),
  ]
  if (props.mode === 'runtime') return [
    { label: t('stats.metric.requests'), value: count(m.request_count) },
    { label: t('overview.requestSuccessRate'), value: percent(m.success_rate?.value), note: `${count(m.success_rate?.numerator)}/${count(m.success_rate?.denominator)}` },
    { label: 'Tokens', value: count(m.total_tokens) },
    { label: t('overview.failedRequests'), value: count(m.failed_request_count) },
    { label: t('overview.avgResponseMs'), value: count(m.latency_ms?.avg) },
    { label: 'P99 (ms)', value: count(m.latency_ms?.p99), note: `${count(m.latency_ms?.sample_count)} ${t('overview.samples')}` },
  ]
  return [
    { label: t('stats.metric.requests'), value: count(m.request_count) },
    { label: t('overview.requestSuccessRate'), value: percent(m.success_rate?.value), note: `${count(m.success_rate?.numerator)}/${count(m.success_rate?.denominator)}` },
    { label: 'Tokens', value: count(m.total_tokens) },
    { label: t('overview.billableConsumption'), value: money(m.billable_amount), note: m.billable_amount?.status === 'known_subtotal' ? t('overview.knownSubtotal') : undefined },
    props.mode === 'employee' ? { label: t('overview.quotaCovered'), value: money(m.quota_covered_amount) } : { label: t('overview.membersWithUsage'), value: count(m.usage_active_users) },
    props.mode === 'employee' ? { label: t('overview.walletConsumed'), value: money(m.wallet_consumed_amount) } : { label: t('overview.enabledMembers'), value: count(m.enabled_users) },
  ]
})
</script>
