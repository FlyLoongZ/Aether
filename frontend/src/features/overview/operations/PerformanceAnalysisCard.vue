<template>
  <Card
    class="min-w-0 overflow-hidden"
    :data-performance-analysis="dimension"
  >
    <div class="flex flex-wrap items-center justify-between gap-x-3 gap-y-1 px-4 pt-4 sm:px-5">
      <h3 class="text-sm font-semibold">
        {{ title }}
      </h3>
      <p
        v-if="rows !== null"
        class="text-xs text-muted-foreground"
      >
        {{ count(rows.length) }} {{ dimension === 'provider' ? t('overview.providers') : t('ui.models') }}
      </p>
    </div>

    <div class="mx-4 my-4 grid grid-cols-2 divide-x rounded-xl bg-muted/40 sm:mx-5">
      <div
        v-for="insight in insightCards"
        :key="insight.key"
        class="min-w-0 px-3 py-3"
        :data-analysis-insight="insight.key"
      >
        <p class="text-xs text-muted-foreground">
          {{ insight.label }}
          <span v-if="insight.value.partial"> · {{ t('overview.knownData') }}</span>
        </p>
        <p
          class="mt-1 truncate text-sm font-semibold"
          :title="insight.value.row?.label"
        >
          {{ insight.value.row?.label || insightState(insight.value.state) }}
        </p>
        <p class="mt-1 text-xs tabular-nums text-muted-foreground">
          <template v-if="insight.value.row">
            <template v-if="insight.key === 'failures'">
              {{ count(insight.value.row.failedCount) }} {{ t('overview.failures') }} ·
            </template>
            {{ insight.key === 'traffic' ? t('overview.ofRequests') : t('overview.ofFailures') }} {{ percentage(insight.value.share) }}
          </template>
          <template v-else>
            {{ t('overview.selectedRange') }}
          </template>
        </p>
      </div>
    </div>

    <div class="max-h-[400px] overflow-auto px-4 pb-2 sm:px-5">
      <table class="w-full min-w-[660px] table-fixed text-sm">
        <thead class="sticky top-0 z-10 bg-card text-xs text-muted-foreground">
          <tr class="border-b">
            <th class="w-[22%] py-3 pr-3 text-left font-medium">
              {{ dimension === 'provider' ? t('overview.provider') : t('ui.model') }}
            </th>
            <th class="w-[16%] px-2 py-3 text-right font-medium">
              {{ t('overview.requests') }}
            </th>
            <th class="w-[16%] px-2 py-3 text-right font-medium">
              {{ t('overview.successRate') }}
            </th>
            <th
              class="w-[10%] px-2 py-3 text-right font-medium"
              :title="t('overview.httpAndResponsesWebsocketRequestsExecutingOnThisNodeStreamsCountUntilTheyEnd')"
            >
              {{ t('overview.concurrency') }}
              <span class="block text-[10px] font-normal">{{ t('overview.currentNode') }}</span>
            </th>
            <th
              class="w-[10%] px-2 py-3 text-right font-medium"
              :title="rpmWindowTitle"
            >
              RPM
              <span class="block text-[10px] font-normal">{{ t('overview.last60s') }}</span>
            </th>
            <th class="w-[13%] px-2 py-3 text-right font-medium">
              {{ t('overview.avgFirstByte') }}
            </th>
            <th class="w-[13%] py-3 pl-2 text-right font-medium">
              {{ t('overview.outputSpeed') }}
              <span class="block text-[10px] font-normal">Tokens/s</span>
            </th>
          </tr>
        </thead>
        <tbody class="divide-y">
          <tr
            v-for="row in rankedRows"
            :key="`${row.id ?? ''}:${row.label}`"
            :data-analysis-row="row.id ?? row.label"
          >
            <td class="break-words py-3 pr-3 [overflow-wrap:anywhere]">
              <RouterLink
                v-if="row.id !== null"
                :to="records(row.id)"
                :data-analysis-entity-link="row.id"
                class="font-medium hover:text-primary hover:underline"
              >
                {{ row.label }}
              </RouterLink>
              <span v-else>{{ row.label }}</span>
            </td>
            <td class="px-2 py-3 text-right tabular-nums">
              {{ count(row.requestCount) }}
              <div class="mt-1 flex items-center justify-end gap-2 text-[11px] text-muted-foreground">
                <span class="h-1 w-9 overflow-hidden rounded-full bg-muted">
                  <span
                    class="block h-full rounded-full bg-primary/45"
                    :style="{ width: `${analysisShare(row.requestCount, totalRequests) ?? 0}%` }"
                  />
                </span>
                <span>{{ percentage(analysisShare(row.requestCount, totalRequests)) }}</span>
              </div>
            </td>
            <td class="px-2 py-3 text-right tabular-nums">
              <span data-analysis-success-rate>{{ percentage(row.successRate) }}</span>
              <div
                class="mt-1 text-[11px]"
                :class="(measuredCount(row.failedCount) ?? 0) > 0 ? 'text-amber-700 dark:text-amber-400' : 'text-muted-foreground'"
              >
                <RouterLink
                  v-if="row.id !== null && measuredCount(row.failedCount) !== null"
                  :to="records(row.id, true)"
                  :data-analysis-failure-link="row.id"
                  class="hover:underline"
                  :aria-label="t('overview.failedRequestsFor', { label: row.label })"
                >
                  {{ t('ui.failed28384d7') }} {{ count(row.failedCount) }}
                </RouterLink>
                <span v-else>{{ t('ui.failed28384d7') }} {{ count(row.failedCount) }}</span>
              </div>
            </td>
            <td
              class="px-2 py-3 text-right tabular-nums"
              data-analysis-concurrency
            >
              {{ count(row.concurrency ?? null) }}
            </td>
            <td
              class="px-2 py-3 text-right tabular-nums"
              data-analysis-rpm
              :title="rpmWindowTitle"
            >
              {{ count(row.rpm ?? null) }}
            </td>
            <td class="px-2 py-3 text-right tabular-nums">
              {{ duration(row.firstByteMs) }}
            </td>
            <td class="py-3 pl-2 text-right tabular-nums">
              {{ count(row.outputTps) }}
            </td>
          </tr>
          <tr v-if="!rankedRows.length">
            <td
              colspan="7"
              class="py-10 text-center text-xs text-muted-foreground"
            >
              {{ rows === null ? t('overview.analysisDataUnavailable') : totalRequests === 0 ? t('overview.noRequestsInThisRange') : t('overview.noGroupedDataInThisRange') }}
            </td>
          </tr>
        </tbody>
      </table>
    </div>
  </Card>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { RouterLink } from 'vue-router'
import { Card } from '@/components/ui'
import { count as formatCount } from '../format'
import { useOverviewQuery } from '../query'
import { analysisShare, analyzeRows, measuredCount, rankAnalysisRows, type AnalysisInsight, type AnalysisRow } from './analysisHelpers'
import { useI18n } from '@/i18n'

const props = defineProps<{
  title: string
  dimension: 'provider' | 'model'
  rows: AnalysisRow[] | null
  totalRequests: number | null
  totalFailures: number | null
  liveWindowSeconds?: number
}>()
const { t } = useI18n()
const { link } = useOverviewQuery('today')
const rpmWindowTitle = computed(() => props.liveWindowSeconds !== undefined && props.liveWindowSeconds < 60
  ? t('overview.nodeObservedForSeconds', { seconds: formatCount(props.liveWindowSeconds) })
  : t('overview.httpAndResponsesWebsocketRequestsEnteringUpstreamExecutionOnThisNodeInTheLast60SecondsRetriesDeduplicated'))
const rankedRows = computed(() => rankAnalysisRows(props.rows ?? []))
const insights = computed(() => analyzeRows(props.rows, props.totalRequests, props.totalFailures))
const insightCards = computed(() => [
  { key: 'traffic', label: t('overview.mostTraffic'), value: insights.value.traffic },
  { key: 'failures', label: t('overview.mostFailures'), value: insights.value.failures },
])

function records(id: string, failed = false) {
  return link('/admin/usage', { [props.dimension === 'provider' ? 'provider_id' : 'model']: id, ...(failed ? { status: 'failed' } : {}) })
}
function count(value: number | null) {
  const measured = measuredCount(value)
  return formatCount(measured)
}
function percentage(value: number | null) {
  const measured = measuredCount(value)
  return measured === null || measured > 100 ? '-' : `${count(measured)}%`
}
function duration(value: number | null) {
  const measured = measuredCount(value)
  if (measured === null) return '-'
  return measured >= 1000 ? `${count(measured / 1000)} s` : `${count(measured)} ms`
}
function insightState(state: AnalysisInsight['state']) {
  if (state === 'empty') return t('overview.noRequests')
  if (state === 'none') return t('overview.noFailedRequests')
  return t('overview.unavailable')
}
</script>
