<template>
  <main class="space-y-5 px-4 pb-8 sm:px-6 lg:px-0">
    <OverviewToolbar
      :title="t('nav.userStats')"
      :range="range"
      :show-range="false"
      :refresh-active="autoRefresh"
      :refresh-title="refreshTitle"
      @update:range="setRange"
      @refresh="toggleAutoRefresh"
    >
      <template #range-picker>
        <TimeRangePicker
          :model-value="{ ...range, preset: relativePreset || undefined }"
          :preset-options="['last1hour', 'today', 'yesterday', 'last24hours', 'last7days', 'last30days', 'last90days', 'custom']"
          :show-granularity="false"
          @update:model-value="handleRangePicker"
        />
      </template>
      <Button
        variant="outline"
        size="sm"
        :disabled="exporting"
        @click="exportCsv('users', requestQuery)"
      >
        <Download class="mr-2 h-4 w-4" />{{ t('overview.exportUserReport') }}
      </Button>
    </OverviewToolbar>
    <OverviewStatus
      :error="error || exportError"
      @retry="refreshAll"
    />
    <UserFinanceSummary
      :summary="data?.data.summary"
      :finance="data?.data.finance_summary"
      :user-count="data?.data.summary?.user_count"
      :active-user-count="data?.data.summary?.active_user_count"
    />
    <UserReports :revision="revision">
      <template #additional>
        <UserUsageStats
          :range="range"
          :revision="revision"
        >
          <template #user-leaderboard="{ selectUser }">
            <TableCard
              :title="t('overview.userRankingsAndAccounts')"
              :description="t('overview.sortByConsumptionRequestsOrTokensConsumptionAndCreditsFollowTheSelectedPeriodBalancesAreCurrent')"
              class="relative min-w-0"
              data-user-accounts
            >
              <template #actions>
                <form
                  class="flex w-full items-center gap-2 md:w-auto"
                  @submit.prevent="patch({ search: search.trim() || undefined, offset: undefined })"
                >
                  <div class="relative min-w-0 flex-1 md:w-48 md:flex-none">
                    <Search class="absolute left-2.5 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-muted-foreground z-10 pointer-events-none" />
                    <Input
                      v-model="search"
                      type="search"
                      :placeholder="t('userStats.search.user')"
                      :aria-label="t('userStats.search.user')"
                      class="h-8 w-full text-xs border-border/60 pl-8"
                    />
                  </div>
                  <Button
                    type="submit"
                    size="sm"
                    variant="outline"
                    class="h-8 text-xs"
                  >
                    {{ t('overview.search') }}
                  </Button>
                </form>
              </template>
              <p
                v-if="query.search"
                class="border-b border-border/60 px-4 py-3 text-xs text-muted-foreground sm:px-6"
              >
                {{ t('overview.theSummaryAndAccountsIncludeMatchingUsersOnly') }}
              </p>
              <div
                class="min-w-0"
                :aria-busy="loading"
              >
                <Table class="min-w-[1120px]">
                  <TableHeader>
                    <TableRow class="border-b border-border/60 hover:bg-transparent">
                      <TableHead class="h-12 w-16 font-semibold">
                        {{ t('overview.no') }}
                      </TableHead>
                      <TableHead class="h-12 font-semibold">
                        {{ t('auth.role.user') }}
                      </TableHead>
                      <SortableTableHead
                        v-for="column in columns"
                        :key="column.key"
                        class="h-12 font-semibold text-right"
                        :column-key="column.key"
                        :active-key="requestQuery.sort"
                        :direction="requestQuery.order"
                        default-direction="desc"
                        align="right"
                        @sort="sort"
                      >
                        {{ column.label }}
                      </SortableTableHead>
                      <TableHead class="h-12 font-semibold text-right">
                        {{ t('overview.recharges') }}
                      </TableHead>
                      <TableHead class="h-12 font-semibold text-right">
                        {{ t('ui.balance') }}
                      </TableHead>
                      <TableHead class="h-12 font-semibold text-right">
                        {{ t('ui.lastUsed39a9046') }}
                      </TableHead>
                      <TableHead class="h-12 font-semibold text-right">
                        {{ t('ui.nodeActions') }}
                      </TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    <TableRow
                      v-for="(user, index) in data?.data.items || []"
                      :key="user.user_id"
                      class="border-b border-border/40 hover:bg-muted/30 transition-colors h-[72px]"
                    >
                      <TableCell
                        class="py-4 text-xs font-medium tabular-nums"
                        data-user-rank
                      >
                        {{ (data?.data.offset ?? requestQuery.offset) + index + 1 }}
                      </TableCell>
                      <TableCell class="max-w-64 py-4 text-xs">
                        <button
                          type="button"
                          class="break-words hover:text-primary hover:underline"
                          @click="accountUser = user"
                        >
                          {{ user.username }}
                        </button>
                        <span
                          v-if="!user.is_active"
                          class="ml-2 text-xs text-muted-foreground"
                        >{{ t('overview.disabled') }}</span>
                        <p class="mt-0.5 break-words text-xs text-muted-foreground">
                          {{ user.email }}
                        </p>
                      </TableCell>
                      <TableCell
                        class="whitespace-nowrap py-4 text-right text-xs font-medium tabular-nums"
                        :title="user.billable_amount?.value ?? ''"
                      >
                        {{ money(user.billable_amount) }}
                      </TableCell>
                      <TableCell class="py-4 text-right text-xs tabular-nums">
                        {{ count(user.request_count) }}
                      </TableCell>
                      <TableCell class="py-4 text-right text-xs tabular-nums">
                        {{ count(user.total_tokens) }}
                      </TableCell>
                      <TableCell class="whitespace-nowrap py-4 text-right text-xs tabular-nums">
                        {{ money(user.finance?.recharge_amount) }}
                      </TableCell>
                      <TableCell class="whitespace-nowrap py-4 text-right text-xs tabular-nums">
                        {{ money(user.finance?.wallet_balance) }}
                      </TableCell>
                      <TableCell class="whitespace-nowrap py-4 text-right text-xs text-muted-foreground">
                        {{ timestamp(user.last_used_at, range.timezone) }}
                      </TableCell>
                      <TableCell class="whitespace-nowrap py-4 text-right text-xs">
                        <Button
                          type="button"
                          variant="ghost"
                          size="icon"
                          :title="t('overview.view') + ' ' + user.username + ' ' + t('overview.usageTrend')"
                          :aria-label="t('overview.view') + ' ' + user.username + ' ' + t('overview.usageTrend')"
                          @click="selectUser(user)"
                        >
                          <ChartNoAxesCombined class="h-4 w-4" />
                        </Button>
                        <Button
                          type="button"
                          variant="ghost"
                          size="icon"
                          :title="t('overview.viewUserAccount', { name: user.username })"
                          :aria-label="t('overview.viewUserAccount', { name: user.username })"
                          @click="accountUser = user"
                        >
                          <ReceiptText class="h-4 w-4" />
                        </Button>
                      </TableCell>
                    </TableRow>
                    <TableRow v-if="!data?.data.items.length">
                      <TableCell
                        colspan="9"
                        class="py-12 text-center text-muted-foreground"
                      >
                        {{ loading ? t('overview.loading') : error ? t('overview.userAccountsUnavailable') : t('overview.noMatchingUsers') }}
                      </TableCell>
                    </TableRow>
                  </TableBody>
                </Table>
              </div>
              <template #pagination>
                <Pagination
                  v-if="data"
                  :total="data.data.total"
                  :current="Math.floor(requestQuery.offset / requestQuery.limit) + 1"
                  :page-size="requestQuery.limit"
                  :page-size-options="[25, 50, 100]"
                  @update:current="changePage"
                  @update:page-size="changePageSize"
                />
              </template>
            </TableCard>
          </template>
        </UserUsageStats>
      </template>
    </UserReports>
    <UserAccountDrawer
      v-if="accountUser"
      :key="accountUser.user_id"
      :user="accountUser"
      :timezone="range.timezone"
      @close="accountUser = null"
    />
  </main>
</template>

<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { Download, Search, ReceiptText, ChartNoAxesCombined } from '@lucide/vue'
import { Button, Input, Pagination, SortableTableHead, Table, TableBody, TableCard, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui'
import TimeRangePicker from '@/components/common/TimeRangePicker.vue'
import type { DateRangeParams } from '@/features/usage/types'
import { overviewApi } from '@/api/overview'
import OverviewToolbar from '@/features/overview/components/OverviewToolbar.vue'
import OverviewStatus from '@/features/overview/components/OverviewStatus.vue'
import UserFinanceSummary from '@/features/overview/users/UserFinanceSummary.vue'
import UserReports from '@/features/overview/users/UserReports.vue'
import UserAccountDrawer from '@/features/overview/users/UserAccountDrawer.vue'
import UserUsageStats from '@/features/overview/users/UserUsageStats.vue'
import { useUserAnalysisRefresh } from '@/features/overview/users/useUserAnalysisRefresh'
import { presetRange, rangeFromQuery, useOverviewQuery } from '@/features/overview/query'
import { useOverviewRequest } from '@/features/overview/useOverviewRequest'
import { useOverviewExport } from '@/features/overview/useOverviewExport'
import { count, money, timestamp } from '@/features/overview/format'
import { useI18n } from '@/i18n'
const { t } = useI18n()
const { query, range, relativePreset, refreshRange, setRange, patch } = useOverviewQuery('today', { rolling: true })
const { revision, autoRefresh, refreshTitle, refreshAll, toggleAutoRefresh } = useUserAnalysisRefresh(refreshRange)
const search = ref(query.value.search || '')
const accountUser = ref<{ user_id: string; username: string } | null>(null)
const paginationUpdating = ref(false)
watch(() => query.value.search, value => { search.value = value || '' })
const requestQuery = computed(() => ({
  ...range.value,
  search: query.value.search,
  limit: Math.max(1, Math.min(100, query.value.limit || 25)),
  offset: query.value.offset || 0,
  sort: ['billable_amount', 'request_count', 'total_tokens'].includes(query.value.sort || '') ? query.value.sort : 'billable_amount',
  order: query.value.order || 'desc',
}))
const scope = computed(() => JSON.stringify({ ...requestQuery.value, ...(relativePreset.value ? { from: undefined, to: undefined, preset: relativePreset.value } : {}) }))
const { data, loading, error } = useOverviewRequest(() => JSON.stringify([requestQuery.value, revision.value]), signal => overviewApi.users(requestQuery.value, signal), { scopeKey: scope })
const { exporting, exportError, exportCsv } = useOverviewExport()
const columns = computed(() => [{ key: 'billable_amount', label: t('overview.consumption') }, { key: 'request_count', label: t('stats.metric.requests') }, { key: 'total_tokens', label: 'Tokens' }])
function sameRange(left: DateRangeParams, right: typeof range.value): boolean {
  return left.from === right.from && left.to === right.to && (left.timezone || right.timezone) === right.timezone
}
function handleRangePicker(value: DateRangeParams) {
  const timezone = value.timezone || range.value.timezone
  if (value.preset) {
    if (value.preset === relativePreset.value) return
    const next = value.preset === 'yesterday'
      ? rangeFromQuery({ preset: value.preset, timezone }, range.value)
      : presetRange(value.preset, timezone)
    void setRange(next, value.preset)
    return
  }
  if (value.from && value.to) {
    if (sameRange(value, range.value)) return
    void setRange({ from: value.from, to: value.to, timezone }, undefined)
    return
  }
  if (value.start_date && value.end_date) {
    const next = rangeFromQuery({ start_date: value.start_date, end_date: value.end_date, timezone }, range.value)
    if (!sameRange(next, range.value)) void setRange(next)
  }
}
function sort({ key, direction }: { key: string; direction: 'asc' | 'desc' }) {
  void patch({ sort: key, order: direction, offset: undefined })
}
async function changePage(page: number) {
  if (loading.value || paginationUpdating.value) return
  paginationUpdating.value = true
  try { await patch({ offset: (page - 1) * requestQuery.value.limit }) } finally { paginationUpdating.value = false }
}
async function changePageSize(limit: number) {
  if (loading.value || paginationUpdating.value) return
  paginationUpdating.value = true
  try { await patch({ limit, offset: undefined }) } finally { paginationUpdating.value = false }
}
</script>
