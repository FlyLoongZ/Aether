<template>
  <section class="space-y-4">
    <div class="flex flex-wrap items-center justify-between gap-2">
      <h2 class="text-sm font-semibold">
        {{ resources ? t('overview.resourceDiagnostics') : t('overview.liveRuntime') }}
      </h2><p class="min-w-0 break-words text-xs text-muted-foreground">
        {{ snapshot.scope.kind === 'cluster' ? t('overview.knownNodes') : t('overview.currentNode') }} · {{ snapshot.node_id || t('overview.unknownNodeId') }} · {{ timestamp(snapshot.observed_at) }}
      </p>
    </div>
    <p
      v-if="snapshot.unavailable_sections.length"
      class="break-words rounded-xl border border-amber-500/20 bg-amber-500/5 px-3 py-2 text-xs text-amber-700 dark:text-amber-400"
    >
      {{ t('overview.notCollected') }}: {{ snapshot.unavailable_sections.join(', ') }}
    </p>
    <p
      v-if="!metrics"
      class="py-8 text-center text-sm text-muted-foreground"
    >
      {{ t('overview.noResourceSamples') }}
    </p>
    <template v-else>
      <div class="grid auto-rows-fr grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-3">
        <Card
          v-for="group in groups"
          :key="group.title"
          class="min-w-0 p-4 sm:p-5"
        >
          <div class="mb-5 flex items-center justify-between gap-3">
            <h3 class="text-sm font-semibold">
              {{ group.title }}
            </h3>
            <span class="flex h-9 w-9 shrink-0 items-center justify-center rounded-xl border border-border/60 bg-muted/30 text-muted-foreground">
              <component
                :is="group.icon"
                class="h-4 w-4"
                aria-hidden="true"
              />
            </span>
          </div><dl class="grid grid-cols-2 gap-x-5 gap-y-5">
            <div
              v-for="item in group.items"
              :key="item.label"
              class="min-w-0"
            >
              <dt class="min-h-8 break-words text-xs leading-4 text-muted-foreground">
                {{ item.label }}
              </dt><dd class="mt-1 min-w-0 font-semibold leading-8 tabular-nums">
                <MetricValue
                  :value="item.value"
                  :max-font-size="24"
                />
              </dd>
            </div>
          </dl>
        </Card>
      </div>
      <Card
        v-if="!resources && snapshot.resilience"
        class="min-w-0 space-y-4 p-4 sm:p-5"
      >
        <div class="flex flex-wrap items-center justify-between gap-3">
          <h3 class="flex items-center gap-2 text-sm font-semibold">
            <ShieldCheck
              class="h-4 w-4 text-muted-foreground"
              aria-hidden="true"
            />
            {{ t('overview.resilience') }}
          </h3><RouterLink
            :to="link('/admin/health-monitor')"
            class="text-xs text-primary hover:underline"
          >
            {{ t('nav.healthMonitor') }}
          </RouterLink>
        </div><dl class="grid grid-cols-2 gap-5">
          <div class="min-w-0">
            <dt class="text-xs text-muted-foreground">
              {{ t('overview.openCircuits') }}
            </dt><dd class="mt-2 font-semibold tabular-nums">
              <MetricValue
                :value="count(snapshot.resilience.error_statistics.open_circuit_breakers)"
                :max-font-size="24"
              />
            </dd>
          </div><div class="min-w-0">
            <dt class="text-xs text-muted-foreground">
              {{ t('overview.degradedCredentials') }}
            </dt><dd class="mt-2 font-semibold tabular-nums">
              <MetricValue
                :value="count(snapshot.resilience.error_statistics.degraded_keys)"
                :max-font-size="24"
              />
            </dd>
          </div>
        </dl><ul
          v-if="snapshot.resilience.recent_errors.length"
          class="divide-y border-t text-xs"
        >
          <li
            v-for="item in snapshot.resilience.recent_errors.slice(0, 8)"
            :key="item.error_id"
            class="flex flex-wrap items-start justify-between gap-2 py-3"
          >
            <span class="min-w-0 break-all">{{ item.error_type }} · {{ item.context.provider_name || item.context.model || item.operation }}</span><RouterLink
              v-if="item.context.request_id"
              :to="link('/admin/usage', { request_id: item.context.request_id })"
              class="min-w-0 break-all font-mono text-primary hover:underline"
            >
              {{ item.context.request_id }}
            </RouterLink><span class="text-muted-foreground">{{ timestamp(item.timestamp) }}</span>
          </li>
        </ul>
      </Card>
      <Card
        v-if="resources && metrics.upstreamTargets.rows.length"
        class="min-w-0 space-y-4 p-4 sm:p-5"
      >
        <h3 class="flex items-center gap-2 text-sm font-semibold">
          <Network
            class="h-4 w-4 text-muted-foreground"
            aria-hidden="true"
          />
          {{ t('overview.upstreamAdmission') }}
        </h3><div class="overflow-x-auto">
          <table class="w-full min-w-[420px] text-sm">
            <thead>
              <tr class="border-b text-xs text-muted-foreground">
                <th class="py-2 text-left">
                  {{ t('ui.target') }}
                </th><th class="px-3 text-right">
                  {{ t('overview.inFlight') }}
                </th><th class="px-3 text-right">
                  {{ t('overview.availablePermits') }}
                </th><th class="px-3 text-right">
                  {{ t('overview.rejectedTotal') }}
                </th>
              </tr>
            </thead><tbody>
              <tr
                v-for="row in metrics.upstreamTargets.rows"
                :key="row.target"
                class="border-b"
              >
                <td class="max-w-64 break-words py-2">
                  {{ row.target }}
                </td><td class="px-3 text-right">
                  {{ count(row.inFlight) }}
                </td><td class="px-3 text-right">
                  {{ count(row.availablePermits) }}
                </td><td class="px-3 text-right">
                  {{ count(row.rejectedTotal) }}
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </Card>
    </template>
  </section>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { RouterLink } from 'vue-router'
import { Activity, Cpu, Database, Layers, ListChecks, ListTodo, Network, Radio, ShieldCheck } from '@lucide/vue'
import { Card } from '@/components/ui'
import { buildGatewayMetricsSummary } from '@/api/monitoring'
import type { OverviewLive } from '@/api/overview'
import MetricValue from '../components/MetricValue.vue'
import { useOverviewQuery } from '../query'
import { count, timestamp } from '../format'
import { useI18n } from '@/i18n'
const props = defineProps<{ snapshot: OverviewLive; resources?: boolean }>()
const { t } = useI18n()
const { link } = useOverviewQuery()
const metrics = computed(() => props.snapshot.metrics || (props.snapshot.metrics_text ? buildGatewayMetricsSummary(props.snapshot.metrics_text) : null))
function bytes(value: number | null | undefined) { return value == null ? '-' : `${count(value / 1024 / 1024)} MiB` }
function basis(value: number | null | undefined) { return value == null ? '-' : `${count(value / 100)}%` }
const groups = computed(() => {
  const m = metrics.value
  if (!m) return []
  const runtime = [
    { title: t('overview.requestAdmission'), icon: Activity, items: [{ label: t('overview.nodeInFlight'), value: count(m.local.inFlight) }, { label: t('overview.nodeAvailablePermits'), value: count(m.local.availablePermits) }, ] },
    { title: t('overview.usageWrites'), icon: ListTodo, items: [{ label: t('overview.awaitingAcknowledgement'), value: count(m.usageQueue.groupPending) }, { label: t('overview.awaitingDelivery'), value: count(m.usageQueue.groupLag) }, { label: t('overview.deadLetters'), value: count(m.usageQueue.dlqLength) }, { label: t('overview.oldestPendingMs'), value: count(m.usageQueue.oldestPendingIdleMs) }] },
  ]
  if (!props.resources) return runtime
  return [
    { title: t('overview.processAndHost'), icon: Cpu, items: [{ label: t('overview.processCpu'), value: basis(m.process.processCpuUsageBasisPoints) }, { label: t('overview.hostCpu'), value: basis(m.process.systemCpuUsageBasisPoints) }, { label: t('overview.processMemory'), value: bytes(m.process.processMemoryBytes) }, { label: t('overview.hostMemory'), value: basis(m.process.systemMemoryUsageBasisPoints) }, { label: t('overview.openFileDescriptors'), value: count(m.process.openFds) }] },
    { title: 'PostgreSQL', icon: Database, items: [{ label: t('overview.poolCheckedOut'), value: `${count(m.databasePool.checkedOut)} / ${count(m.databasePool.max)}` }, { label: t('overview.databaseWaits'), value: count(m.postgres.waitingConnections) }, { label: t('overview.lockWaits'), value: count(m.postgres.lockWaitingConnections) }, { label: t('overview.oldestQueryMs'), value: count(m.postgres.oldestActiveQueryAgeMs) }, { label: t('overview.blockCacheHit'), value: basis(m.postgres.blockCacheHitRateBasisPoints) }] },
    { title: 'Redis', icon: Radio, items: [{ label: t('overview.usedMemory'), value: bytes(m.redisRuntime.usedMemoryBytes) }, { label: t('overview.blockedClients'), value: count(m.redisRuntime.blockedClients) }, { label: t('overview.hitRate'), value: basis(m.redisRuntime.keyspaceHitRateBasisPoints) }, { label: t('overview.operationsSecond'), value: count(m.redisRuntime.instantaneousOpsPerSec) }, { label: t('overview.evictionsTotal'), value: count(m.redisRuntime.evictedKeysTotal) }] },
    { title: t('overview.tasksAndScheduling'), icon: Layers, items: [{ label: t('overview.activeTasks'), value: count(m.backgroundTasks.active) }, { label: t('overview.unexpectedExitsTotal'), value: count(m.backgroundTasks.unexpectedExitsTotal) }, { label: t('nav.asyncTasks'), value: count(m.tokioRuntime.aliveTasks) }, { label: t('overview.schedulingQueue'), value: count(m.tokioRuntime.globalQueueDepth) }] },
    { title: t('overview.auditCounters'), icon: ListChecks, items: [{ label: t('overview.pendingFlush'), value: count(m.usageCounter.pendingRows) }, { label: t('overview.oldestPendingS'), value: count(m.usageCounter.oldestPendingAgeSeconds) }, { label: t('overview.failedBatchesTotal'), value: count(m.usageCounter.flushFailedBatchesTotal) }, { label: t('overview.candidateQueue'), value: count(m.requestCandidateQueue.depth) }] },
  ]
})
</script>
