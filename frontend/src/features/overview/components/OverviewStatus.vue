<template>
  <div
    v-if="error"
    role="alert"
    class="flex flex-wrap items-center justify-between gap-2 border-l-2 border-destructive bg-destructive/5 px-3 py-2 text-sm"
  >
    <span class="min-w-0 break-words">{{ t('overview.unableToLoadData') }}: {{ error }}</span>
    <Button
      variant="outline"
      size="sm"
      :disabled="loading"
      @click="$emit('retry')"
    >
      {{ t('ui.retryButton') }}
    </Button>
  </div>
  <div
    v-if="loading && !meta"
    class="flex min-h-40 items-center justify-center gap-2 text-sm text-muted-foreground"
    role="status"
  >
    <Loader2 class="h-4 w-4 animate-spin" />{{ t('overview.loading') }}
  </div>
  <div
    v-if="meta"
    class="flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-muted-foreground"
  >
    <span :class="partial ? 'text-amber-700 dark:text-amber-400' : ''">{{ partial ? t('overview.partialData') : t('overview.complete') }}</span>
    <span>{{ t('overview.usageCoverage') }} {{ count(meta.coverage.usage_available_count) }}/{{ count(meta.coverage.request_count) }}</span>
    <span>{{ t('overview.pricingCoverage') }} {{ count(meta.coverage.pricing_available_count) }}/{{ count(meta.coverage.request_count) }}</span>
    <span>{{ t('overview.settled') }} {{ count(meta.coverage.settled_count) }}/{{ count(meta.coverage.request_count) }}</span>
    <span
      v-if="meta.coverage.unrecoverable_bucket_count"
      class="text-amber-700 dark:text-amber-400"
    >{{ t('overview.sourceDataRemovedUnrecoverableBuckets') }} {{ count(meta.coverage.unrecoverable_bucket_count) }}</span>
    <span
      v-if="stale"
      class="text-amber-700 dark:text-amber-400"
    >{{ t('overview.dataDelayed') }}</span>
    <span>{{ t('overview.updated') }} {{ timestamp(meta.generated_at, meta.range.timezone) }}</span>
    <span v-if="meta.data_through">{{ t('overview.dataThrough') }} {{ timestamp(meta.data_through, meta.range.timezone) }}</span>
  </div>
  <div
    v-if="meta?.projection"
    class="flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-muted-foreground"
  >
    <span v-if="!meta.projection.read_enabled">{{ t('overview.readingSourceFacts') }}</span>
    <span>{{ t('overview.aggregateCoverageThrough') }} {{ timestamp(meta.projection.projection_through, meta.range.timezone) }}</span>
    <span>{{ t('overview.awaitingRebuild') }} {{ count(meta.projection.dirty_bucket_count) }}</span>
    <span>{{ t('overview.notBuilt') }} {{ count(meta.projection.missing_bucket_count) }}</span>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { Loader2 } from '@lucide/vue'
import { Button } from '@/components/ui'
import type { OverviewMeta } from '@/api/overview'
import { count, timestamp } from '../format'
import { useI18n } from '@/i18n'
const props = defineProps<{ meta?: OverviewMeta | null; loading?: boolean; error?: string | null }>()
defineEmits<{ retry: [] }>()
const { t } = useI18n()
const partial = computed(() => props.meta?.coverage.status !== 'complete')
const stale = computed(() => !!props.meta?.data_through && Date.parse(props.meta.data_through) < Math.min(Date.parse(props.meta.range.to), Date.now()) - 300_000)
</script>
