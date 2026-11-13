<template>
  <div class="flex flex-wrap items-center justify-between gap-3 border-t pt-3 text-xs text-muted-foreground">
    <span>{{ count(total) }} {{ t('overview.results') }} · {{ total ? count(offset + 1) : 0 }}-{{ count(Math.min(offset + limit, total)) }}</span>
    <div class="flex items-center gap-2">
      <select
        class="h-8 rounded-md border bg-background px-2"
        :aria-label="t('overview.pageSize')"
        :value="limit"
        @change="$emit('size', Number(($event.target as HTMLSelectElement).value))"
      >
        <option
          v-for="size in [25, 50, 100]"
          :key="size"
          :value="size"
        >
          {{ size }}
        </option>
      </select>
      <Button
        size="icon"
        variant="outline"
        class="h-8 w-8"
        :disabled="loading || offset <= 0"
        :title="t('overview.previousPage')"
        :aria-label="t('overview.previousPage')"
        @click="$emit('page', Math.max(0, offset - limit))"
      >
        <ChevronLeft class="h-4 w-4" />
      </Button>
      <Button
        size="icon"
        variant="outline"
        class="h-8 w-8"
        :disabled="loading || offset + limit >= total"
        :title="t('overview.nextPage')"
        :aria-label="t('overview.nextPage')"
        @click="$emit('page', offset + limit)"
      >
        <ChevronRight class="h-4 w-4" />
      </Button>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ChevronLeft, ChevronRight } from '@lucide/vue'
import { Button } from '@/components/ui'
import { count } from '../format'
import { useI18n } from '@/i18n'
defineProps<{ total: number; offset: number; limit: number; loading?: boolean }>()
defineEmits<{ page: [offset: number]; size: [limit: number] }>()
const { t } = useI18n()
</script>
