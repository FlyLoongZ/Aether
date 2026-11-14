<template>
  <div :class="hasNumericOnlyItems ? 'min-w-0' : 'space-y-2'">
    <div
      data-testid="pool-quota-rows"
      :class="hasNumericOnlyItems ? 'grid grid-cols-2 gap-x-3 gap-y-1.5 min-w-0' : 'space-y-2'"
    >
      <div
        v-for="(item, idx) in items"
        :key="`${item.label}-${idx}`"
        :class="item.numericOnly
          ? 'flex min-w-0 items-baseline justify-between gap-2 text-[10px] leading-4'
          : mobile
            ? 'flex flex-col gap-1 min-w-0'
            : 'flex flex-col gap-1 min-w-[140px] max-w-[208px]'"
      >
        <div :class="item.numericOnly ? 'contents' : 'flex items-center justify-between text-[10px] leading-none'">
          <span
            data-testid="pool-quota-period-label"
            :class="item.numericOnly
              ? 'min-w-0 truncate text-muted-foreground'
              : 'text-muted-foreground font-medium shrink-0'"
            :title="item.numericOnly ? item.label : undefined"
          >{{ item.label }}</span>
          <span
            v-if="item.resetText && !item.numericOnly"
            data-testid="pool-quota-reset-text"
            class="text-muted-foreground/80 tabular-nums truncate"
            :title="item.resetText"
          >{{ item.resetText }}</span>
        </div>

        <div :class="item.numericOnly ? 'contents' : 'flex items-center gap-1.5'">
          <div
            v-if="!item.numericOnly"
            data-testid="pool-quota-progress-track"
            class="relative flex-1 h-1.5 rounded-full bg-border overflow-hidden"
          >
            <div
              class="absolute left-0 top-0 h-full rounded-full transition-all duration-300"
              :class="item.barClass"
              :style="{ width: `${item.remainingPercent}%` }"
            />
          </div>
          <span
            data-testid="pool-quota-meter-text"
            class="shrink-0 text-[10px] font-medium tabular-nums leading-none"
            :class="[item.numericOnly ? 'text-right' : '', item.meterClass]"
          >{{ item.meterText }}</span>
        </div>
      </div>
    </div>

    <div
      v-if="resetCreditText"
      data-testid="pool-quota-reset-credits"
      class="mt-2 border-t border-border/50 pt-1.5 text-[10px] leading-4 text-muted-foreground"
    >
      <div class="flex flex-wrap items-center gap-x-1">
        <button
          v-if="canConsumeResetCredit"
          type="button"
          class="font-medium text-primary hover:underline disabled:pointer-events-none disabled:opacity-60"
          :disabled="consumingResetCredit"
          @click="emit('consume-reset-credit')"
        >
          {{ consumingResetCredit ? t('ui.resetting') : t('ui.clickToReset') }}
        </button>
        <span>{{ resetCreditText }}</span>
      </div>
      <div
        v-if="resetCreditItems.length"
        class="truncate tabular-nums"
        :title="resetCreditItems.join(' · ')"
      >
        {{ resetCreditItems.join(' · ') }}
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { useI18n } from '@/i18n'
import type { QuotaProgressDisplayItem } from '../utils/poolKeyQuotaProgress'

const props = withDefaults(defineProps<{
  items: QuotaProgressDisplayItem[]
  mobile?: boolean
  resetCreditText?: string | null
  resetCreditItems?: string[]
  canConsumeResetCredit?: boolean
  consumingResetCredit?: boolean
}>(), {
  mobile: false,
  resetCreditText: null,
  resetCreditItems: () => [],
  canConsumeResetCredit: false,
  consumingResetCredit: false,
})

const emit = defineEmits<{
  'consume-reset-credit': []
}>()

const { t } = useI18n()

const hasNumericOnlyItems = computed(
  () => props.items.length > 0 && props.items.every(item => item.numericOnly),
)
</script>
