<template>
  <div
    v-if="variant === 'mobile'"
    class="rounded-xl border border-border/50 bg-muted/30 px-3 py-2 text-xs"
  >
    <div class="text-muted-foreground mb-1">
      {{ t('ui.quotae284fc9') }}
    </div>
    <PoolQuotaDisplay
      v-if="items.length"
      :items="items"
      :reset-credit-text="resetCreditText"
      :reset-credit-items="resetCreditItems"
      :can-consume-reset-credit="canConsumeResetCredit"
      :consuming-reset-credit="consumingResetCredit"
      mobile
      @consume-reset-credit="emit('consume-reset-credit')"
    />
    <div
      v-else-if="fallbackText"
      :class="textClass"
    >
      {{ fallbackText }}
    </div>
    <div
      v-else
      class="text-muted-foreground"
    >
      -
    </div>
  </div>

  <template v-else>
    <div
      v-if="items.length"
      class="w-full max-w-[208px]"
    >
      <PoolQuotaDisplay
        :items="items"
        :reset-credit-text="resetCreditText"
        :reset-credit-items="resetCreditItems"
        :can-consume-reset-credit="canConsumeResetCredit"
        :consuming-reset-credit="consumingResetCredit"
        @consume-reset-credit="emit('consume-reset-credit')"
      />
    </div>
    <span
      v-else-if="fallbackText"
      :class="textClass"
    >
      {{ fallbackText }}
    </span>
    <span
      v-else
      class="text-xs text-muted-foreground"
    >-</span>
  </template>
</template>

<script setup lang="ts">
import { useI18n } from '@/i18n'
import type { QuotaProgressDisplayItem } from '../utils/poolKeyQuotaProgress'
import PoolQuotaDisplay from './PoolQuotaDisplay.vue'

withDefaults(defineProps<{
  items: QuotaProgressDisplayItem[]
  fallbackText?: string | null
  textClass?: string
  variant?: 'desktop' | 'mobile'
  resetCreditText?: string | null
  resetCreditItems?: string[]
  canConsumeResetCredit?: boolean
  consumingResetCredit?: boolean
}>(), {
  fallbackText: null,
  textClass: '',
  variant: 'desktop',
  resetCreditText: null,
  resetCreditItems: () => [],
  canConsumeResetCredit: false,
  consumingResetCredit: false,
})

const emit = defineEmits<{
  'consume-reset-credit': []
}>()

const { t } = useI18n()
</script>
