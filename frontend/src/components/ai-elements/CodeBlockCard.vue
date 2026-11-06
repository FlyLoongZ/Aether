<template>
  <div class="rounded-lg border border-border/80 bg-muted/40 overflow-hidden text-xs">
    <!-- 代码块头部 -->
    <div
      v-if="language || showCopy"
      class="flex items-center justify-between px-3 py-1.5 bg-muted/60 border-b border-border/60 text-muted-foreground"
    >
      <span class="font-mono text-[11px] font-medium uppercase tracking-wider">
        {{ language || 'text' }}
      </span>

      <button
        v-if="showCopy"
        type="button"
        class="flex items-center gap-1 text-[11px] hover:text-foreground transition-colors p-1 rounded"
        :title="copied ? t('ui.aiCopied') : t('ui.aiCopyCode')"
        @click="handleCopy"
      >
        <Check
          v-if="copied"
          class="w-3.5 h-3.5 text-green-500"
        />
        <Copy
          v-else
          class="w-3.5 h-3.5"
        />
        <span>{{ copied ? t('ui.aiCopied') : t('ui.aiCopy') }}</span>
      </button>
    </div>

    <!-- 代码内容 -->
    <div class="relative">
      <pre
        class="m-0 p-3 font-mono text-xs overflow-x-auto whitespace-pre-wrap break-words leading-relaxed text-foreground/90 transition-all duration-200"
        :class="[!expanded && isLongContent ? 'max-h-[360px] overflow-hidden' : '']"
      >{{ code }}</pre>

      <div
        v-if="isLongContent && !expanded"
        class="absolute inset-x-0 bottom-0 h-16 bg-gradient-to-t from-muted/90 to-transparent flex items-end justify-center pb-2 pointer-events-auto"
      >
        <button
          type="button"
          class="px-2.5 py-1 text-xs font-medium text-primary bg-background/90 hover:bg-background border border-border/80 rounded-md shadow-xs transition-colors"
          @click="expanded = true"
        >
          {{ t('ui.aiExpandAllCode', { count: lineCount }) }}
        </button>
      </div>
    </div>

    <div
      v-if="isLongContent && expanded"
      class="p-2 border-t border-border/40 bg-muted/20 flex justify-end"
    >
      <button
        type="button"
        class="text-xs text-muted-foreground hover:text-foreground transition-colors"
        @click="expanded = false"
      >
        {{ t('ui.aiCollapseCode') }}
      </button>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed } from 'vue'
import { Check, Copy } from '@lucide/vue'
import { useClipboard } from '@/composables/useClipboard'
import { useI18n } from '@/i18n'


const props = withDefaults(defineProps<{
  code: string
  language?: string
  showCopy?: boolean
}>(), {
  language: '',
  showCopy: true,
})

const { t } = useI18n()

const expanded = ref(false)
const copied = ref(false)
const { copyToClipboard } = useClipboard()

const lineCount = computed(() => {
  if (!props.code) return 0
  return props.code.split('\n').length
})

const isLongContent = computed(() => {
  return lineCount.value > 20 || props.code.length > 1200
})

async function handleCopy() {
  if (!props.code) return
  await copyToClipboard(props.code, true)
  copied.value = true
  setTimeout(() => {
    copied.value = false
  }, 2000)
}
</script>
