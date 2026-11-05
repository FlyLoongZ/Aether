<template>
  <Collapsible
    v-model:open="isOpen"
    class="rounded-lg border border-border/70 bg-muted/20 shadow-xs overflow-hidden transition-all duration-200"
  >
    <!-- 思考过程头部 (ReasoningTrigger)，必须包含唯一个根元素传给 as-child 的 CollapsibleTrigger -->
    <CollapsibleTrigger as-child>
      <button
        type="button"
        class="flex w-full items-center justify-between gap-3 px-3 py-2 text-left hover:bg-muted/40 transition-colors select-none group"
      >
        <div class="flex items-center gap-2 min-w-0">
          <Brain class="w-4 h-4 shrink-0 text-muted-foreground group-hover:text-primary transition-colors" />
          <span class="text-xs font-semibold text-muted-foreground group-hover:text-foreground transition-colors">
            {{ cleanTitle }}
          </span>
          <span
            v-if="showCharCount"
            class="text-[11px] text-muted-foreground/80 font-mono"
          >
            ({{ charCount }} 字符)
          </span>
        </div>

        <div class="flex items-center gap-1.5 shrink-0">
          <span
            role="button"
            tabindex="0"
            class="p-1 rounded text-muted-foreground/70 hover:text-foreground hover:bg-muted/60 transition-colors flex items-center justify-center cursor-pointer"
            :title="copied ? '已复制' : '复制思考过程'"
            @click.stop="handleCopy"
            @keydown.enter.stop="handleCopy"
          >
            <Check
              v-if="copied"
              class="w-3.5 h-3.5 text-green-500"
            />
            <Copy
              v-else
              class="w-3.5 h-3.5"
            />
          </span>
          <ChevronRight
            class="w-4 h-4 text-muted-foreground transition-transform duration-200 group-data-[state=open]:rotate-90"
          />
        </div>
      </button>
    </CollapsibleTrigger>

    <!-- 思考过程内容 (ReasoningContent) -->
    <CollapsibleContent class="border-t border-border/50 bg-muted/15 p-3">
      <pre class="m-0 font-mono text-xs leading-relaxed whitespace-pre-wrap break-words text-foreground/90">{{ content }}</pre>
    </CollapsibleContent>
  </Collapsible>
</template>

<script setup lang="ts">
import { ref, computed } from 'vue'
import { Brain, Check, Copy, ChevronRight } from '@lucide/vue'
import { Collapsible, CollapsibleTrigger, CollapsibleContent } from '@/components/ui'
import { useClipboard } from '@/composables/useClipboard'

const props = withDefaults(defineProps<{
  /** 思考内容 */
  content: string
  /** 标题 */
  title?: string
  /** 默认展开状态 */
  defaultOpen?: boolean
}>(), {
  title: '思考过程',
  defaultOpen: false,
})

const isOpen = ref(props.defaultOpen)
const copied = ref(false)
const { copyToClipboard } = useClipboard()

const charCount = computed(() => props.content?.length || 0)

const cleanTitle = computed(() => props.title || '思考过程')

const showCharCount = computed(() => {
  if (charCount.value <= 0) return false
  return !/\(\d+\s*字符\)/.test(cleanTitle.value)
})

async function handleCopy() {
  if (!props.content) return
  await copyToClipboard(props.content, true)
  copied.value = true
  setTimeout(() => {
    copied.value = false
  }, 2000)
}
</script>
