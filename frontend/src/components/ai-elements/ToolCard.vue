<template>
  <Collapsible
    v-model:open="isOpen"
    class="rounded-lg border border-border bg-card/60 shadow-xs overflow-hidden transition-all duration-200"
    :class="isError ? 'border-destructive/30 bg-destructive/[0.02]' : ''"
  >
    <!-- 工具卡片头部 (ToolHeader)，必须包含唯一个根元素传给 as-child 的 CollapsibleTrigger -->
    <CollapsibleTrigger as-child>
      <button
        type="button"
        class="flex w-full items-center justify-between gap-3 px-3 py-2.5 text-left hover:bg-muted/40 transition-colors select-none group"
      >
        <!-- 左侧：图标 + 工具名 + toolId + 状态徽章 + 行数 -->
        <div class="flex items-center gap-2 min-w-0">
          <Wrench class="w-4 h-4 shrink-0 text-muted-foreground group-hover:text-foreground transition-colors" />
          <span class="text-xs font-semibold text-foreground truncate">
            {{ title }}
          </span>
          <span
            v-if="toolId"
            class="font-mono text-[11px] px-1.5 py-0.5 rounded bg-muted/70 text-muted-foreground truncate max-w-[120px]"
            :title="toolId"
          >
            #{{ toolId }}
          </span>
          <Badge
            :variant="isError ? 'destructive' : isCall ? 'secondary' : 'outline'"
            class="text-[11px] h-5 px-1.5 gap-1 shrink-0 font-normal"
          >
            <AlertCircle
              v-if="isError"
              class="w-3 h-3"
            />
            <CheckCircle2
              v-else-if="!isCall"
              class="w-3 h-3 text-green-500"
            />
            <span>{{ statusLabel }}</span>
          </Badge>
          <span
            v-if="lineCount > 1"
            class="text-[11px] text-muted-foreground shrink-0 hidden sm:inline"
          >
            {{ t('ui.aiLineCountParen', { count: lineCount }) }}
          </span>
        </div>

        <!-- 右侧：复制按钮 + 折叠图标 -->
        <div class="flex items-center gap-1.5 shrink-0">
          <span
            role="button"
            tabindex="0"
            class="p-1 rounded text-muted-foreground/70 hover:text-foreground hover:bg-muted/60 transition-colors flex items-center justify-center cursor-pointer"
            :title="copied ? t('ui.aiCopied') : t('ui.aiCopyContent')"
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

    <!-- 工具卡片内容 (ToolContent) -->
    <CollapsibleContent class="border-t border-border/60">
      <!-- 输入参数 (ToolInput) -->
      <div
        v-if="input"
        class="p-3 bg-muted/20 border-b border-border/40 text-xs"
      >
        <div class="text-[11px] font-medium text-muted-foreground uppercase tracking-wider mb-1.5 flex items-center gap-1">
          <span>{{ t('ui.aiToolInput') }}</span>
        </div>
        <pre class="m-0 p-2.5 rounded bg-muted/60 font-mono text-xs overflow-x-auto whitespace-pre-wrap break-words leading-relaxed text-foreground/90 border border-border/40">{{ formattedInput }}</pre>
      </div>

      <!-- 输出结果 (ToolOutput) -->
      <div
        v-if="output"
        class="p-3 text-xs"
        :class="isError ? 'bg-destructive/[0.04]' : 'bg-muted/10'"
      >
        <div class="text-[11px] font-medium text-muted-foreground uppercase tracking-wider mb-1.5 flex items-center justify-between">
          <span>{{ isError ? t('ui.errorMessage') : t('ui.aiExecutionResult') }}</span>
          <span
            v-if="charCount > 0"
            class="text-[11px] font-mono text-muted-foreground/80 lowercase"
          >
            {{ t('ui.aiCharCount', { count: charCount }) }}
          </span>
        </div>

        <div class="relative">
          <pre
            class="m-0 p-2.5 rounded bg-muted/50 font-mono text-xs overflow-x-auto whitespace-pre-wrap break-words leading-relaxed border border-border/40 transition-all duration-200"
            :class="[
              isError ? 'text-destructive' : 'text-foreground/90',
              !expanded && isLongContent ? 'max-h-[300px] overflow-hidden' : ''
            ]"
          >{{ formattedOutput }}</pre>

          <!-- 长文本渐变蒙层与展开按钮（避免文字被截半行） -->
          <div
            v-if="isLongContent && !expanded"
            class="absolute inset-x-0 bottom-0 h-16 bg-gradient-to-t from-muted/90 to-transparent rounded-b flex items-end justify-center pb-2 pointer-events-auto"
          >
            <button
              type="button"
              class="px-2.5 py-1 text-xs font-medium text-primary bg-background/90 hover:bg-background border border-border/80 rounded-md shadow-xs transition-colors flex items-center gap-1"
              @click="expanded = true"
            >
              <span>{{ t('ui.aiExpandAllContent', { count: lineCount }) }}</span>
            </button>
          </div>
        </div>

        <div
          v-if="isLongContent && expanded"
          class="mt-2 flex justify-end"
        >
          <button
            type="button"
            class="text-xs text-muted-foreground hover:text-foreground transition-colors"
            @click="expanded = false"
          >
            {{ t('ui.aiCollapseContent') }}
          </button>
        </div>
      </div>
    </CollapsibleContent>
  </Collapsible>
</template>

<script setup lang="ts">
import { ref, computed } from 'vue'
import { Wrench, Check, Copy, ChevronRight, AlertCircle, CheckCircle2 } from '@lucide/vue'
import { Collapsible, CollapsibleTrigger, CollapsibleContent, Badge } from '@/components/ui'
import { useClipboard } from '@/composables/useClipboard'
import { useI18n } from '@/i18n'


const props = withDefaults(defineProps<{
  /** 工具名称或标题 */
  toolName?: string
  /** 工具调用 ID */
  toolId?: string
  /** 工具输入参数 */
  input?: string | Record<string, unknown>
  /** 工具执行结果 */
  output?: string | Record<string, unknown>
  /** 是否发生错误 */
  isError?: boolean
  /** 是否默认展开 */
  defaultOpen?: boolean
}>(), {
  toolName: '',
  toolId: '',
  input: undefined,
  output: undefined,
  isError: false,
  defaultOpen: true,
})

const { t } = useI18n()

const isOpen = ref(props.defaultOpen)
const expanded = ref(false)
const copied = ref(false)
const { copyToClipboard } = useClipboard()

const isCall = computed(() => Boolean(props.input && !props.output))

const title = computed(() => {
  if (props.toolName) return props.toolName
  if (isCall.value) return '工具调用'
  return '工具结果'
})

const statusLabel = computed(() => {
  if (props.isError) return '执行失败'
  if (isCall.value) return '调用参数'
  return '执行成功'
})

const formattedInput = computed(() => {
  if (!props.input) return ''
  if (typeof props.input === 'string') {
    try {
      const parsed = JSON.parse(props.input)
      return JSON.stringify(parsed, null, 2)
    } catch {
      return props.input
    }
  }
  return JSON.stringify(props.input, null, 2)
})

const formattedOutput = computed(() => {
  if (!props.output) return ''
  if (typeof props.output === 'string') return props.output
  return JSON.stringify(props.output, null, 2)
})

const lineCount = computed(() => {
  const content = formattedOutput.value || formattedInput.value
  if (!content) return 0
  return content.split('\n').length
})

const charCount = computed(() => {
  return formattedOutput.value.length
})

// 超过 15 行或超过 1000 字符视为长文本
const isLongContent = computed(() => {
  return lineCount.value > 15 || charCount.value > 1000
})

async function handleCopy() {
  const text = formattedOutput.value || formattedInput.value
  if (!text) return
  await copyToClipboard(text, true)
  copied.value = true
  setTimeout(() => {
    copied.value = false
  }, 2000)
}
</script>
