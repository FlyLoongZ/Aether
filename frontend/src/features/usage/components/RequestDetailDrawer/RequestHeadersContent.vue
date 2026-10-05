<template>
  <div>
    <!-- 对比模式 - 并排 Diff -->
    <div v-show="viewMode === 'compare'">
      <div
        v-if="!resolvedClientHeaders || !resolvedProviderHeaders"
        class="text-sm text-muted-foreground"
      >
        {{ emptyMessage }}
      </div>
      <Card
        v-else
        class="bg-muted/30 overflow-hidden"
      >
        <!-- Diff 头部 -->
        <div class="flex border-b bg-muted/50">
          <div class="flex-1 px-3 py-2 text-xs text-muted-foreground border-r flex items-center justify-between">
            <span class="font-medium">{{ clientLabel }}</span>
            <span class="text-destructive">-{{ headerStats.removed + headerStats.modified }}</span>
          </div>
          <div class="flex-1 px-3 py-2 text-xs text-muted-foreground flex items-center justify-between">
            <span class="font-medium">{{ providerLabel }}</span>
            <span class="text-green-600 dark:text-green-400">+{{ headerStats.added + headerStats.modified }}</span>
          </div>
        </div>

        <!-- 并排 Diff 内容 -->
        <div class="flex font-mono text-xs max-h-[500px]">
          <!-- 左侧：客户端 -->
          <div
            ref="leftPanelRef"
            class="w-1/2 min-w-0 border-r overflow-x-auto overflow-y-auto"
            @scroll="onLeftScroll"
          >
            <template
              v-for="entry in sortedEntries"
              :key="'left-' + entry.key"
            >
              <!-- 删除的行 -->
              <div
                v-if="entry.status === 'removed'"
                class="flex items-start bg-destructive/10 px-3 py-0.5"
              >
                <span class="text-destructive">
                  "{{ entry.key }}": <template
                    v-if="isMaskedEntry(entry)"
                  ><span
                    class="reveal-mask"
                    data-testid="masked-header-value"
                    title="点击显示原文"
                    @click="revealKey(entry.key)"
                  >{{ MASKED_HEADER_VALUE }}</span></template><template v-else>"{{ entry.clientValue }}"</template>
                </span>
              </div>
              <!-- 修改的行 - 旧值 -->
              <div
                v-else-if="entry.status === 'modified'"
                class="flex items-start bg-amber-500/10 px-3 py-0.5"
              >
                <span class="text-amber-600 dark:text-amber-400">
                  "{{ entry.key }}": <template
                    v-if="isMaskedEntry(entry)"
                  ><span
                    class="reveal-mask"
                    data-testid="masked-header-value"
                    title="点击显示原文"
                    @click="revealKey(entry.key)"
                  >{{ MASKED_HEADER_VALUE }}</span></template><template v-else>"{{ entry.clientValue }}"</template>
                </span>
              </div>
              <!-- 新增的行 - 左侧空白占位 -->
              <div
                v-else-if="entry.status === 'added'"
                class="flex items-start bg-muted/30 px-3 py-0.5"
              >
                <span class="text-muted-foreground/30 italic">（无）</span>
              </div>
              <!-- 未变化的行 -->
              <div
                v-else
                class="flex items-start px-3 py-0.5 hover:bg-muted/50"
              >
                <span class="text-muted-foreground">
                  "{{ entry.key }}": <template
                    v-if="isMaskedEntry(entry)"
                  ><span
                    class="reveal-mask"
                    data-testid="masked-header-value"
                    title="点击显示原文"
                    @click="revealKey(entry.key)"
                  >{{ MASKED_HEADER_VALUE }}</span></template><template v-else>"{{ entry.clientValue }}"</template>
                </span>
              </div>
            </template>
          </div>
          <!-- 右侧：提供商 -->
          <div
            ref="rightPanelRef"
            class="w-1/2 min-w-0 overflow-x-auto overflow-y-auto"
            @scroll="onRightScroll"
          >
            <template
              v-for="entry in sortedEntries"
              :key="'right-' + entry.key"
            >
              <!-- 删除的行 - 右侧空白占位 -->
              <div
                v-if="entry.status === 'removed'"
                class="flex items-start bg-muted/30 px-3 py-0.5"
              >
                <span class="text-muted-foreground/50 line-through">
                  "{{ entry.key }}": <template
                    v-if="isMaskedEntry(entry)"
                  ><span
                    class="reveal-mask"
                    data-testid="masked-header-value"
                    title="点击显示原文"
                    @click="revealKey(entry.key)"
                  >{{ MASKED_HEADER_VALUE }}</span></template><template v-else>"{{ entry.clientValue }}"</template>
                </span>
              </div>
              <!-- 修改的行 - 新值 -->
              <div
                v-else-if="entry.status === 'modified'"
                class="flex items-start bg-amber-500/10 px-3 py-0.5"
              >
                <span class="text-amber-600 dark:text-amber-400">
                  "{{ entry.key }}": <template
                    v-if="isMaskedEntry(entry)"
                  ><span
                    class="reveal-mask"
                    data-testid="masked-header-value"
                    title="点击显示原文"
                    @click="revealKey(entry.key)"
                  >{{ MASKED_HEADER_VALUE }}</span></template><template v-else>"{{ entry.providerValue }}"</template>
                </span>
              </div>
              <!-- 新增的行 -->
              <div
                v-else-if="entry.status === 'added'"
                class="flex items-start bg-green-500/10 px-3 py-0.5"
              >
                <span class="text-green-600 dark:text-green-400">
                  "{{ entry.key }}": <template
                    v-if="isMaskedEntry(entry)"
                  ><span
                    class="reveal-mask"
                    data-testid="masked-header-value"
                    title="点击显示原文"
                    @click="revealKey(entry.key)"
                  >{{ MASKED_HEADER_VALUE }}</span></template><template v-else>"{{ entry.providerValue }}"</template>
                </span>
              </div>
              <!-- 未变化的行 -->
              <div
                v-else
                class="flex items-start px-3 py-0.5 hover:bg-muted/50"
              >
                <span class="text-muted-foreground">
                  "{{ entry.key }}": <template
                    v-if="isMaskedEntry(entry)"
                  ><span
                    class="reveal-mask"
                    data-testid="masked-header-value"
                    title="点击显示原文"
                    @click="revealKey(entry.key)"
                  >{{ MASKED_HEADER_VALUE }}</span></template><template v-else>"{{ entry.providerValue }}"</template>
                </span>
              </div>
            </template>
          </div>
        </div>
      </Card>
    </div>

    <!-- 格式化模式 - 直接使用 JsonContent -->
    <div v-show="viewMode === 'formatted'">
      <JsonContent
        :data="maskedHeaderData"
        :view-mode="viewMode"
        :expand-depth="currentExpandDepth"
        :is-dark="isDark"
        :empty-message="emptyMessage"
        :masked-keys="maskedKeysForCurrentView"
        @reveal-mask="revealKey"
      />
    </div>

    <!-- 原始模式 -->
    <div v-show="viewMode === 'raw'">
      <div
        v-if="!maskedHeaderData || Object.keys(maskedHeaderData).length === 0"
        class="text-sm text-muted-foreground"
      >
        无请求头信息
      </div>
      <Card
        v-else
        class="bg-muted/30"
      >
        <div
          class="p-4 overflow-x-auto"
          @click="onRawPreviewClick"
        >
          <!-- eslint-disable vue/no-v-html -->
          <pre
            class="text-xs font-mono whitespace-pre-wrap"
            v-html="rawPreviewHtml"
          />
          <!-- eslint-enable vue/no-v-html -->
        </div>
      </Card>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, ref } from 'vue'
import Card from '@/components/ui/card.vue'
import JsonContent from './JsonContent.vue'
import type { RequestDetail } from '@/api/dashboard'
import {
  hiddenSensitiveHeaderKeys,
  isSensitiveHeaderName,
  MASKED_HEADER_VALUE,
  maskedJsonPreviewHtml,
  maskSensitiveHeaderValues,
  normalizeHeaderName,
} from '../../utils/sensitiveHeaders'

const props = withDefaults(defineProps<{
  detail: RequestDetail
  viewMode: 'compare' | 'formatted' | 'raw'
  dataSource: 'client' | 'provider'
  currentHeaderData: Record<string, unknown> | null
  currentExpandDepth: number
  hasProviderHeaders: boolean
  headerStats: { added: number; modified: number; removed: number; unchanged: number }
  isDark: boolean
  // 泛化 props：允许传入任意 header 对和标签，用于复用为响应头对比
  clientHeaders?: Record<string, unknown> | null
  providerHeaders?: Record<string, unknown> | null
  clientLabel?: string
  providerLabel?: string
  emptyMessage?: string
}>(), {
  clientHeaders: undefined,
  providerHeaders: undefined,
  clientLabel: '客户端请求头',
  providerLabel: '提供商请求头',
  emptyMessage: '无请求头信息',
})

// 解析实际使用的 header 数据
const resolvedClientHeaders = computed(() =>
  props.clientHeaders ?? props.detail.request_headers ?? {}
)
const resolvedProviderHeaders = computed(() =>
  props.providerHeaders ?? props.detail.provider_request_headers ?? {}
)

// 敏感头默认遮蔽，点击掩码本身显示原文（仅前端展示层，不改变接口返回）
const revealedKeys = ref<Set<string>>(new Set())

function revealKey(key: string) {
  const normalized = normalizeHeaderName(key)
  if (revealedKeys.value.has(normalized)) return
  revealedKeys.value = new Set(revealedKeys.value).add(normalized)
}

const maskedHeaderData = computed(() =>
  maskSensitiveHeaderValues(props.currentHeaderData, revealedKeys.value)
)

/** 当前仍处于遮蔽状态的敏感头名称，供 JSON 视图把掩码变成可点击值。 */
function hiddenKeysFor(headers: Record<string, unknown> | null | undefined): string[] {
  return hiddenSensitiveHeaderKeys(headers, revealedKeys.value)
}

const maskedKeysForCurrentView = computed(() => hiddenKeysFor(props.currentHeaderData))

const rawPreviewHtml = computed(() =>
  maskedJsonPreviewHtml(props.currentHeaderData, revealedKeys.value)
)

function onRawPreviewClick(event: MouseEvent) {
  const maskKey = (event.target as HTMLElement | null)?.dataset?.maskKey
  if (maskKey) revealKey(maskKey)
}

function isMaskedEntry(entry: { key: string }): boolean {
  return isSensitiveHeaderName(entry.key)
    && !revealedKeys.value.has(normalizeHeaderName(entry.key))
}

const leftPanelRef = ref<HTMLElement | null>(null)
const rightPanelRef = ref<HTMLElement | null>(null)
let isSyncingScroll = false

function onLeftScroll() {
  if (isSyncingScroll) return
  isSyncingScroll = true
  if (leftPanelRef.value && rightPanelRef.value) {
    rightPanelRef.value.scrollTop = leftPanelRef.value.scrollTop
  }
  requestAnimationFrame(() => { isSyncingScroll = false })
}

function onRightScroll() {
  if (isSyncingScroll) return
  isSyncingScroll = true
  if (leftPanelRef.value && rightPanelRef.value) {
    leftPanelRef.value.scrollTop = rightPanelRef.value.scrollTop
  }
  requestAnimationFrame(() => { isSyncingScroll = false })
}

// 合并并排序的条目（用于并排显示）
const sortedEntries = computed(() => {
  const clientHeaders = resolvedClientHeaders.value
  const providerHeaders = resolvedProviderHeaders.value

  const clientKeys = new Set(Object.keys(clientHeaders))
  const providerKeys = new Set(Object.keys(providerHeaders))
  const allKeys = Array.from(new Set([...clientKeys, ...providerKeys])).sort()

  return allKeys.map(key => {
    const inClient = clientKeys.has(key)
    const inProvider = providerKeys.has(key)
    const clientValue = clientHeaders[key]
    const providerValue = providerHeaders[key]

    let status: 'added' | 'removed' | 'modified' | 'unchanged'
    if (inClient && inProvider) {
      status = clientValue === providerValue ? 'unchanged' : 'modified'
    } else if (inClient) {
      status = 'removed'
    } else {
      status = 'added'
    }

    return {
      key,
      clientValue,
      providerValue,
      status
    }
  })
})
</script>

<style scoped>
/* 敏感头掩码：点击自身即显示原文 */
.reveal-mask {
  cursor: pointer;
  border-radius: 3px;
  background: hsl(var(--muted-foreground) / 0.15);
  padding: 0 3px;
  letter-spacing: 1px;
}

.reveal-mask:hover {
  background: hsl(var(--primary) / 0.2);
  color: hsl(var(--primary));
}
</style>
