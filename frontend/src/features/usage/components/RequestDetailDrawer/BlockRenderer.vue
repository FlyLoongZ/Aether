<template>
  <div class="flex flex-col gap-2.5">
    <template
      v-for="(block, index) in blocks"
      :key="index"
    >
      <!-- 文本块 -->
      <pre
        v-if="block.type === 'text' && block.content && block.content.trim()"
        class="m-0 text-sm leading-relaxed whitespace-pre-wrap break-words font-sans text-foreground/90"
        :class="block.className"
      >{{ block.content.trim() }}</pre>

      <!-- 思考过程块 (Reasoning) -->
      <ReasoningCard
        v-else-if="block.type === 'collapsible' && isThinkingBlock(block)"
        :title="block.title"
        :content="extractThinkingContent(block)"
        :default-open="block.defaultOpen"
        :class="block.className"
      />

      <!-- 常规可折叠块 -->
      <details
        v-else-if="block.type === 'collapsible'"
        class="group cursor-pointer bg-muted/30 rounded-lg px-3 py-2 border border-border/60"
        :class="block.className"
        :open="block.defaultOpen"
      >
        <summary class="flex items-center gap-2 list-none select-none [&::-webkit-details-marker]:hidden text-xs text-muted-foreground font-medium">
          <ChevronRight class="w-4 h-4 text-muted-foreground transition-transform duration-200 group-open:rotate-90" />
          <span>{{ block.title }}</span>
        </summary>
        <div class="mt-2 p-3 bg-muted/40 rounded-lg border border-border/40">
          <BlockRenderer :blocks="block.content" />
        </div>
      </details>

      <!-- 代码块 -->
      <CodeBlockCard
        v-else-if="block.type === 'code'"
        :code="block.code"
        :language="block.language"
      />

      <!-- 工具调用块 (AI Tool Call) -->
      <ToolCard
        v-else-if="block.type === 'tool_use'"
        :tool-name="block.toolName"
        :tool-id="block.toolId"
        :input="block.input"
      />

      <!-- 工具结果块 (AI Tool Result) -->
      <ToolCard
        v-else-if="block.type === 'tool_result'"
        :output="block.content"
        :is-error="block.isError"
      />

      <!-- 消息块 (AI Message) -->
      <template v-else-if="block.type === 'message'">
        <!-- 若为 tool 角色且子项全为工具结果，直接平铺渲染工具卡片，避免外层套娃双重标题 -->
        <template v-if="block.role === 'tool' && isAllToolBlocks(block.content)">
          <BlockRenderer :blocks="block.content" />
        </template>
        <MessageCard
          v-else
          :role="block.role"
          :role-label="block.roleLabel"
          :badges="block.badges"
        >
          <BlockRenderer :blocks="block.content" />
        </MessageCard>
      </template>

      <!-- 徽章块 -->
      <Badge
        v-else-if="block.type === 'badge'"
        :variant="block.variant || 'secondary'"
        class="w-fit"
      >
        {{ block.label }}
      </Badge>

      <!-- 图片块 -->
      <div
        v-else-if="block.type === 'image'"
        class="bg-muted/30 p-3 rounded-lg border border-border/50"
      >
        <img
          v-if="block.src"
          :src="block.src"
          :alt="block.alt || t('ui.image')"
          class="max-w-full max-h-[400px] rounded-lg object-contain"
        >
        <div
          v-else
          class="flex items-center gap-2 text-muted-foreground text-xs"
        >
          <ImageIcon class="w-4 h-4" />
          <span>{{ block.mimeType || block.alt || t('ui.image') }}</span>
        </div>
      </div>

      <!-- 错误块 -->
      <div
        v-else-if="block.type === 'error'"
        class="flex items-center gap-2 p-3 bg-destructive/10 border border-destructive/20 rounded-lg text-destructive text-sm"
      >
        <AlertCircle class="w-4 h-4 shrink-0" />
        <span>{{ block.message }}</span>
        <span
          v-if="block.code"
          class="font-mono text-xs opacity-80"
        >{{ block.code }}</span>
      </div>

      <!-- 容器块 -->
      <div
        v-else-if="block.type === 'container'"
        class="flex flex-col gap-2"
        :class="block.className"
      >
        <div
          v-if="block.header"
          class="flex items-center gap-2"
        >
          <BlockRenderer :blocks="block.header" />
        </div>
        <BlockRenderer :blocks="block.children" />
      </div>

      <!-- 分隔符块 -->
      <hr
        v-else-if="block.type === 'divider'"
        class="border-0 border-t border-border my-2"
      >

      <!-- 标签块 -->
      <div
        v-else-if="block.type === 'label'"
        class="flex gap-2 text-sm"
      >
        <span class="text-muted-foreground">{{ block.label }}:</span>
        <span :class="block.mono ? 'font-mono' : ''">{{ block.value }}</span>
      </div>
    </template>
  </div>
</template>

<script setup lang="ts">
import { useI18n } from '@/i18n'

import { AlertCircle, ChevronRight, Image as ImageIcon } from '@lucide/vue'
import Badge from '@/components/ui/badge.vue'
import { ToolCard, ReasoningCard, MessageCard, CodeBlockCard } from '@/components/ai-elements'
import type { RenderBlock, CollapsibleRenderBlock, CodeRenderBlock, TextRenderBlock } from '../../conversation'

defineProps<{
  blocks: RenderBlock[]
}>()

const { t } = useI18n()

function isThinkingBlock(block: CollapsibleRenderBlock): boolean {
  return block.title?.includes('思考') || block.className?.includes('thinking') || false
}

function extractThinkingContent(block: CollapsibleRenderBlock): string {
  const codeChild = block.content.find((b): b is CodeRenderBlock => b.type === 'code')
  if (codeChild) return codeChild.code
  const textChild = block.content.find((b): b is TextRenderBlock => b.type === 'text')
  if (textChild) return textChild.content
  return ''
}

function isAllToolBlocks(blocks: RenderBlock[]): boolean {
  if (!blocks || blocks.length === 0) return false
  return blocks.every(b => b.type === 'tool_result' || b.type === 'tool_use')
}

</script>
