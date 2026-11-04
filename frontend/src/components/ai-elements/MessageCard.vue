<template>
  <div
    class="rounded-lg overflow-hidden transition-all duration-200 border"
    :class="roleClasses"
  >
    <!-- 消息头部 -->
    <div
      class="flex items-center justify-between gap-2 px-3 py-2 text-xs font-medium border-b"
      :class="headerClasses"
    >
      <div class="flex items-center gap-2 min-w-0">
        <component
          :is="roleIcon"
          class="w-4 h-4 shrink-0"
        />
        <span class="font-semibold">{{ displayLabel }}</span>
        <template v-if="badges && badges.length > 0">
          <Badge
            v-for="(badge, badgeIndex) in badges"
            :key="badgeIndex"
            :variant="badge.variant || 'secondary'"
            class="text-[11px] h-5 px-1.5 font-normal"
          >
            {{ badge.label }}
          </Badge>
        </template>
      </div>

      <slot name="header-actions" />
    </div>

    <!-- 消息正文 -->
    <div class="p-3 text-sm leading-relaxed">
      <slot />
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, type Component } from 'vue'
import { User, Bot, Settings, Wrench } from '@lucide/vue'
import { Badge } from '@/components/ui'
import type { BadgeRenderBlock } from '@/features/usage/conversation/render'

const props = withDefaults(defineProps<{
  role: 'user' | 'assistant' | 'system' | 'tool'
  roleLabel?: string
  badges?: BadgeRenderBlock[]
}>(), {
  roleLabel: '',
  badges: () => [],
})

const roleClasses = computed(() => {
  switch (props.role) {
    case 'user':
      return 'bg-primary/[0.04] border-primary/20 shadow-xs'
    case 'assistant':
      return 'bg-card border-border shadow-xs'
    case 'system':
      return 'bg-muted/20 border-dashed border-border'
    case 'tool':
      return 'bg-muted/30 border-border shadow-xs'
    default:
      return 'bg-muted/30 border-border'
  }
})

const headerClasses = computed(() => {
  switch (props.role) {
    case 'user':
      return 'text-primary border-primary/10 bg-primary/[0.02]'
    case 'assistant':
      return 'text-foreground border-border/60 bg-muted/30'
    case 'system':
      return 'text-muted-foreground border-border/40 bg-muted/10'
    case 'tool':
      return 'text-muted-foreground border-border/60 bg-muted/20'
    default:
      return 'text-foreground border-border/60 bg-muted/20'
  }
})

const roleIcon = computed<Component>(() => {
  switch (props.role) {
    case 'user': return User
    case 'assistant': return Bot
    case 'system': return Settings
    case 'tool': return Wrench
    default: return User
  }
})

const displayLabel = computed(() => {
  if (props.roleLabel) return props.roleLabel
  switch (props.role) {
    case 'user': return 'User'
    case 'assistant': return 'Assistant'
    case 'system': return 'System'
    case 'tool': return 'Tool'
    default: return props.role
  }
})
</script>
